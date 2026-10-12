from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
from threading import Barrier

import httpx
import pytest

from sinai.crm import AmoCRM
from sinai.domain import Conflict, WELCOME_REGISTERED
from sinai.engine import Engine
from sinai.store import SCHEMA, Store
from test_subscription_fixes import env, job, start_update
from test_delivery_history import HistoryAPI


def legacy_database(path):
    schema = SCHEMA.replace('lead_id INTEGER, pipeline_id', 'lead_id INTEGER UNIQUE, pipeline_id')
    schema = schema.replace(', UNIQUE(platform,lead_id), UNIQUE(id,platform)', '')
    schema = schema.replace("lead_id INTEGER NOT NULL, platform TEXT NOT NULL CHECK(platform IN ('tg','max')),\n "
                            "user_id INTEGER NOT NULL UNIQUE, created_at REAL NOT NULL,\n "
                            'PRIMARY KEY(lead_id,platform), FOREIGN KEY(user_id,platform) REFERENCES users(id,platform)',
                            'lead_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL UNIQUE REFERENCES users(id),\n created_at REAL NOT NULL')
    c = sqlite3.connect(path)
    c.executescript(schema)
    c.execute('PRAGMA user_version=1')
    for uid, platform, eid, lead in ((1, 'max', 185607445, 32426619), (2, 'tg', 8608404966, None)):
        c.execute("INSERT INTO users(id,platform,external_id,chat_id,name,selected_group,group_name,subscribed_at,"
                  "last_seen,active,lead_id,state) VALUES (?,?,?,?,?,'active','active',1000,2000,1,?,?)",
                  (uid, platform, eid, eid, 'Existing', lead, '{"draft":"keep"}'))
    c.execute('INSERT INTO claims VALUES (32426619,1,1000)')
    c.execute("INSERT INTO notes VALUES (1,32426619,'[sinai-subscription:1:32426619]','done',777)")
    c.execute("INSERT INTO settings VALUES ('completed_interval','2')")
    c.execute("INSERT INTO jobs(key,lane,kind,payload,status,available_at,created_at,result) "
              "VALUES ('auto:1:1','delivery','deliver',?,'done',3000,2500,?)",
              (json.dumps({'uid':1,'month':1}), json.dumps({'message':{'timestamp':3000000,'body':{'text':'sent'}}})))
    c.commit()
    c.row_factory = sqlite3.Row
    saved = {table: [dict(r) for r in c.execute('SELECT * FROM ' + table)]
             for table in ('users','notes','jobs','settings','campaigns','media','audit','delivery_notes')}
    c.close()
    return saved


def test_migration_preserves_every_row_and_backup_and_allows_example(tmp_path):
    path = tmp_path / 'old.sqlite3'
    saved = legacy_database(path)
    s = Store(str(path))
    try:
        for table, expected in saved.items():
            assert s.rows('SELECT * FROM ' + table) == expected
        assert s.one('PRAGMA user_version')['user_version'] == 2
        assert s.rows('PRAGMA foreign_key_check') == []
        assert s.one('SELECT * FROM claims')['platform'] == 'max'
        assert len(list(tmp_path.glob('*.pre-platform-v2-*.backup'))) == 1
        s.start(2, 32426619)
        assert s.claim_job('crm')['kind'] == 'bind'
        s.bind_verified(2, 32426619, 11037010, 8993570)
        assert len(s.rows('SELECT * FROM users WHERE lead_id=32426619')) == 2
        assert s.user(2)['subscribed_at'] == 1000
        assert len(s.rows('SELECT * FROM notes')) == 2
    finally:
        s.close()
    again = Store(str(path))
    assert again.user(2)['lead_id'] == 32426619 and again.user(2)['subscribed_at'] == 1000
    assert len(list(tmp_path.glob('*.pre-platform-v2-*.backup'))) == 1
    again.close()


@pytest.mark.parametrize('platform', ['tg','max'])
def test_second_account_same_platform_rejected_by_store_and_sql(env, platform):
    cfg, s = env
    first = s.touch_user(platform, 101, 101, 'First')
    second = s.touch_user(platform, 102, 102, 'Second')
    for u in (first, second):
        s.choose_group(u['id'], 'active')
    s.request_bind(first['id'], 32426619)
    s.bind_verified(first['id'], 32426619, 1, 8993570)
    with pytest.raises(Conflict):
        s.request_bind(second['id'], 32426619)
    with pytest.raises(sqlite3.IntegrityError):
        s.execute('UPDATE users SET lead_id=32426619 WHERE id=?', (second['id'],))
    with pytest.raises(sqlite3.IntegrityError):
        s.execute('INSERT INTO claims VALUES (?,?,?,1000)', (32426619, platform, second['id']))


@pytest.mark.parametrize('platforms,expected', [(('tg','max'), 2), (('tg','tg'), 1), (('max','max'), 1)])
def test_concurrent_claims_on_independent_connections(tmp_path, platforms, expected):
    path = str(tmp_path / 'race.sqlite3')
    s = Store(path)
    ids = []
    for eid, platform in enumerate(platforms, 101):
        u = s.touch_user(platform, eid, eid, 'Race')
        s.choose_group(u['id'], 'active')
        ids.append(u['id'])
    s.close()
    barrier = Barrier(2)
    def claim(uid):
        other = Store(path)
        try:
            barrier.wait(timeout=10)
            try:
                other.request_bind(uid, 32426619)
                other.bind_verified(uid, 32426619, 1, 8993570)
                return True
            except Conflict:
                return False
        finally:
            other.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(claim, ids)) == expected
    s = Store(path)
    assert len(s.rows('SELECT * FROM claims')) == expected
    assert len(s.rows('SELECT * FROM users WHERE lead_id=32426619')) == expected
    assert not s.rows('PRAGMA foreign_key_check')
    s.close()


async def test_registered_tg_deep_link_same_lead_as_max_without_contact_or_buttons(env):
    cfg, s = env
    mx = s.touch_user('max', 185607445, 185607445, 'MAX')
    tg = s.touch_user('tg', 8608404966, 8608404966, 'TG')
    for u in (mx,tg):
        s.choose_group(u['id'], 'active')
    s.bind_verified(mx['id'], 32426619, 11037010, 8993570)
    anchor = s.user(tg['id'])['subscribed_at']
    engine = Engine(cfg,s,None,None)
    await engine.ui.handle(job(start_update(uid=8608404966,text='/start 32426619')))
    reply = json.loads(s.one("SELECT payload FROM jobs WHERE key='reply:1:1'")['payload'])
    assert reply['text'] == WELCOME_REGISTERED
    assert not reply['contact'] and not reply['buttons']
    s.bind_verified(tg['id'],32426619,11037010,8993570)
    await engine.ui.handle(job(start_update(uid=8608404966,text='/start 32426619'), 2))
    assert len(s.rows('SELECT * FROM claims')) == 2
    assert s.user(tg['id'])['subscribed_at'] == anchor
    assert not s.rows("SELECT * FROM jobs WHERE kind='phone'")


async def test_shared_lead_keeps_notes_sync_and_deliveries_independent(env):
    cfg, s = env
    users = [s.touch_user(platform,333,333,platform) for platform in ('tg','max')]
    for u in users:
        s.choose_group(u['id'],'active')
        s.bind_verified(u['id'],32426619,11037010,8993570)
    remote = HistoryAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg,s,h)
        for u in users:
            await crm.ensure_note(u['id'])
            s.apply_crm(u['id'],32426619,8993570,8993570)
            await crm.ensure_note(u['id'])
    assert len(remote.notes) == 2
    assert len({n['note_id'] for n in s.rows('SELECT * FROM notes')}) == 2
    for u in users:
        assert s.user(u['id'])['group_name'] == 'completed'
    s.schedule(users[0]['last_seen']+100*86400,2,cfg.timezone)
    sends = s.rows("SELECT * FROM jobs WHERE kind='deliver'")
    assert len(sends) == 2 and len({j['key'] for j in sends}) == 2


def test_claim_platform_must_match_owner(env):
    cfg,s = env
    u = s.touch_user('tg',101,101,'Owner')
    with pytest.raises(sqlite3.IntegrityError):
        s.execute('INSERT INTO claims VALUES (32426619,\'max\',?,1000)',(u['id'],))


def test_failed_migration_rolls_back_existing_data(tmp_path):
    path = tmp_path / 'invalid.sqlite3'
    saved = legacy_database(path)
    c = sqlite3.connect(path)
    c.execute('UPDATE claims SET lead_id=999 WHERE user_id=1')
    c.commit()
    c.close()
    with pytest.raises(sqlite3.IntegrityError):
        Store(str(path))
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    for table, expected in saved.items():
        assert [dict(r) for r in c.execute('SELECT * FROM '+table)] == expected
    assert dict(c.execute('SELECT * FROM claims').fetchone()) == {'lead_id':999,'user_id':1,'created_at':1000}
    assert c.execute('PRAGMA user_version').fetchone()[0] == 1
    assert not c.execute("SELECT name FROM sqlite_master WHERE name LIKE '%_platform_v2'").fetchall()
    c.close()
    assert len(list(tmp_path.glob('*.backup'))) == 1


def test_two_connections_migrate_once(tmp_path):
    path = tmp_path / 'concurrent.sqlite3'
    legacy_database(path)
    def open_database(_):
        s = Store(str(path))
        try:
            return s.one('PRAGMA user_version')['user_version'], len(s.rows('SELECT * FROM users'))
        finally:
            s.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(open_database, range(2))) == [(2,2),(2,2)]
    assert len(list(tmp_path.glob('*.backup'))) == 1


def test_releasing_one_platform_claim_preserves_other(env):
    cfg,s = env
    users = [s.touch_user(p,101,101,p) for p in ('tg','max')]
    for u in users:
        s.choose_group(u['id'],'active')
        s.request_bind(u['id'],32426619)
    s.invalid_lead(users[0]['id'],32426619)
    remaining = s.rows('SELECT * FROM claims')
    assert len(remaining) == 1 and remaining[0]['platform'] == 'max'
