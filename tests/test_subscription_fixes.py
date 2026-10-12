import asyncio
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import httpx
import pytest

from sinai.config import Config
from sinai.crm import AmoCRM
from sinai.domain import WELCOME, next_automatic_date
from sinai.engine import Engine
from sinai.http import APIError
from sinai.platforms import Platforms
from sinai.store import Store
from sinai.ui import UI


@pytest.fixture
def env(tmp_path):
    cfg = Config('https://example.amocrm.ru', 'test', 'test', 'test', 111, 222,
                 db_path=str(tmp_path / 'db.sqlite3'))
    store = Store(cfg.db_path)
    yield cfg, store
    store.close()


def start_update(uid=111, text='/start', platform='tg'):
    if platform == 'max':
        return {'update_type': 'bot_started', 'timestamp': 1, 'chat_id': uid,
                'user': {'user_id': uid, 'name': 'Test'}, 'payload': text.split(' ', 1)[1] if ' ' in text else None}
    return {'update_id': 1, 'message': {'message_id': 1, 'chat': {'id': uid, 'type': 'private'},
            'from': {'id': uid, 'first_name': 'Test'}, 'text': text}}


def job(raw, jid=1, platform='tg'):
    return {'id': jid, 'payload': {'raw': raw, 'platform': platform}}


@pytest.mark.parametrize('platform', ['tg', 'max'])
async def test_start_exact_greeting_and_conditional_buttons(env, platform):
    cfg, s = env
    ui = UI(cfg, s, None)
    await ui.handle(job(start_update(platform=platform), platform=platform))
    first = json.loads(s.one("SELECT payload FROM jobs WHERE kind='ui'")['payload'])
    assert first['text'] == WELCOME
    assert first['buttons'] == json.loads(json.dumps(UI.group_buttons()))
    u = s.one('SELECT * FROM users')
    s.choose_group(u['id'], 'active')
    anchor = s.user(u['id'])['subscribed_at']
    await ui.handle(job(start_update(platform=platform), 2, platform))
    second = json.loads(s.one("SELECT payload FROM jobs WHERE key='reply:2:1'")['payload'])
    assert second['text'] == WELCOME
    assert second['buttons'] is None
    assert s.user(u['id'])['subscribed_at'] == anchor


async def test_deep_link_reserves_without_contact_and_retries_failed_bind(env):
    cfg, s = env
    ui = UI(cfg, s, None)
    await ui.handle(job(start_update(text='/start 32426619')))
    u = s.one('SELECT * FROM users')
    assert s.choose_group(u['id'], 'active') == 'binding'
    bind = s.one("SELECT * FROM jobs WHERE kind='bind'")
    s.finish_job(bind['id'], 'failed', 'temporary outage')
    assert s.choose_group(u['id'], 'active') == 'binding'
    assert s.one('SELECT status FROM jobs WHERE id=?', (bind['id'],))['status'] == 'queued'
    assert s.one('SELECT * FROM claims')['lead_id'] == 32426619
    assert not s.rows("SELECT * FROM jobs WHERE kind='phone'")


async def test_returning_unlinked_user_with_deep_link_needs_no_group_click(env):
    cfg, s = env
    ui = UI(cfg, s, None)
    u = s.touch_user('tg', 111, 111, 'Test')
    s.choose_group(u['id'], 'completed')
    await ui.handle(job(start_update(text='/start 32426619')))
    assert s.one("SELECT status FROM jobs WHERE kind='bind'")['status'] == 'queued'
    reply = json.loads(s.one("SELECT payload FROM jobs WHERE kind='ui'")['payload'])
    assert reply['buttons'] is None


class FakeNotes:
    def __init__(self):
        self.notes = {}
        self.posts = 0
        self.patches = 0
        self.fail_after_accept = False
        self.reject = False

    def __call__(self, request):
        path = request.url.path
        if path == '/api/v4/leads/32426619' and request.method == 'GET':
            return httpx.Response(200, json={'id': 32426619, 'pipeline_id': 11037010})
        if request.method == 'POST':
            self.posts += 1
            if self.reject:
                return httpx.Response(429, json={'code': 'rate_limit'}, headers={'Retry-After': '1'})
            payload = json.loads(request.content)[0]
            self.notes[777] = {'id': 777, 'entity_id': 32426619, **payload}
            if self.fail_after_accept:
                raise httpx.ReadTimeout('lost response', request=request)
            return httpx.Response(200, json={'_embedded': {'notes': [{'id': 777, 'entity_id': 32426619}]}})
        if request.method == 'PATCH':
            self.patches += 1
            self.notes[777].update(json.loads(request.content))
            return httpx.Response(200, json={'_embedded': {'notes': [{'id': 777}]}})
        if path.endswith('/777'):
            return httpx.Response(200, json=self.notes[777])
        return httpx.Response(200, json={'_embedded': {'notes': list(self.notes.values())}})


def subscribe(s):
    u = s.touch_user('tg', 111, 111, 'Test')
    s.start(u['id'], 32426619)
    s.choose_group(u['id'], 'active')
    s.bind_verified(u['id'], 32426619, 11037010, 8993570)
    return u['id']


async def test_note_post_readback_update_without_duplicates(env):
    cfg, s = env
    uid = subscribe(s)
    remote = FakeNotes()
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg, s, h)
        await crm.ensure_note(uid)
        text = remote.notes[777]['params']['text']
        for expected in ('Подписался на бот «Синай»', 'Платформа: Telegram', 'ID пользователя: 111',
                         'Дата и время подписки:', 'Группа рассылок: действующие',
                         'Следующая автоматическая рассылка:', 'Периодичность рассылки: ежемесячно'):
            assert expected in text
        await crm.ensure_note(uid)
        assert remote.posts == 1 and remote.patches == 0
        s.apply_crm(uid, 32426619, 8993570, 8993570)
        await crm.ensure_note(uid)
        assert 'Группа рассылок: завершённые' in remote.notes[777]['params']['text']
        assert 'каждые 2 месяца' in remote.notes[777]['params']['text']
        s.set_setting('completed_interval', 1)
        await crm.ensure_note(uid)
        assert remote.posts == 1 and remote.patches == 2
        assert s.one('SELECT * FROM notes')['note_id'] == 777


async def test_lost_post_response_reconciles_without_second_post(env):
    cfg, s = env
    uid = subscribe(s)
    remote = FakeNotes()
    remote.fail_after_accept = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg, s, h)
        with pytest.raises(APIError):
            await crm.ensure_note(uid)
        assert s.one('SELECT status FROM notes')['status'] == 'unknown'
        await crm.ensure_note(uid)
        assert remote.posts == 1
        assert s.one('SELECT status FROM notes')['status'] == 'done'


async def test_real_worker_executes_bind_then_note(env):
    cfg, s = env
    u = s.touch_user('tg', 111, 111, 'Test')
    s.start(u['id'], 32426619)
    s.choose_group(u['id'], 'active')
    remote = FakeNotes()
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        engine = Engine(cfg, s, None, AmoCRM(cfg, s, h))
        worker = asyncio.create_task(engine.worker('crm'))
        try:
            async with asyncio.timeout(5):
                while s.one('SELECT status FROM notes') != {'status': 'done'}:
                    await asyncio.sleep(.05)
        finally:
            engine.stop.set()
            await worker
        assert remote.posts == 1
        assert not s.rows("SELECT * FROM jobs WHERE lane='crm' AND status!='done'")


async def test_note_429_keeps_pending_and_retries(env):
    cfg, s = env
    uid = subscribe(s)
    remote = FakeNotes()
    remote.reject = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg, s, h)
        with pytest.raises(APIError) as caught:
            await crm.ensure_note(uid)
        assert caught.value.retryable
        assert s.one('SELECT status FROM notes')['status'] == 'pending'
        remote.reject = False
        await crm.ensure_note(uid)
        assert len(remote.notes) == 1


def test_reconciliation_restores_missing_note_job_and_prevents_claim_conflict(env):
    cfg, s = env
    uid = subscribe(s)
    s.execute("DELETE FROM jobs WHERE kind='note'")
    s.reconcile_subscriptions()
    assert s.one("SELECT status FROM jobs WHERE kind='note'")['status'] == 'queued'
    other = s.touch_user('max', 222, 222, 'Other')
    s.start(other['id'], 32426619)
    assert s.choose_group(other['id'], 'active') == 'conflict'
    assert s.one('SELECT user_id FROM claims')['user_id'] == uid


def test_next_date_handles_end_of_month_and_consumed_period():
    tz = 'Asia/Krasnoyarsk'
    anchor = datetime(2026, 1, 31, 12, 30, 15, tzinfo=ZoneInfo(tz)).timestamp()
    now = datetime(2026, 2, 20, tzinfo=ZoneInfo(tz)).timestamp()
    expected = datetime(2026, 2, 28, 12, 30, 15, tzinfo=ZoneInfo(tz)).timestamp()
    assert next_automatic_date(anchor, now, 1, tz) == expected
    assert datetime.fromtimestamp(next_automatic_date(anchor, now, 2, tz), ZoneInfo(tz)).day == 31
    assert datetime.fromtimestamp(next_automatic_date(anchor, now, 1, tz, last_month=1), ZoneInfo(tz)).month == 3


async def test_delivery_refreshes_next_date_in_same_note(env):
    cfg, s = env
    uid = subscribe(s)
    anchor = datetime(2026, 1, 31, 12, 30, 15, tzinfo=ZoneInfo(cfg.timezone)).timestamp()
    s.execute('UPDATE users SET subscribed_at=? WHERE id=?', (anchor, uid))
    remote = FakeNotes()
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg, s, h)
        await crm.ensure_note(uid)
        previous = remote.notes[777]['params']['text']
        s.execute("UPDATE jobs SET status='done' WHERE kind='note'")
        s.enqueue('auto:test', 'delivery', 'deliver', {'uid':uid,'month':12})
        delivery = s.one("SELECT id FROM jobs WHERE key='auto:test'")
        s.finish_job(delivery['id'], 'done')
        assert s.one("SELECT status FROM jobs WHERE kind='note'")['status'] == 'queued'
        await crm.ensure_note(uid)
        assert remote.notes[777]['params']['text'] != previous
        assert remote.posts == 1 and remote.patches == 1


async def test_wire_start_sends_no_inline_keyboard_to_registered_user(env):
    cfg, s = env
    u = s.touch_user('tg', 111, 111, 'Test')
    s.choose_group(u['id'], 'active')
    sent = []
    def respond(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={'ok': True, 'result': {'message_id': 10}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as h:
        platform = Platforms(cfg, h, h)
        engine = Engine(cfg, s, platform, None)
        await engine.ui.handle(job(start_update()))
        ui_job = s.claim_job('ui_send')
        await engine.dispatch(ui_job)
    assert sent[0]['text'] == WELCOME
    assert 'inline_keyboard' not in sent[0].get('reply_markup', {})
