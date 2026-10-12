import asyncio
import json

import httpx
import pytest

from sinai.crm import AmoCRM
from sinai.engine import Engine
from sinai.http import APIError
from test_subscription_fixes import env, subscribe


class HistoryAPI:
    def __init__(self):
        self.notes = {}
        self.posts = 0
        self.lose_response = False
        self.reject = False

    def __call__(self, request):
        if request.method == 'POST':
            self.posts += 1
            if self.reject:
                return httpx.Response(429, json={'code': 'rate_limit'})
            nid = 1000 + len(self.notes)
            self.notes[nid] = {'id': nid, 'entity_id': 32426619, **json.loads(request.content)[0]}
            if self.lose_response:
                raise httpx.ReadTimeout('lost response', request=request)
            return httpx.Response(200, json={'_embedded': {'notes': [{'id': nid}]}})
        if request.url.path.rsplit('/', 1)[-1].isdigit():
            nid = int(request.url.path.rsplit('/', 1)[-1])
            if request.method == 'PATCH':
                self.notes[nid].update(json.loads(request.content))
            return httpx.Response(200, json=self.notes[nid])
        return httpx.Response(200, json={'_embedded': {'notes': list(self.notes.values())}})


def sent(s, uid, key='auto:1:1', payload=None, result=None, status='done'):
    s.enqueue(key, 'delivery', 'deliver', payload or {'uid': uid, 'month': 1})
    jid = s.one('SELECT id FROM jobs WHERE key=?', (key,))['id']
    s.finish_job(jid, status, result=result or {'date': 1791790000, 'text': 'Полный отправленный текст'})
    return jid


async def test_auto_history_is_separate_from_subscription_and_idempotent(env):
    cfg, s = env
    uid = subscribe(s)
    jid = sent(s, uid)
    remote = HistoryAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg, s, h)
        await crm.ensure_note(uid)
        await crm.ensure_delivery_note(jid)
        await crm.ensure_delivery_note(jid)
    assert remote.posts == 2  # One subscription note and one immutable delivery note.
    text = next(n['params']['text'] for n in remote.notes.values() if 'sinai-delivery:' in n['params']['text'])
    for field in ('Telegram', 'ID получателя: 111', 'Красноярск', 'UTC+0700', 'Автоматическая рассылка',
                  'Месяц подписки: 1', 'Полный отправленный текст', 'прочтение не подтверждено',
                  'Следующая автоматическая рассылка:'):
        assert field in text
    assert s.one('SELECT status FROM delivery_notes')['status'] == 'done'


@pytest.mark.parametrize('status', ['queued', 'failed', 'unknown', 'skipped'])
def test_non_success_creates_no_history(env, status):
    cfg, s = env
    uid = subscribe(s)
    sent(s, uid, status=status)
    s.backfill_delivery_notes()
    assert not s.rows('SELECT * FROM delivery_notes')


def test_legacy_max_backfill_creates_only_note_job_and_preserves_send(env):
    cfg, s = env
    u = s.touch_user('max', 185607445, 185607445, 'Test')
    s.choose_group(u['id'], 'active')
    s.bind_verified(u['id'], 32426619, 11037010, 8993570)
    s.enqueue('auto:1:1', 'delivery', 'deliver', {'uid': u['id'], 'month': 1})
    result = {'message': {'timestamp': 1791790000123, 'body': {'text': 'MAX исходный текст', 'mid': 'm1'}}}
    # Emulate the old version: confirmed done exists, with no history record.
    s.execute("UPDATE jobs SET status='done',result=? WHERE key='auto:1:1'", (json.dumps(result),))
    assert s.backfill_delivery_notes() == 1
    assert s.backfill_delivery_notes() == 0
    note = s.one('SELECT * FROM delivery_notes')
    snapshot = json.loads(note['snapshot'])
    assert snapshot['text'] == 'MAX исходный текст'
    assert snapshot['sent_at'] == 1791790000.123
    assert note['lead_id'] == 32426619
    assert len(s.rows("SELECT * FROM jobs WHERE kind='deliver'")) == 1
    assert s.one("SELECT status FROM jobs WHERE key='auto:1:1'")['status'] == 'done'
    assert s.one("SELECT lane FROM jobs WHERE kind='delivery_note'")['lane'] == 'crm_history'


async def test_manual_history_preserves_exact_html_and_caption(env):
    cfg, s = env
    uid = subscribe(s)
    jid = sent(s, uid, key='campaign:abc:1:0', payload={'uid': uid, 'cid': 'abc', 'part': 0,
               'text': '<b>Важно</b>\nПолная подпись', 'media': 'file1'})
    remote = HistoryAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        await AmoCRM(cfg, s, h).ensure_delivery_note(jid)
    text = next(iter(remote.notes.values()))['params']['text']
    assert '<b>Важно</b>\nПолная подпись' in text
    assert 'Ручная рассылка' in text and 'Вложение: file1' in text
    assert 'Месяц подписки:' not in text


async def test_lost_history_post_does_not_repost_or_resend(env):
    cfg, s = env
    uid = subscribe(s)
    jid = sent(s, uid)
    remote = HistoryAPI()
    remote.lose_response = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        crm = AmoCRM(cfg, s, h)
        with pytest.raises(APIError):
            await crm.ensure_delivery_note(jid)
        assert s.one('SELECT status FROM delivery_notes')['status'] == 'unknown'
        await crm.ensure_delivery_note(jid)
    assert remote.posts == 1
    assert len(s.rows("SELECT * FROM jobs WHERE kind='deliver'")) == 1
    assert s.one('SELECT status FROM delivery_notes')['status'] == 'done'


async def test_history_error_retries_independently(env):
    cfg, s = env
    uid = subscribe(s)
    jid = sent(s, uid)
    remote = HistoryAPI()
    remote.reject = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        engine = Engine(cfg, s, None, AmoCRM(cfg, s, h))
        with pytest.raises(APIError):
            await engine.dispatch(s.claim_job('crm_history'))
        assert s.one('SELECT status FROM jobs WHERE id=?', (jid,))['status'] == 'done'
        remote.reject = False
        await engine.crm.ensure_delivery_note(jid)
    assert len(remote.notes) == 1


async def test_history_worker_backoff_never_requeues_delivery(env):
    cfg, s = env
    uid = subscribe(s)
    jid = sent(s, uid)
    remote = HistoryAPI()
    remote.reject = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(remote)) as h:
        engine = Engine(cfg, s, None, AmoCRM(cfg, s, h))
        worker = asyncio.create_task(engine.worker('crm_history'))
        try:
            async with asyncio.timeout(5):
                while not (s.one("SELECT error FROM jobs WHERE kind='delivery_note'") or {}).get('error'):
                    await asyncio.sleep(.05)
                history = s.one("SELECT * FROM jobs WHERE kind='delivery_note'")
                assert history['status'] == 'queued' and history['available_at'] > history['created_at']
                assert s.one('SELECT status FROM jobs WHERE id=?', (jid,))['status'] == 'done'
                remote.reject = False
                s.execute("UPDATE jobs SET available_at=0 WHERE kind='delivery_note'")
                while s.one('SELECT status FROM delivery_notes')['status'] != 'done':
                    await asyncio.sleep(.05)
        finally:
            engine.stop.set()
            await worker
    assert len(remote.notes) == 1
    assert len(s.rows("SELECT * FROM jobs WHERE kind='deliver'")) == 1


def test_legacy_auto_without_full_text_is_not_fabricated(env):
    cfg, s = env
    uid = subscribe(s)
    s.enqueue('auto:1:1', 'delivery', 'deliver', {'uid': uid, 'month': 1})
    s.execute("UPDATE jobs SET status='done' WHERE key='auto:1:1'")
    s.backfill_delivery_notes()
    assert s.one('SELECT status FROM delivery_notes')['status'] == 'incomplete'
    assert not s.rows("SELECT * FROM jobs WHERE kind='delivery_note'")


def test_repeated_finish_and_backfill_create_one_history_record(env):
    cfg, s = env
    uid = subscribe(s)
    jid = sent(s, uid)
    s.finish_job(jid, 'done', result={'date': 1791790000, 'text': 'Полный отправленный текст'})
    s.backfill_delivery_notes()
    assert len(s.rows('SELECT * FROM delivery_notes')) == 1
    assert len(s.rows("SELECT * FROM jobs WHERE kind='delivery_note'")) == 1
