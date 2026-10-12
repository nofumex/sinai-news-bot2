import asyncio
import json
import time

from .domain import normalize_phone, exact_subscription_date
from .http import APIError, RateLimit, request


class AmoCRM:
    def __init__(self, cfg, store, client):
        self.cfg, self.store, self.client = cfg, store, client
        self.rate = RateLimit(5)
        self.oauth_lock = asyncio.Lock()

    async def tokens(self):
        return await self.store.call("setting", "oauth", {"access_token": self.cfg.amo_token,
                                                           "refresh_token": self.cfg.refresh_token})

    async def refresh(self, expired_access):
        async with self.oauth_lock:
            tokens = await self.tokens()
            if tokens["access_token"] != expired_access:
                return
            if not all((self.cfg.client_id, self.cfg.client_secret, self.cfg.redirect_uri, tokens.get("refresh_token"))):
                raise APIError("amocrm", 401, "oauth_credentials_missing")
            data = await request(self.client, "amocrm", "POST", self.cfg.amo_url + "/oauth2/access_token", json={
                "client_id": self.cfg.client_id, "client_secret": self.cfg.client_secret,
                "grant_type": "refresh_token", "refresh_token": tokens["refresh_token"],
                "redirect_uri": self.cfg.redirect_uri,
            })
            if not data.get("access_token") or not data.get("refresh_token"):
                raise APIError("amocrm", 401, "invalid_oauth_response")
            # Atomic persistence of rotating refresh token, before any subsequent request.
            await self.store.call("set_setting", "oauth", {
                "access_token": data["access_token"], "refresh_token": data["refresh_token"],
                "expires_at": time.time() + data["expires_in"],
            })

    async def api(self, method, path, **kwargs):
        tokens = await self.tokens()
        if tokens.get("expires_at", float("inf")) < time.time() + 60:
            await self.refresh(tokens["access_token"])
            tokens = await self.tokens()
        for attempt in range(2):
            await self.rate.wait()
            try:
                return await request(self.client, "amocrm", method, self.cfg.amo_url + path,
                                     headers={"Authorization": "Bearer " + tokens["access_token"]}, **kwargs)
            except APIError as e:
                if e.status == 401 and attempt == 0:
                    await self.refresh(tokens["access_token"])
                    tokens = await self.tokens()
                    continue
                raise

    async def lead(self, lead_id):
        return await self.api("GET", f"/api/v4/leads/{lead_id}")

    async def collection(self, path, entity, params=None):
        page = 1
        while True:
            data = await self.api("GET", path, params={**(params or {}), "page": page, "limit": 250})
            for item in data.get("_embedded", {}).get(entity, []):
                yield item
            if not data.get("_links", {}).get("next"):
                break
            page += 1

    async def find_phone_lead(self, phone):
        contacts = {}
        # Query is fuzzy: never accept it without exact normalized PHONE comparison.
        # Last ten digits also finds local 8(...) and +7(...) Russian representations.
        queries = {phone, phone.lstrip("+")}
        if phone.startswith("+7"):
            queries.add(phone[-10:])
        for query in sorted(queries):
            async for c in self.collection("/api/v4/contacts", "contacts", {"query": query, "with": "leads"}):
                fields = c.get("custom_fields_values") or []
                if any(f.get("field_code") == "PHONE" and any(
                    normalize_phone(str(v.get("value", ""))) == phone for v in f.get("values", [])
                ) for f in fields):
                    contacts[c["id"]] = c
        if len(contacts) != 1:
            return None, "Контакт не найден" if not contacts else "Несколько контактов с этим телефоном"
        contact = next(iter(contacts.values()))
        leads = {x["id"] for x in contact.get("_embedded", {}).get("leads", [])}
        if len(leads) != 1:
            return None, "Нет связанных сделок" if not leads else "Несколько связанных сделок"
        return next(iter(leads)), ""

    async def ensure_note(self, uid):
        note = await self.store.call("one", "SELECT * FROM notes WHERE user_id=?", (uid,))
        u = await self.store.call('user', uid)
        if not note or not u or not u['lead_id']:
            return
        interval = await self.store.call('setting', 'completed_interval', self.cfg.completed_interval)
        every, next_at = await self.store.call('automatic_info', uid, interval, self.cfg.timezone)
        stable_marker = f"[sinai-user:{u['platform']}:{u['external_id']}:{note['lead_id']}]"
        text = (f'Подписался на бот «Синай»\n'
                f"Платформа: {'Telegram' if u['platform'] == 'tg' else 'MAX'}\n"
                f"ID пользователя: {u['external_id']}\n"
                f"Дата и время подписки: {exact_subscription_date(u['subscribed_at'], self.cfg.timezone)}\n"
                f"Группа рассылок: {'завершённые' if u['group_name'] == 'completed' else 'действующие'}\n"
                f"Следующая автоматическая рассылка: {exact_subscription_date(next_at, self.cfg.timezone)}\n"
                f"Периодичность рассылки: {'ежемесячно' if every == 1 else 'каждые 2 месяца'}\n"
                f"{note['marker']}\n{stable_marker}")
        remote = None
        if note['note_id']:
            try:
                remote = await self.api('GET', f"/api/v4/leads/{note['lead_id']}/notes/{note['note_id']}")
            except APIError as exc:
                if exc.status != 404:
                    raise
        if remote is None:
            async for candidate in self.collection(f"/api/v4/leads/{note['lead_id']}/notes", 'notes'):
                old_text = candidate.get('params', {}).get('text', '')
                if stable_marker in old_text or (note['marker'] in old_text and
                                                  f"ID пользователя: {u['external_id']}\n" in old_text):
                    remote = candidate
                    break
        if remote is not None:
            if remote.get('entity_id') != note['lead_id'] or remote.get('note_type') != 'common':
                raise APIError('amocrm', 409, 'subscription_note_mismatch')
            # Persist the recovered ID before PATCH; retrying a PATCH is idempotent.
            await self.store.call('execute', "UPDATE notes SET note_id=?,status='updating' WHERE user_id=?",
                                  (remote['id'], uid))
            if remote.get('params', {}).get('text') != text:
                await self.api('PATCH', f"/api/v4/leads/{note['lead_id']}/notes/{remote['id']}",
                               json={'note_type': 'common', 'params': {'text': text}})
                remote = await self.api('GET', f"/api/v4/leads/{note['lead_id']}/notes/{remote['id']}")
            await self.verify_note(uid, note['lead_id'], remote, text)
            return
        if note["status"] in ("writing", "unknown"):
            # A lost POST response is reconciled by reads only, never another POST.
            raise APIError("amocrm", 503, "note_reconciliation_pending")
        await self.store.call("execute", "UPDATE notes SET status='writing' WHERE user_id=?", (uid,))
        try:
            data = await self.api("POST", f"/api/v4/leads/{note['lead_id']}/notes",
                                  json=[{"note_type": "common", "params": {"text": text}}])
        except APIError as e:
            await self.store.call("execute", "UPDATE notes SET status=? WHERE user_id=?",
                                  ("unknown" if e.uncertain else "pending", uid))
            raise
        notes = data.get("_embedded", {}).get("notes", [])
        if not notes:
            await self.store.call("execute", "UPDATE notes SET status='unknown' WHERE user_id=?", (uid,))
            raise APIError("amocrm", 503, "note_response_missing", uncertain=True)
        await self.store.call("execute", "UPDATE notes SET note_id=? WHERE user_id=?", (notes[0]['id'], uid))
        remote = await self.api('GET', f"/api/v4/leads/{note['lead_id']}/notes/{notes[0]['id']}")
        await self.verify_note(uid, note['lead_id'], remote, text)

    async def verify_note(self, uid, lead_id, remote, expected_text):
        if remote.get('entity_id') != lead_id or remote.get('params', {}).get('text') != expected_text:
            raise APIError('amocrm', 503, 'subscription_note_not_verified')
        await self.store.call('execute', "UPDATE notes SET status='done',note_id=? WHERE user_id=?", (remote['id'], uid))

    async def ensure_delivery_note(self, job_id):
        note = await self.store.call('one', 'SELECT * FROM delivery_notes WHERE job_id=?', (job_id,))
        send = await self.store.call('one', 'SELECT status FROM jobs WHERE id=? AND kind=\'deliver\'', (job_id,))
        if not note or not send or send['status'] != 'done' or not note['lead_id'] or note['status'] in ('done', 'incomplete'):
            return
        s = json.loads(note['snapshot'])
        if s['text'] is None:
            return
        kind = 'Автоматическая рассылка' if s['month'] is not None else 'Ручная рассылка'
        details = f"Месяц подписки: {s['month']}\n" if s['month'] is not None else f"Рассылка: {s['cid']}\n"
        if s.get('part') is not None:
            details += f"Часть сообщения: {s['part'] + 1}\n"
        text = (f"Отправлено сообщение бота «Синай»\n"
                f"Платформа: {'Telegram' if s['platform'] == 'tg' else 'MAX'}\n"
                f"ID получателя: {s['external_id']}\n"
                f"Дата и время отправки (Красноярск): {exact_subscription_date(s['sent_at'], 'Asia/Krasnoyarsk')}\n"
                f"Источник времени: {s['time_source']}\n"
                f"Тип рассылки: {kind}\n{details}"
                f"Статус доставки: API подтвердил успешную отправку; прочтение не подтверждено\n"
                f"Следующая автоматическая рассылка: {exact_subscription_date(s['next_at'], 'Asia/Krasnoyarsk')}\n")
        if s.get('recovered'):
            text += 'Восстановлено по подтверждённой отправке SQLite. Следующая дата рассчитана по текущим настройкам.\n'
        if s.get('media'):
            text += f"Вложение: {s['media']}\n"
        text += f"\nПолный текст отправленного сообщения:\n{s['text']}\n\n{note['marker']}"
        path = f"/api/v4/leads/{note['lead_id']}/notes"
        remote = None
        if note['note_id']:
            remote = await self.api('GET', f"{path}/{note['note_id']}")
        else:
            async for candidate in self.collection(path, 'notes'):
                if note['marker'] in candidate.get('params', {}).get('text', ''):
                    remote = candidate
                    break
        if remote is None:
            if note['status'] in ('writing', 'unknown'):
                # POST might have succeeded; only reconcile until its marker appears.
                raise APIError('amocrm', 503, 'delivery_note_reconciliation_pending')
            await self.store.call('execute', "UPDATE delivery_notes SET status='writing' WHERE job_id=?", (job_id,))
            try:
                data = await self.api('POST', path, json=[{'note_type': 'common', 'params': {'text': text}}])
            except APIError as exc:
                await self.store.call('execute', 'UPDATE delivery_notes SET status=?,error=? WHERE job_id=?',
                                      ('unknown' if exc.uncertain else 'pending', str(exc), job_id))
                raise
            receipt = data.get('_embedded', {}).get('notes', [])
            if not receipt:
                await self.store.call('execute', "UPDATE delivery_notes SET status='unknown' WHERE job_id=?", (job_id,))
                raise APIError('amocrm', 503, 'delivery_note_response_missing', uncertain=True)
            nid = receipt[0]['id']
            await self.store.call('execute', 'UPDATE delivery_notes SET note_id=? WHERE job_id=?', (nid, job_id))
            remote = await self.api('GET', f'{path}/{nid}')
        if remote.get('entity_id') != note['lead_id'] or remote.get('params', {}).get('text') != text:
            raise APIError('amocrm', 503, 'delivery_note_not_verified')
        await self.store.call('execute', "UPDATE delivery_notes SET status='done',note_id=?,error=NULL WHERE job_id=?",
                              (remote['id'], job_id))

