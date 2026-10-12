import asyncio
import time

from .domain import normalize_phone, subscription_date
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
        if note["status"] == "done":
            return
        async for remote in self.collection(f"/api/v4/leads/{note['lead_id']}/notes", "notes"):
            if note["marker"] in remote.get("params", {}).get("text", ""):
                await self.store.call("execute", "UPDATE notes SET status='done',note_id=? WHERE user_id=?",
                                      (remote["id"], uid))
                return
        if note["status"] in ("writing", "unknown"):
            # A lost POST response is reconciled by reads only, never another POST.
            raise APIError("amocrm", 503, "note_reconciliation_pending")
        u = await self.store.call("user", uid)
        text = (f"Подписка на официальный чат-бот компании «Синай»\n"
                f"Бот: {'sinai_news_bot (Telegram)' if u['platform'] == 'tg' else 'Синай (MAX)'}\n"
                f"Платформа: {'Telegram' if u['platform'] == 'tg' else 'MAX'}\n"
                f"ID пользователя: {u['external_id']}\n"
                f"Дата подписки: {subscription_date(u['subscribed_at'], self.cfg.timezone)}\n{note['marker']}")
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
        await self.store.call("execute", "UPDATE notes SET status='done',note_id=? WHERE user_id=?",
                              (notes[0]["id"], uid))

