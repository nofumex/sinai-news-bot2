import asyncio
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlparse

from .http import APIError, RateLimit, request


def event(platform, raw):
    """Normalize documented private-dialog updates, ignoring group messages."""
    if platform == "tg":
        callback = raw.get("callback_query")
        msg = (callback or {}).get("message") or raw.get("message")
        if not msg or msg.get("chat", {}).get("type") != "private":
            membership = raw.get("my_chat_member")
            if membership and membership["chat"]["type"] == "private":
                return {"external_id": membership["chat"]["id"], "chat_id": membership["chat"]["id"],
                        "name": membership["chat"].get("first_name", ""),
                        "stopped": membership["new_chat_member"]["status"] == "kicked", "text": ""}
            return None
        sender = callback["from"] if callback else msg.get("from", {})
        if sender.get("is_bot") or not sender.get("id"):
            return None
        result = {"external_id": sender["id"], "chat_id": msg["chat"]["id"],
                  "name": sender.get("first_name", ""), "text": msg.get("text", "") if not callback else "",
                  "callback": callback.get("data", "") if callback else None,
                  "callback_id": callback["id"] if callback else None,
                  "contact": msg.get("contact") if not callback else None, "media": []}
        if not callback:
            for field, kind in (("photo", "image"), ("video", "video"), ("document", "file")):
                if msg.get(field):
                    obj = msg[field][-1] if field == "photo" else msg[field]
                    result["media"].append({"source": {"file_id": obj["file_id"], "size": obj.get("file_size", 0)},
                                             "kind": kind, "name": obj.get("file_name", f"{kind}.{'jpg' if kind == 'image' else 'mp4' if kind == 'video' else 'bin'}")})
            if result["media"]:
                result["text"] = msg.get("caption", "")
        return result
    kind = raw.get("update_type")
    if kind in ("bot_started", "bot_stopped"):
        user = raw.get("user", {})
        return {"external_id": user.get("user_id"), "chat_id": raw.get("chat_id", user.get("user_id")),
                "name": user.get("name", ""), "text": "/start " + (raw.get("payload") or "") if kind == "bot_started" else "",
                "stopped": kind == "bot_stopped"}
    if kind not in ("message_created", "message_callback"):
        return None
    msg = raw.get("message") or {}
    if msg.get("recipient", {}).get("chat_type") != "dialog":
        return None
    callback = raw.get("callback")
    user = callback["user"] if callback else msg.get("sender", {})
    if user.get("is_bot") or not user.get("user_id"):
        return None
    body = msg.get("body") or {}
    attachments = body.get("attachments") or []
    return {"external_id": user["user_id"], "chat_id": msg["recipient"]["chat_id"],
            "name": user.get("name", ""), "text": body.get("text") or "" if not callback else "",
            "callback": callback.get("payload", "") if callback else None,
            "callback_id": callback["callback_id"] if callback else None,
            "contact": next((a["payload"] for a in attachments if a["type"] == "contact"), None) if not callback else None,
            "media": [{"source": a["payload"], "kind": a["type"], "name": a.get("filename", f"{a['type']}.{'jpg' if a['type'] == 'image' else 'mp4' if a['type'] == 'video' else 'bin'}")}
                      for a in attachments if a["type"] in ("image", "video", "file")] if not callback else []}


class Platforms:
    def __init__(self, cfg, client, poll_client):
        self.cfg, self.client, self.poll_client = cfg, client, poll_client
        self.rates = {"tg": RateLimit(20), "max": RateLimit(20)}
        self.dialog_rates = {}

    def tg_url(self, method):
        return f"https://api.telegram.org/bot{self.cfg.tg_token}/{method}"

    async def tg(self, method, **kwargs):
        await self.rates["tg"].wait()
        return (await request(self.client, "telegram", "POST", self.tg_url(method), **kwargs))["result"]

    async def max(self, method, path, **kwargs):
        await self.rates["max"].wait()
        return await request(self.client, "max", method, self.cfg.max_url + path,
                             headers={"Authorization": self.cfg.max_token}, **kwargs)

    async def poll(self, platform, cursor):
        if platform == "tg":
            payload = {"timeout": 30, "allowed_updates": ["message", "callback_query", "my_chat_member"]}
            if cursor is not None:
                payload["offset"] = cursor
            data = await request(self.poll_client, "telegram", "POST", self.tg_url("getUpdates"), json=payload)
            updates = data["result"]
            return updates, max((u["update_id"] + 1 for u in updates), default=cursor)
        params = {"timeout": 30, "types": "message_created,message_callback,bot_started,bot_stopped", "limit": 100}
        if cursor is not None:
            params["marker"] = cursor
        data = await request(self.poll_client, "max", "GET", self.cfg.max_url + "/updates",
                             headers={"Authorization": self.cfg.max_token}, params=params)
        return data.get("updates", []), data.get("marker")

    async def ack(self, platform, callback_id):
        if platform == "tg":
            return await self.tg("answerCallbackQuery", json={"callback_query_id": callback_id})
        return await self.max("POST", "/answers", params={"callback_id": callback_id}, json={})

    async def send(self, user, text, buttons=None, contact=False, media=None):
        platform = user["platform"]
        key = (platform, user["external_id"])
        # One global delivery worker plus UI worker, shared per-dialog limit.
        limiter = self.dialog_rates.setdefault(key, RateLimit(1 if platform == "tg" else 2))
        await limiter.wait()
        if platform == "tg":
            payload = {"chat_id": user["chat_id"], "parse_mode": "HTML"}
            if contact:
                payload["reply_markup"] = {"keyboard": [[{"text": "📱 Поделиться моим контактом", "request_contact": True}]],
                                            "resize_keyboard": True, "one_time_keyboard": True}
            elif buttons:
                payload["reply_markup"] = {"inline_keyboard": [[{"text": title, "callback_data": data}
                                                                 for title, data in row] for row in buttons]}
            else:
                payload["reply_markup"] = {"remove_keyboard": True} if not media else {}
            if not media:
                payload["text"] = text
                return await self.tg("sendMessage", json=payload)
            field, method = {"image": ("photo", "sendPhoto"), "video": ("video", "sendVideo"),
                             "file": ("document", "sendDocument")}[media["kind"]]
            payload["caption"] = text
            if media.get("tg_file"):
                payload[field] = media["tg_file"]
                return await self.tg(method, json=payload)
            if not media.get("path"):
                raise APIError("media", 400, "not_prepared")
            form = {k: json.dumps(v) if isinstance(v, dict) else str(v) for k, v in payload.items()}
            with Path(media["path"]).open("rb") as file:
                return await self.tg(method, data=form, files={field: (media["name"], file)})
        attachments = []
        if media:
            if not media.get("max_payload"):
                raise APIError("media", 400, "not_prepared")
            attachments.append({"type": media["kind"], "payload": json.loads(media["max_payload"])})
        if contact:
            attachments.append({"type": "inline_keyboard", "payload": {"buttons": [[
                {"type": "request_contact", "text": "📱 Поделиться контактом"}]]}})
        elif buttons:
            attachments.append({"type": "inline_keyboard", "payload": {"buttons": [[
                {"type": "callback", "text": title, "payload": data} for title, data in row] for row in buttons]}})
        return await self.max("POST", "/messages", params={"user_id": user["external_id"]},
                              json={"text": text or None, "format": "html", "attachments": attachments})

    async def preflight(self):
        tg = await self.tg("getMe", json={})
        max_bot = await self.max("GET", "/me")
        webhook = await self.tg("getWebhookInfo", json={})
        subscriptions = await self.max("GET", "/subscriptions")
        if webhook.get("url") or subscriptions.get("subscriptions"):
            raise APIError("polling", 409, "existing_webhook_remove_before_start")
        return {"telegram": tg.get("username"), "max": max_bot.get("username")}

    @staticmethod
    def trusted_url(url):
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        roots = ("telegram.org", "max.ru", "oneme.ru", "okcdn.ru", "mycdn.me", "ok.ru")
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443):
            raise APIError("media", 400, "untrusted_url")
        if not any(host == root or host.endswith("." + root) for root in roots):
            raise APIError("media", 400, "untrusted_cdn")
        return url

    async def download(self, url, destination):
        # Validate each redirect; no credentials are sent to CDN requests.
        for _ in range(5):
            self.trusted_url(url)
            async with self.client.stream("GET", url, follow_redirects=False, timeout=120) as response:
                if response.is_redirect:
                    url = str(response.url.join(response.headers["location"]))
                    continue
                if not response.is_success:
                    raise APIError("media", response.status_code, "download")
                size = 0
                with destination.open("wb") as out:
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self.cfg.media_bytes:
                            raise APIError("media", 413, "file_too_large")
                        await asyncio.to_thread(out.write, chunk)
                return
        raise APIError("media", 400, "redirect_limit")

    async def prepare_media(self, store, mid):
        m = await store.call("one", "SELECT * FROM media WHERE id=?", (mid,))
        source = json.loads(m["source"])
        destination = Path(self.cfg.media_dir).resolve() / (mid + Path(m["name"]).suffix[:10])
        if not m["path"]:
            if m["origin"] == "tg":
                if source.get("size", 0) > self.cfg.media_bytes:
                    raise APIError("media", 413, "file_too_large")
                file = await self.tg("getFile", json={"file_id": source["file_id"]})
                url = f"https://api.telegram.org/file/bot{self.cfg.tg_token}/{file['file_path']}"
            else:
                url = source.get("url")
                if m["kind"] == "video":
                    token = source.get("token", "")
                    if not re.fullmatch(r"[a-zA-Z0-9_-]+", token):
                        raise APIError("media", 400, "invalid_video_token")
                    detail = await self.max("GET", f"/videos/{token}")
                    urls = detail.get("urls") or {}
                    url = next((urls[k] for k in ("mp4_1080", "mp4_720", "mp4_480", "mp4_360", "mp4_240", "mp4_144") if urls.get(k)), None)
                if not url:
                    raise APIError("media", 400, "download_url_missing")
            temp = destination.with_suffix(destination.suffix + ".part")
            try:
                await self.download(url, temp)
                temp.replace(destination)
            except BaseException:
                temp.unlink(missing_ok=True)
                raise
            await store.call("execute", "UPDATE media SET path=? WHERE id=?", (str(destination), mid))
        else:
            destination = Path(m["path"])
        if not m["max_payload"]:
            upload = await self.max("POST", "/uploads", params={"type": m["kind"]})
            url = self.trusted_url(upload["url"])
            with destination.open("rb") as file:
                try:
                    response = await self.client.post(url, files={"data": (m["name"], file)}, timeout=120)
                    response.raise_for_status()
                except Exception:
                    raise APIError("media", 503, "upload_failed") from None
            token = upload.get("token")
            if not token:
                try:
                    body = response.json()
                    if m["kind"] == "image":
                        token = next(iter(body["photos"].values()))["token"]
                    else:
                        token = body["token"]
                except (ValueError, KeyError, StopIteration, TypeError):
                    raise APIError("media", 503, "upload_token_missing") from None
            await store.call("execute", "UPDATE media SET max_payload=?,error=NULL WHERE id=?",
                             (json.dumps({"token": token}), mid))

    @staticmethod
    def media_identity(platform, source, kind):
        return hashlib.sha256(json.dumps([platform, source, kind], sort_keys=True).encode()).hexdigest()[:32]

