"""Persistent inbox, independent CRM/media/UI/delivery lanes, crash recovery."""
import asyncio
from html import escape
import logging
import random
import time
import uuid

from .domain import ACTIVE_TEXT, COMPLETED_TEXT, Conflict, due_month, subscription_date, message_parts
from .http import APIError
from .ui import UI

log = logging.getLogger(__name__)


class Skipped(Exception):
    pass


class Engine:
    def __init__(self, cfg, store, platforms, crm):
        self.cfg, self.store, self.platforms, self.crm = cfg, store, platforms, crm
        self.ui = UI(cfg, store, platforms)
        self.stop = asyncio.Event()
        self.started_at = time.time()

    async def pause(self, seconds):
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def poller(self, platform):
        failures = 0
        while not self.stop.is_set():
            try:
                cursor = await self.store.call("setting", f"cursor:{platform}")
                updates, next_cursor = await self.platforms.poll(platform, cursor)
                # Persist every event AND cursor before the next request acknowledges it.
                await self.store.call("ingest", platform, updates, next_cursor)
                await self.store.call("set_setting", f"poll_ok:{platform}", time.time())
                failures = 0
                if not updates:
                    await self.pause(0.25)
            except APIError as exc:
                failures += 1
                log.error("poll platform=%s error=%s", platform, exc)
                await self.store.call("set_setting", f"poll_error:{platform}", str(exc))
                if exc.status == 409:
                    # A second poller on the same token must be corrected by the operator.
                    self.stop.set()
                    raise RuntimeError("Polling conflict: stop duplicate instances or remove webhook") from None
                await self.pause(max(exc.retry_after, min(120, 2 ** min(failures, 7))))
            except Exception as exc:
                log.error("poll platform=%s error_type=%s", platform, type(exc).__name__)
                await self.pause(15)

    async def worker(self, lane):
        while not self.stop.is_set():
            job = await self.store.call("claim_job", lane)
            if not job:
                await self.pause(0.15 if lane in ("inbox", "ui_send") else 0.5)
                continue
            try:
                result = await self.dispatch(job)
            except Skipped:
                await self.store.call("finish_job", job["id"], "skipped")
            except Conflict as exc:
                await self.store.call("finish_job", job["id"], "failed", str(exc))
                await self.notify_job_error(job, str(exc))
            except APIError as exc:
                sending = job["kind"] in ("deliver", "ui")
                if exc.uncertain and sending:
                    status = "unknown"
                elif (exc.retryable or (exc.uncertain and job['kind'] == 'note')) and (job["attempts"] < 12 or job["kind"] == "note"):
                    status = "queued"
                else:
                    status = "failed"
                delay = max(exc.retry_after, min(3600, 2 ** min(job["attempts"], 11)) + random.random())
                await self.store.call("finish_job", job["id"], status, str(exc), delay)
                log.warning("job=%s kind=%s state=%s error=%s", job["id"], job["kind"], status, exc)
                if status in ("failed", "unknown"):
                    await self.notify_job_error(job, str(exc))
            except asyncio.CancelledError:
                # recover() will mark interrupted external sends unknown at next startup.
                raise
            except Exception as exc:
                # Never print HTTP exceptions (their repr may contain bot token URLs).
                reason = f"Internal error: {type(exc).__name__}"
                status = "unknown" if job["kind"] in ("deliver", "ui") else "failed"
                await self.store.call("finish_job", job["id"], status, reason)
                log.error("job=%s kind=%s error_type=%s", job["id"], job["kind"], type(exc).__name__)
                await self.notify_job_error(job, reason)
            else:
                await self.store.call("finish_job", job["id"], "done", result=result)

    async def notify_job_error(self, job, error):
        if job["kind"] == "ui":
            return  # Avoid recursive failed notifications.
        p = job["payload"]
        uid = p.get("uid") or p.get("owner")
        if job["kind"] == "deliver":
            return  # Mass failures are available in admin statistics, not one alert per recipient.
        if uid:
            u = await self.store.call("user", uid)
            if u:
                await self.ui.say(u, f"<b>Задача требует проверки</b>\n#{job['id']}: {escape(error)}\n"
                                  "Подписка и выбранная группа сохранены. Администратор может проверить очередь.",
                                  key=f"error_notice:{job['id']}")
        if p.get("actor") and p["actor"] != uid:
            actor = await self.store.call("user", p["actor"])
            if actor:
                await self.ui.say(actor, f"<b>Привязка не выполнена</b>\n{escape(error)}", key=f"actor_error:{job['id']}")

    async def dispatch(self, job):
        p, kind = job["payload"], job["kind"]
        if kind == "update":
            return await self.ui.handle(job)
        if kind == "ui":
            u = await self.store.call("user", p["uid"])
            media = await self.store.call("one", "SELECT * FROM media WHERE id=?", (p["media"],)) if p.get("media") else None
            return await self.platforms.send(u, p["text"], p.get("buttons"), p.get("contact", False), media)
        if kind == "bind":
            return await self.bind(job)
        if kind == "phone":
            u = await self.store.call("user", p["uid"])
            if u["lead_id"] or u["pending_lead"] or u["phone"] != p["phone"]:
                return
            lead, reason = await self.crm.find_phone_lead(p["phone"])
            if lead:
                await self.store.call("request_bind", u["id"], lead)
            else:
                await self.store.call("execute", "UPDATE users SET sync_reason=? WHERE id=? AND lead_id IS NULL", (reason, u["id"]))
                await self.ui.say(u, "<b>Подписка сохранена ✓</b>\n" + escape(reason) +
                                  ". Автоматическая привязка не выполнена; вы получаете рассылки своей группы.", key=f"phone_result:{job['id']}")
        elif kind == "note":
            await self.crm.ensure_note(p["uid"])
        elif kind == "sync":
            # Catalog changes and current states; fan out per-lead jobs to avoid one huge retry.
            catalog = [x async for x in self.crm.collection("/api/v4/leads/pipelines", "pipelines")]
            await self.store.call("set_setting", "crm_pipelines", catalog)
            users = await self.store.call("rows", "SELECT id,lead_id FROM users WHERE lead_id IS NOT NULL")
            epoch = str(job["id"])
            for u in users:
                await self.store.call("enqueue", f"sync_lead:{epoch}:{u['id']}", "crm", "sync_lead",
                                      {"uid": u["id"], "lead": u["lead_id"]})
            await self.store.call("set_setting", "crm_last_sync", subscription_date(time.time(), self.cfg.timezone))
        elif kind == "sync_lead":
            try:
                lead = await self.crm.lead(p["lead"])
            except APIError as exc:
                if exc.status == 404:
                    await self.store.call("execute", "UPDATE users SET sync_reason='CRM: сделка недоступна' WHERE id=?", (p["uid"],))
                raise
            if "pipeline_id" not in lead:
                raise APIError("amocrm", 503, "lead_pipeline_missing")
            await self.store.call("apply_crm", p["uid"], p["lead"], lead["pipeline_id"], self.cfg.completed_pipeline)
        elif kind == "deliver":
            return await self.deliver(job)
        elif kind == "expand":
            await self.store.call("expand_campaign", p["cid"], p.get('after', 0))
        elif kind == "preview":
            await self.preview(job)
        else:
            raise ValueError("Unknown job kind")

    async def bind(self, job):
        p = job["payload"]
        u = await self.store.call("user", p["uid"])
        if u["lead_id"] == p["lead"]:
            return
        try:
            lead = await self.crm.lead(p["lead"])
        except APIError as exc:
            if exc.status == 404:
                await self.store.call("invalid_lead", u["id"], p["lead"])
            raise
        if not lead.get("id") or "pipeline_id" not in lead:
            raise APIError("amocrm", 503, "lead_invalid_response")
        await self.store.call("bind_verified", u["id"], p["lead"], lead["pipeline_id"], self.cfg.completed_pipeline)
        await self.ui.say(u, "<b>Подписка привязана к вашей сделке ✓</b>\nГруппа рассылок определяется по состоянию сделки в amoCRM.",
                          key=f"bind_result:{u['id']}:{p['lead']}")
        if p.get("actor"):
            actor = await self.store.call("user", p["actor"])
            await self.ui.say(actor, f"<b>Привязка сохранена ✓</b>\nПользователь {u['platform'].upper()} {u['external_id']} → сделка {p['lead']}.",
                              key=f"bind_actor:{job['id']}")

    async def deliver(self, job):
        p = job["payload"]
        u = await self.store.call("user", p["uid"])
        if not u["active"] or not u["subscribed_at"]:
            raise Skipped()
        if u['lead_id'] and (u['crm_checked_at'] or 0) < max(self.started_at, time.time() - 2 * self.cfg.sync_seconds):
            raise APIError('delivery', 429, 'waiting_fresh_crm_state', 30)
        if "month" in p:
            interval = await self.store.call("setting", "completed_interval", self.cfg.completed_interval) if u["group_name"] == "completed" else 1
            if due_month(u["subscribed_at"], time.time(), interval, self.cfg.timezone) != p["month"]:
                raise Skipped()  # Old backlog or group/interval changed; current scheduler will recalculate.
            consumed = await self.store.call('one', "SELECT MAX(CAST(json_extract(payload,'$.month') AS INTEGER)) month "
                                             "FROM jobs WHERE kind='deliver' AND json_extract(payload,'$.uid')=? "
                                             "AND id<>? AND status IN ('done','running','unknown')", (u['id'], job['id']))
            if consumed and consumed['month'] is not None and consumed['month'] >= p['month']:
                raise Skipped()
            text = COMPLETED_TEXT if u["group_name"] == "completed" else ACTIVE_TEXT
            media = None
        else:
            c = await self.store.call("campaign", p["cid"])
            if not c or c["status"] == "cancelled":
                raise Skipped()
            # Group membership checked again immediately before the actual send.
            if c["audience"] in ("active", "completed") and u["group_name"] != c["audience"]:
                raise Skipped()
            if c["audience"] == "unsynced" and u["lead_id"]:
                raise Skipped()
            prior = await self.store.call("rows", "SELECT status FROM jobs WHERE key LIKE ? AND id<?",
                                          (f"campaign:{p['cid']}:{u['id']}:%", job["id"]))
            if any(x["status"] in ("queued", "running") for x in prior):
                raise APIError("delivery", 429, "waiting_previous_part", 2)
            text = p["text"]
            media = await self.store.call("one", "SELECT * FROM media WHERE id=?", (p["media"],)) if p.get("media") else None
        try:
            result = await self.platforms.send(u, text, media=media)
        except APIError as exc:
            if exc.status == 403:
                await self.store.call("execute", "UPDATE users SET active=0 WHERE id=?", (u["id"],))
            raise
        # Cache Telegram reusable file_id for uploads originating on MAX.
        if media and u["platform"] == "tg":
            field = {"image": "photo", "video": "video", "file": "document"}[media["kind"]]
            sent_media = result.get(field)
            if isinstance(sent_media, list):
                sent_media = sent_media[-1]
            if sent_media and sent_media.get("file_id"):
                await self.store.call("execute", "UPDATE media SET tg_file=? WHERE id=?", (sent_media["file_id"], media["id"]))
        return result

    async def preview(self, job):
        p = job["payload"]
        c = await self.store.call("campaign", p["cid"], p["owner"])
        if not c or c["status"] not in ("draft", "preview"):
            raise Skipped()
        snapshot = c["payload"]
        for part in snapshot.get("media", []):
            await self.platforms.prepare_media(self.store, part["id"])
        current = await self.store.call("campaign", c["id"], p["owner"])
        if current["payload"] != snapshot or current["status"] not in ("draft", "preview"):
            raise Skipped()
        # Both platform previews are delivered to configured administrator accounts.
        parts = message_parts(snapshot)
        for platform, external_id in (("tg", self.cfg.admin_id), ("max", self.cfg.max_admin_id)):
            user = await self.store.call("one", "SELECT * FROM users WHERE platform=? AND external_id=?", (platform, external_id))
            if not user:
                raise APIError("preview", 400, f"admin_must_start_{platform}_bot")
            for index, part in enumerate(parts):
                await self.ui.say(user, part['text'], key=f"preview_send:{c['id']}:{p['version']}:{platform}:{index}",
                                  media=part['media'])
        if not await self.store.call("mark_preview", c["id"], p["owner"], snapshot, p["version"]):
            raise Skipped()
        owner = await self.store.call("user", p["owner"])
        await self.ui.say(owner, "<b>Предпросмотр подготовлен</b>\nПроверьте сообщения на обеих платформах. "
                          "Рассылка отправится только после подтверждения. Если текст и подпись различаются, общий текст будет отдельным сообщением.",
                          [[("✓ Отправить рассылку", f"confirm:{c['id']}:{p['version']}")],
                           [("‹ Изменить", f"draft:{c['id']}")], [("Отменить", f"cancel:{c['id']}")]],
                          key=f"preview_controls:{job['id']}")

    async def scheduler(self):
        next_sync = 0
        while not self.stop.is_set():
            now = time.time()
            if now >= next_sync:
                await self.store.call("enqueue", f"sync:{uuid.uuid4().hex}", "crm", "sync", {})
                next_sync = now + self.cfg.sync_seconds
            interval = await self.store.call("setting", "completed_interval", self.cfg.completed_interval)
            await self.store.call("schedule", now, interval, self.cfg.timezone)
            await self.store.call("execute", "UPDATE campaigns SET status='done' WHERE status='sending' "
                                  "AND NOT EXISTS (SELECT 1 FROM jobs WHERE key LIKE 'campaign:' || campaigns.id || ':%' "
                                  "AND status IN ('queued','running')) AND NOT EXISTS "
                                  "(SELECT 1 FROM jobs WHERE kind='expand' AND json_extract(payload,'$.cid')=campaigns.id "
                                  "AND status IN ('queued','running'))")
            await self.store.call("set_setting", "heartbeat", now)
            await self.pause(15)

    async def run(self):
        await self.store.call("recover")
        log.info("Starting polling and durable queue workers")
        async with asyncio.TaskGroup() as group:
            for platform in ("tg", "max"):
                group.create_task(self.poller(platform))
            for lane in ("inbox", "ui_send", "preview_send", "crm", "media", "bulk", "delivery"):
                group.create_task(self.worker(lane))
            group.create_task(self.scheduler())

