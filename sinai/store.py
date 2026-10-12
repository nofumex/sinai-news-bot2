"""Short SQLite transactions, serialized off the event loop; no network inside transactions."""
import asyncio
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
import uuid

from .domain import Conflict, due_month, message_parts, next_automatic_date

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS users (
 id INTEGER PRIMARY KEY, platform TEXT NOT NULL CHECK(platform IN ('tg','max')),
 external_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, name TEXT NOT NULL DEFAULT '',
 selected_group TEXT CHECK(selected_group IN ('active','completed')),
 group_name TEXT CHECK(group_name IN ('active','completed')), subscribed_at REAL,
 last_seen REAL NOT NULL, active INTEGER NOT NULL DEFAULT 1,
 phone TEXT, pending_lead INTEGER, lead_id INTEGER UNIQUE, pipeline_id INTEGER,
 crm_checked_at REAL, sync_reason TEXT NOT NULL DEFAULT 'Не привязан',
 state TEXT NOT NULL DEFAULT '{}', UNIQUE(platform,external_id)
);
CREATE TABLE IF NOT EXISTS claims (
 lead_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL UNIQUE REFERENCES users(id),
 created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS notes (
 user_id INTEGER PRIMARY KEY REFERENCES users(id), lead_id INTEGER NOT NULL,
 marker TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'pending', note_id INTEGER
);
CREATE TABLE IF NOT EXISTS jobs (
 id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, lane TEXT NOT NULL, kind TEXT NOT NULL,
 payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
 available_at REAL NOT NULL, created_at REAL NOT NULL, error TEXT, result TEXT
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(lane,status,available_at,id);
CREATE INDEX IF NOT EXISTS jobs_delivery_period ON jobs(kind,json_extract(payload,'$.uid'),status,
 CAST(json_extract(payload,'$.month') AS INTEGER));
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS campaigns (
 id TEXT PRIMARY KEY, owner INTEGER NOT NULL REFERENCES users(id), audience TEXT NOT NULL,
 payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft', created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS media (
 id TEXT PRIMARY KEY, origin TEXT NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL,
 name TEXT NOT NULL, path TEXT, tg_file TEXT, max_payload TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS audit (
 id INTEGER PRIMARY KEY, created_at REAL NOT NULL, action TEXT NOT NULL, user_id INTEGER,
 detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_notes (
 job_id INTEGER PRIMARY KEY REFERENCES jobs(id), marker TEXT NOT NULL UNIQUE,
 user_id INTEGER NOT NULL REFERENCES users(id), lead_id INTEGER,
 snapshot TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', note_id INTEGER,
 error TEXT
);
PRAGMA user_version=1;
"""


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.conn.executescript(SCHEMA)

    async def call(self, method, *args, **kwargs):
        def run():
            with self.lock:
                return getattr(self, method)(*args, **kwargs)
        return await asyncio.to_thread(run)

    @contextmanager
    def transaction(self):
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise

    def rows(self, sql, args=()):
        return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        r = self.conn.execute(sql, args).fetchone()
        return dict(r) if r else None

    def execute(self, sql, args=()):
        return self.conn.execute(sql, args).rowcount

    def setting(self, key, default=None):
        row = self.one("SELECT value FROM settings WHERE key=?", (key,))
        return json.loads(row["value"]) if row else default

    def set_setting(self, key, value):
        self.execute("INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, json.dumps(value, ensure_ascii=False)))

    def audit(self, action, uid, detail):
        self.execute("INSERT INTO audit(created_at,action,user_id,detail) VALUES (?,?,?,?)",
                     (time.time(), action, uid, detail))

    def enqueue(self, key, lane, kind, payload, delay=0):
        now = time.time()
        self.execute("INSERT OR IGNORE INTO jobs(key,lane,kind,payload,available_at,created_at) VALUES (?,?,?,?,?,?)",
                     (key, lane, kind, json.dumps(payload, ensure_ascii=False), now + delay, now))

    def ingest(self, platform, updates, cursor):
        with self.transaction():
            for raw in updates:
                identity = str(raw["update_id"]) if platform == "tg" else hashlib.sha256(
                    json.dumps(raw, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                self.enqueue(f"in:{platform}:{identity}", "inbox", "update", {"platform": platform, "raw": raw})
            if cursor is not None:
                self.set_setting(f"cursor:{platform}", cursor)

    def recover(self):
        with self.transaction():
            # A killed process may have sent the external request. Never resend blindly.
            self.execute("UPDATE jobs SET status='unknown',error='Process stopped during send' "
                         "WHERE status='running' AND kind IN ('deliver','ui')")
            self.execute("UPDATE jobs SET status='queued' WHERE status='running'")

    def claim_job(self, lane):
        with self.transaction():
            row = self.one("SELECT * FROM jobs WHERE lane=? AND status='queued' AND available_at<=? "
                           "ORDER BY id LIMIT 1", (lane, time.time()))
            if row:
                self.execute("UPDATE jobs SET status='running',attempts=attempts+1 WHERE id=?", (row["id"],))
                row["attempts"] += 1
                row["payload"] = json.loads(row["payload"])
            return row

    def finish_job(self, jid, status="done", error=None, delay=0, result=None):
        with self.transaction():
            self.execute("UPDATE jobs SET status=?,error=?,available_at=?,result=? WHERE id=?",
                         (status, error, time.time() + delay, json.dumps(result), jid))
            if status in ('done', 'failed', 'unknown'):
                job = self.one('SELECT * FROM jobs WHERE id=?', (jid,))
                if job and job['kind'] == 'deliver':
                    payload = json.loads(job['payload'])
                    if 'month' in payload:
                        self.queue_note(payload['uid'])
                    if status == 'done':
                        self.record_delivery_note(job)

    def record_delivery_note(self, job, completed_interval=2):
        """Snapshot confirmed sends only. Never enqueue a platform send during backfill."""
        if job['kind'] != 'deliver' or job['status'] != 'done':
            return False
        if self.one('SELECT job_id FROM delivery_notes WHERE job_id=?', (job['id'],)):
            return False
        p = json.loads(job['payload'])
        u = self.user(p['uid'])
        result = json.loads(job['result'] or 'null') or {}
        if not isinstance(result, dict):
            result = {}
        saved = result.get('_sinai_delivery') or {}
        message = result.get('message') or result
        body = message.get('body') or {}
        # Automatic payloads historically contain only uid/month, so use the actual API receipt.
        text = saved.get('text', p.get('text'))
        if text is None:
            text = body.get('text') if 'text' in body else message.get('text', message.get('caption'))
        sent_at = saved.get('sent_at')
        source = saved.get('time_source')
        if sent_at is None and message.get('timestamp'):
            sent_at, source = message['timestamp'] / 1000, 'ответ платформы'
        if sent_at is None and message.get('date'):
            sent_at, source = message['date'], 'ответ платформы'
        if sent_at is None:
            sent_at, source = job['available_at'], 'время фиксации подтверждения отправки в SQLite'
        every = saved.get('interval') or (self.setting('completed_interval', completed_interval) if u['group_name'] == 'completed' else 1)
        consumed = self.one("SELECT MAX(CAST(json_extract(payload,'$.month') AS INTEGER)) month FROM jobs "
                            "WHERE kind='deliver' AND json_extract(payload,'$.uid')=? AND available_at<=? "
                            "AND status IN ('done','unknown','failed')", (u['id'], sent_at + 1))
        last_month = max(p.get('month', 0), (consumed or {}).get('month') or 0)
        next_at = next_automatic_date(u['subscribed_at'], sent_at, every, 'Asia/Krasnoyarsk', last_month)
        platform = saved.get('platform', u['platform'])
        external_id = saved.get('external_id', u['external_id'])
        lead = saved.get('lead_id', u['lead_id'])
        marker = f"[sinai-delivery:{platform}:{external_id}:{job['key']}]"
        snapshot = {'platform': platform, 'external_id': external_id, 'sent_at': sent_at,
                    'time_source': source, 'text': text, 'month': p.get('month'), 'cid': p.get('cid'),
                    'part': p.get('part'), 'media': p.get('media'), 'next_at': next_at,
                    'interval_source': 'на момент отправки' if saved else 'текущие настройки при восстановлении',
                    'recovered': not bool(saved)}
        status = 'pending' if lead else 'waiting_link'
        error = None
        if text is None:
            status, error = 'incomplete', 'Full sent text absent from stored API receipt; no text invented'
        self.execute('INSERT OR IGNORE INTO delivery_notes(job_id,marker,user_id,lead_id,snapshot,status,error) '
                     'VALUES (?,?,?,?,?,?,?)', (job['id'], marker, u['id'], lead, json.dumps(snapshot, ensure_ascii=False), status, error))
        if status == 'pending':
            self.enqueue(f"delivery_note:{job['id']}", 'crm_history', 'delivery_note', {'job_id': job['id']})
            self.queue_note(u['id'])
        return True

    def backfill_delivery_notes(self, limit=500, completed_interval=2):
        with self.transaction():
            jobs = self.rows("SELECT j.* FROM jobs j WHERE j.kind='deliver' AND j.status='done' "
                             "AND NOT EXISTS (SELECT 1 FROM delivery_notes n WHERE n.job_id=j.id) "
                             "ORDER BY j.id LIMIT ?", (limit,))
            for job in jobs:
                self.record_delivery_note(job, completed_interval)
            for note in self.rows("SELECT n.job_id,u.lead_id FROM delivery_notes n JOIN users u ON u.id=n.user_id "
                                  "WHERE n.status='waiting_link' AND u.lead_id IS NOT NULL LIMIT ?", (limit,)):
                self.execute("UPDATE delivery_notes SET lead_id=?,status='pending' WHERE job_id=?", (note['lead_id'], note['job_id']))
                self.enqueue(f"delivery_note:{note['job_id']}", 'crm_history', 'delivery_note', {'job_id': note['job_id']})
            # Repair a missing note job, retaining backoff and ambiguous POST state.
            for note in self.rows("SELECT job_id FROM delivery_notes WHERE status IN ('pending','writing','unknown') LIMIT ?", (limit,)):
                self.enqueue(f"delivery_note:{note['job_id']}", 'crm_history', 'delivery_note', {'job_id': note['job_id']})
            return len(jobs)

    def touch_user(self, platform, external_id, chat_id, name):
        self.execute("INSERT INTO users(platform,external_id,chat_id,name,last_seen) VALUES (?,?,?,?,?) "
                     "ON CONFLICT(platform,external_id) DO UPDATE SET chat_id=excluded.chat_id, "
                     "name=excluded.name,last_seen=excluded.last_seen",
                     (platform, external_id, chat_id, name[:200], time.time()))
        return self.one("SELECT * FROM users WHERE platform=? AND external_id=?", (platform, external_id))

    def user(self, uid):
        return self.one("SELECT * FROM users WHERE id=?", (uid,))

    def state(self, uid, value):
        self.execute("UPDATE users SET state=? WHERE id=?", (json.dumps(value), uid))

    def start(self, uid, lead):
        with self.transaction():
            u = self.user(uid)
            # Repeat /start never resets subscription, claim, phone or scheduler anchor.
            if not u["lead_id"] and lead:
                self.execute("UPDATE users SET pending_lead=? WHERE id=?", (lead, uid))
                # Returning subscribers see no group buttons; their prior choice is sufficient.
                if u['selected_group'] is not None:
                    try:
                        self.reserve(uid, lead)
                        self.queue_bind(uid, lead)
                    except Conflict:
                        self.execute("UPDATE users SET sync_reason='Конфликт привязки' WHERE id=?", (uid,))
            elif u['lead_id']:
                self.queue_note(uid)
            self.execute("UPDATE users SET active=1,state='{}' WHERE id=?", (uid,))

    def reserve(self, uid, lead):
        u = self.user(uid)
        if not u or not u["subscribed_at"]:
            raise Conflict("Сначала выберите группу")
        if u["lead_id"] and u["lead_id"] != lead:
            raise Conflict("Пользователь уже привязан к другой сделке")
        row = self.one("SELECT * FROM claims WHERE lead_id=? OR user_id=?", (lead, uid))
        if row and (row["user_id"] != uid or row["lead_id"] != lead):
            raise Conflict("Сделка или пользователь уже закреплены")
        self.execute("INSERT OR IGNORE INTO claims VALUES (?,?,?)", (lead, uid, time.time()))

    def request_bind(self, uid, lead, actor=None):
        with self.transaction():
            self.reserve(uid, lead)
            self.queue_bind(uid, lead, actor)
            self.execute("UPDATE users SET sync_reason='Проверка сделки' WHERE id=? AND lead_id IS NULL", (uid,))

    def queue_bind(self, uid, lead, actor=None):
        key = f"bind:{uid}:{lead}"
        payload = {"uid": uid, "lead": lead, "actor": actor}
        self.enqueue(key, 'crm', 'bind', payload)
        self.execute("UPDATE jobs SET status='queued',attempts=0,available_at=0,payload=? "
                     "WHERE key=? AND status IN ('failed','done') AND NOT EXISTS "
                     "(SELECT 1 FROM users WHERE id=? AND lead_id=?)", (json.dumps(payload), key, uid, lead))

    def queue_note(self, uid):
        u = self.user(uid)
        if not u or not u['lead_id']:
            return
        lead = u['lead_id']
        marker = f"[sinai-subscription:{uid}:{lead}]"
        self.execute("INSERT OR IGNORE INTO notes(user_id,lead_id,marker) VALUES (?,?,?)", (uid, lead, marker))
        key = f'note:{uid}:{lead}'
        self.enqueue(key, 'crm', 'note', {'uid': uid})
        # Retain retry/backoff of pending jobs, but reconcile previously terminal ones.
        self.execute("UPDATE jobs SET status='queued',available_at=0,attempts=0 WHERE key=? AND status IN ('done','failed')", (key,))

    def automatic_info(self, uid, interval, tz, now=None):
        u = self.user(uid)
        every = interval if u['group_name'] == 'completed' else 1
        consumed = self.one("SELECT MAX(CAST(json_extract(payload,'$.month') AS INTEGER)) month FROM jobs "
                            "WHERE kind='deliver' AND json_extract(payload,'$.uid')=? "
                            "AND status IN ('done','running','unknown','failed')", (uid,))
        last = (consumed or {}).get('month') or 0
        return every, next_automatic_date(u['subscribed_at'], now or time.time(), every, tz, last)

    def reconcile_subscriptions(self):
        with self.transaction():
            for u in self.rows("SELECT * FROM users WHERE subscribed_at IS NOT NULL"):
                if u['lead_id']:
                    self.queue_note(u['id'])
                elif u['pending_lead'] and u['selected_group'] is not None:
                    try:
                        self.reserve(u['id'], u['pending_lead'])
                        self.queue_bind(u['id'], u['pending_lead'])
                    except Conflict:
                        self.execute("UPDATE users SET sync_reason='Конфликт привязки' WHERE id=?", (u['id'],))

    def choose_group(self, uid, group):
        if group not in ("active", "completed"):
            raise ValueError("group")
        with self.transaction():
            u = self.user(uid)
            self.execute("UPDATE users SET selected_group=?,group_name=CASE WHEN lead_id IS NULL THEN ? "
                         "ELSE group_name END,subscribed_at=COALESCE(subscribed_at,?),active=1 WHERE id=?",
                         (group, group, time.time(), uid))
            if u["pending_lead"] and not u["lead_id"]:
                try:
                    self.reserve(uid, u["pending_lead"])
                    self.queue_bind(uid, u['pending_lead'])
                    return "binding"
                except Conflict:
                    self.execute("UPDATE users SET sync_reason='Конфликт привязки' WHERE id=?", (uid,))
                    return "conflict"
            return "linked" if u["lead_id"] else "contact"

    def phone_search(self, uid, phone):
        with self.transaction():
            u = self.user(uid)
            if not u["subscribed_at"] or u["lead_id"] or u["pending_lead"]:
                return False
            self.execute("UPDATE users SET phone=?,sync_reason='Поиск по телефону' WHERE id=?", (phone, uid))
            self.enqueue(f"phone:{uid}:{phone}", "crm", "phone", {"uid": uid, "phone": phone})
            return True

    def bind_verified(self, uid, lead, pipeline, completed_pipeline):
        with self.transaction():
            self.reserve(uid, lead)
            self.execute("UPDATE users SET lead_id=?,pending_lead=NULL,pipeline_id=?,group_name=?, "
                         "crm_checked_at=?,sync_reason='Синхронизирован' WHERE id=?",
                         (lead, pipeline, "completed" if pipeline == completed_pipeline else "active", time.time(), uid))
            marker = f"[sinai-subscription:{uid}:{lead}]"
            self.execute("INSERT OR IGNORE INTO notes(user_id,lead_id,marker) VALUES (?,?,?)", (uid, lead, marker))
            self.queue_note(uid)
            self.audit("bind", uid, str(lead))

    def invalid_lead(self, uid, lead):
        with self.transaction():
            self.execute("DELETE FROM claims WHERE user_id=? AND lead_id=? AND NOT EXISTS "
                         "(SELECT 1 FROM users WHERE id=? AND lead_id IS NOT NULL)", (uid, lead, uid))
            self.execute("UPDATE users SET pending_lead=NULL,sync_reason='Сделка не найдена' "
                         "WHERE id=? AND lead_id IS NULL", (uid,))

    def apply_crm(self, uid, lead, pipeline, completed_pipeline):
        with self.transaction():
            self.execute("UPDATE users SET pipeline_id=?,group_name=?,crm_checked_at=?,sync_reason='Синхронизирован' "
                         "WHERE id=? AND lead_id=?", (pipeline, "completed" if pipeline == completed_pipeline else "active",
                                                  time.time(), uid, lead))
            self.queue_note(uid)

    def create_campaign(self, owner, audience):
        cid = uuid.uuid4().hex[:16]
        self.execute("INSERT INTO campaigns(id,owner,audience,payload,created_at) VALUES (?,?,?,?,?)",
                     (cid, owner, audience, '{"text":"","media":[]}', time.time()))
        return cid

    def campaign(self, cid, owner=None):
        r = self.one("SELECT * FROM campaigns WHERE id=?", (cid,))
        if r and (owner is None or r["owner"] == owner):
            r["payload"] = json.loads(r["payload"])
            return r
        return None

    def edit_campaign(self, cid, owner, payload):
        return self.execute("UPDATE campaigns SET payload=?,status='draft' WHERE id=? AND owner=? "
                            "AND status IN ('draft','preview')", (json.dumps(payload, ensure_ascii=False), cid, owner))

    def confirm_campaign(self, cid, owner):
        with self.transaction():
            c = self.campaign(cid, owner)
            if not c or c["status"] != "preview":
                return False
            self.execute("UPDATE campaigns SET status='queued' WHERE id=?", (cid,))
            self.enqueue(f"expand:{cid}", "bulk", "expand", {"cid": cid})
            self.audit("campaign_confirm", owner, cid)
            return True

    def mark_preview(self, cid, owner, payload, version):
        with self.transaction():
            c = self.campaign(cid, owner)
            if not c or c["status"] not in ("draft", "preview") or c["payload"] != payload:
                return False
            self.execute("UPDATE campaigns SET status='preview' WHERE id=?", (cid,))
            self.set_setting(f"preview:{cid}", version)
            return True

    def expand_campaign(self, cid, after=0):
        with self.transaction():
            c = self.campaign(cid)
            if c["status"] not in ("queued", "sending"):
                return
            audience = c["audience"]
            query = "SELECT id FROM users WHERE subscribed_at IS NOT NULL AND active=1 AND id>?"
            args = (after,)
            if audience in ("active", "completed"):
                query += " AND group_name=?"
                args += (audience,)
            if audience == "unsynced":
                query += " AND lead_id IS NULL"
            batch = self.rows(query + " ORDER BY id LIMIT 500", args)
            for u in batch:
                # Each media part has independent delivery status; no repeated earlier parts on retry.
                for index, part in enumerate(message_parts(c["payload"])):
                    self.enqueue(f"campaign:{cid}:{u['id']}:{index}", "delivery", "deliver",
                                 {"uid": u["id"], "cid": cid, **part, "part": index})
            self.execute("UPDATE campaigns SET status='sending' WHERE id=?", (cid,))
            if len(batch) == 500:
                cursor = batch[-1]['id']
                self.enqueue(f'expand:{cid}:{cursor}', 'bulk', 'expand', {'cid': cid, 'after': cursor})

    def schedule(self, now, interval, tz):
        with self.transaction():
            for u in self.rows("SELECT * FROM users WHERE subscribed_at IS NOT NULL AND active=1"):
                every = interval if u["group_name"] == "completed" else 1
                month = due_month(u["subscribed_at"], now, every, tz)
                if month is None:
                    continue
                consumed = self.one("SELECT MAX(CAST(json_extract(payload,'$.month') AS INTEGER)) month "
                                    "FROM jobs WHERE kind='deliver' AND json_extract(payload,'$.uid')=? "
                                    "AND status IN ('done','running','unknown')", (u['id'],))
                if consumed and consumed['month'] is not None and consumed['month'] >= month:
                    continue
                self.enqueue(f"auto:{u['id']}:{month}", "delivery", "deliver", {"uid": u["id"], "month": month})
                self.execute("UPDATE jobs SET status='queued',available_at=?,attempts=0 WHERE key=? AND status='skipped'",
                             (time.time(), f"auto:{u['id']}:{month}"))

    def stats(self):
        users = self.one("SELECT COUNT(*) total, SUM(platform='tg') tg,SUM(platform='max') max, "
                         "SUM(group_name='active') current,SUM(group_name='completed') completed, "
                         "SUM(lead_id IS NOT NULL) synced,SUM(lead_id IS NULL) unsynced,SUM(active=1) active, "
                         "SUM(last_seen>?) recent FROM users WHERE subscribed_at IS NOT NULL",
                         (time.time() - 30 * 86400,))
        jobs = self.rows("SELECT status,COUNT(*) n FROM jobs WHERE kind='deliver' GROUP BY status")
        return users, {r["status"]: r["n"] for r in jobs}

    def close(self):
        self.conn.close()

