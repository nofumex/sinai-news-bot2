from html import escape
from html.parser import HTMLParser
import json
import re
import uuid

from .domain import Conflict, WELCOME, max_contact_phone, normalize_phone, subscription_date
from .platforms import event

GROUPS = {"active": "Действующие", "completed": "Завершённые", "all": "Все подписчики", "unsynced": "Несинхронизированные"}
BACK = [[("‹ Назад", "admin")]]


class CommonHTML(HTMLParser):
    """Shared safe formatting subset supported by both messaging platforms."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []

    def handle_starttag(self, tag, attrs):
        if tag not in {"b", "strong", "i", "em", "u", "s", "strike", "a", "code", "pre"}:
            raise ValueError(f"Тег {tag} не поддерживается")
        if tag == "a":
            if len(attrs) != 1 or attrs[0][0] != "href" or not re.match(r"^https?://", attrs[0][1] or ""):
                raise ValueError("Для ссылки используйте <a href=\"https://…\">")
        elif attrs:
            raise ValueError("Атрибуты этого тега не поддерживаются")
        self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack.pop() != tag:
            raise ValueError("HTML-теги должны быть закрыты в правильном порядке")

    def handle_startendtag(self, tag, attrs):
        raise ValueError("Самозакрывающиеся теги не поддерживаются")

    def handle_comment(self, data):
        raise ValueError("HTML-комментарии не поддерживаются")


def validate_html(text, limit=3900):
    if len(text) > limit:
        raise ValueError(f"Максимум {limit} символов")
    parser = CommonHTML()
    parser.feed(text)
    parser.close()
    if parser.stack:
        raise ValueError("Закройте все HTML-теги")


class UI:
    def __init__(self, cfg, store, platforms):
        self.cfg, self.store, self.platforms = cfg, store, platforms

    def is_admin(self, u):
        return u["external_id"] == (self.cfg.admin_id if u["platform"] == "tg" else self.cfg.max_admin_id)

    async def say(self, u, text, buttons=None, contact=False, key=None, media=None):
        await self.store.call("enqueue", key or f"ui:{uuid.uuid4().hex}", "ui_send", "ui",
                              {"uid": u["id"], "text": text, "buttons": buttons, "contact": contact, "media": media})

    async def handle(self, job):
        p = job["payload"]
        e = event(p["platform"], p["raw"])
        if not e or not e.get("external_id"):
            return
        u = await self.store.call("touch_user", p["platform"], e["external_id"], e["chat_id"], e.get("name", ""))
        if e.get("stopped") is not None:
            await self.store.call("execute", "UPDATE users SET active=? WHERE id=?", (int(not e["stopped"]), u["id"]))
            if e["stopped"]:
                return
        if e.get("callback_id"):
            # Ack does not wait for CRM, a media upload or campaign expansion.
            try:
                await self.platforms.ack(u["platform"], e["callback_id"])
            except Exception:
                pass  # Old callback acknowledgements can expire after a restart.
        count = 0
        async def say(text, buttons=None, contact=False):
            nonlocal count
            count += 1
            await self.say(u, text, buttons, contact, f"reply:{job['id']}:{count}")

        text = e.get("text", "")
        callback = e.get("callback")
        command = text.split(maxsplit=1)[0].split("@")[0] if text.strip() else ""
        if command == "/start":
            payload = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) > 1 else ""
            lead = int(payload) if re.fullmatch(r"[1-9][0-9]{0,17}", payload) else None
            await self.store.call("start", u["id"], lead)
            if u["subscribed_at"]:
                await say("<b>Ваша подписка сохранена ✓</b>\n" +
                          f"Группа: {GROUPS[u['group_name']]}. Дата подписки не меняется.",
                          [[("Выбрать группу", "groups")], [("Моя подписка", "profile")]])
            else:
                await say(WELCOME, self.group_buttons())
            return
        if command == "/stop" or callback == "stop":
            await self.store.call("execute", "UPDATE users SET active=0 WHERE id=?", (u["id"],))
            await say("<b>Подписка приостановлена.</b>\nВозобновить: /start")
            return
        if command in ("/admin", "/message") or (callback and callback.startswith(("admin", "uns:", "card:", "bind:", "interval:", "new", "draft:", "preview:", "confirm:", "cancel:", "progress:", "jobs", "retry:", "sync"))):
            if not self.is_admin(u):
                await say("Эта команда доступна только администратору.")
                return
            await self.admin(u, e, say)
            return
        if callback == "groups":
            await say("<b>Выберите группу</b>\nДля привязанных пользователей группа определяется по amoCRM.", self.group_buttons())
            return
        if callback and callback.startswith("group:"):
            group = callback.split(":", 1)[1]
            if group not in ("active", "completed"):
                return
            result = await self.store.call("choose_group", u["id"], group)
            await self.store.call("state", u["id"], {})
            if result == "contact":
                await say("<b>Вы подписаны ✓</b>\nПоделитесь вашим контактом для поиска существующей сделки. "
                          "Рассылки доступны и без привязки.", contact=True)
            elif result == "binding":
                await say("<b>Вы подписаны ✓</b>\nПроверяем сделку в фоне. Результат придёт отдельным сообщением.")
            elif result == "conflict":
                await say("<b>Вы подписаны ✓</b>\nЭта ссылка уже закреплена за другим пользователем. "
                          "Рассылки вашей группы сохранены; обратитесь к администратору для проверки.")
            else:
                current = await self.store.call("user", u["id"])
                await say(f"<b>Вы подписаны ✓</b>\nГруппа по amoCRM: {GROUPS[current['group_name']]}")
            return
        if callback == "profile" or command == "/status":
            await say("<b>Моя подписка</b>\n" + (f"Группа: {GROUPS[u['group_name']]}\n"
                      f"Дата: {subscription_date(u['subscribed_at'], self.cfg.timezone)}\n"
                      f"CRM: {escape(u['sync_reason'])}\n"
                      f"Рассылки: {'включены' if u['active'] else 'приостановлены'}" if u["subscribed_at"] else "Выберите группу: /start"),
                      [[("Приостановить рассылки", "stop")]])
            return
        if e.get("contact"):
            if not u["subscribed_at"]:
                await say("Сначала выберите группу: /start")
                return
            if u["lead_id"] or u["pending_lead"]:
                await say("Привязка по ссылке уже выполняется или сохранена. Контакт не требуется.")
                return
            contact = e["contact"]
            phone = normalize_phone(contact.get("phone_number", "")) if u["platform"] == "tg" and contact.get("user_id") == u["external_id"] else None
            if u["platform"] == "max":
                phone = max_contact_phone(contact, self.cfg.max_token, u["external_id"])
            if not phone:
                await say("Не удалось подтвердить принадлежность номера. Используйте кнопку «Поделиться контактом».", contact=True)
                return
            await self.store.call("phone_search", u["id"], phone)
            await say("<b>Контакт подтверждён ✓</b>\nИщем вашу сделку в фоне. Вы уже получаете рассылки выбранной группы.")
            return
        if self.is_admin(u):
            state = json.loads(u["state"])
            if state.get("mode") == "find_user":
                match = re.fullmatch(r"(tg|max)\s+([1-9][0-9]{0,17})", text.strip().lower())
                if not match:
                    await say("Введите <code>tg 123456789</code> или <code>max 123456789</code>.", BACK)
                    return
                target = await self.store.call("one", "SELECT * FROM users WHERE platform=? AND external_id=?",
                                               (match[1], int(match[2])))
                if not target or not target['subscribed_at']:
                    await say("Подписчик не найден. Он должен сначала выбрать группу в боте.", BACK)
                    return
                await self.store.call("state", u["id"], {"mode": "bind", "target": target['id']})
                await say(f"<b>Пользователь найден</b>\n{escape(target['name'])} · {target['external_id']}\nВведите lead ID.", BACK)
                return
            if state.get("mode") == "bind":
                if not re.fullmatch(r"[1-9][0-9]{0,17}", text.strip()):
                    await say("Введите только числовой lead ID.", BACK)
                    return
                try:
                    await self.store.call("request_bind", state["target"], int(text), u["id"])
                except Conflict as exc:
                    await say(f"<b>Привязка отклонена</b>\n{escape(str(exc))}", BACK)
                    return
                await self.store.call("state", u["id"], {})
                await say("<b>Задача принята</b>\nПроверяем существование сделки в фоне. Результат будет отправлен сюда.", BACK)
                return
            if state.get("mode") == "draft":
                await self.update_draft(u, state["cid"], e, say)
                return
        await say("<b>Синай · новости и бонусная программа</b>\n/start — подписаться\n/status — моя подписка\n/stop — приостановить рассылки")

    @staticmethod
    def group_buttons():
        return [[("Я действующий клиент", "group:active")], [("Моя процедура уже завершена", "group:completed")]]

    async def admin(self, u, e, say):
        cb = e.get("callback") or ("new" if e["text"].split()[0] == "/message" else "admin")
        if cb == "admin":
            await self.store.call("state", u["id"], {})
            s, j = await self.store.call("stats")
            n = lambda k: s[k] or 0
            interval = await self.store.call("setting", "completed_interval", self.cfg.completed_interval)
            last = await self.store.call("setting", "crm_last_sync", "ещё не выполнялась")
            await say(f"<b>Синай · панель управления</b>\n\n"
                      f"👥 Подписчиков: <b>{n('total')}</b>\nTelegram: {n('tg')} · MAX: {n('max')}\n"
                      f"Действующие: {n('current')} · Завершённые: {n('completed')}\n"
                      f"Синхронизированы: {n('synced')} · Без привязки: {n('unsynced')}\n"
                      f"Рассылки включены: {n('active')} · Заходили за 30 дней: {n('recent')}\n\n"
                      f"✉️ Отправлено: {j.get('done',0)} · Ошибки: {j.get('failed',0)}\n"
                      f"В очереди: {j.get('queued',0)} · Доставка неизвестна: {j.get('unknown',0)}\n"
                      f"Завершённым: каждые {interval} мес.\nCRM: {escape(str(last))}",
                      [[("🔎 Несинхронизированные", "uns:0")], [("🔗 Привязать по ID пользователя", "bind:any")],
                       [("✉️ Создать рассылку", "new")], [("📅 1 месяц", "interval:1"), ("📅 2 месяца", "interval:2")],
                       [("↻ Проверить CRM", "sync"), ("⚠️ Ошибки очереди", "jobs:0")], [("↻ Обновить", "admin")]])
        elif cb.startswith("interval:"):
            value = int(cb.split(":")[1])
            if value in (1, 2):
                await self.store.call("set_setting", "completed_interval", value)
                await self.store.call("audit", "interval", u["id"], str(value))
            await say(f"<b>Интервал сохранён: {value} мес.</b>", BACK)
        elif cb == "sync":
            await self.store.call("enqueue", f"manual_sync:{uuid.uuid4().hex}", "crm", "sync", {})
            await say("<b>Проверка CRM поставлена в очередь.</b>", BACK)
        elif cb.startswith("uns:"):
            page = max(0, int(cb.split(":")[1]))
            rows = await self.store.call("rows", "SELECT * FROM users WHERE subscribed_at IS NOT NULL "
                                         "AND lead_id IS NULL ORDER BY id LIMIT 9 OFFSET ?", (page * 8,))
            buttons = [[(f"{'TG' if r['platform'] == 'tg' else 'MAX'} · {r['name'][:18] or r['external_id']}", f"card:{r['id']}")] for r in rows[:8]]
            nav = []
            if page:
                nav.append(("‹ Ранее", f"uns:{page-1}"))
            if len(rows) > 8:
                nav.append(("Далее ›", f"uns:{page+1}"))
            if nav:
                buttons.append(nav)
            await say(f"<b>Несинхронизированные · {page+1}</b>\nВыберите пользователя." if rows else "<b>Нет пользователей без привязки.</b>", buttons + BACK)
        elif cb == "bind:any":
            await self.store.call("state", u["id"], {"mode": "find_user"})
            await say("Введите платформу и ID пользователя: <code>tg 123456789</code> или <code>max 123456789</code>.", BACK)
        elif cb.startswith("card:"):
            target = await self.store.call("user", int(cb.split(":")[1]))
            if not target:
                await say("Пользователь не найден.", BACK)
                return
            await say(f"<b>Карточка подписчика</b>\n{escape(target['name'])}\n"
                      f"Платформа: {target['platform'].upper()}\nID: <code>{target['external_id']}</code>\n"
                      f"Группа: {GROUPS.get(target['group_name'], 'Не подписан')}\n"
                      f"Телефон: {escape(target['phone'] or 'не предоставлен')}\n"
                      f"Сделка: {target['lead_id'] or 'не привязан'}\n"
                      f"Статус: {escape(target['sync_reason'])}\n"
                      f"Подписка: {subscription_date(target['subscribed_at'], self.cfg.timezone) if target['subscribed_at'] else 'нет'}",
                      [[("🔗 Ввести lead ID", f"bind:{target['id']}")], [("‹ К списку", "uns:0")]] + BACK)
        elif cb.startswith("bind:"):
            target = int(cb.split(":")[1])
            await self.store.call("state", u["id"], {"mode": "bind", "target": target})
            await say("<b>Ручная привязка</b>\nВведите lead ID существующей сделки. Привязки к другим сделкам не заменяются.", BACK)
        elif cb == "new":
            await self.store.call("state", u["id"], {})
            await say("<b>Новая рассылка · аудитория</b>", [[(title, f"new:{key}")] for key, title in GROUPS.items()] + BACK)
        elif cb.startswith("new:"):
            audience = cb.split(":")[1]
            if audience not in GROUPS:
                return
            cid = await self.store.call("create_campaign", u["id"], audience)
            await self.store.call("state", u["id"], {"mode": "draft", "cid": cid})
            await self.draft_help(cid, say)
        elif cb.startswith("draft:"):
            cid = cb.split(":")[1]
            c = await self.store.call("campaign", cid, u["id"])
            if not c or c["status"] not in ("draft", "preview"):
                await say("Черновик уже отправлен или отменён.", BACK)
                return
            await self.store.call("state", u["id"], {"mode": "draft", "cid": cid})
            await self.draft_help(cid, say)
        elif cb.startswith("preview:"):
            cid = cb.split(":")[1]
            c = await self.store.call("campaign", cid, u["id"])
            if not c or c["status"] not in ("draft", "preview"):
                return
            if not c["payload"].get("text") and not c["payload"].get("media"):
                await say("Добавьте текст или вложения.", [[("‹ Черновик", f"draft:{cid}")]])
                return
            await self.store.call("state", u["id"], {})
            version = uuid.uuid4().hex[:8]
            await self.store.call("enqueue", f"preview_prepare:{cid}:{version}", "media", "preview", {"cid": cid, "owner": u["id"], "version": version})
            await say("<b>Готовим предпросмотр Telegram и MAX.</b>\nМедиа загружаются в фоне. После предпросмотра появится подтверждение.", BACK)
        elif cb.startswith("confirm:"):
            _, cid, version = cb.split(":")
            latest = await self.store.call("setting", f"preview:{cid}")
            if latest != version:
                await say("Этот предпросмотр устарел. Подготовьте новый.", [[("↻ Предпросмотр", f"preview:{cid}")]] + BACK)
                return
            pending = await self.store.call("rows", "SELECT status FROM jobs WHERE key LIKE ?", (f"preview_send:{cid}:{version}:%",))
            if not pending or any(j["status"] != "done" for j in pending):
                await say("Предпросмотр ещё не доставлен на обе платформы. Проверьте ошибки очереди или повторите предпросмотр.", [[("↻ Предпросмотр", f"preview:{cid}")]] + BACK)
                return
            if await self.store.call("confirm_campaign", cid, u["id"]):
                await self.store.call("state", u["id"], {})
                await say("<b>Рассылка подтверждена ✓</b>\nПолучатели и отправки формируются в фоне.", [[("📊 Прогресс", f"progress:{cid}")]] + BACK)
            else:
                await say("Рассылка уже подтверждена, отменена или изменена.", BACK)
        elif cb.startswith("cancel:"):
            cid = cb.split(":")[1]
            await self.store.call("execute", "UPDATE campaigns SET status='cancelled' WHERE id=? AND owner=? "
                                  "AND status IN ('draft','preview')", (cid, u["id"]))
            await self.store.call("state", u["id"], {})
            await say("<b>Черновик закрыт.</b>", BACK)
        elif cb.startswith("progress:"):
            cid = cb.split(":")[1]
            c = await self.store.call("campaign", cid)
            if not c:
                return
            rows = await self.store.call("rows", "SELECT status,COUNT(*) n FROM jobs WHERE kind='deliver' "
                                         "AND key LIKE ? GROUP BY status", (f"campaign:{cid}:%",))
            counts = {r["status"]: r["n"] for r in rows}
            total = sum(counts.values())
            done = counts.get("done", 0)
            percent = int(100 * done / total) if total else 0
            bar = "▰" * (percent // 10) + "▱" * (10 - percent // 10)
            await say(f"<b>Рассылка · {cid}</b>\n{bar} {percent}%\n"
                      f"Частей сообщений: {total}\nОтправлено: {done}\n"
                      f"В очереди: {counts.get('queued',0)} · Отправляется: {counts.get('running',0)}\n"
                      f"Ошибки: {counts.get('failed',0)} · Неизвестно: {counts.get('unknown',0)}\n"
                      f"Пропущено: {counts.get('skipped',0)}",
                      [[("↻ Обновить", f"progress:{cid}")]] + BACK)
        elif cb.startswith("jobs"):
            page = max(0, int(cb.split(":")[1])) if ":" in cb else 0
            rows = await self.store.call("rows", "SELECT id,kind,status,error FROM jobs WHERE status IN ('failed','unknown') "
                                         "ORDER BY id DESC LIMIT 6 OFFSET ?", (page * 5,))
            lines = [f"<code>#{r['id']}</code> {r['kind']} · {r['status']}\n{escape(r['error'] or '')}" for r in rows[:5]]
            buttons = [[(f"Повторить #{r['id']}", f"retry:{r['id']}")] for r in rows[:5] if r["status"] == "failed"]
            nav = []
            if page:
                nav.append(("‹ Ранее", f"jobs:{page-1}"))
            if len(rows) > 5:
                nav.append(("Далее ›", f"jobs:{page+1}"))
            if nav:
                buttons.append(nav)
            await say("<b>Ошибки очереди</b>\nНеизвестные доставки автоматически не повторяются.\n\n" + ("\n\n".join(lines) or "Ошибок нет."), buttons + BACK)
        elif cb.startswith("retry:"):
            jid = int(cb.split(":")[1])
            await self.store.call("execute", "UPDATE jobs SET status='queued',attempts=0,available_at=0 "
                                  "WHERE id=? AND status='failed'", (jid,))
            await self.store.call("audit", "retry", u["id"], str(jid))
            await say("Повторная попытка поставлена в очередь. Неизвестные доставки не повторяются.", BACK)

    async def draft_help(self, cid, say):
        await say("<b>Конструктор рассылки</b>\nПришлите текст HTML или фотографии, видео, документы с подписями. "
                  "Текстовое сообщение заменяет общий текст; вложения добавляются по одному.\n"
                  "Форматирование: <code>&lt;b&gt;жирный&lt;/b&gt;</code>, <code>&lt;i&gt;курсив&lt;/i&gt;</code>, ссылки.\n"
                  "До 10 вложений, до 1000 символов в подписи; текст без медиа — до 3900.\n"
                  "<code>/clear</code> — очистить вложения и текст.",
                  [[("👁 Предпросмотр", f"preview:{cid}")], [("Отменить", f"cancel:{cid}")]] + BACK)

    async def update_draft(self, u, cid, e, say):
        c = await self.store.call("campaign", cid, u["id"])
        if not c or c["status"] not in ("draft", "preview"):
            await self.store.call("state", u["id"], {})
            await say("Этот черновик больше нельзя изменять.", BACK)
            return
        payload = c["payload"]
        try:
            if e["text"] == "/clear":
                payload = {"text": "", "media": []}
            elif e.get("media"):
                if len(payload["media"]) + len(e["media"]) > 10:
                    raise ValueError("Максимум 10 вложений")
                validate_html(e["text"], 1000)
                validate_html(payload["text"], 1000)
                for media in e["media"]:
                    mid = self.platforms.media_identity(u["platform"], media["source"], media["kind"])
                    await self.store.call("execute", "INSERT OR IGNORE INTO media(id,origin,source,kind,name,tg_file,max_payload) "
                                          "VALUES (?,?,?,?,?,?,?)", (mid, u["platform"], json.dumps(media["source"]), media["kind"],
                                          media["name"], media["source"].get("file_id") if u["platform"] == "tg" else None,
                                          json.dumps({"token": media["source"]["token"]}) if u["platform"] == "max" and media["source"].get("token") else None))
                    payload["media"].append({"id": mid, "caption": e["text"]})
            else:
                validate_html(e["text"], 1000 if payload["media"] else 3900)
                payload["text"] = e["text"]
            await self.store.call("edit_campaign", cid, u["id"], payload)
        except ValueError as exc:
            await say(f"<b>Проверьте сообщение</b>\n{escape(str(exc))}", [[("‹ Черновик", f"draft:{cid}")]])
            return
        await say(f"<b>Черновик сохранён ✓</b>\nВложений: {len(payload['media'])} · Текст: {len(payload['text'])} символов",
                  [[("👁 Предпросмотр", f"preview:{cid}")], [("Отменить", f"cancel:{cid}")]] + BACK)

