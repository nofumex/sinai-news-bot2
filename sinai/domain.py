from datetime import datetime, timezone
import hashlib
import hmac
import re
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
import phonenumbers

WELCOME = 'Добро пожаловать! Это официальный чат-бот компании „Синай“. Здесь мы будем отправлять вам особенно важные новости'
ACTIVE_TEXT = 'Напоминаем о нашей бонусной программе. За приведённого клиента вы получите скидку к оплате 8000 рублей. Если вы уже рассчитались, мы переведём эту сумму вам на карту. Ваш знакомый получит скидку 5000 рублей'
COMPLETED_TEXT = 'Напоминаем вам о бонусной программе. За каждого приведённого клиента вы получите 8000 рублей, а он получит скидку 5000 рублей'


class Conflict(Exception):
    pass


def normalize_phone(raw: str) -> str | None:
    try:
        p = phonenumbers.parse(raw, "RU")
        if phonenumbers.is_possible_number(p) and phonenumbers.is_valid_number(p):
            return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    return None


def max_contact_phone(payload: dict, token: str, user_id: int) -> str | None:
    vcf = payload.get("vcf_info", "")
    if not isinstance(vcf, str) or not isinstance(payload.get("hash"), str):
        return None
    # JSON decoding normally already produces CRLF. Handle literal escapes too.
    vcf = vcf.replace("\\r\\n", "\r\n")
    expected = hmac.new(token.encode(), vcf.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, payload["hash"]):
        return None
    info = payload.get("max_info") or {}
    if "user_id" in info and str(info["user_id"]) != str(user_id):
        return None
    vcf = re.sub(r"\r?\n[ \t]", "", vcf)
    phones = {normalize_phone(m) for m in re.findall(r"^TEL[^:]*:(?:tel:)?([^\r\n]+)", vcf, re.M | re.I)}
    phones.discard(None)
    return next(iter(phones)) if len(phones) == 1 else None


def due_month(subscribed_at: float, now: float, interval: int, tz: str) -> int | None:
    anchor = datetime.fromtimestamp(subscribed_at, ZoneInfo(tz))
    current = datetime.fromtimestamp(now, ZoneInfo(tz))
    months = (current.year - anchor.year) * 12 + current.month - anchor.month
    if current < anchor + relativedelta(months=months):
        months -= 1
    period = months - months % interval
    return period if period >= interval else None


def subscription_date(stamp: float, tz: str) -> str:
    return datetime.fromtimestamp(stamp, timezone.utc).astimezone(ZoneInfo(tz)).strftime("%d.%m.%Y %H:%M %Z")


def message_parts(payload):
    media = payload.get("media") or []
    text = payload.get("text", "")
    if not media:
        return [{"text": text, "media": None}]
    parts = []
    first_caption = media[0].get("caption", "")
    if text and first_caption and text != first_caption:
        parts.append({"text": text, "media": None})
    for index, part in enumerate(media):
        caption = part.get("caption") or (text if index == 0 else "")
        parts.append({"text": caption, "media": part["id"]})
    return parts

