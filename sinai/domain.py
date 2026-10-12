from datetime import datetime, timezone
import hashlib
import hmac
import re
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta
import phonenumbers

WELCOME_REGISTERED = (
    '<b>Добро пожаловать в бот компании «Синай»!</b>\n\n'
    'Здесь мы будем делиться важными новостями, полезной информацией и специальными предложениями для наших клиентов.'
)
WELCOME = WELCOME_REGISTERED + '\n\nЧтобы получать актуальные сообщения, выберите свой статус:'
SUBSCRIBED_TEXT = (
    '<b>Вы успешно подписались!</b>\n\n'
    'Теперь вы будете получать важные новости и предложения компании «Синай».\n\n'
    'Спасибо, что остаётесь с нами!'
)
CONTACT_TEXT = (
    '<b>Остался один шаг</b>\n\n'
    'Чтобы связать подписку с вашей процедурой, поделитесь своим номером телефона.\n\n'
    'Это поможет нам отправлять вам актуальную информацию.'
)
ACTIVE_TEXT = (
    '🎁 <b>Бонусная программа «Синай»</b>\n\n'
    'Знаете кого-то, кому нужна помощь с долгами? Порекомендуйте нашу компанию и получите вознаграждение.\n\n'
    '<b>Что получаете вы:</b>\n'
    '<b>8 000 ₽</b> — скидка на оплату наших услуг.\n'
    'Если вы уже полностью рассчитались, перечислим эту сумму вам на карту.\n\n'
    '<b>Что получает ваш знакомый:</b>\n'
    '<b>5 000 ₽</b> — скидка на услуги компании.\n\n'
    'Просто расскажите о нас тем, кому может понадобиться помощь.'
)
COMPLETED_TEXT = (
    '🎁 <b>Получите 8 000 ₽ за рекомендацию</b>\n\n'
    'В компании «Синай» действует бонусная программа для наших клиентов.\n\n'
    'Если вы порекомендуете нас знакомому, которому нужна помощь с долгами:\n\n'
    '✓ Вы получите <b>8 000 ₽</b> на карту.\n'
    '✓ Ваш знакомый получит <b>скидку 5 000 ₽</b>.\n\n'
    'Мы будем рады помочь вашим близким так же, как когда-то помогли вам.'
)
STATUS_TEXT = (
    '<b>Ваша подписка активна ✓</b>\n\n'
    'Вы подписаны на новости и специальные предложения компании «Синай».\n\n'
    'Все важные сообщения будут приходить прямо сюда.'
)
STOP_TEXT = (
    '<b>Рассылки приостановлены</b>\n\n'
    'Вы больше не будете получать автоматические сообщения компании «Синай».\n\n'
    'Чтобы возобновить подписку, отправьте команду /start.'
)
NOT_SUBSCRIBED_TEXT = 'Подпишитесь на новости компании «Синай»: /start'


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


def next_automatic_date(subscribed_at, now, interval, tz, last_month=0):
    anchor = datetime.fromtimestamp(subscribed_at, ZoneInfo(tz))
    period = due_month(subscribed_at, now, interval, tz) or interval
    if period <= last_month:
        period = (last_month // interval + 1) * interval
    # If the current period is still unconsumed and overdue, expose its scheduled date.
    return (anchor + relativedelta(months=period)).timestamp()


def exact_subscription_date(stamp, tz):
    return datetime.fromtimestamp(stamp, ZoneInfo(tz)).strftime('%d.%m.%Y %H:%M:%S (UTC%z)')


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

