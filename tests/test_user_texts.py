import json
import re

import pytest

from sinai.domain import (WELCOME, WELCOME_REGISTERED, SUBSCRIBED_TEXT, CONTACT_TEXT,
                          ACTIVE_TEXT, COMPLETED_TEXT, STATUS_TEXT, STOP_TEXT)
from sinai.ui import UI, validate_html
from test_subscription_fixes import env


@pytest.mark.parametrize('text', [WELCOME, WELCOME_REGISTERED, SUBSCRIBED_TEXT, CONTACT_TEXT,
                                 ACTIVE_TEXT, COMPLETED_TEXT, STATUS_TEXT, STOP_TEXT])
def test_templates_use_balanced_shared_html_and_preserve_paragraphs(text):
    validate_html(text)
    assert set(re.findall(r'<[^>]+>', text)) == {'<b>', '</b>'}
    assert '\n\n' in text
    assert text == text.strip() and '\r' not in text
    assert len(text) < 3900


def test_welcome_registered_omits_only_status_paragraph():
    assert WELCOME == WELCOME_REGISTERED + '\n\nЧтобы получать актуальные сообщения, выберите свой статус:'
    assert UI.group_buttons() == [[('Я действующий клиент', 'group:active')],
                                  [('Моя процедура уже завершена', 'group:completed')]]


def test_bonus_amounts_and_line_breaks_match_requested_markup():
    assert '🎁 <b>Бонусная программа «Синай»</b>\n\n' in ACTIVE_TEXT
    assert '<b>Что получаете вы:</b>\n<b>8 000 ₽</b> — скидка на оплату наших услуг.\n' in ACTIVE_TEXT
    assert '<b>Что получает ваш знакомый:</b>\n<b>5 000 ₽</b> — скидка на услуги компании.' in ACTIVE_TEXT
    assert COMPLETED_TEXT.startswith('🎁 <b>Получите 8 000 ₽ за рекомендацию</b>\n\n')
    assert '✓ Вы получите <b>8 000 ₽</b> на карту.\n✓ Ваш знакомый получит <b>скидку 5 000 ₽</b>.' in COMPLETED_TEXT


def command(platform, text):
    if platform == 'max':
        return {'update_type': 'message_created', 'message': {
            'sender': {'user_id': 333, 'name': 'Test'},
            'recipient': {'chat_id': 333, 'chat_type': 'dialog'}, 'body': {'text': text}}}
    return {'update_id': 1, 'message': {'from': {'id': 333, 'first_name': 'Test'},
                                      'chat': {'id': 333, 'type': 'private'}, 'text': text}}


@pytest.mark.parametrize('platform', ['tg', 'max'])
async def test_status_stop_resume_use_exact_templates_and_keep_behavior(env, platform):
    cfg, s = env
    ui = UI(cfg, s, None)
    u = s.touch_user(platform, 333, 333, 'Test')
    s.choose_group(u['id'], 'active')
    anchor = s.user(u['id'])['subscribed_at']
    for jid, (text, expected) in enumerate([('/status', STATUS_TEXT), ('/stop', STOP_TEXT),
                                          ('/status', STOP_TEXT), ('/start', WELCOME_REGISTERED)], 1):
        await ui.handle({'id': jid, 'payload': {'platform': platform, 'raw': command(platform, text)}})
        reply = json.loads(s.one('SELECT payload FROM jobs WHERE key=?', (f'reply:{jid}:1',))['payload'])
        assert reply['text'] == expected
        if text == '/status':
            assert reply['buttons'] == [[['Приостановить рассылки', 'stop']]]
        if text == '/start':
            assert reply['buttons'] is None
    assert s.user(u['id'])['active'] == 1
    assert s.user(u['id'])['subscribed_at'] == anchor


@pytest.mark.parametrize('with_deep_link,expected,contact', [(False, CONTACT_TEXT, True),
                                                         (True, SUBSCRIBED_TEXT, False)])
async def test_subscription_and_contact_prompts_are_exact(env, with_deep_link, expected, contact):
    cfg, s = env
    ui = UI(cfg, s, None)
    u = s.touch_user('tg', 333, 333, 'Test')
    if with_deep_link:
        s.start(u['id'], 32426619)
    raw = {'callback_query': {'id': 'test', 'data': 'group:active', 'from': {'id': 333, 'first_name': 'Test'},
                             'message': {'chat': {'id': 333, 'type': 'private'}}}}
    await ui.handle({'id': 1, 'payload': {'platform': 'tg', 'raw': raw}})
    reply = json.loads(s.one("SELECT payload FROM jobs WHERE key='reply:1:1'")['payload'])
    assert reply['text'] == expected and reply['contact'] is contact
    assert s.user(u['id'])['selected_group'] == 'active'
