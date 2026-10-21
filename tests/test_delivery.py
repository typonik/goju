import asyncio
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ.get('BOT_SOURCE', str(ROOT / 'simple_working_bot.py')))
spec = importlib.util.spec_from_file_location('subject_bot', SOURCE)
subject = importlib.util.module_from_spec(spec)
spec.loader.exec_module(subject)
OK = {'ok': True, 'result': {'message_id': 7}}


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = patch.dict(os.environ, {'BOT_TOKEN': 'unit-test-token'})
        self.env.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        self.bot = subject.SimpleBot()
        self.bot.send_message = AsyncMock(return_value=copy.deepcopy(OK))
        self.bot.edit_message = AsyncMock(return_value=copy.deepcopy(OK))
        self.bot.process_pending_welcome_jobs = AsyncMock()

    async def asyncTearDown(self):
        if self.bot.broadcast_collect_timer:
            self.bot.broadcast_collect_timer.cancel()
        if self.bot.session:
            await self.bot.close_session()
        os.chdir(self.cwd)
        self.tmp.cleanup()
        self.env.stop()

    def incoming(self, text='/start'):
        return {'chat': {'id': 42, 'type': 'private'}, 'from': {'id': 42},
                'message_id': 1, 'text': text}

    def welcome(self, delay=0, active=True):
        return {'id': 'w1', 'text': 'Hello', 'entities': [], 'type': 'text',
                'delay': delay, 'active': active, 'url_buttons': []}

    async def test_start_enrolls_broadcast_recipient(self):
        await self.bot.handle_message(self.incoming())
        self.assertIn(42, self.bot.subscribers)
        self.assertIn(42, self.bot.started_users)

    async def test_any_private_message_enrolls_recipient(self):
        await self.bot.handle_message(self.incoming('hello'))
        self.assertIn(42, self.bot.subscribers)

    async def test_start_delivers_short_welcome_for_direct_user(self):
        self.bot.welcome_messages = [self.welcome()]
        await self.bot.handle_message(self.incoming())
        self.assertEqual(len(self.bot.pending_welcome_jobs), 1)

    async def test_inactive_welcome_never_queued(self):
        self.bot.welcome_messages = [self.welcome(active=False)]
        self.bot.started_users.add(42)
        await self.bot.send_welcome_to_user({'id': 42})
        self.assertEqual(self.bot.pending_welcome_jobs, [])

    async def test_short_and_weekly_welcome_survive_reload(self):
        self.bot.welcome_messages = [self.welcome(7 * 86400)]
        await self.bot.handle_message(self.incoming())
        before = copy.deepcopy(self.bot.pending_welcome_jobs)
        self.assertEqual(len(before), 1)
        other = subject.SimpleBot()
        self.assertIn(42, other.started_users)
        self.assertEqual(other.pending_welcome_jobs, before)

    async def test_broadcast_markup_preserves_button_style(self):
        self.bot.broadcast_buttons = [{'text': 'Go', 'url': 'https://example.com',
                                       'style': 'success', 'icon_custom_emoji_id': '12345'}]
        self.assertEqual(self.bot.broadcast_markup()['inline_keyboard'][0][0],
                         self.bot.broadcast_buttons[0])

    async def test_welcome_style_and_entities_reach_wire(self):
        self.bot.make_request_with_retry = AsyncMock(return_value=OK)
        w = self.welcome()
        w['text'] = '😀🌟'
        w['entities'] = [{'type': 'custom_emoji', 'offset': 2, 'length': 2,
                          'custom_emoji_id': '12345'}]
        w['url_buttons'] = [{'text': 'Go', 'url': 'https://example.com', 'style': 'primary'}]
        await self.bot.send_single_welcome({'id': 42}, w)
        data = self.bot.make_request_with_retry.call_args.args[2]
        self.assertEqual(json.loads(data['entities']), w['entities'])
        self.assertEqual(json.loads(data['reply_markup'])['inline_keyboard'][0][0]['style'], 'primary')

    async def test_no_ambiguous_welcome_retry(self):
        self.bot.process_pending_welcome_jobs = subject.SimpleBot.process_pending_welcome_jobs.__get__(self.bot)
        self.bot.started_users.add(42)
        self.bot.send_single_welcome = AsyncMock(return_value={
            'ok': False, 'error_code': 0, 'description': 'Timeout', 'delivery_unknown': True})
        self.bot.pending_welcome_jobs = [{'id': 'j1', 'user_id': 42, 'chat_id': 99,
            'due_at': 0, 'created_at': time.time(), 'welcome': self.welcome(), 'attempts': 0}]
        await self.bot.process_pending_welcome_jobs()
        self.assertEqual(self.bot.send_single_welcome.await_count, 1)
        self.assertEqual(self.bot.pending_welcome_jobs[0]['status'], 'unknown')

    async def test_stop_excludes_and_start_reenables(self):
        await self.bot.handle_message(self.incoming())
        await self.bot.handle_message(self.incoming('/stop'))
        self.assertNotIn(42, self.bot.broadcast_targets())
        await self.bot.handle_message(self.incoming())
        self.assertIn(42, self.bot.broadcast_targets())

    async def test_group_start_is_not_private_permission(self):
        m = self.incoming()
        m['chat'] = {'id': -10042, 'type': 'supergroup'}
        await self.bot.handle_message(m)
        self.assertNotIn(42, self.bot.started_users)

    async def test_token_env_takes_precedence(self):
        with patch.dict(os.environ, {'BOT_TOKEN': '  test-token  '}):
            other = subject.SimpleBot()
        self.assertEqual(other.bot_token, 'test-token')

    async def test_weekly_welcome_sent_once_after_seven_days(self):
        self.bot.welcome_messages = [self.welcome(7 * 86400)]
        await self.bot.handle_message(self.incoming())
        due = self.bot.pending_welcome_jobs[0]['due_at']
        other = subject.SimpleBot()
        other.send_single_welcome = AsyncMock(return_value=OK)
        with patch.object(subject.time, 'time', return_value=due + 1):
            await other.process_pending_welcome_jobs()
            await other.process_pending_welcome_jobs()
        other.send_single_welcome.assert_awaited_once()
        self.assertEqual(other.pending_welcome_jobs, [])
        third = subject.SimpleBot()
        self.assertEqual(third.pending_welcome_jobs, [])

    async def test_join_only_weekly_waits_for_start_and_keeps_due_date(self):
        self.bot.welcome_messages = [self.welcome(7 * 86400)]
        self.bot.send_message = AsyncMock(return_value=OK)
        await self.bot.send_welcome_to_user({'id': 42, 'join_chat_id': 99})
        self.assertEqual(self.bot.pending_welcome_jobs[0]['status'], 'waiting_start')
        due = self.bot.pending_welcome_jobs[0]['due_at']
        await self.bot.handle_message(self.incoming())
        self.assertEqual(len(self.bot.pending_welcome_jobs), 1)
        self.assertEqual(self.bot.pending_welcome_jobs[0]['status'], 'pending')
        self.assertEqual(self.bot.pending_welcome_jobs[0]['due_at'], due)
        self.assertEqual(self.bot.pending_welcome_jobs[0]['chat_id'], 42)

    async def test_repeated_start_does_not_duplicate_chain(self):
        self.bot.welcome_messages = [self.welcome()]
        await self.bot.handle_message(self.incoming())
        await self.bot.handle_message(self.incoming())
        self.assertEqual(len(self.bot.pending_welcome_jobs), 1)

    async def test_membership_is_not_broadcast_permission(self):
        self.bot.add_subscriber(99)
        self.assertNotIn(99, self.bot.broadcast_targets())

    async def test_write_access_service_enrolls(self):
        m = self.incoming()
        m.pop('text')
        m['write_access_allowed'] = {'from_request': True}
        await self.bot.handle_message(m)
        self.assertIn(42, self.bot.broadcast_targets())

    async def test_block_and_private_message_restore(self):
        await self.bot.handle_message(self.incoming())
        await self.bot.handle_membership({'chat': {'id': 42, 'type': 'private'},
                                         'new_chat_member': {'status': 'kicked'}})
        self.assertNotIn(42, self.bot.broadcast_targets())
        await self.bot.handle_message(self.incoming('hello'))
        self.assertIn(42, self.bot.broadcast_targets())

    async def test_historical_starts_migrate_but_members_do_not(self):
        Path('started_users.json').write_text('[42]')
        Path('subscribers.json').write_text('[99]')
        other = subject.SimpleBot()
        self.assertIn(42, other.broadcast_targets())
        self.assertNotIn(99, other.broadcast_targets())

    async def test_interrupted_welcome_is_unknown_on_restart(self):
        from delivery_support import atomic_json
        job = {'id': 'j', 'status': 'sending', 'user_id': 42, 'due_at': 0,
               'welcome': self.welcome(), 'schedule_v': 2}
        atomic_json('pending_welcomes.json', [job])
        other = subject.SimpleBot()
        self.assertEqual(other.pending_welcome_jobs[0]['status'], 'unknown')

    async def test_legacy_overdue_welcome_not_discarded(self):
        from delivery_support import atomic_json
        job = {'id': 'j', 'user_id': 42, 'due_at': 1, 'created_at': 1,
               'welcome': self.welcome(), 'status': 'pending'}
        atomic_json('pending_welcomes.json', [job])
        other = subject.SimpleBot()
        self.assertEqual(len(other.pending_welcome_jobs), 1)

    async def test_opt_out_persists_after_reload(self):
        await self.bot.handle_message(self.incoming())
        await self.bot.handle_message(self.incoming('/stop'))
        other = subject.SimpleBot()
        self.assertNotIn(42, other.broadcast_targets())

    async def test_failed_approval_is_retained(self):
        req = {'user_id': 42}
        self.bot.pending_requests = [req]
        self.bot.user_requests = {42: req}
        self.bot.approve_chat_join_request = AsyncMock(return_value={'ok': False, 'error_code': 403})
        await self.bot.delayed_auto_accept(42, 0)
        self.assertIn(42, self.bot.user_requests)
        self.assertNotIn(42, self.bot.subscribers)

    async def test_successful_approval_survives_restart(self):
        req = {'user_id': 42}
        self.bot.pending_requests = [req]
        self.bot.user_requests = {42: req}
        self.bot.approve_chat_join_request = AsyncMock(return_value={'ok': True})
        self.assertEqual(await self.bot.approve_requests([req]), 1)
        other = subject.SimpleBot()
        self.assertEqual(other.pending_requests, [])
        self.assertIn(42, other.subscribers)
        self.assertNotIn(42, other.broadcast_targets())

    async def test_button_parser_utf16_and_colors(self):
        from delivery_support import parse_buttons
        text = '😀 Go | https://example.com | зелёный\n⭐ Next | https://example.org | красный'
        offset = len(text.splitlines(keepends=True)[0].encode('utf-16-le')) // 2
        es = [{'type': 'custom_emoji', 'offset': offset, 'length': 1, 'custom_emoji_id': '12345'}]
        rows = parse_buttons(text, es)
        self.assertEqual(rows[0]['style'], 'success')
        self.assertEqual(rows[1]['style'], 'danger')
        self.assertEqual(rows[1]['icon_custom_emoji_id'], '12345')

    async def test_invalid_buttons_are_rejected_atomically(self):
        from delivery_support import parse_buttons
        for text in ('X | javascript:alert(1)', 'X | https://example.com | yellow',
                     'X | https://example.com | синий | invalid'):
            with self.assertRaises(ValueError):
                parse_buttons(text)

    async def test_two_column_buttons_remain_compatible(self):
        from delivery_support import parse_buttons
        self.assertEqual(parse_buttons('Go | https://example.com'),
                         [{'text': 'Go', 'url': 'https://example.com'}])

    async def test_snapshot_preserves_text_and_caption_entities(self):
        from delivery_support import snapshot_message
        es = [{'type': 'custom_emoji', 'offset': 2, 'length': 2, 'custom_emoji_id': '12345'}]
        for kind in ('photo', 'video', 'document', 'animation', 'audio', 'voice'):
            media = {'file_id': 'saved-file'}
            m = {kind: [media] if kind == 'photo' else media, 'caption': '😀🌟', 'caption_entities': es}
            item = snapshot_message(m)
            self.assertEqual(item['payload']['caption_entities'], es)
            self.assertEqual(item['payload'][kind], 'saved-file')

    async def test_no_recipients_does_not_fallback_to_admins(self):
        self.bot.send_broadcast_item = AsyncMock(return_value=OK)
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, {'items': [{'message_id': 1}], 'buttons': []})
        self.bot.send_broadcast_item.assert_not_called()

    async def test_broadcast_resume_keeps_confirmed_progress(self):
        self.bot.register_private(42)
        data = {'items': [{'message_id': 1}, {'message_id': 2}], 'buttons': []}
        job = self.bot.journal.create(data, [42], 1)
        self.bot.journal.mark(job, 42, 'pending', 1, message_id=11)
        other = subject.SimpleBot()
        other.send_message = AsyncMock(return_value=OK)
        other.send_broadcast_item = AsyncMock(return_value=OK)
        await other.resume_broadcasts()
        other.send_broadcast_item.assert_awaited_once_with(42, data['items'][1], None)
        self.assertEqual(other.journal.pending(), [])

    async def test_interrupted_broadcast_not_blindly_retried(self):
        self.bot.register_private(42)
        self.bot.register_private(43)
        data = {'items': [{'message_id': 1}], 'buttons': []}
        job = self.bot.journal.create(data, [42, 43], 1)
        self.bot.journal.mark(job, 42, 'sending')
        other = subject.SimpleBot()
        other.send_message = AsyncMock(return_value=OK)
        other.send_broadcast_item = AsyncMock(return_value=OK)
        await other.resume_broadcasts()
        other.send_broadcast_item.assert_awaited_once_with(43, data['items'][0], None)
        self.assertIn('Результат неизвестен: 1', other.send_message.call_args.args[1])

    async def test_broadcast_403_marks_blocked_without_deleting_history(self):
        self.bot.register_private(42)
        self.bot.send_broadcast_item = AsyncMock(return_value={
            'ok': False, 'error_code': 403, 'description': 'Forbidden: bot was blocked by the user'})
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, {'items': [{}]})
        self.assertNotIn(42, self.bot.broadcast_targets())
        self.assertIn(42, self.bot.subscribers)

    async def test_broadcast_500_is_unknown_not_requeued(self):
        self.bot.register_private(42)
        self.bot.send_broadcast_item = AsyncMock(return_value={
            'ok': False, 'error_code': 500, 'description': 'Internal Server Error', 'delivery_unknown': True})
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, {'items': [{}]})
        self.assertEqual(self.bot.journal.pending(), [])
        self.assertIn('Результат неизвестен: 1', self.bot.send_message.call_args.args[1])

    async def test_daily_broadcast_catches_up_and_deduplicates(self):
        self.bot.daily_broadcast.update(enabled=True, time='00:00', items=[{'message_id': 1}])
        await self.bot.check_daily_broadcast()
        await self.bot.check_daily_broadcast()
        self.assertEqual(len(self.bot.journal.pending()), 1)
        other = subject.SimpleBot()
        await other.check_daily_broadcast()
        self.assertEqual(len(other.journal.pending()), 1)

    async def test_bad_token_fails_before_polling(self):
        self.assertTrue(hasattr(self.bot, '_run_connected'), 'Missing authentication preflight')
        self.bot.initialize_session = AsyncMock()
        self.bot.close_session = AsyncMock()
        self.bot.make_request_with_retry = AsyncMock(return_value={'ok': False, 'error_code': 401})
        self.bot.get_updates = AsyncMock()
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(self.bot.run(), timeout=0.2)
        self.bot.get_updates.assert_not_called()
        self.assertFalse(self.bot.is_running)

    async def test_presentation_preserves_source_and_downgrades_only_emoji(self):
        es = [{'type': 'bold', 'offset': 0, 'length': 2},
              {'type': 'custom_emoji', 'offset': 0, 'length': 2, 'custom_emoji_id': '12345'}]
        payload = {'text': '🌟', 'entities': es, 'reply_markup': {'inline_keyboard': [[
            {'text': 'Go', 'url': 'https://example.com', 'style': 'success', 'icon_custom_emoji_id': '12345'}]]}}
        original = copy.deepcopy(payload)
        self.bot.presentation_error({'error_code': 400, 'description': 'CUSTOM_EMOJI_INVALID'})
        sent = self.bot.presentation(payload)
        self.assertEqual(payload, original)
        self.assertEqual(sent['entities'], es[:1])
        self.assertEqual(sent['reply_markup']['inline_keyboard'][0][0]['style'], 'success')
        self.assertNotIn('icon_custom_emoji_id', sent['reply_markup']['inline_keyboard'][0][0])

    async def test_single_process_lock(self):
        from runtime_guard import ProcessLease
        with ProcessLease('bot.lock'):
            with self.assertRaises(RuntimeError):
                with ProcessLease('bot.lock'):
                    self.fail('second process acquired the lock')
        with ProcessLease('bot.lock'):
            pass

    async def test_full_admin_welcome_editor_roundtrip(self):
        admin = self.bot.admin_ids[0]
        m = {'chat': {'id': admin, 'type': 'private'}, 'from': {'id': admin},
             'message_id': 10, 'text': '😀🌟', 'entities': [
                 {'type': 'custom_emoji', 'offset': 2, 'length': 2, 'custom_emoji_id': '12345'}]}
        self.bot.waiting_for_welcome_text = True
        await self.bot.handle_message(m)
        self.assertEqual(self.bot.welcome_messages[0]['entities'], m['entities'])
        self.bot.current_editing_id = 0
        self.bot.waiting_for_buttons = True
        m['text'] = '🌟 Открыть | https://example.com | зелёный'
        m['entities'] = [{'type': 'custom_emoji', 'offset': 0, 'length': 2, 'custom_emoji_id': '12345'}]
        await self.bot.handle_message(m)
        other = subject.SimpleBot()
        b = other.welcome_messages[0]['url_buttons'][0]
        self.assertEqual(b['style'], 'success')
        self.assertEqual(b['icon_custom_emoji_id'], '12345')
        self.assertEqual(other.welcome_messages[0]['entities'][0]['offset'], 2)

    async def test_full_broadcast_button_wizard(self):
        admin = self.bot.admin_ids[0]
        self.bot.broadcast_preview_data = {'items': [{'message_id': 1}], 'buttons': []}
        self.bot.broadcast_waiting_for_button_title = True
        m = {'chat': {'id': admin, 'type': 'private'}, 'from': {'id': admin},
             'message_id': 10, 'text': ' 🌟 Go ', 'entities': [
                 {'type': 'custom_emoji', 'offset': 1, 'length': 2, 'custom_emoji_id': '12345'}]}
        await self.bot.handle_message(m)
        m['text'] = 'https://example.com | синий'
        m['entities'] = []
        await self.bot.handle_message(m)
        self.assertEqual(self.bot.broadcast_buttons[0]['style'], 'primary')
        self.assertEqual(self.bot.broadcast_buttons[0]['icon_custom_emoji_id'], '12345')

    async def test_delay_accepts_russian_words_and_week(self):
        self.assertEqual(self.bot.parse_custom_delay('7 дней'), 604800)
        self.assertEqual(self.bot.parse_custom_delay('1 неделя'), 604800)
        self.assertEqual(self.bot.parse_custom_delay('1д 2ч 3м'), 93780)
        self.assertIsNone(self.bot.parse_custom_delay('1д garbage'))

    async def test_invitation_precedes_instant_approval_when_first_welcome_is_delayed(self):
        self.bot.welcome_messages = [self.welcome(15)]
        await self.bot.send_welcome_to_user({'id': 42, 'join_chat_id': 99})
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.bot.send_message.call_args.args[0], 99)
        self.assertIn('start=', self.bot.send_message.call_args.args[2]['inline_keyboard'][0][0]['url'])

    async def test_atomic_persistence_failure_prevents_send(self):
        self.bot.process_pending_welcome_jobs = subject.SimpleBot.process_pending_welcome_jobs.__get__(self.bot)
        self.bot.register_private(42)
        self.bot.send_single_welcome = AsyncMock(return_value=OK)
        self.bot.pending_welcome_jobs = [{'id': 'j', 'user_id': 42, 'chat_id': 42,
            'due_at': 0, 'created_at': time.time(), 'welcome': self.welcome(), 'attempts': 0}]
        with patch.object(subject, 'atomic_json', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                await self.bot.process_pending_welcome_jobs()
        self.bot.send_single_welcome.assert_not_called()

    async def test_accidental_admin_photo_does_not_create_welcome(self):
        admin = self.bot.admin_ids[0]
        m = {'chat': {'id': admin, 'type': 'private'}, 'from': {'id': admin},
             'message_id': 10, 'photo': [{'file_id': 'photo'}]}
        await self.bot.handle_message(m)
        self.assertEqual(self.bot.welcome_messages, [])

    async def test_other_admin_cannot_overwrite_active_draft(self):
        self.bot.editor_user_id = self.bot.admin_ids[0]
        self.bot.waiting_for_welcome_text = True
        admin = self.bot.admin_ids[1]
        m = {'chat': {'id': admin, 'type': 'private'}, 'from': {'id': admin},
             'message_id': 10, 'text': 'wrong draft'}
        await self.bot.handle_message(m)
        self.assertEqual(self.bot.welcome_messages, [])

    async def test_group_admin_callback_cannot_send_broadcast(self):
        admin = self.bot.admin_ids[0]
        self.bot.answer_callback_query = AsyncMock(return_value=OK)
        self.bot.dispatch_callback = AsyncMock()
        await self.bot.handle_callback_query({'id': 'cb', 'from': {'id': admin},
            'data': 'confirm_broadcast', 'message': {'chat': {'id': -10042, 'type': 'supergroup'}, 'message_id': 1}})
        self.bot.dispatch_callback.assert_not_called()

    async def test_manual_accept_count_uses_confirmed_results(self):
        admin = self.bot.admin_ids[0]
        m = {'chat': {'id': admin, 'type': 'private'}, 'from': {'id': admin},
             'message_id': 10, 'text': '2'}
        self.bot.pending_requests = [{'user_id': 42}, {'user_id': 43}, {'user_id': 44}]
        self.bot.user_requests = {r['user_id']: r for r in self.bot.pending_requests}
        self.bot.approve_chat_join_request = AsyncMock(side_effect=[{'ok': True}, {'ok': False, 'error_code': 400}])
        await self.bot.start_manual_accept_count(m)
        await self.bot.handle_message(m)
        self.assertNotIn(42, self.bot.user_requests)
        self.assertIn(43, self.bot.user_requests)
        self.assertIn(44, self.bot.user_requests)


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertTrue(hasattr(subject.SimpleBot, 'presentation'), 'Missing bounded HTTP transport')
        self.env = patch.dict(os.environ, {'BOT_TOKEN': 'unit-test-token'})
        self.env.start()
        from aiohttp import web
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        self.bot = subject.SimpleBot()
        self.calls = []
        self.responses = []

        async def handler(request):
            self.calls.append(await request.json())
            response = self.responses.pop(0) if self.responses else OK
            return web.json_response(response, status=response.get('error_code', 200))

        app = web.Application()
        app.router.add_post('/sendMessage', handler)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        self.bot.base_url = f'http://127.0.0.1:{port}'
        self.bot.pace.interval = 0
        self.bot.pace.per_chat = 0

    async def asyncTearDown(self):
        await self.bot.close_session()
        await self.runner.cleanup()
        os.chdir(self.cwd)
        self.tmp.cleanup()
        self.env.stop()

    async def test_http_429_retries_exact_same_message(self):
        self.responses = [{'ok': False, 'error_code': 429, 'parameters': {'retry_after': 1}}, OK]
        result = await self.bot.send_message_with_markup(42, 'Hello', entities=[])
        self.assertTrue(result['ok'])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0], self.calls[1])

    async def test_http_500_not_repeated(self):
        self.responses = [{'ok': False, 'error_code': 500, 'description': 'Internal Server Error'}]
        result = await self.bot.send_message_with_markup(42, 'Hello', entities=[])
        self.assertTrue(result['delivery_unknown'])
        self.assertEqual(len(self.calls), 1)

    async def test_http_custom_emoji_400_retries_without_icons(self):
        self.responses = [{'ok': False, 'error_code': 400, 'description': 'CUSTOM_EMOJI_INVALID'}, OK]
        markup = {'inline_keyboard': [[{'text': 'Go', 'url': 'https://example.com',
                    'style': 'success', 'icon_custom_emoji_id': '12345'}]]}
        es = [{'type': 'custom_emoji', 'offset': 0, 'length': 2, 'custom_emoji_id': '12345'}]
        result = await self.bot.send_message_with_markup(42, '🌟', markup, es)
        self.assertTrue(result['ok'])
        self.assertEqual(json.loads(self.calls[1]['entities']), [])
        button = json.loads(self.calls[1]['reply_markup'])['inline_keyboard'][0][0]
        self.assertEqual(button['style'], 'success')
        self.assertNotIn('icon_custom_emoji_id', button)

    async def test_http_permanent_400_not_retried(self):
        self.responses = [{'ok': False, 'error_code': 400, 'description': 'Bad Request: chat not found'}]
        result = await self.bot.send_message_with_markup(42, 'Hello', entities=[])
        self.assertFalse(result['ok'])
        self.assertEqual(len(self.calls), 1)


if __name__ == '__main__':
    unittest.main()
