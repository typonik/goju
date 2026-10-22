"""No Telegram requests: synthetic IDs and mocked sends only."""
import asyncio
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from test_delivery import subject, OK


class AudienceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = patch.dict(os.environ, {'BOT_TOKEN': 'unit-test-token'})
        self.env.start()
        self.temp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.temp.name)
        self.bot = subject.SimpleBot()
        self.bot.send_message = AsyncMock(return_value=copy.deepcopy(OK))
        self.bot.edit_message = AsyncMock(return_value=copy.deepcopy(OK))
        self.bot.send_broadcast_item = AsyncMock(return_value=copy.deepcopy(OK))
        self.bot.edit_message_reply_markup = AsyncMock(return_value=copy.deepcopy(OK))

    async def asyncTearDown(self):
        for task in getattr(self.bot, 'background_tasks', set()):
            task.cancel()
        if self.bot.session:
            await self.bot.close_session()
        os.chdir(self.cwd)
        self.temp.cleanup()
        self.env.stop()

    def data(self, **kwargs):
        return dict({'items': [{'snapshot': {'method': 'sendMessage', 'payload': {
            'text': 'Post', 'entities': []}}}], 'buttons': []}, **kwargs)

    def seed(self):
        self.bot.register_private(1)
        self.bot.subscribers.update((2, 3, 4, 5, 6, 7))
        self.bot.recipient(3, blocked=True)
        self.bot.recipient(4, opted_out=True)
        self.bot.recipient(5, delivery_status='needs_start')
        self.bot.recipient(6, delivery_status='deactivated')
        self.bot.recipient(7, delivery_status='unavailable')

    async def test_unknown_is_visible_but_default_off(self):
        self.seed()
        c = self.bot.audience_counts()
        self.assertEqual(c, {'total': 7, 'confirmed': 1, 'unknown': 1, 'blocked': 1,
                             'opted_out': 1, 'needs_start': 1, 'deactivated': 1, 'unavailable': 1})
        self.assertEqual(self.bot.broadcast_targets(), [1])
        self.assertEqual(self.bot.broadcast_targets(include_unknown=True), [1, 2])

    async def test_default_broadcast_reports_unknown_not_telegram_failures(self):
        self.seed()
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, self.data())
        self.assertEqual(self.bot.send_broadcast_item.await_count, 1)
        text = self.bot.send_message.call_args.args[1]
        self.assertIn('Включено в эту отправку: 1', text)
        self.assertIn('Не проверяли старые диалоги: 1', text)
        self.assertIn('блокировки: 1, отписки: 1', text)

    async def test_explicit_check_includes_only_unknown_and_confirmed(self):
        self.seed()
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, self.data(include_unknown=True))
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [1, 2])
        self.assertEqual(self.bot.recipient_category(2), 'confirmed')
        self.assertNotIn(2, self.bot.started_users)
        self.assertNotIn('can_write', self.bot.recipient_state['2'])

    async def test_cannot_initiate_is_needs_start_not_blocked(self):
        self.bot.subscribers.add(2)
        self.bot.send_broadcast_item.return_value = {'ok': False, 'error_code': 403,
            'description': "Forbidden: bot can't initiate conversation with a user"}
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, self.data(include_unknown=True))
        self.assertEqual(self.bot.recipient_category(2), 'needs_start')
        self.assertFalse(self.bot.recipient_state['2'].get('blocked', False))
        self.assertEqual(self.bot.broadcast_targets(include_unknown=True), [])
        self.assertIn('Telegram требует запуск бота: 1', self.bot.send_message.call_args.args[1])

    async def test_chat_not_found_is_not_blocked(self):
        self.bot.remember_delivery(2, {'ok': False, 'error_code': 400, 'description': 'Bad Request: chat not found'})
        self.assertEqual(self.bot.recipient_category(2), 'needs_start')
        self.assertFalse(self.bot.recipient_state['2'].get('blocked', False))

    async def test_explicit_block_stores_reason_and_source(self):
        result = {'ok': False, 'error_code': 403, 'description': 'Forbidden: bot was blocked by the user'}
        self.bot.remember_delivery(2, result)
        self.assertEqual(self.bot.recipient_category(2), 'blocked')
        self.assertEqual(self.bot.recipient_state['2']['block_reason'], result['description'])
        self.assertEqual(self.bot.recipient_state['2']['block_source'], 'telegram_api')

    async def test_generic_403_is_unavailable_not_blocked(self):
        self.bot.remember_delivery(2, {'ok': False, 'error_code': 403, 'description': 'Forbidden: another access failure'})
        self.assertEqual(self.bot.recipient_category(2), 'unavailable')
        self.assertFalse(self.bot.recipient_state['2'].get('blocked', False))

    async def test_deactivated_is_not_user_block(self):
        self.bot.remember_delivery(2, {'ok': False, 'error_code': 403, 'description': 'Forbidden: user is deactivated'})
        self.assertEqual(self.bot.recipient_category(2), 'deactivated')

    async def test_content_error_does_not_change_recipient_permission(self):
        self.bot.remember_delivery(2, {'ok': False, 'error_code': 400, 'description': "Bad Request: can't parse entities"})
        self.assertEqual(self.bot.recipient_category(2), 'unknown')

    async def test_timeout_does_not_mark_blocked(self):
        self.bot.remember_delivery(2, {'ok': False, 'error_code': 0, 'description': 'Timeout', 'delivery_unknown': True})
        self.assertEqual(self.bot.recipient_category(2), 'unknown')

    async def test_private_message_restores_access_without_undoing_optout(self):
        self.bot.recipient(2, blocked=True, opted_out=True, delivery_status='needs_start')
        self.bot.register_private(2)
        self.assertEqual(self.bot.recipient_category(2), 'opted_out')
        self.bot.register_private(2, restart=True)
        self.assertEqual(self.bot.recipient_category(2), 'confirmed')

    async def test_legacy_block_is_preserved(self):
        self.bot.subscribers.add(2)
        self.bot.recipient(2, blocked=True)
        self.assertNotIn(2, self.bot.broadcast_targets(include_unknown=True))
        self.assertIn('без сохранённой причины: <b>1</b>', self.bot.audience_text())

    async def test_started_migration_repairs_missing_flag_without_clearing_blocks(self):
        Path('started_users.json').write_text('[1, 2]')
        Path('recipient_state.json').write_text(json.dumps({'1': {'welcome_enrolled': True}, '2': {'blocked': True}}))
        other = subject.SimpleBot()
        self.assertIn(1, other.broadcast_targets())
        self.assertNotIn(2, other.broadcast_targets(include_unknown=True))

    async def test_followup_excludes_original_receipts(self):
        self.bot.register_private(1)
        self.bot.subscribers.update((2, 3, 4))
        self.bot.recipient(4, blocked=True)
        root = self.bot.journal.create(self.data(), [1], 1)
        self.bot.journal.mark(root, 1, 'sent', 1, message_id=88)
        self.bot.journal.finish(root)
        draft = self.bot.latest_check_data()
        self.assertEqual(draft['target_ids'], [2, 3])
        self.assertEqual(draft['source_job_id'], root)
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, draft)
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [2, 3])
        self.assertEqual(self.bot.latest_check_data()['target_ids'], [])

    async def test_followup_excludes_ambiguous_and_reserved_receipts(self):
        self.bot.subscribers.update((1, 2, 3))
        root = self.bot.journal.create(self.data(), [1], 1)
        self.bot.journal.mark(root, 1, 'unknown')
        self.bot.journal.finish(root)
        data = self.data(source_job_id=root, include_unknown=True)
        child = self.bot.journal.create(data, [2], 1)
        self.assertEqual(self.bot.selected_targets(data), [3])
        self.assertIn(2, self.bot.journal.campaign_seen(root))
        self.assertNotEqual(root, child)

    async def test_repeated_followup_click_does_not_duplicate(self):
        self.bot.subscribers.add(2)
        root = self.bot.journal.create(self.data(), [], 1)
        self.bot.journal.finish(root)
        data = self.bot.latest_check_data()
        await asyncio.gather(self.bot.execute_broadcast_async({'chat': {'id': 1}}, data),
                             self.bot.execute_broadcast_async({'chat': {'id': 1}}, copy.deepcopy(data)))
        self.bot.send_broadcast_item.assert_awaited_once()

    async def test_followup_does_not_expand_when_new_users_arrive_after_preview(self):
        self.bot.subscribers.add(2)
        root = self.bot.journal.create(self.data(), [], 1)
        self.bot.journal.finish(root)
        data = self.bot.latest_check_data()
        self.bot.subscribers.add(3)
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, data)
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [2])

    async def test_preview_only_sends_to_admin_until_confirm(self):
        self.bot.subscribers.update((2, 3))
        root = self.bot.journal.create(self.data(), [], 1)
        self.bot.journal.finish(root)
        await self.bot.prepare_latest_broadcast_check({'chat': {'id': 1}, 'message_id': 7})
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [1])
        self.assertEqual(self.bot.broadcast_preview_data['target_ids'], [2, 3])
        self.assertEqual(self.bot.journal.pending(), [])

    async def test_toggle_changes_only_draft_no_recipient_messages(self):
        self.bot.subscribers.add(2)
        self.bot.broadcast_preview_data = self.data()
        self.assertEqual(self.bot.selected_targets(self.bot.broadcast_preview_data), [])
        await self.bot.toggle_unknown_audience({'chat': {'id': 1}, 'message_id': 7})
        self.assertEqual(self.bot.selected_targets(self.bot.broadcast_preview_data), [2])
        self.bot.send_broadcast_item.assert_not_called()

    async def test_optout_during_probe_prevents_remaining_parts(self):
        self.bot.subscribers.add(2)
        async def send(uid, item, markup):
            self.bot.recipient(uid, opted_out=True)
            return OK
        self.bot.send_broadcast_item.side_effect = send
        data = self.data(include_unknown=True)
        data['items'] *= 2
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, data)
        self.bot.send_broadcast_item.assert_awaited_once()
        self.assertEqual(self.bot.recipient_category(2), 'opted_out')

    async def test_resume_followup_does_not_exclude_its_own_reservation(self):
        self.bot.subscribers.add(2)
        self.bot.save_subscribers()
        root = self.bot.journal.create(self.data(), [], 1)
        self.bot.journal.finish(root)
        data = self.bot.latest_check_data()
        job = self.bot.journal.create(data, [2], 1)
        other = subject.SimpleBot()
        other.send_message = AsyncMock(return_value=OK)
        other.send_broadcast_item = AsyncMock(return_value=OK)
        await other.resume_broadcasts()
        other.send_broadcast_item.assert_awaited_once()
        self.assertEqual(other.journal.get(job)[3], 'complete')

    async def test_daily_does_not_probe_unknown_even_if_legacy_setting_true(self):
        self.bot.register_private(1)
        self.bot.subscribers.add(2)
        self.bot.daily_broadcast.update(enabled=True, time='00:00', items=self.data()['items'], include_unknown=True)
        await self.bot.check_daily_broadcast()
        await self.bot.resume_broadcasts()
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [1])

    async def test_previous_success_remains_eligible_after_reload(self):
        self.bot.subscribers.add(2)
        self.bot.save_subscribers()
        self.bot.remember_delivery(2, OK)
        other = subject.SimpleBot()
        self.assertIn(2, other.broadcast_targets())
        self.assertNotIn(2, other.started_users)

    async def test_check_403_pauses_until_user_start_not_marked_blocked_welcome(self):
        self.bot.register_private(2)
        self.bot.pending_welcome_jobs = [{'id': 'w1', 'user_id': 2, 'chat_id': 2,
            'due_at': 0, 'created_at': 0, 'attempts': 0,
            'welcome': {'text': 'Hi', 'entities': [], 'type': 'text', 'active': True}}]
        self.bot.send_single_welcome = AsyncMock(return_value={
            'ok': False, 'error_code': 403, 'description': "Forbidden: bot can't initiate conversation with a user"})
        await self.bot.process_pending_welcome_jobs()
        self.assertEqual(self.bot.recipient_category(2), 'needs_start')
        self.assertFalse(self.bot.recipient_state['2']['blocked'])

    async def test_membership_does_not_silently_clear_block_on_unknown_status(self):
        self.bot.subscribers.add(2)
        self.bot.recipient(2, blocked=True)
        await self.bot.handle_membership({'chat': {'id': 2, 'type': 'private'},
                                         'new_chat_member': {'status': 'left'}})
        self.assertEqual(self.bot.recipient_category(2), 'blocked')

    async def test_resume_429_probe_uses_saved_unknown_optin(self):
        self.bot.subscribers.add(2)
        self.bot.save_subscribers()
        self.bot.send_broadcast_item.return_value = {'ok': False, 'error_code': 429,
            'description': 'Too Many Requests', 'parameters': {'retry_after': 1}}
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, self.data(include_unknown=True))
        self.assertEqual(len(self.bot.journal.pending()), 1)
        other = subject.SimpleBot()
        other.send_message = AsyncMock(return_value=OK)
        other.send_broadcast_item = AsyncMock(return_value=OK)
        await other.resume_broadcasts()
        other.send_broadcast_item.assert_awaited_once()

    async def test_bad_token_does_not_poison_recipient_state(self):
        self.bot.subscribers.add(2)
        self.bot.send_broadcast_item.return_value = {'ok': False, 'error_code': 401, 'description': 'Unauthorized'}
        await self.bot.execute_broadcast_async({'chat': {'id': 1}}, self.data(include_unknown=True))
        self.assertEqual(self.bot.recipient_category(2), 'unknown')
        self.assertEqual(len(self.bot.journal.pending()), 1)

    async def test_full_admin_followup_requires_confirmation_and_skips_original(self):
        admin = self.bot.admin_ids[0]
        self.bot.subscribers.update((1, 2, 3))
        root = self.bot.journal.create(self.data(), [1], admin)
        self.bot.journal.mark(root, 1, 'sent', 1, message_id=88)
        self.bot.journal.finish(root)
        self.bot.answer_callback_query = AsyncMock(return_value=OK)
        message = {'chat': {'id': admin, 'type': 'private'}, 'message_id': 7}
        await self.bot.handle_callback_query({'id': 'prepare', 'from': {'id': admin},
            'data': 'broadcast_reconcile', 'message': message})
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [admin])
        self.assertEqual(self.bot.journal.pending(), [])
        await self.bot.handle_callback_query({'id': 'confirm', 'from': {'id': admin},
            'data': 'confirm_broadcast', 'message': message})
        await asyncio.gather(*list(self.bot.background_tasks))
        self.assertEqual([c.args[0] for c in self.bot.send_broadcast_item.await_args_list], [admin, 2, 3])
        self.assertIsNone(self.bot.broadcast_preview_data)

    async def test_failed_preview_invalidates_confirmation(self):
        self.bot.subscribers.add(2)
        root = self.bot.journal.create(self.data(), [], 1)
        self.bot.journal.finish(root)
        self.bot.send_broadcast_item.return_value = {'ok': False, 'error_code': 400,
                                                    'description': 'Invalid media'}
        await self.bot.prepare_latest_broadcast_check({'chat': {'id': 1}, 'message_id': 7})
        self.assertIsNone(self.bot.broadcast_preview_data)
        self.assertEqual(self.bot.journal.pending(), [])
