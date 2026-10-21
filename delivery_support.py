"""Durable delivery receipts, recipient eligibility and Bot API presentation."""
import asyncio
import copy
import html
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager

log = logging.getLogger(__name__)
STYLES = {'синий': 'primary', 'зелёный': 'success', 'зеленый': 'success',
          'красный': 'danger', 'обычный': None, 'primary': 'primary',
          'success': 'success', 'danger': 'danger', 'default': None, '': None}
BUTTON_HELP = (
    'Одна кнопка на строку:\n<code>Текст | https://ссылка | цвет | ID эмодзи</code>\n'
    'Цвет и ID необязательны. Цвет: <b>синий, зелёный, красный, обычный</b>.\n'
    'Пример: <code>Открыть | https://example.com | зелёный</code>\n'
    'Premium-эмодзи можно вставить прямо в подпись кнопки — ID сохранится автоматически.'
)


def atomic_json(path, data):
    path = Path(path)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def read_json(path, default):
    if not Path(path).exists():
        return copy.deepcopy(default)
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def parse_buttons(text, entities=()):
    rows = []
    offset = 0
    for line_no, line in enumerate(text.splitlines(keepends=True), 1):
        if not line.strip():
            offset += len(line.encode('utf-16-le')) // 2
            continue
        parts = [p.strip() for p in line.split('|')]
        if not 2 <= len(parts) <= 4 or not 0 < len(parts[0]) <= 64:
            raise ValueError(f'Строка {line_no}: нужны подпись и ссылка, подпись до 64 символов.')
        if not re.fullmatch(r'(?:https?://[^\s/]+\S*|tg://\S+)', parts[1], re.I):
            raise ValueError(f'Строка {line_no}: нужна ссылка https://, http:// или tg://.')
        b = {'text': parts[0], 'url': parts[1]}
        if len(parts) >= 3:
            color = parts[2].lower()
            if color not in STYLES:
                raise ValueError(f'Строка {line_no}: неизвестный цвет.')
            if STYLES[color]:
                b['style'] = STYLES[color]
        label_end = offset + len(line.split('|')[0].encode('utf-16-le')) // 2
        icon = next((e.get('custom_emoji_id') for e in entities
                     if e.get('type') == 'custom_emoji'
                     and offset <= e.get('offset', -1) < label_end), None)
        if len(parts) == 4 and parts[3]:
            icon = parts[3]
        if icon:
            if not str(icon).isascii() or not str(icon).isdigit() or len(str(icon)) > 30:
                raise ValueError(f'Строка {line_no}: ID эмодзи должен содержать цифры.')
            b['icon_custom_emoji_id'] = str(icon)
        rows.append(b)
        offset += len(line.encode('utf-16-le')) // 2
    if not rows or len(rows) > 20:
        raise ValueError('Нужно от 1 до 20 кнопок.')
    return rows


def button_markup(buttons):
    return {'inline_keyboard': [[copy.deepcopy(b)] for b in buttons]} if buttons else None


def parse_duration(value):
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    if re.fullmatch(r'[0-9]+', value):
        return int(value)
    token = re.compile(r'([0-9]+)\s*(недель|недели|неделя|нед|н|дней|день|дня|д|часов|часа|час|ч|минуты|минута|минут|мин|м|секунды|секунда|секунд|сек|с)\s*')
    pos, total = 0, 0
    while pos < len(value):
        match = token.match(value, pos)
        if not match:
            return None
        multiplier = {'н': 604800, 'д': 86400, 'ч': 3600, 'м': 60, 'с': 1}[match[2][0]]
        total += int(match[1]) * multiplier
        pos = match.end()
    return total if pos else None


def snapshot_message(message):
    if 'text' in message:
        return {'method': 'sendMessage', 'payload': {
            'text': message['text'], 'entities': copy.deepcopy(message.get('entities', []))}}
    for kind in ('animation', 'photo', 'video', 'document', 'audio', 'voice'):
        if kind in message:
            media = message[kind][-1] if kind == 'photo' else message[kind]
            return {'method': 'send' + kind.title(), 'payload': {
                kind: media['file_id'], 'caption': message.get('caption', ''),
                'caption_entities': copy.deepcopy(message.get('caption_entities', []))}}
    return None


class Pace:
    """One global send clock, one per-chat clock, and shared flood cooldown."""
    def __init__(self, per_second=20.0, per_chat=1.05):
        self.interval = 1 / per_second
        self.per_chat = per_chat
        self.lock = asyncio.Lock()
        self.next_global = 0.0
        self.next_chat = {}
        self.cooldown = 0.0

    async def wait(self, chat_id):
        async with self.lock:
            while True:
                now = time.monotonic()
                due = max(self.next_global, self.next_chat.get(chat_id, 0), self.cooldown)
                if due <= now:
                    break
                await asyncio.sleep(due - now)
            now = time.monotonic()
            self.next_global = now + self.interval
            self.next_chat[chat_id] = now + self.per_chat
            if len(self.next_chat) > 4096:
                self.next_chat = {k: v for k, v in self.next_chat.items() if v > now}

    def flood(self, seconds):
        self.cooldown = max(self.cooldown, time.monotonic() + max(1.0, float(seconds)))


class Journal:
    """Commit a sending marker before HTTP; interrupted sends remain unknown."""
    def __init__(self, path='broadcast_delivery.sqlite3'):
        self.path = path
        with self.connect() as c:
            c.execute('CREATE TABLE IF NOT EXISTS jobs '
                      '(id TEXT PRIMARY KEY, body TEXT NOT NULL, admin INTEGER NOT NULL, '
                      'status TEXT NOT NULL, created REAL NOT NULL)')
            c.execute('CREATE TABLE IF NOT EXISTS receipts '
                      '(job TEXT, uid INTEGER, next_item INTEGER DEFAULT 0, '
                      'status TEXT DEFAULT "pending", error TEXT DEFAULT "", '
                      'messages TEXT DEFAULT "[]", PRIMARY KEY(job, uid))')
            c.execute("UPDATE receipts SET status='unknown', error='Перезапуск во время отправки' "
                      "WHERE status='sending'")

    @contextmanager
    def connect(self):
        c = sqlite3.connect(self.path, timeout=10)
        try:
            c.execute('PRAGMA synchronous=FULL')
            with c:
                yield c
        finally:
            c.close()

    def create(self, data, targets, admin, job_id=None):
        job = job_id or uuid.uuid4().hex
        with self.connect() as c:
            exists = c.execute('SELECT id FROM jobs WHERE id=?', (job,)).fetchone()
            if exists:
                return job
            c.execute('INSERT INTO jobs VALUES (?,?,?,?,?)',
                      (job, json.dumps(data, ensure_ascii=False), admin, 'pending', time.time()))
            c.executemany('INSERT INTO receipts(job,uid) VALUES (?,?)', ((job, u) for u in targets))
        return job

    def pending(self):
        with self.connect() as c:
            return [(j, json.loads(b), a) for j, b, a in c.execute(
                "SELECT id,body,admin FROM jobs WHERE status='pending' ORDER BY created")]

    def targets(self, job):
        with self.connect() as c:
            return list(c.execute("SELECT uid,next_item FROM receipts WHERE job=? AND status='pending' ORDER BY uid", (job,)))

    def mark(self, job, uid, status, next_item=None, error='', message_id=None):
        with self.connect() as c:
            row = c.execute('SELECT next_item,messages FROM receipts WHERE job=? AND uid=?', (job, uid)).fetchone()
            messages = json.loads(row[1])
            if message_id is not None:
                messages.append(message_id)
            c.execute('UPDATE receipts SET status=?,next_item=?,error=?,messages=? WHERE job=? AND uid=?',
                      (status, row[0] if next_item is None else next_item, error,
                       json.dumps(messages), job, uid))

    def finish(self, job):
        with self.connect() as c:
            c.execute("UPDATE jobs SET status='complete' WHERE id=?", (job,))
            return dict(c.execute('SELECT status,COUNT(*) FROM receipts WHERE job=? GROUP BY status', (job,)))


class DeliveryMixin:
    def init_delivery(self):
        self.recipient_state = read_json('recipient_state.json', {})
        if not isinstance(self.recipient_state, dict):
            raise ValueError('recipient_state.json: ожидался объект; проверьте резервную копию')
        # Only prior direct interaction proves long-lived access. Channel membership does not.
        for uid in self.started_users:
            self.recipient_state.setdefault(str(uid), {'can_write': True, 'source': 'legacy_start'})
        self.journal = Journal()
        self.broadcast_lock = asyncio.Lock()
        self.pace = Pace()
        self.ui_notice = ''
        self.no_custom_emoji = False
        self.no_button_styles = False
        self.broadcast_title_entities = []
        self.broadcast_task = None
        self.editor_user_id = None
        self.waiting_for_manual_count = False
        self.background_tasks = set()
        self.accept_lock = asyncio.Lock()
        self.pending_requests = read_json('pending_requests.json', [])
        self.user_requests = {int(r['user_id']): r for r in self.pending_requests}
        # Persist migration separately; preserve the historical membership lists.
        atomic_json('recipient_state.json', self.recipient_state)

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        task.add_done_callback(self.task_done)
        return task

    @staticmethod
    def task_done(task):
        if not task.cancelled() and task.exception():
            log.error('Фоновая задача завершилась с ошибкой: %s', type(task.exception()).__name__)

    def recipient(self, uid, **changes):
        state = self.recipient_state.setdefault(str(uid), {})
        state.update(changes)
        atomic_json('recipient_state.json', self.recipient_state)

    def register_private(self, uid, restart=False, source='private_message'):
        old = self.recipient_state.get(str(uid), {})
        self.recipient(uid, can_write=True, blocked=False, source=source,
                       opted_out=False if restart else old.get('opted_out', False))
        if uid not in self.started_users:
            self.started_users.add(uid)
            self.save_started_users()
        if uid not in self.subscribers:
            self.add_subscriber(uid)

    def broadcast_targets(self):
        return sorted(int(uid) for uid, s in self.recipient_state.items()
                      if s.get('can_write') and not s.get('blocked') and not s.get('opted_out'))

    def may_deliver(self, uid):
        s = self.recipient_state.get(str(uid), {})
        return not (s.get('blocked') or s.get('opted_out'))

    def editor_busy(self, uid):
        active = self.broadcast_preview_data or any(
            bool(value) for key, value in vars(self).items()
            if key.startswith('waiting_for_') or key.startswith('broadcast_waiting_for_'))
        return active and self.editor_user_id is not None and self.editor_user_id != uid

    async def handle_membership(self, update):
        chat = update['chat']
        if chat.get('type') == 'private':
            uid = chat['id']
            blocked = update.get('new_chat_member', {}).get('status') == 'kicked'
            self.recipient(uid, blocked=blocked)

    async def approve_requests(self, requests):
        accepted = 0
        async with self.accept_lock:
            for request in list(requests):
                uid = request['user_id']
                if uid not in self.user_requests:
                    continue
                result = await self.approve_chat_join_request(self.channel_id, uid)
                if not result.get('ok'):
                    log.warning('Заявка %s осталась в очереди: %s', uid, result.get('error_code'))
                    continue
                self.add_subscriber(uid)
                self._remove_request_safely(uid)
                accepted += 1
        return accepted

    async def handle_manual_count_input(self, message, text):
        try:
            count = int(text)
            if count <= 0:
                raise ValueError
        except ValueError:
            await self.send_message(message['chat']['id'], 'Введите целое число больше нуля.')
            return
        self.waiting_for_manual_count = False
        accepted = await self.approve_requests(self.pending_requests[:count])
        await self.send_message(message['chat']['id'], f'✅ Принято: {accepted}. Осталось: {len(self.pending_requests)}.')

    def presentation(self, data):
        payload = copy.deepcopy(data)
        if not payload:
            return payload
        markup = payload.get('reply_markup')
        encoded = isinstance(markup, str)
        if encoded:
            markup = json.loads(markup)
        if markup:
            for row in markup.get('inline_keyboard', []):
                for b in row:
                    if b.get('callback_data') and 'style' not in b:
                        label = b.get('text', '').lower()
                        if any(word in label for word in ('удалить', 'отмен', 'очистить', 'выключить')):
                            b['style'] = 'danger'
                        elif any(word in label for word in ('отправить', 'подтвердить', 'включить', 'сохранить')):
                            b['style'] = 'success'
                    if self.no_custom_emoji:
                        b.pop('icon_custom_emoji_id', None)
                    if self.no_button_styles:
                        b.pop('style', None)
            payload['reply_markup'] = json.dumps(markup, ensure_ascii=False) if encoded else markup
        if self.no_custom_emoji:
            for key in ('entities', 'caption_entities'):
                if key in payload:
                    value = payload[key]
                    es = json.loads(value) if isinstance(value, str) else value
                    es = [e for e in es if e.get('type') != 'custom_emoji']
                    payload[key] = json.dumps(es, ensure_ascii=False) if isinstance(value, str) else es
            if payload.get('parse_mode') == 'HTML':
                for key in ('text', 'caption'):
                    if key in payload:
                        payload[key] = re.sub(r'<tg-emoji\b[^>]*>(.*?)</tg-emoji>', r'\1', payload[key], flags=re.S)
        return payload

    def presentation_error(self, result):
        if result.get('error_code') != 400:
            return False
        desc = result.get('description', '').lower()
        if any(s in desc for s in ('custom_emoji', 'custom emoji', 'premium', 'emoji_id', 'emoji-id')) and not self.no_custom_emoji:
            self.no_custom_emoji = True
            self.ui_notice = 'Telegram отклонил Premium-эмодзи: отправляю обычные эмодзи. Проверьте Premium владельца бота.'
        elif ('button_style' in desc or 'button style' in desc) and not self.no_button_styles:
            self.no_button_styles = True
            self.ui_notice = 'Telegram отклонил цвета: отправляю обычные кнопки.'
        else:
            return False
        log.warning(self.ui_notice)
        return True

    async def send_broadcast_item(self, uid, item, markup):
        if item.get('snapshot'):
            snapshot = item['snapshot']
            payload = copy.deepcopy(snapshot['payload'])
            payload['chat_id'] = uid
            if markup:
                payload['reply_markup'] = markup
            return await self.make_request_with_retry('POST', f"{self.base_url}/{snapshot['method']}", payload)
        return await self.copy_message(uid, item['from_chat_id'], item['message_id'], markup)

    async def execute_broadcast_async(self, message, data, job_id=None):
        admin = message['chat']['id']
        if not data.get('items'):
            await self.send_message(admin, 'В черновике нет сообщений.')
            return
        job = self.journal.create(copy.deepcopy(data), self.broadcast_targets(), admin, job_id)
        async with self.broadcast_lock:
            # Journal is the source of truth, including on startup recovery.
            if not any(j == job for j, _, _ in self.journal.pending()):
                return
            await self.send_message(admin, '📤 Рассылка начата. Прогресс сохраняется на диске.')
            for uid, start in self.journal.targets(job):
                if uid not in self.broadcast_targets():
                    self.journal.mark(job, uid, 'skipped', error='Отписка или закрытый диалог')
                    continue
                for index in range(start, len(data['items'])):
                    if not self.may_deliver(uid):
                        self.journal.mark(job, uid, 'skipped', error='Отписка во время отправки')
                        break
                    self.journal.mark(job, uid, 'sending')
                    markup = button_markup(data.get('buttons', [])) if index == len(data['items']) - 1 else None
                    try:
                        result = await self.send_broadcast_item(uid, data['items'][index], markup)
                    except Exception:
                        result = {'ok': False, 'delivery_unknown': True, 'description': 'Сбой сети во время отправки'}
                    if result.get('ok'):
                        last = index == len(data['items']) - 1
                        self.journal.mark(job, uid, 'sent' if last else 'pending', index + 1,
                                          message_id=result.get('result', {}).get('message_id'))
                        continue
                    desc = result.get('description', 'Ошибка Telegram')
                    if result.get('error_code') == 401:
                        self.journal.mark(job, uid, 'pending', error='Токен отклонён Telegram')
                        await self.send_message(admin, 'Рассылка приостановлена: Telegram отклонил токен.')
                        return
                    if result.get('error_code') == 429 and not result.get('delivery_unknown'):
                        self.journal.mark(job, uid, 'pending', error=desc)
                        await self.send_message(admin, 'Рассылка приостановлена: лимит или сбой Telegram. Продолжение автоматически.')
                        return
                    terminal = result.get('error_code') == 403 or 'chat not found' in desc.lower()
                    if terminal:
                        self.recipient(uid, blocked=True, last_error=desc)
                    state = 'unknown' if result.get('delivery_unknown') or result.get('error_code', 0) == 0 else 'failed'
                    self.journal.mark(job, uid, state, error=desc)
                    break
            counts = self.journal.finish(job)
            text = ('✅ <b>Рассылка завершена</b>\n'
                    f"Доставлено полностью: {counts.get('sent', 0)}\n"
                    f"Ошибки: {counts.get('failed', 0)}\n"
                    f"Пропущено (отписка/блокировка): {counts.get('skipped', 0)}\n"
                    f"Результат неизвестен: {counts.get('unknown', 0)}\n"
                    'Неизвестные отправки автоматически не повторяются, чтобы не создавать дубли.')
            if self.ui_notice:
                text += '\n' + html.escape(self.ui_notice)
            await self.send_message(admin, text)

    async def resume_broadcasts(self):
        for job, data, admin in self.journal.pending():
            await self.execute_broadcast_async({'chat': {'id': admin}}, data, job_id=job)

    async def delivery_tick(self):
        """Schedule independently of long polling and network update errors."""
        while self.is_running:
            try:
                await self.process_pending_welcome_jobs()
                await self.check_schedule()
                await self.check_daily_broadcast()
                if self.auto_accept:
                    now = time.time()
                    due = [r for r in self.pending_requests
                           if now >= float(r.get('request_time', now)) + self.auto_delay]
                    if due:
                        await self.approve_requests(due)
                if (self.broadcast_task is None or self.broadcast_task.done()) and self.journal.pending():
                    self.broadcast_task = self.spawn(self.resume_broadcasts())
            except Exception as e:
                log.error('Ошибка фоновой доставки: %s', type(e).__name__)
            await asyncio.sleep(1)
