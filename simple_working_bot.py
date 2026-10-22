#!/usr/bin/env python3
"""
Оптимизированный Telegram бот приветствия с улучшенной производительностью
"""

import asyncio
import copy
import html
import logging
import aiohttp
import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import random
import time
import uuid
from dotenv import load_dotenv
from delivery_support import DeliveryMixin, atomic_json, parse_buttons, button_markup, snapshot_message, parse_duration, BUTTON_HELP

load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = -1001140244688
BOT_USERNAME = "godjuusatoru_bot"
WELCOME_STORE_PATH = "welcome_messages.json"
SETTINGS_STORE_PATH = "bot_settings.json"
PENDING_WELCOME_STORE_PATH = "pending_welcomes.json"
STARTED_USERS_PATH = "started_users.json"
SUBSCRIBER_DATES_PATH = "subscriber_dates.json"
ADMIN_IDS = [8235497168, 375064528]
MAX_WELCOME_DELAY_SECONDS = 30 * 24 * 60 * 60  # 30 дней
WELCOME_RETRY_SECONDS = 5 * 60
WELCOME_MAX_ATTEMPTS = 5
WELCOME_SEND_INTERVAL = 0.05  # пауза между отправками, лимит Telegram ~30/сек
WELCOME_BATCH_LIMIT = 20  # сколько созревших задач берём за один проход
WELCOME_FAILED_TTL = 3 * 24 * 60 * 60  # через сколько выкидывать недоставленные
WELCOME_SCHEDULE_VERSION = 2  # 2 = задержка считается от момента заявки
WELCOME_STALE_GRACE = 60 * 60  # насколько можно опоздать, прежде чем задача устарела
# Пока заявка не обработана, Telegram даёт 5 минут на письмо человеку,
# который не открывал диалог с ботом. Берём с запасом.
WELCOME_JOIN_WINDOW = 240
GATE_START_PAYLOAD = "welcome"
GATE_BUTTON_TEXT = "🔓 ОТКРЫТЬ ДОСТУП"

class SimpleBot(DeliveryMixin):
    def __init__(self):
        self.bot_token = os.getenv("BOT_TOKEN", BOT_TOKEN).strip()
        self.channel_id = int(os.getenv("CHANNEL_ID", str(CHANNEL_ID)))
        self.admin_ids = [int(x.strip()) for x in os.getenv("ADMIN_IDS", ",".join(map(str, ADMIN_IDS))).split(",") if x.strip()]
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.welcome_messages = []
        self.auto_accept = False
        self.auto_delay = 0
        self.waiting_for_buttons = False
        self.current_editing_id = None
        self.last_preview_message_id = None
        # Подписчики для рассылки
        self.subscribers = set()
        # Файл для сохранения подписчиков
        self.subscribers_file = 'subscribers.json'
        # Даты подписок для статистики. Хранятся в отдельном файле,
        # формат списка подписчиков не меняется.
        self.subscriber_dates = {}
        # Черновик рассылки. Храним исходные сообщения Telegram и рассылаем
        # через copyMessage, поэтому entities, скрытые ссылки и медиа не
        # преобразуются и доходят до получателя без потерь.
        self.waiting_for_broadcast = False
        self.broadcast_preview_data = None
        self.broadcast_items = []
        self.broadcast_buttons = []
        self.broadcast_collect_timer = None
        self.broadcast_preview_message_id = None
        self.broadcast_waiting_for_button_title = False
        self.broadcast_waiting_for_button_url = False
        self.broadcast_pending_button_title = None
        self.broadcast_waiting_for_schedule_time = False
        self.daily_broadcast = {
            "enabled": False,
            "time": "09:00",
            "items": [],
            "buttons": [],
            "last_sent_date": None,
        }
        # Список заявок на вступление с датами
        self.pending_requests = []
        # Состояния ожидания ввода
        self.waiting_for_date = False
        self.waiting_for_link = False
        self.waiting_for_schedule_count = False
        self.waiting_for_date_count = False
        self.waiting_for_schedule_time = False
        self.waiting_for_delay = False
        self.waiting_for_welcome_delay = False
        self.waiting_for_welcome_text = False
        self.waiting_for_welcome_text_edit = False
        self.current_welcome_text_id = None
        self.current_welcome_delay_id = None
        self.current_schedule_data = None  # Сохраняем данные расписания
        # Очередь приветствий обрабатывается строго по одному проходу за раз.
        self.welcome_queue_lock = asyncio.Lock()
        self.last_query_id = None
        self.query_answered = True
        # Сохраненные расписания (словарь: день -> время и количество)
        self.schedules = {}
        # Оптимизация: словарь для отслеживания заявок по user_id
        self.user_requests = {}
        # Словарь для отслеживания отправленных приветствий
        self.sent_welcomes = set()
        # Отложенные приветствия хранятся на диске и переживают перезапуск.
        self.pending_welcome_jobs = []
        # Кто открыл диалог с ботом. Только этим людям Telegram разрешает
        # писать без ограничения по времени.
        self.started_users = set()

        # Оптимизации производительности
        self.session = None
        self.connector = None
        self.rate_limiter = None
        self.is_running = False

        # Загрузка сохраненных приветствий
        self.load_welcome_messages()
        # Загрузка настроек
        self.load_settings()
        # Загрузка подписчиков
        self.load_subscribers()
        # Загрузка дат подписок для статистики
        self.load_subscriber_dates()
        # Загрузка сохраненной очереди приветствий
        self.load_pending_welcome_jobs()
        # Загрузка списка тех, кто уже открыл диалог с ботом
        self.load_started_users()
        self.init_delivery()
        recovered = False
        for job in self.pending_welcome_jobs:
            if job.get("status") == "sending":
                job.update(status="unknown", last_error="Перезапуск во время отправки")
                recovered = True
        if recovered:
            self.save_pending_welcome_jobs()

    def load_started_users(self):
        """Загрузить тех, кто нажимал Start, чтобы им можно было писать всегда."""
        try:
            if not os.path.exists(STARTED_USERS_PATH):
                return
            with open(STARTED_USERS_PATH, "r", encoding="utf-8") as file:
                data = json.load(file)
            if isinstance(data, list):
                self.started_users = {int(item) for item in data}
                logger.info(f"Загружено открытых диалогов: {len(self.started_users)}")
        except Exception as error:
            logger.error(f"Ошибка загрузки открытых диалогов: {error}")

    def save_started_users(self):
        atomic_json(STARTED_USERS_PATH, sorted(self.started_users))

    def gate_keyboard(self):
        """Кнопка-диплинк, открывающая боту доступ к личке пользователя."""
        return {"inline_keyboard": [[{
            "text": "🔓 Получать сообщения от бота",
            "style": "success",
            "url": f"https://t.me/{getattr(self, 'bot_username', BOT_USERNAME)}?start={GATE_START_PAYLOAD}",
        }]]}

    @staticmethod
    def valid_schedule_time(value):
        """Проверить время строго в формате ЧЧ:ММ."""
        if not re.match(r"^\d{2}:\d{2}$", value):
            return False
        hour, minute = (int(part) for part in value.split(":"))
        return 0 <= hour <= 23 and 0 <= minute <= 59

    def moscow_now(self):
        return datetime.now(ZoneInfo("Europe/Moscow"))

    @staticmethod
    def format_delay(seconds):
        """Компактно показать длительную задержку в кнопках и меню."""
        seconds = max(0, int(seconds or 0))
        if seconds == 0:
            return "сразу"
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if days:
            parts.append(f"{days}д")
        if hours:
            parts.append(f"{hours}ч")
        if minutes:
            parts.append(f"{minutes}м")
        if secs and not days:
            parts.append(f"{secs}с")
        return " ".join(parts[:2])

    async def initialize_session(self):
        """Инициализация HTTP сессии и rate limiter"""
        if self.connector is None:
            self.connector = aiohttp.TCPConnector(
                limit=50,  # Ограничение количества соединений
                limit_per_host=20,  # Ограничение на хост Telegram
                ttl_dns_cache=300,
                use_dns_cache=True,
                keepalive_timeout=60,
                enable_cleanup_closed=True,
                ssl=True,  # Проверяем сертификат Telegram
                family=2  # Используем только IPv4
            )

        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=45,  # Общий таймаут 45 сек
                connect=10,  # Таймаут соединения 10 сек
                sock_read=15,  # Таймаут чтения 15 сек
                sock_connect=10  # Таймаут сокет соединения 10 сек
            )
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=timeout,
                headers={
                    'Connection': 'keep-alive',
                    'User-Agent': 'TelegramBot/1.0'
                }
            )

        if self.rate_limiter is None:
            self.rate_limiter = asyncio.Semaphore(25)  # Максимум 25 одновременных запросов

    async def close_session(self):
        """Закрытие HTTP сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
        if self.connector:
            await self.connector.close()

    async def make_request_with_retry(self, method, url, data=None, params=None, max_retries=3):
        name = url.rsplit("/", 1)[-1]
        sending = name.startswith(("send", "copy", "forward"))
        await self.initialize_session()
        for attempt in range(max_retries + 2):
            payload = self.presentation(data)
            if sending and payload and payload.get("chat_id"):
                await self.pace.wait(payload["chat_id"])
            elif self.pace.cooldown > time.monotonic():
                await asyncio.sleep(self.pace.cooldown - time.monotonic())
            try:
                async with self.rate_limiter:
                    if method == 'POST':
                        async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=35)) as response:
                            result = await response.json()
                    else:
                        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=35)) as response:
                            result = await response.json()
                if result.get('ok'):
                    return result
                if result.get('error_code') == 429:
                    retry_after = max(1, float(result.get('parameters', {}).get('retry_after', 1)))
                    self.pace.flood(retry_after)
                    if attempt < max_retries - 1:
                        continue
                    return result
                if self.presentation_error(result) and attempt < max_retries + 1:
                    continue
                if result.get('error_code', 0) >= 500 and sending:
                    return dict(result, delivery_unknown=True)
                return result
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                # Never log the request URL: it contains a credential.
                log_type = type(e).__name__
                if sending:
                    return {'ok': False, 'error_code': 0, 'description': log_type, 'delivery_unknown': True}
                if attempt >= max_retries - 1:
                    return {'ok': False, 'error_code': 0, 'description': log_type}
                await asyncio.sleep(min(8, 2 ** attempt))
        return {'ok': False, 'error_code': 0, 'description': 'Retry budget exhausted', 'delivery_unknown': sending}

    def load_welcome_messages(self):
        """Загрузить приветствия из JSON файла"""
        try:
            if os.path.exists(WELCOME_STORE_PATH):
                with open(WELCOME_STORE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Простая валидация структуры
                    if isinstance(data, list):
                        # Проверяем и исправляем структуру каждого приветствия
                        for msg in data:
                            # Убеждаемся, что поле delay существует и является числом
                            if "delay" not in msg:
                                msg["delay"] = 0
                            elif not isinstance(msg["delay"], int):
                                try:
                                    msg["delay"] = int(msg["delay"])
                                except (ValueError, TypeError):
                                    msg["delay"] = 0

                            # Убеждаемся, что поле active существует
                            if "active" not in msg:
                                msg["active"] = False

                        self.welcome_messages = data
                        self.sort_welcome_messages()
                        logger.info(f"Загружено приветствий: {len(self.welcome_messages)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки приветствий: {e}")

    def sort_welcome_messages(self):
        """Держать список в порядке отправки - по возрастанию задержки."""
        self.welcome_messages.sort(key=lambda msg: max(0, int(msg.get("delay", 0) or 0)))

    def save_welcome_messages(self):
        atomic_json(WELCOME_STORE_PATH, self.welcome_messages)

    def load_pending_welcome_jobs(self):
        """Загрузить отложенные приветствия после перезапуска."""
        try:
            if not os.path.exists(PENDING_WELCOME_STORE_PATH):
                return
            with open(PENDING_WELCOME_STORE_PATH, "r", encoding="utf-8") as file:
                jobs = json.load(file)
            if isinstance(jobs, list):
                self.pending_welcome_jobs = [
                    job for job in jobs
                    if isinstance(job, dict) and job.get("id") and job.get("welcome")
                ]
                logger.info(f"Загружено отложенных приветствий: {len(self.pending_welcome_jobs)}")
                self.migrate_pending_welcome_jobs()
        except Exception as error:
            logger.error(f"Ошибка загрузки очереди приветствий: {error}")

    def migrate_pending_welcome_jobs(self):
        """Пересчитать старые задачи, где задержки складывались друг с другом."""
        now = time.time()
        migrated = 0
        dropped = 0
        kept = []
        for job in self.pending_welcome_jobs:
            if job.get("schedule_v") == WELCOME_SCHEDULE_VERSION:
                kept.append(job)
                continue
            created_at = float(job.get("created_at") or 0)
            if created_at:
                delay = max(0, int((job.get("welcome") or {}).get("delay", 0) or 0))
                job["due_at"] = created_at + delay
            job["schedule_v"] = WELCOME_SCHEDULE_VERSION
            migrated += 1
            # Момент отправки давно прошёл - выкидываем, иначе после
            # перезапуска на людей вывалится вся накопленная цепочка сразу.
            kept.append(job)
        if migrated:
            self.pending_welcome_jobs = kept
            logger.info(
                f"Пересчитано время отправки у {migrated} задач "
                f"(задержка считается от момента заявки), просроченных удалено: {dropped}"
            )
            self.save_pending_welcome_jobs()

    def save_pending_welcome_jobs(self):
        # Persistence failure must prevent sending, not erase delivery receipts.
        atomic_json(PENDING_WELCOME_STORE_PATH, self.pending_welcome_jobs)

    def load_settings(self):
        """Загрузить настройки бота из JSON"""
        try:
            if os.path.exists(SETTINGS_STORE_PATH):
                with open(SETTINGS_STORE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                    self.auto_accept = bool(data.get("auto_accept", False))
                    self.auto_delay = int(data.get("auto_delay", 0) or 0)
                    # schedules как словарь
                    schedules = data.get("schedules", {})
                    if isinstance(schedules, dict):
                        self.schedules = schedules
                    daily_broadcast = data.get("daily_broadcast", {})
                    if isinstance(daily_broadcast, dict):
                        self.daily_broadcast.update({
                            key: daily_broadcast[key]
                            for key in self.daily_broadcast
                            if key in daily_broadcast
                        })
                    logger.info("Настройки загружены")
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек: {e}")

    def save_settings(self):
        atomic_json(SETTINGS_STORE_PATH, {
            "auto_accept": self.auto_accept, "auto_delay": self.auto_delay,
            "schedules": self.schedules, "daily_broadcast": self.daily_broadcast,
        })

    def load_subscribers(self):
        """Загрузить список подписчиков из JSON файла"""
        try:
            if os.path.exists(self.subscribers_file):
                with open(self.subscribers_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.subscribers = set(data)
                        logger.info(f"Загружено подписчиков: {len(self.subscribers)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки подписчиков: {e}")

    def save_subscribers(self):
        atomic_json(self.subscribers_file, sorted(self.subscribers))

    def add_subscriber(self, user_id):
        """Добавить подписчика и сохранить"""
        self.subscribers.add(user_id)
        self.save_subscribers()
        # Фиксируем дату подписки для статистики (уже известную не перезаписываем)
        if str(user_id) not in self.subscriber_dates:
            self.subscriber_dates[str(user_id)] = self.moscow_now().isoformat()
            self.save_subscriber_dates()
        logger.info(f"Добавлен подписчик: {user_id}")

    def load_subscriber_dates(self):
        """Загрузить даты подписок из отдельного файла"""
        try:
            if os.path.exists(SUBSCRIBER_DATES_PATH):
                with open(SUBSCRIBER_DATES_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self.subscriber_dates = data
                        logger.info(f"Загружено дат подписок: {len(self.subscriber_dates)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки дат подписок: {e}")

    def save_subscriber_dates(self):
        atomic_json(SUBSCRIBER_DATES_PATH, self.subscriber_dates)

    def subscription_counts(self):
        """Посчитать подписки за сегодня и за последние 7 дней"""
        today = self.moscow_now().date()
        week_start = today - timedelta(days=6)
        today_count = 0
        week_count = 0
        for joined_at in self.subscriber_dates.values():
            try:
                joined_date = datetime.fromisoformat(joined_at).date()
            except (TypeError, ValueError):
                continue
            if joined_date == today:
                today_count += 1
            if joined_date >= week_start:
                week_count += 1
        return today_count, week_count

    def convert_entities_to_html(self, text, entities):
        """Конвертировать Telegram entities в HTML форматирование"""
        if not entities:
            return text

        # Создаем список изменений (offset, length, replacement)
        replacements = []

        for entity in entities:
            offset = entity['offset']
            length = entity['length']
            entity_type = entity['type']

            # Извлекаем текст entity
            entity_text = text[offset:offset + length]

            # Применяем форматирование в зависимости от типа
            if entity_type == 'bold':
                formatted = f'<b>{entity_text}</b>'
            elif entity_type == 'italic':
                formatted = f'<i>{entity_text}</i>'
            elif entity_type == 'underline':
                formatted = f'<u>{entity_text}</u>'
            elif entity_type == 'strikethrough':
                formatted = f'<s>{entity_text}</s>'
            elif entity_type == 'code':
                formatted = f'<code>{entity_text}</code>'
            elif entity_type == 'pre':
                formatted = f'<pre>{entity_text}</pre>'
            elif entity_type == 'text_link':
                url = entity.get('url', '')
                formatted = f'<a href="{url}">{entity_text}</a>'
            elif entity_type == 'text_mention':
                user = entity.get('user', {})
                user_id = user.get('id', '')
                formatted = f'<a href="tg://user?id={user_id}">{entity_text}</a>'
            elif entity_type == 'url':
                formatted = f'<a href="{entity_text}">{entity_text}</a>'
            else:
                # Для неизвестных типов оставляем как есть
                formatted = entity_text

            replacements.append((offset, length, formatted))

        # Сортируем по offset в обратном порядке, чтобы не сбивать индексы
        replacements.sort(key=lambda x: x[0], reverse=True)

        # Применяем замены
        result = text
        for offset, length, formatted in replacements:
            result = result[:offset] + formatted + result[offset + length:]

        return result

    async def send_message(self, chat_id, text, reply_markup=None):
        """Отправить сообщение"""
        url = f"{self.base_url}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def edit_message(self, chat_id, message_id, text, reply_markup=None):
        """Редактировать сообщение"""
        url = f"{self.base_url}/editMessageText"
        data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def edit_message_caption(self, chat_id, message_id, caption, reply_markup=None):
        """Редактировать подпись у медиа-сообщения"""
        url = f"{self.base_url}/editMessageCaption"
        data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "caption": caption,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def answer_callback_query(self, callback_query_id, text=None):
        """Ответить на callback запрос"""
        url = f"{self.base_url}/answerCallbackQuery"
        data = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text

        return await self.make_request_with_retry('POST', url, data)

    async def get_updates(self, offset=None):
        """Получить обновления"""
        url = f"{self.base_url}/getUpdates"
        # Короткий long polling: очередь приветствий проверяется после каждого
        # ответа, поэтому с timeout=30 сообщение с задержкой 15 секунд могло
        # уйти на полминуты позже.
        params = {"timeout": 5}
        if offset:
            params["offset"] = offset

        return await self.make_request_with_retry('GET', url, params=params)

    async def approve_chat_join_request(self, chat_id, user_id):
        """Одобрить заявку на вступление"""
        url = f"{self.base_url}/approveChatJoinRequest"
        data = {
            "chat_id": chat_id,
            "user_id": user_id
        }
        return await self.make_request_with_retry('POST', url, data)

    async def delete_message(self, chat_id, message_id):
        """Удалить сообщение"""
        url = f"{self.base_url}/deleteMessage"
        data = {
            "chat_id": chat_id,
            "message_id": message_id
        }

        return await self.make_request_with_retry('POST', url, data)

    async def send_photo(self, chat_id, photo_id, caption=None):
        """Отправить фото"""
        url = f"{self.base_url}/sendPhoto"
        data = {
            "chat_id": chat_id,
            "photo": photo_id
        }
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"

        return await self.make_request_with_retry('POST', url, data)

    async def send_media_group(self, chat_id, media_list):
        """Отправить группу медиа (несколько фото)"""
        url = f"{self.base_url}/sendMediaGroup"
        data = {
            "chat_id": chat_id,
            "media": json.dumps(media_list)
        }

        return await self.make_request_with_retry('POST', url, data)

    async def send_video(self, chat_id, video_id, caption=None):
        """Отправить видео"""
        url = f"{self.base_url}/sendVideo"
        data = {
            "chat_id": chat_id,
            "video": video_id
        }
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"

        return await self.make_request_with_retry('POST', url, data)

    async def send_document(self, chat_id, document_id, caption=None):
        """Отправить документ"""
        url = f"{self.base_url}/sendDocument"
        data = {
            "chat_id": chat_id,
            "document": document_id
        }
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"

        return await self.make_request_with_retry('POST', url, data)

    async def copy_message(self, chat_id, from_chat_id, message_id, reply_markup=None):
        """Скопировать исходное сообщение Telegram без потери entities и медиа."""
        data = {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_id": message_id,
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        return await self.make_request_with_retry(
            'POST', f"{self.base_url}/copyMessage", data
        )

    async def edit_message_reply_markup(self, chat_id, message_id, reply_markup):
        """Обновить только кнопки у предпросмотра, не меняя его содержимое."""
        data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": json.dumps(reply_markup),
        }
        return await self.make_request_with_retry(
            'POST', f"{self.base_url}/editMessageReplyMarkup", data
        )

    @staticmethod
    def telegram_error(result):
        """Вернуть описание неуспешного ответа Bot API или пустую строку."""
        if isinstance(result, dict) and not result.get("ok"):
            return result.get("description") or "Неизвестная ошибка Telegram"
        return ""

    def reset_broadcast_draft(self):
        """Очистить только временный черновик рассылки, не затрагивая базу."""
        if self.broadcast_collect_timer:
            self.broadcast_collect_timer.cancel()
        self.broadcast_collect_timer = None
        self.waiting_for_broadcast = False
        self.broadcast_items = []
        self.broadcast_buttons = []
        self.broadcast_preview_data = None
        self.broadcast_preview_message_id = None
        self.broadcast_waiting_for_button_title = False
        self.broadcast_waiting_for_button_url = False
        self.broadcast_pending_button_title = None
        self.broadcast_waiting_for_schedule_time = False

    def broadcast_markup(self, preview=False):
        """Кнопки получателя и, только в предпросмотре, кнопки управления."""
        rows = [[copy.deepcopy(button)] for button in self.broadcast_buttons]
        if preview:
            checking = bool((self.broadcast_preview_data or {}).get('include_unknown'))
            rows.extend([
                [{"text": ("☑" if checking else "☐") + " Разово проверить старые диалоги",
                  "callback_data": "broadcast_toggle_unknown"}],
                [{"text": "➕ Добавить кнопку", "callback_data": "broadcast_add_button"}],
                [{"text": "↩️ Удалить последнюю", "callback_data": "broadcast_remove_button"}],
                [{"text": "⏰ Отправлять каждый день", "callback_data": "broadcast_schedule_daily"}],
                [{"text": "✅ Отправить рассылку", "callback_data": "confirm_broadcast"}],
                [{"text": "❌ Отменить", "callback_data": "cancel_broadcast"}],
            ])
        return {"inline_keyboard": rows} if rows else None

    async def collect_broadcast_item(self, message):
        """Добавить любое входящее сообщение в черновик и дождаться паузы."""
        self.broadcast_items.append({
            "from_chat_id": message["chat"]["id"],
            "message_id": message["message_id"],
            "snapshot": snapshot_message(message),
        })
        if self.broadcast_collect_timer:
            self.broadcast_collect_timer.cancel()

        async def finish_collection():
            await asyncio.sleep(2)
            if self.waiting_for_broadcast and self.broadcast_items:
                self.waiting_for_broadcast = False
                self.broadcast_preview_data = {
                    "items": list(self.broadcast_items),
                    "buttons": list(self.broadcast_buttons),
                }
                await self.show_broadcast_preview(message["chat"]["id"])

        self.broadcast_collect_timer = self.spawn(finish_collection())

    async def handle_message(self, message):
        """Обработка сообщений"""
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]

        if message["chat"].get("type") != "private":
            return
        text_command = (message.get("text") or "").split(maxsplit=1)[0].split("@")[0] if message.get("text") else ""
        was_started = user_id in self.started_users
        self.register_private(user_id, restart=text_command == "/start",
                              source="write_access_allowed" if message.get("write_access_allowed") else "private_message")
        if user_id in self.admin_ids and self.editor_busy(user_id):
            await self.send_message(chat_id, "Другой администратор редактирует черновик. Дождитесь завершения.")
            return
        if text_command in ("/stop", "/unsubscribe"):
            self.recipient(user_id, opted_out=True)
            await self.send_message(chat_id, "Рассылки и отложенные сообщения отключены. Чтобы включить снова, нажмите /start.")
            return
        if text_command == "/start" and user_id not in self.admin_ids:
            await self.handle_user_start(message, first_time=not was_started)
            return

        # Обработка текстовых сообщений
        if "text" in message:
            text = message["text"]

            # /start может прийти с параметром из диплинка: "/start welcome"
            if text_command == "/start":
                if user_id in self.admin_ids:
                    await self.send_message(chat_id, "🔧 <b>Админ панель</b>\n\nВыберите действие:",
                                          self.get_main_menu())
                else:
                    await self.handle_user_start(message)

            elif text == "/admin" and user_id in self.admin_ids:
                await self.send_message(chat_id, "🔧 <b>Админ панель</b>\n\nВыберите действие:",
                                      self.get_main_menu())

            # Обработка приветственных сообщений от админа
            elif user_id in self.admin_ids and chat_id == user_id:  # Личные сообщения админу
                # Название и URL кнопки вводятся после создания предпросмотра.
                if self.broadcast_waiting_for_schedule_time:
                    schedule_time = text.strip()
                    if not self.valid_schedule_time(schedule_time):
                        await self.send_message(chat_id, "Введите время в формате ЧЧ:ММ, например <b>09:30</b>.")
                        return
                    if not self.broadcast_preview_data:
                        self.broadcast_waiting_for_schedule_time = False
                        await self.send_message(chat_id, "Черновик рассылки уже закрыт. Создайте его заново.")
                        return
                    self.broadcast_waiting_for_schedule_time = False
                    self.daily_broadcast = {
                        "enabled": True,
                        "time": schedule_time,
                        "items": list(self.broadcast_preview_data["items"]),
                        "buttons": list(self.broadcast_buttons),
                        "last_sent_date": None,
                    }
                    self.save_settings()
                    await self.send_message(
                        chat_id,
                        f"✅ Ежедневная рассылка включена: <b>{schedule_time}</b> по Москве.\n"
                        "Она будет отправляться из сохранённого черновика каждый день только в открытые диалоги "
                        "и тем, кому ранее удалось доставить сообщение. Проверка старых диалогов разовая: "
                        "её запускают кнопкой «Отправить рассылку» или проверкой последнего поста."
                    )
                    return
                elif self.broadcast_waiting_for_button_title:
                    self.broadcast_waiting_for_button_title = False
                    self.broadcast_pending_button_title = text.strip()
                    shift = len(text[:len(text) - len(text.lstrip())].encode('utf-16-le')) // 2
                    self.broadcast_title_entities = [dict(e, offset=e['offset'] - shift)
                        for e in message.get("entities", []) if e.get('offset', 0) >= shift]
                    if not self.broadcast_pending_button_title or len(self.broadcast_pending_button_title) > 64:
                        await self.send_message(chat_id, "Введите непустое название кнопки.")
                        self.broadcast_waiting_for_button_title = True
                    else:
                        self.broadcast_waiting_for_button_url = True
                        await self.send_message(chat_id, "Теперь отправьте ссылку. Можно дописать цвет и ID: <code>https://example.com | зелёный | 12345</code>. Цвет и ID необязательны.")
                    return
                elif self.broadcast_waiting_for_button_url:
                    try:
                        button = parse_buttons(self.broadcast_pending_button_title + " | " + text.strip(),
                                               self.broadcast_title_entities)[0]
                    except ValueError as error:
                        await self.send_message(chat_id, html.escape(str(error)))
                        return
                    self.broadcast_buttons.append(button)
                    self.broadcast_waiting_for_button_url = False
                    self.broadcast_pending_button_title = None
                    self.broadcast_preview_data["buttons"] = list(self.broadcast_buttons)
                    if self.broadcast_preview_message_id:
                        await self.edit_message_reply_markup(
                            chat_id, self.broadcast_preview_message_id,
                            self.broadcast_markup(preview=True),
                        )
                    await self.send_message(chat_id, "✅ Кнопка добавлена. Можно добавить ещё или запустить рассылку.")
                    return
                # Режим рассылки: сохраняем ссылку на исходное сообщение, а не HTML.
                elif self.waiting_for_broadcast:
                    await self.collect_broadcast_item(message)
                    return
                # Ожидание ввода даты для ручного приема
                elif self.waiting_for_manual_count:
                    await self.handle_manual_count_input(message, text)
                    return
                elif self.waiting_for_date:
                    self.waiting_for_date = False
                    await self.handle_date_input(message, text)
                    return
                # Ожидание ввода ссылки для ручного приема
                elif self.waiting_for_link:
                    self.waiting_for_link = False
                    await self.handle_link_input(message, text)
                    return
                # Ожидание ввода количества для расписания
                elif self.waiting_for_schedule_count:
                    self.waiting_for_schedule_count = False
                    await self.handle_schedule_count_input(message, text)
                    return
                # Ожидание ввода количества для даты
                elif self.waiting_for_date_count:
                    self.waiting_for_date_count = False
                    await self.handle_date_count_input(message, text)
                    return
                # Ожидание ввода времени для расписания
                elif self.waiting_for_schedule_time:
                    self.waiting_for_schedule_time = False
                    await self.handle_schedule_time_input(message, text)
                    return
                # Ожидание ввода задержки
                elif self.waiting_for_delay:
                    self.waiting_for_delay = False
                    await self.handle_delay_input(message, text)
                    return
                # Ожидание ввода задержки приветствия
                elif self.waiting_for_welcome_delay:
                    await self.handle_welcome_delay_input(message, text)
                    return
                # Редактирование текста/подписи уже созданного приветствия
                elif self.waiting_for_welcome_text_edit:
                    await self.handle_welcome_text_edit(message)
                    return
                # Проверяем, не настраивает ли админ URL кнопки
                elif hasattr(self, 'waiting_for_buttons') and self.waiting_for_buttons:
                    await self.handle_url_buttons_input(message, text)
                # Проверяем, не добавляет ли админ приветствие
                elif self.waiting_for_welcome_text:
                    await self.handle_welcome_text(message, text)
                else:
                    # Игнорируем случайные сообщения от админа
                    pass

        # Любой поддерживаемый Bot API медиа-тип можно добавить в рассылку.
        elif any(key in message for key in ("photo", "video", "animation", "document", "audio", "voice")) and user_id in self.admin_ids and chat_id == user_id:
            if self.waiting_for_welcome_text_edit:
                await self.handle_welcome_text_edit(message)
                return
            if self.waiting_for_broadcast and chat_id == user_id:
                await self.collect_broadcast_item(message)
                return
            if not self.waiting_for_welcome_text:
                return
            if "photo" in message:
                await self.handle_welcome_photo(message)
            elif "document" in message:
                await self.handle_welcome_document(message)
            elif "video" in message:
                await self.handle_welcome_video(message)

    async def handle_callback_query(self, callback_query):
        """Обработка callback запросов"""
        query_id = callback_query["id"]
        data = callback_query["data"]
        logger.info(f"Обрабатываем callback: {data}")
        user_id = callback_query["from"]["id"]
        message = callback_query["message"]

        if user_id not in self.admin_ids or message["chat"].get("type", "private") != "private" or message["chat"]["id"] != user_id:
            await self.answer_callback_query(query_id, "У вас нет прав администратора!")
            return

        if self.editor_busy(user_id):
            await self.answer_callback_query(query_id, "Другой администратор редактирует черновик.")
            return
        self.editor_user_id = user_id

        # Ответ на нажатие даём один раз: либо это осмысленная всплывашка из
        # notify(), либо в конце просто гасим "часики" на кнопке.
        self.last_query_id = query_id
        self.query_answered = False
        try:
            await self.dispatch_callback(message, data)
        finally:
            if not self.query_answered:
                self.query_answered = True
                await self.answer_callback_query(query_id)

    async def dispatch_callback(self, message, data):
        """Разобрать нажатую кнопку админки."""
        if data == "welcome_menu":
            self.waiting_for_welcome_text = False
            self.waiting_for_welcome_text_edit = False
            self.current_welcome_text_id = None
            self.waiting_for_buttons = False
            self.current_editing_id = None
            await self.show_welcome_menu(message)
        elif data == "welcome_queue":
            await self.process_pending_welcome_jobs()
            await self.show_welcome_queue(message)
        elif data == "welcome_queue_retry":
            await self.retry_failed_welcome_jobs(message)
        elif data == "welcome_queue_clear_failed":
            await self.clear_failed_welcome_jobs(message)
        elif data == "accept_menu":
            await self.show_accept_menu(message)
        elif data == "settings_menu":
            await self.show_settings_menu(message)
        elif data == "statistics":
            await self.show_statistics(message)
        elif data == "broadcast":
            await self.show_broadcast_menu(message)
        elif data == "broadcast_audience":
            await self.send_message(message["chat"]["id"], self.audience_text())
        elif data == "broadcast_reconcile":
            await self.prepare_latest_broadcast_check(message)
        elif data == "broadcast_toggle_unknown":
            await self.toggle_unknown_audience(message)
        elif data == "broadcast_new":
            self.reset_broadcast_draft()
            self.waiting_for_broadcast = True
            await self.edit_message(
                message["chat"]["id"],
                message["message_id"],
                "📢 <b>Новая рассылка</b>\n\n"
                "Пришлите текст, фото, видео, GIF, файл или несколько сообщений подряд. "
                "После паузы в 2 секунды покажу точный предпросмотр.\n\n"
                "Форматирование Telegram и скрытые ссылки сохраняются один в один.",
                {"inline_keyboard": [[{"text": "⬅️ Назад", "callback_data": "back_to_main"}]]}
            )
        elif data == "broadcast_schedule_daily":
            if not self.broadcast_preview_data:
                await self.send_message(message["chat"]["id"], "Сначала создайте и проверьте черновик рассылки.")
            else:
                self.broadcast_waiting_for_schedule_time = True
                await self.send_message(
                    message["chat"]["id"],
                    "Во сколько отправлять эту рассылку каждый день?\n"
                    "Введите время по Москве в формате <b>ЧЧ:ММ</b>, например <b>09:30</b>.",
                )
        elif data == "broadcast_schedule_disable":
            self.daily_broadcast["enabled"] = False
            self.save_settings()
            await self.show_broadcast_menu(message)
        elif data == "broadcast_add_button":
            if not self.broadcast_preview_data:
                await self.send_message(message["chat"]["id"], "Черновик рассылки уже закрыт.")
            else:
                self.broadcast_waiting_for_button_title = True
                await self.send_message(message["chat"]["id"], "Введите текст кнопки, например: <b>Открыть сайт</b>")
        elif data == "broadcast_remove_button":
            if not self.broadcast_buttons:
                await self.notify("Добавленных кнопок пока нет.")
            else:
                removed = self.broadcast_buttons.pop()
                self.broadcast_preview_data["buttons"] = list(self.broadcast_buttons)
                await self.edit_message_reply_markup(
                    message["chat"]["id"], message["message_id"],
                    self.broadcast_markup(preview=True),
                )
                await self.send_message(message["chat"]["id"], f"Удалена кнопка: {html.escape(removed['text'])}")
        elif data == "confirm_broadcast":
            # Подтверждение отправки рассылки - запускаем в отдельной задаче чтобы не блокировать
            if self.broadcast_preview_data:
                # Копируем данные и сразу очищаем, чтобы повторное нажатие не сработало
                broadcast_data = dict(self.broadcast_preview_data)
                broadcast_data["buttons"] = list(self.broadcast_buttons)
                self.broadcast_preview_data = None
                self.spawn(self.execute_broadcast_async(message, broadcast_data))
                # Сразу обновляем сообщение
                try:
                    await self.send_message(
                        message["chat"]["id"],
                        "🚀 Рассылка запущена! Вы получите отчет по завершению."
                    )
                except Exception as e:
                    logger.error(f"Ошибка уведомления о запуске: {e}")
            else:
                await self.send_message(message["chat"]["id"], "❌ Рассылка уже запущена или данные пусты.")
        elif data == "cancel_broadcast":
            self.reset_broadcast_draft()
            await self.edit_message(
                message["chat"]["id"],
                message["message_id"],
                "❌ Рассылка отменена.",
                {"inline_keyboard": [[{"text": "🔧 Админ панель", "callback_data": "back_to_main"}]]}
            )
        elif data == "back_to_main":
            self.reset_broadcast_draft()
            for key in list(vars(self)):
                if key.startswith('waiting_for_'):
                    setattr(self, key, False)
            self.editor_user_id = None
            await self.edit_message(message["chat"]["id"], message["message_id"],
                                  "🔧 <b>Админ панель</b>\n\nВыберите действие:",
                                  self.get_main_menu())
        elif data == "welcome_add":
            await self.start_add_welcome(message)
        elif data == "welcome_list":
            await self.show_welcome_list(message)
        elif data == "accept_auto":
            await self.show_auto_accept_menu(message)
        elif data == "accept_manual":
            await self.show_manual_accept_menu(message)
        elif data == "accept_schedule":
            await self.show_schedule_accept_menu(message)
        elif data == "settings_admins":
            await self.show_admins_menu(message)
        elif data == "settings_channel":
            await self.show_channel_settings(message)
        elif data == "settings_token":
            await self.show_token_settings(message)
        elif data == "auto_accept_toggle":
            await self.toggle_auto_accept(message)
        elif data == "auto_accept_delay":
            await self.show_delay_menu(message)
        elif data == "manual_accept_all":
            await self.accept_all_users(message)
        elif data == "manual_accept_count":
            await self.start_manual_accept_count(message)
        elif data == "manual_accept_date":
            await self.start_manual_accept_date(message)
        elif data == "manual_accept_link":
            await self.start_manual_accept_link(message)
        elif data.startswith("schedule_day_"):
            await self.handle_schedule_day_selection(message, data)
        elif data.startswith("schedule_count_"):
            await self.handle_schedule_count_selection(message, data)
        elif data == "schedule_add_day":
            await self.show_schedule_day_menu(message)
        elif data == "schedule_edit":
            await self.show_schedule_edit_menu(message)
        elif data == "schedule_clear_all":
            await self.delete_schedule(message)
        elif data.startswith("schedule_edit_day_"):
            await self.handle_schedule_edit_day(message, data)
        elif data.startswith("schedule_edit_time_"):
            await self.handle_schedule_edit_time(message, data)
        elif data.startswith("schedule_edit_count_"):
            await self.handle_schedule_edit_count(message, data)
        elif data.startswith("schedule_toggle_day_"):
            await self.handle_schedule_toggle_day(message, data)
        elif data.startswith("schedule_delete_day_"):
            await self.handle_schedule_delete_day(message, data)
        elif data.startswith("date_accept_all_"):
            await self.handle_date_accept_all(message, data)
        elif data.startswith("date_accept_custom_"):
            await self.handle_date_accept_custom(message, data)
        elif data == "delay_custom":
            await self.start_delay_input(message)
        elif data.startswith("delay_"):
            await self.handle_delay_selection(message, data)
        elif data.startswith("welcome_edit_"):
            self.waiting_for_welcome_text_edit = False
            self.current_welcome_text_id = None
            self.waiting_for_buttons = False
            await self.show_welcome_edit_menu(message, data)
        elif data.startswith("welcome_preview_"):
            await self.show_welcome_preview(message, data)
        elif data.startswith("welcome_text_edit_"):
            await self.start_welcome_text_edit(message, data)
        elif data.startswith("welcome_delay_custom_"):
            logger.info(f"Обрабатываем welcome_delay_custom: {data}")
            await self.start_welcome_delay_input(message, data)
        elif data.startswith("welcome_delay_"):
            await self.show_welcome_delay_menu(message, data)
        elif data.startswith("welcome_buttons_"):
            await self.show_welcome_buttons_menu(message, data)
        elif data.startswith("welcome_delete_"):
            await self.delete_welcome_message(message, data)
        elif data.startswith("welcome_toggle_"):
            await self.toggle_welcome_message(message, data)
        elif data.startswith("set_delay_"):
            await self.set_welcome_delay(message, data)
        elif data.startswith("clear_buttons_"):
            await self.clear_welcome_buttons(message, data)

    async def handle_new_chat_members(self, message):
        """Обработка новых участников канала"""
        if message["chat"]["id"] != self.channel_id:
            return

        for member in message["new_chat_members"]:
            if not member.get("is_bot", False):
                await self.send_welcome_to_user(member)

    async def handle_user_start(self, message, first_time=None):
        user_id = message["from"]["id"]
        if message["chat"].get("type") != "private":
            return
        if first_time is None:
            first_time = user_id not in self.started_users
        self.register_private(user_id, restart=True)
        if user_id in self.user_requests:
            await self.approve_requests([self.user_requests[user_id]])
        if first_time:
            await self.send_welcome_to_user({"id": user_id}, force=True)
        await self.send_message(message["chat"]["id"],
            "✅ Сообщения от бота включены. Они могут приходить и спустя неделю. "
            "Чтобы отключить рассылки и отложенные сообщения, отправьте /stop.")

    async def send_welcome_to_user(self, user, force=False):
        user_id = user["id"]
        if not self.may_deliver(user_id):
            return 0
        existing = [j for j in self.pending_welcome_jobs if j.get("user_id") == user_id]
        if existing and not force:
            return 0
        state = self.recipient_state.get(str(user_id), {})
        if (user_id in self.sent_welcomes or state.get("welcome_enrolled")) and not force:
            return 0
        active = sorted((m for m in self.welcome_messages if m.get("active", False)),
                        key=lambda m: max(0, int(m.get("delay", 0) or 0)))
        now = time.time()
        origin = min((j.get("created_at", now) for j in existing), default=user.get("request_time", now))
        gated = user_id not in self.started_users
        delivered = set(state.get("welcome_delivered", []))
        existing_keys = {j.get("welcome_key") for j in existing}
        if force:
            for job in existing:
                job["chat_id"] = user_id
                job["gate"] = False
                if job.get("status") == "waiting_start":
                    job["status"] = "pending"
        queued = 0
        previous_due = None
        for index, welcome in enumerate(active):
            key = welcome.get("id") or welcome.get("created_at") or str(index)
            if key in delivered or key in existing_keys:
                continue
            due = origin + max(0, int(welcome.get("delay", 0) or 0))
            if previous_due is not None and due <= previous_due:
                due = previous_due + 1.05
            previous_due = due
            waiting = gated and (due > origin + WELCOME_JOIN_WINDOW or not user.get("join_chat_id"))
            self.pending_welcome_jobs.append({
                "id": uuid.uuid4().hex, "user_id": user_id,
                "chat_id": user_id if not gated else user.get("join_chat_id", user_id),
                "due_at": due, "created_at": origin, "sequence": index + 1,
                "status": "waiting_start" if waiting else "pending", "attempts": 0,
                "last_error": "", "schedule_v": WELCOME_SCHEDULE_VERSION,
                "gate": gated and index == 0, "welcome_key": key,
                "welcome": copy.deepcopy(welcome),
                "join_expires_at": origin + WELCOME_JOIN_WINDOW if gated else None,
            })
            queued += 1
        self.save_pending_welcome_jobs()
        self.recipient(user_id, welcome_enrolled=True)
        immediate_gate = any(j.get("user_id") == user_id and j.get("gate")
                             and j.get("status") == "pending" and j.get("due_at", 0) <= now
                             for j in self.pending_welcome_jobs)
        if gated and not immediate_gate and user.get("join_chat_id") and now < origin + WELCOME_JOIN_WINDOW:
            await self.send_message(user["join_chat_id"],
                "Чтобы получать сообщения и после вступления в канал, нажмите кнопку и затем «Запустить». "
                "Отключение сообщений: /stop.", self.gate_keyboard())
        await self.process_pending_welcome_jobs()
        return queued

    async def process_pending_welcome_jobs(self):
        """Отправить созревшие задачи и сохранить итог каждой попытки."""
        # Метод вызывается из нескольких мест: после каждого опроса, из /start
        # и по кнопке очереди в админке. Без замка два вызова успевали забрать
        # одну и ту же задачу, пока первый ждал ответа Telegram, и человек
        # получал сообщение дважды.
        if self.welcome_queue_lock.locked():
            return
        async with self.welcome_queue_lock:
            await self.process_due_welcome_jobs()

    async def process_due_welcome_jobs(self):
        now = time.time()
        due_jobs = sorted((j for j in self.pending_welcome_jobs
                           if j.get("status", "pending") == "pending" and float(j.get("due_at", 0)) <= now),
                          key=lambda j: (j.get("due_at", 0), j.get("sequence", 0)))[:WELCOME_BATCH_LIMIT]
        for job in due_jobs:
            uid = job["user_id"]
            if not self.may_deliver(uid):
                job.update(status="cancelled", last_error="Отписка или блокировка")
                self.save_pending_welcome_jobs()
                continue
            if uid in self.started_users:
                job["chat_id"] = uid
                job["gate"] = False
            elif not job.get("join_expires_at") or now >= job["join_expires_at"] or uid not in self.user_requests:
                job.update(status="waiting_start", last_error="Ожидается запуск бота пользователем")
                self.save_pending_welcome_jobs()
                continue
            welcome = next((m for m in self.welcome_messages
                            if (m.get("id") or m.get("created_at")) == job.get("welcome_key")), job["welcome"])
            if not welcome.get("active", False):
                job.update(status="cancelled", last_error="Приветствие выключено")
                self.save_pending_welcome_jobs()
                continue
            job["status"] = "sending"
            self.save_pending_welcome_jobs()
            result = await self.send_single_welcome(
                {"id": uid, "chat_id": job.get("chat_id", uid)}, welcome,
                extra_buttons=self.gate_keyboard() if job.get("gate") else None)
            if result.get("ok"):
                state = self.recipient_state.get(str(uid), {})
                delivered = list(state.get("welcome_delivered", []))
                if job.get("welcome_key") and job["welcome_key"] not in delivered:
                    delivered.append(job["welcome_key"])
                self.recipient(uid, welcome_delivered=delivered)
                self.pending_welcome_jobs.remove(job)
                self.save_pending_welcome_jobs()
                continue
            job["attempts"] = int(job.get("attempts", 0)) + 1
            job["last_error"] = self.telegram_error(result)
            code = result.get("error_code", 0)
            failure = self.delivery_failure(result)
            if failure:
                self.remember_delivery(uid, result)
            if result.get("delivery_unknown") or code == 0 or code >= 500:
                job["status"] = "unknown"
            elif code == 401:
                job.update(status="pending", due_at=now + WELCOME_RETRY_SECONDS)
            elif code == 403 and uid not in self.started_users and failure not in ('blocked', 'deactivated'):
                job["status"] = "waiting_start"
            elif code in (400, 403, 404) or job["attempts"] >= WELCOME_MAX_ATTEMPTS:
                job["status"] = "failed"
            else:
                delay = result.get("parameters", {}).get("retry_after", WELCOME_RETRY_SECONDS)
                job.update(status="pending", due_at=time.time() + max(1, float(delay)))
            self.save_pending_welcome_jobs()

    async def send_single_welcome(self, user, welcome_msg, extra_buttons=None):
        """Отправить одно приветственное сообщение"""
        chat_id = user.get("chat_id", user["id"])

        # Создаем URL кнопки если есть
        reply_markup = None
        if welcome_msg.get("url_buttons"):
            keyboard = []
            for button in welcome_msg["url_buttons"]:
                keyboard.append([copy.deepcopy(button)])
            reply_markup = {"inline_keyboard": keyboard}
        if extra_buttons:
            keyboard = list(reply_markup["inline_keyboard"]) if reply_markup else []
            keyboard.extend(extra_buttons["inline_keyboard"])
            reply_markup = {"inline_keyboard": keyboard}

        # Отправляем сообщение в зависимости от типа
        try:
            if welcome_msg["type"] == "text":
                result = await self.send_message_with_markup(
                    chat_id, welcome_msg["text"], reply_markup,
                    entities=welcome_msg.get("entities"),
                )
            elif welcome_msg["type"] == "photo":
                result = await self.send_photo_with_markup(
                    chat_id, welcome_msg["photo_id"], welcome_msg["text"], reply_markup,
                    entities=welcome_msg.get("entities"),
                )
            elif welcome_msg["type"] == "document":
                result = await self.send_document_with_markup(
                    chat_id, welcome_msg["document_id"], welcome_msg["text"], reply_markup,
                    entities=welcome_msg.get("entities"),
                )
            elif welcome_msg["type"] == "video":
                result = await self.send_video_with_markup(
                    chat_id, welcome_msg["video_id"], welcome_msg["text"], reply_markup,
                    entities=welcome_msg.get("entities"),
                )
            else:
                result = {"ok": False, "error_code": 400, "description": "Неизвестный тип приветствия"}

            if result.get("ok"):
                logger.info(f"Приветствие доставлено пользователю {user['id']}")
            else:
                logger.warning(
                    f"Приветствие не доставлено пользователю {user['id']}: "
                    f"{self.telegram_error(result)}"
                )
            return result
        except Exception as e:
            logger.error(f"Ошибка отправки приветствия пользователю {user['id']}: {e}")
            return {"ok": False, "error_code": 0, "description": type(e).__name__, "delivery_unknown": True}

    async def send_message_with_markup(self, chat_id, text, reply_markup=None, entities=None):
        """Отправить сообщение с разметкой"""
        url = f"{self.base_url}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
        }
        if entities is not None:
            data["entities"] = json.dumps(entities, ensure_ascii=False)
        else:
            data["parse_mode"] = "HTML"
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def send_photo_with_markup(self, chat_id, photo_id, caption=None, reply_markup=None, entities=None):
        """Отправить фото с разметкой"""
        url = f"{self.base_url}/sendPhoto"
        data = {
            "chat_id": chat_id,
            "photo": photo_id
        }
        if caption:
            data["caption"] = caption
            if entities is not None:
                data["caption_entities"] = json.dumps(entities, ensure_ascii=False)
            else:
                data["parse_mode"] = "HTML"
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def send_document_with_markup(self, chat_id, document_id, caption=None, reply_markup=None, entities=None):
        """Отправить документ с разметкой"""
        url = f"{self.base_url}/sendDocument"
        data = {
            "chat_id": chat_id,
            "document": document_id
        }
        if caption:
            data["caption"] = caption
            if entities is not None:
                data["caption_entities"] = json.dumps(entities, ensure_ascii=False)
            else:
                data["parse_mode"] = "HTML"
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    def get_main_menu(self):
        """Главное меню"""
        return {
            "inline_keyboard": [
                [{"text": "📝 Приветствия", "callback_data": "welcome_menu"}],
                [{"text": "👥 Прием подписчиков", "callback_data": "accept_menu"}],
                [{"text": "⚙️ Настройки", "callback_data": "settings_menu"}],
                [{"text": "📊 Статистика", "callback_data": "statistics"}],
                [{"text": "📢 Рассылка", "callback_data": "broadcast"}]
            ]
        }

    async def show_screen(self, message, text, keyboard=None):
        """Показать экран правкой текущего сообщения, чтобы не плодить новые."""
        result = await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)
        if result.get("ok"):
            return result
        description = (result.get("description") or "")
        if "message is not modified" in description:
            return result
        # У медиа правится подпись, а не текст.
        result = await self.edit_message_caption(
            message["chat"]["id"], message["message_id"], text, keyboard
        )
        if result.get("ok"):
            return result
        return await self.send_message(message["chat"]["id"], text, keyboard)

    async def notify(self, text):
        """Короткое уведомление всплывашкой поверх кнопок, без сообщения в чат."""
        if getattr(self, "last_query_id", None) and not self.query_answered:
            self.query_answered = True
            await self.answer_callback_query(self.last_query_id, text)

    async def welcome_not_found(self, message):
        """Кнопка из устаревшего меню: показать актуальный список приветствий."""
        await self.notify("Список изменился, открываю актуальный")
        await self.show_welcome_menu(message)

    async def show_welcome_menu(self, message):
        """Показать меню приветствий"""
        text = "📝 <b>Управление приветствиями</b>\n\n"

        keyboard_buttons = []

        if self.welcome_messages:
            text += "Выберите приветствие для редактирования:\n\n"
            for i, msg in enumerate(self.welcome_messages):
                delay_text = f" (⏱️ {self.format_delay(msg.get('delay', 0))})"
                type_emoji = "📝" if msg['type'] == 'text' else "📷" if msg['type'] == 'photo' else "📄"
                preview = msg['text'][:30] if msg['text'] else f"{msg['type'].upper()}"

                keyboard_buttons.append([{"text": f"{type_emoji} {i+1}. {preview}...{delay_text}", "callback_data": f"welcome_edit_{i}"}])
        else:
            text += "Приветственные сообщения не настроены.\n"

        waiting_count = sum(job.get("status", "pending") == "pending" for job in self.pending_welcome_jobs)
        failed_count = sum(job.get("status") == "failed" for job in self.pending_welcome_jobs)
        keyboard_buttons.extend([
            [{"text": "➕ Добавить новое", "callback_data": "welcome_add"}],
            [{"text": f"⏳ Очередь: {waiting_count}  ⚠️ {failed_count}", "callback_data": "welcome_queue"}],
            [{"text": "⬅️ Назад", "callback_data": "back_to_main"}]
        ])

        keyboard = {"inline_keyboard": keyboard_buttons}

        await self.show_screen(message, text, keyboard)

    async def show_welcome_queue(self, message):
        """Показать админу состояние сохраняемой очереди."""
        pending = [job for job in self.pending_welcome_jobs if job.get("status", "pending") == "pending"]
        failed = [job for job in self.pending_welcome_jobs if job.get("status") == "failed"]
        text = "⏳ <b>Очередь приветствий</b>\n\n"
        text += f"Ожидают отправки: <b>{len(pending)}</b>\n"
        text += f"Не доставлены: <b>{len(failed)}</b>\n"
        text += f"Открыли диалог с ботом: <b>{len(self.started_users)}</b>\n"
        text += f"Ждут запуска бота: <b>{sum(j.get('status') == 'waiting_start' for j in self.pending_welcome_jobs)}</b>\n"
        text += f"Неизвестный результат (без автоповтора): <b>{sum(j.get('status') == 'unknown' for j in self.pending_welcome_jobs)}</b>\n"
        if pending:
            next_job = min(pending, key=lambda job: float(job.get("due_at", 0)))
            next_time = datetime.fromtimestamp(
                float(next_job["due_at"]), ZoneInfo("Europe/Moscow")
            ).strftime("%d.%m.%Y %H:%M:%S")
            text += f"Ближайшая отправка: <b>{next_time} МСК</b>\n"
            text += f"Пользователей в очереди: <b>{len({job['user_id'] for job in pending})}</b>\n"
        if failed:
            errors = {}
            for job in failed:
                error = job.get("last_error") or "Неизвестная ошибка"
                errors[error] = errors.get(error, 0) + 1
            text += "\n<b>Последние причины:</b>\n"
            for error, count in list(errors.items())[:3]:
                text += f"• {html.escape(error[:120])} — {count}\n"

        keyboard = {"inline_keyboard": [
            [{"text": "🔄 Обновить", "callback_data": "welcome_queue"}],
            [{"text": "♻️ Повторить ошибки", "callback_data": "welcome_queue_retry"}],
            [{"text": "🧹 Убрать ошибки", "callback_data": "welcome_queue_clear_failed"}],
            [{"text": "⬅️ К приветствиям", "callback_data": "welcome_menu"}],
        ]}
        await self.show_screen(message, text, keyboard)

    async def retry_failed_welcome_jobs(self, message):
        failed = [job for job in self.pending_welcome_jobs if job.get("status") == "failed"]
        now = time.time()
        for job in failed:
            job.update({"status": "pending", "attempts": 0, "due_at": now, "last_error": ""})
        if failed:
            self.save_pending_welcome_jobs()
            await self.process_pending_welcome_jobs()
        await self.show_welcome_queue(message)

    async def clear_failed_welcome_jobs(self, message):
        self.pending_welcome_jobs = [
            job for job in self.pending_welcome_jobs if job.get("status") != "failed"
        ]
        self.save_pending_welcome_jobs()
        await self.show_welcome_queue(message)

    async def show_accept_menu(self, message):
        """Показать меню приема подписчиков"""
        text = f"👥 <b>Прием подписчиков</b>\n\n"
        text += f"Автоприем: {'✅ Включен' if self.auto_accept else '❌ Выключен'}\n"
        if self.auto_accept:
            text += f"Задержка: {self.auto_delay} секунд\n"

        keyboard = {
            "inline_keyboard": [
                [{"text": "🤖 Автоприем", "callback_data": "accept_auto"}],
                [{"text": "✋ Ручной прием", "callback_data": "accept_manual"}],
                [{"text": "⏰ По расписанию", "callback_data": "accept_schedule"}],
                [{"text": "⬅️ Назад", "callback_data": "back_to_main"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_settings_menu(self, message):
        """Показать меню настроек"""
        text = "⚙️ <b>Настройки</b>\n\n"
        text += f"Админов: 1\n"
        text += f"Канал: {self.channel_id}\n"

        keyboard = {
            "inline_keyboard": [
                [{"text": "👥 Админы", "callback_data": "settings_admins"}],
                [{"text": "🔗 Канал", "callback_data": "settings_channel"}],
                [{"text": "🔑 Токен", "callback_data": "settings_token"}],
                [{"text": "⬅️ Назад", "callback_data": "back_to_main"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_statistics(self, message):
        today, week = self.subscription_counts()
        text = ("📊 <b>Статистика</b>\n\n"
                f"Записей в базе подписчиков: {len(self.subscribers)}\n"
                f"Добавлено в базу сегодня (МСК): {today}\n"
                f"Добавлено в базу за 7 дней: {week}\n\n" + self.audience_text())
        await self.show_screen(message, text, {"inline_keyboard": [
            [{"text": "📢 К рассылкам", "callback_data": "broadcast"}],
            [{"text": "⬅️ Назад", "callback_data": "back_to_main"}]]})

    async def show_broadcast_menu(self, message):
        schedule = (f"Ежедневная рассылка: <b>{self.daily_broadcast['time']} МСК</b>"
                    if self.daily_broadcast.get('enabled') else "Ежедневная рассылка: <b>выключена</b>")
        keyboard = [[{"text": "🆕 Новая рассылка", "callback_data": "broadcast_new"}],
                    [{"text": "🔎 Проверить получателей последнего поста", "callback_data": "broadcast_reconcile"}],
                    [{"text": "👥 Кто получит сообщения", "callback_data": "broadcast_audience"}]]
        if self.daily_broadcast.get('enabled'):
            keyboard.append([{"text": "🛑 Выключить ежедневную", "callback_data": "broadcast_schedule_disable"}])
        keyboard.append([{"text": "⬅️ Назад", "callback_data": "back_to_main"}])
        await self.edit_message(message['chat']['id'], message['message_id'],
            "📢 <b>Рассылки</b>\n\n" + schedule + "\n\n" + self.audience_text()
            + "\n\nПроверка последнего поста не повторяет уже учтённые отправки. "
              "Сначала показывается предпросмотр, затем требуется подтверждение.\n"
            + html.escape(self.ui_notice), {"inline_keyboard": keyboard})

    async def start_broadcast(self, message):
        """Совместимость с прежним внутренним вызовом меню рассылки."""
        await self.show_broadcast_menu(message)

    async def show_broadcast_preview(self, chat_id):
        if not self.broadcast_preview_data:
            return
        try:
            items = self.broadcast_preview_data['items']
            self.broadcast_preview_message_id = None
            for index, item in enumerate(items):
                markup = self.broadcast_markup(preview=True) if index == len(items) - 1 else None
                result = await self.send_broadcast_item(chat_id, item, markup)
                error = self.telegram_error(result)
                if error:
                    raise RuntimeError(error)
                if index == len(items) - 1:
                    self.broadcast_preview_message_id = result['result']['message_id']
            await self.send_message(chat_id, self.preview_audience_text(self.broadcast_preview_data))
        except Exception as error:
            # A failed preview must not leave an actionable unverified draft.
            self.broadcast_preview_data = None
            await self.send_message(chat_id, 'Ошибка предпросмотра: ' + html.escape(str(error))
                                    + '. Отправка отменена; создайте черновик заново.')

    async def execute_broadcast(self, message):
        """Выполнить рассылку (старый метод, оставлен для совместимости)"""
        if not self.broadcast_preview_data:
            await self.send_message(message["chat"]["id"], "❌ Нет данных для рассылки")
            return
        broadcast_data = self.broadcast_preview_data
        self.broadcast_preview_data = None
        await self.execute_broadcast_async(message, broadcast_data)

    async def execute_broadcast_async(self, message, data, job_id=None):
        return await super().execute_broadcast_async(message, data, job_id)

    async def show_welcome_list(self, message):
        """Показать список приветствий"""
        text = "📋 <b>Все приветственные сообщения</b>\n\n"

        if not self.welcome_messages:
            text += "Приветственные сообщения не найдены."
        else:
            for i, msg in enumerate(self.welcome_messages):
                delay_text = f" (задержка: {msg.get('delay', 0)}с)" if msg.get('delay', 0) > 0 else ""
                text += f"✅ <b>ID{i+1}</b>{delay_text}\n"
                text += f"<i>{msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}</i>\n\n"

        keyboard = {
            "inline_keyboard": [
                [{"text": "⬅️ Назад", "callback_data": "welcome_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def start_add_welcome(self, message):
        """Начать добавление приветствия"""
        self.waiting_for_welcome_text = True
        text = "📝 <b>Добавление приветствия</b>\n\nОтправьте текст, фото, документ или видео для приветствия:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "welcome_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_auto_accept_menu(self, message):
        """Показать меню автоприема"""
        text = f"🤖 <b>Автоприем</b>\n\n"
        text += f"Статус: {'✅ Включен' if self.auto_accept else '❌ Выключен'}\n"
        text += f"Задержка: {self.auto_delay} секунд\n\n"
        text += "Выберите действие:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Выключить" if self.auto_accept else "✅ Включить", "callback_data": "auto_accept_toggle"}],
                [{"text": "⏱️ Задержка", "callback_data": "auto_accept_delay"}],
                [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_manual_accept_menu(self, message):
        """Показать меню ручного приема"""
        text = f"✋ <b>Ручной прием</b>\n\n"
        text += f"В очереди: {len(self.pending_requests)} пользователей\n\n"
        text += "Выберите действие:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "📊 Количество", "callback_data": "manual_accept_count"}],
                [{"text": "✅ Принять всех", "callback_data": "manual_accept_all"}],
                [{"text": "📅 По дате", "callback_data": "manual_accept_date"}],
                [{"text": "🔗 По ссылке", "callback_data": "manual_accept_link"}],
                [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_schedule_accept_menu(self, message):
        """Показать меню приема по расписанию"""
        text = f"⏰ <b>Прием по расписанию</b>\n\n"

        if self.schedules:
            # Показываем все настроенные расписания
            text += f"✅ <b>Настроенные расписания:</b>\n\n"

            days_names = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]

            for day_data, schedule in self.schedules.items():
                if day_data == "daily":
                    day_name = "Ежедневно"
                else:
                    day_name = days_names[int(day_data)]

                status_icon = "🟢" if schedule.get("active", False) else "🔴"
                text += f"{status_icon} <b>{day_name}</b> - {schedule['time']} ({'Все' if schedule['count'] == 'all' else schedule['count']})\n"

            text += "\nВыберите действие:"

            keyboard = {
                "inline_keyboard": [
                    [{"text": "➕ Добавить день", "callback_data": "schedule_add_day"}],
                    [{"text": "✏️ Редактировать", "callback_data": "schedule_edit"}],
                    [{"text": "🗑️ Очистить все", "callback_data": "schedule_clear_all"}],
                    [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
                ]
            }
        else:
            text += "Расписание не настроено\n"
            text += "\nВыберите день недели:"

            keyboard = {
                "inline_keyboard": [
                    [{"text": "Понедельник", "callback_data": "schedule_day_1"}, {"text": "Вторник", "callback_data": "schedule_day_2"}],
                    [{"text": "Среда", "callback_data": "schedule_day_3"}, {"text": "Четверг", "callback_data": "schedule_day_4"}],
                    [{"text": "Пятница", "callback_data": "schedule_day_5"}, {"text": "Суббота", "callback_data": "schedule_day_6"}],
                    [{"text": "Воскресенье", "callback_data": "schedule_day_0"}],
                    [{"text": "Ежедневно", "callback_data": "schedule_day_daily"}],
                    [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
                ]
            }

        await self.show_screen(message, text, keyboard)

    async def show_schedule_edit_menu(self, message):
        """Показать меню редактирования расписания"""
        text = f"✏️ <b>Редактирование расписания</b>\n\n"
        text += "Выберите день для редактирования:"

        if not self.schedules:
            text += "\n❌ Нет настроенных расписаний"
            keyboard = {
                "inline_keyboard": [
                    [{"text": "⬅️ Назад", "callback_data": "schedule_accept"}]
                ]
            }
        else:
            days_names = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
            keyboard_buttons = []

            for day_data, schedule in self.schedules.items():
                if day_data == "daily":
                    day_name = "Ежедневно"
                else:
                    day_name = days_names[int(day_data)]

                status_icon = "🟢" if schedule.get("active", False) else "🔴"
                button_text = f"{status_icon} {day_name} ({schedule['time']})"
                keyboard_buttons.append([{"text": button_text, "callback_data": f"schedule_edit_day_{day_data}"}])

            keyboard_buttons.append([{"text": "⬅️ Назад", "callback_data": "schedule_accept"}])

            keyboard = {"inline_keyboard": keyboard_buttons}

        await self.show_screen(message, text, keyboard)

    async def toggle_schedule(self, message):
        """Включить/выключить расписание"""
        if self.schedules:
            # Переключаем все расписания
            for day_data in self.schedules:
                self.schedules[day_data]["active"] = not self.schedules[day_data].get("active", False)
            status = "включены" if self.schedules[list(self.schedules.keys())[0]]["active"] else "выключены"
            self.save_settings()
            await self.send_message(message["chat"]["id"], f"✅ Все расписания {status}")
            await self.show_schedule_accept_menu(message)
        else:
            await self.send_message(message["chat"]["id"], "❌ Расписания не настроены")

    async def delete_schedule(self, message):
        """Удалить расписание"""
        self.schedules = {}
        self.save_settings()
        await self.send_message(message["chat"]["id"], "✅ Все расписания удалены")
        await self.show_schedule_accept_menu(message)

    async def show_schedule_day_menu(self, message):
        """Показать меню выбора дня недели для добавления расписания"""
        text = f"📅 <b>Добавить день в расписание</b>\n\n"
        text += "Выберите день недели:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "Понедельник", "callback_data": "schedule_day_1"}, {"text": "Вторник", "callback_data": "schedule_day_2"}],
                [{"text": "Среда", "callback_data": "schedule_day_3"}, {"text": "Четверг", "callback_data": "schedule_day_4"}],
                [{"text": "Пятница", "callback_data": "schedule_day_5"}, {"text": "Суббота", "callback_data": "schedule_day_6"}],
                [{"text": "Воскресенье", "callback_data": "schedule_day_0"}],
                [{"text": "Ежедневно", "callback_data": "schedule_day_daily"}],
                [{"text": "⬅️ Назад", "callback_data": "schedule_accept"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_schedule_edit_day(self, message, data):
        """Обработка редактирования конкретного дня расписания"""
        day_data = data.replace("schedule_edit_day_", "")

        if day_data not in self.schedules:
            await self.send_message(message["chat"]["id"], "❌ Расписание для этого дня не найдено")
            return

        schedule = self.schedules[day_data]
        days_names = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
        day_name = "Ежедневно" if day_data == "daily" else days_names[int(day_data)]

        text = f"✏️ <b>Редактирование расписания</b>\n\n"
        text += f"День: {day_name}\n"
        text += f"Время: {schedule['time']}\n"
        text += f"Количество: {'Все' if schedule['count'] == 'all' else schedule['count']}\n"
        text += f"Статус: {'🟢 Активно' if schedule.get('active', False) else '🔴 Выключено'}\n\n"
        text += "Выберите действие:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "✏️ Изменить время", "callback_data": f"schedule_edit_time_{day_data}"}],
                [{"text": "✏️ Изменить количество", "callback_data": f"schedule_edit_count_{day_data}"}],
                [{"text": f"{'🔴 Выключить' if schedule.get('active', False) else '🟢 Включить'}", "callback_data": f"schedule_toggle_day_{day_data}"}],
                [{"text": "🗑️ Удалить день", "callback_data": f"schedule_delete_day_{day_data}"}],
                [{"text": "⬅️ Назад", "callback_data": "schedule_edit"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_schedule_edit_time(self, message, data):
        """Редактирование времени для дня расписания"""
        day_data = data.replace("schedule_edit_time_", "")
        self.waiting_for_schedule_time = True
        self.current_schedule_data = {"day": day_data, "count": self.schedules[day_data]["count"]}

        days_names = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
        day_name = "Ежедневно" if day_data == "daily" else days_names[int(day_data)]

        text = f"✏️ <b>Изменение времени</b>\n\n"
        text += f"День: {day_name}\n"
        text += f"Текущее время: {self.schedules[day_data]['time']}\n\n"
        text += "Введите новое время в формате ЧЧ:ММ (например, 14:30):"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": f"schedule_edit_day_{day_data}"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_schedule_edit_count(self, message, data):
        """Редактирование количества для дня расписания"""
        day_data = data.replace("schedule_edit_count_", "")
        self.waiting_for_schedule_count = True
        self.current_schedule_data = {"day": day_data}

        days_names = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
        day_name = "Ежедневно" if day_data == "daily" else days_names[int(day_data)]

        text = f"✏️ <b>Изменение количества</b>\n\n"
        text += f"День: {day_name}\n"
        text += f"Текущее количество: {'Все' if self.schedules[day_data]['count'] == 'all' else self.schedules[day_data]['count']}\n\n"
        text += "Выберите новое количество:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "1", "callback_data": f"schedule_count_1_{day_data}"}, {"text": "5", "callback_data": f"schedule_count_5_{day_data}"}],
                [{"text": "10", "callback_data": f"schedule_count_10_{day_data}"}, {"text": "20", "callback_data": f"schedule_count_20_{day_data}"}],
                [{"text": "✅ Принять всех", "callback_data": f"schedule_count_all_{day_data}"}],
                [{"text": "✏️ Ввести свое", "callback_data": f"schedule_count_custom_{day_data}"}],
                [{"text": "❌ Отмена", "callback_data": f"schedule_edit_day_{day_data}"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_schedule_toggle_day(self, message, data):
        """Включить/выключить конкретный день расписания"""
        day_data = data.replace("schedule_toggle_day_", "")
        if day_data in self.schedules:
            self.schedules[day_data]["active"] = not self.schedules[day_data].get("active", False)
            status = "включен" if self.schedules[day_data]["active"] else "выключен"
            await self.send_message(message["chat"]["id"], f"✅ День {status}")
            await self.handle_schedule_edit_day(message, f"schedule_edit_day_{day_data}")

    async def handle_schedule_delete_day(self, message, data):
        """Удалить конкретный день расписания"""
        day_data = data.replace("schedule_delete_day_", "")
        if day_data in self.schedules:
            days_names = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
            day_name = "Ежедневно" if day_data == "daily" else days_names[int(day_data)]
            del self.schedules[day_data]
            await self.send_message(message["chat"]["id"], f"✅ Расписание для {day_name} удалено")
            await self.show_schedule_edit_menu(message)

    async def start_delay_input(self, message):
        """Начать ввод задержки"""
        self.waiting_for_delay = True
        text = "⏱️ <b>Ввод задержки</b>\n\n"
        text += "Введите задержку в одном из форматов:\n"
        text += "• Секунды: 30, 60, 120\n"
        text += "• Минуты: 2м, 5м, 10м\n"
        text += "• Часы: 1ч, 2ч, 24ч\n"
        text += "• Комбинированный: 1ч 30м, 2ч 15м 30с\n\n"
        text += "Примеры: 30, 2м, 1ч, 1ч 30м"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "auto_accept_delay"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_delay_input(self, message, delay_text):
        """Обработка ввода задержки"""
        try:
            delay_seconds = self.parse_custom_delay(delay_text)
            if delay_seconds is None:
                await self.send_message(message["chat"]["id"], "❌ Неверный формат задержки. Попробуйте еще раз.")
                return

            if delay_seconds < 0:
                await self.send_message(message["chat"]["id"], "❌ Задержка не может быть отрицательной.")
                return

            if delay_seconds > 86400:  # 24 часа
                await self.send_message(message["chat"]["id"], "❌ Максимальная задержка: 24 часа.")
                return

            self.auto_delay = delay_seconds
            self.save_settings()

            # Форматируем время для отображения
            if delay_seconds < 60:
                delay_display = f"{delay_seconds} секунд"
            elif delay_seconds < 3600:
                minutes = delay_seconds // 60
                seconds = delay_seconds % 60
                delay_display = f"{minutes}м {seconds}с" if seconds > 0 else f"{minutes} минут"
            else:
                hours = delay_seconds // 3600
                minutes = (delay_seconds % 3600) // 60
                delay_display = f"{hours}ч {minutes}м" if minutes > 0 else f"{hours} часов"

            await self.send_message(message["chat"]["id"], f"✅ Задержка установлена: {delay_display}")
            await self.show_delay_menu(message)

        except Exception as e:
            await self.send_message(message["chat"]["id"], "❌ Ошибка при обработке задержки. Попробуйте еще раз.")

    def parse_custom_delay(self, delay_text):
        return parse_duration(delay_text)

    async def start_welcome_delay_input(self, message, data):
        """Начать ввод задержки для приветствия"""
        logger.info(f"start_welcome_delay_input вызвана с data: {data}")
        try:
            welcome_id = int(data.replace("welcome_delay_custom_", ""))
            logger.info(f"Извлечен welcome_id: {welcome_id}")
        except ValueError as e:
            # Если не удается извлечь число, берем 0
            welcome_id = 0
            logger.error(f"Ошибка извлечения welcome_id: {e}")
        self.waiting_for_welcome_delay = True
        self.current_welcome_delay_id = welcome_id
        logger.info(f"Установлено waiting_for_welcome_delay=True, current_welcome_delay_id={welcome_id}")

        text = "⏱️ <b>Ввод задержки приветствия</b>\n\n"
        text += "Введите задержку в одном из форматов:\n"
        text += "• Секунды: 30, 60, 120\n"
        text += "• Минуты: 2м, 5м, 10м\n"
        text += "• Часы: 1ч, 24ч, 48ч\n"
        text += "• Дни: 2д, 7д, 30д\n"
        text += "• Комбинированный: 1д 12ч, 2д 15м 30с\n\n"
        text += "Для отложенных сообщений человек должен запустить бота. Заявка в канал даёт только до 5 минут связи до её обработки."

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": f"welcome_delay_{welcome_id}"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_welcome_delay_input(self, message, delay_text):
        """Обработка ввода задержки приветствия"""
        try:
            delay_seconds = self.parse_custom_delay(delay_text)
            if delay_seconds is None:
                await self.send_message(message["chat"]["id"], "❌ Неверный формат задержки. Попробуйте еще раз.")
                return

            if delay_seconds < 0:
                await self.send_message(message["chat"]["id"], "❌ Задержка не может быть отрицательной.")
                return

            if delay_seconds > MAX_WELCOME_DELAY_SECONDS:
                await self.send_message(message["chat"]["id"], "❌ Максимальная задержка для приветствий: 30 дней.")
                return

            welcome_id = self.current_welcome_delay_id
            if welcome_id is not None and welcome_id < len(self.welcome_messages):
                self.welcome_messages[welcome_id]["delay"] = delay_seconds
                self.save_welcome_messages()
                self.waiting_for_welcome_delay = False
                self.current_welcome_delay_id = None

                # Форматируем время для отображения
                if delay_seconds < 60:
                    delay_display = f"{delay_seconds} секунд"
                elif delay_seconds < 3600:
                    minutes = delay_seconds // 60
                    seconds = delay_seconds % 60
                    delay_display = f"{minutes}м {seconds}с" if seconds > 0 else f"{minutes} минут"
                else:
                    hours = delay_seconds // 3600
                    minutes = (delay_seconds % 3600) // 60
                    delay_display = f"{hours}ч {minutes}м" if minutes > 0 else f"{hours} часов"

                await self.send_message(message["chat"]["id"], f"✅ Задержка приветствия установлена: {delay_display}")
                await self.show_welcome_delay_menu(message, f"welcome_delay_{welcome_id}")
            else:
                await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено.")

        except Exception as e:
            await self.send_message(message["chat"]["id"], "❌ Ошибка при обработке задержки. Попробуйте еще раз.")

    async def show_admins_menu(self, message):
        """Показать меню админов"""
        text = "👥 <b>Управление админами</b>\n\n"
        text += f"• Админы: {', '.join([f'@{admin_id}' for admin_id in self.admin_ids])}\n"
        text += "\nВыберите действие:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "➕ Добавить админа", "callback_data": "admin_add"}],
                [{"text": "➖ Удалить админа", "callback_data": "admin_remove"}],
                [{"text": "⬅️ Назад", "callback_data": "settings_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_channel_settings(self, message):
        """Показать настройки канала"""
        text = f"🔗 <b>Настройки канала</b>\n\n"
        text += f"ID канала: <code>{self.channel_id}</code>\n"
        text += f"Текущий канал: {self.channel_id}\n\n"
        text += "Для смены канала обратитесь к разработчику."

        keyboard = {
            "inline_keyboard": [
                [{"text": "⬅️ Назад", "callback_data": "settings_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def show_token_settings(self, message):
        """Показать настройки токена"""
        text = "🔑 <b>Настройки токена</b>\n\n"
        text += "Токен бота скрыт для безопасности.\n"
        text += "Для смены токена обратитесь к разработчику."

        keyboard = {
            "inline_keyboard": [
                [{"text": "⬅️ Назад", "callback_data": "settings_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def toggle_auto_accept(self, message):
        """Переключить автоприем"""
        self.auto_accept = not self.auto_accept
        self.save_settings()
        await self.show_auto_accept_menu(message)

    async def show_delay_menu(self, message):
        """Показать меню задержки"""
        text = "⏱️ <b>Выберите задержку</b>"

        keyboard = {
            "inline_keyboard": [
                [{"text": "30 секунд", "callback_data": "delay_30"}, {"text": "1 минута", "callback_data": "delay_60"}],
                [{"text": "2 минуты", "callback_data": "delay_120"}, {"text": "5 минут", "callback_data": "delay_300"}],
                [{"text": "10 минут", "callback_data": "delay_600"}, {"text": "30 минут", "callback_data": "delay_1800"}],
                [{"text": "1 час", "callback_data": "delay_3600"}, {"text": "24 часа", "callback_data": "delay_86400"}],
                [{"text": "✏️ Ввести свое", "callback_data": "delay_custom"}],
                [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def accept_all_users(self, message):
        accepted = await self.approve_requests(self.pending_requests)
        await self.show_screen(message, f"✅ Принято: {accepted}. Осталось в очереди: {len(self.pending_requests)}.",
            {"inline_keyboard": [[{"text": "⬅️ Назад", "callback_data": "accept_menu"}]]})

    async def start_manual_accept_count(self, message):
        """Начать ручной прием по количеству"""
        self.waiting_for_manual_count = True
        text = "📊 <b>Прием по количеству</b>\n\nВведите количество пользователей для приема:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def start_manual_accept_date(self, message):
        """Начать ручной прием по дате"""
        self.waiting_for_date = True
        text = "📅 <b>Прием по дате</b>\n\nВведите дату в формате ДД.ММ.ГГГГ (например, 15.12.2024):"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def start_manual_accept_link(self, message):
        """Начать ручной прием по ссылке"""
        self.waiting_for_link = True
        text = "🔗 <b>Прием по ссылке</b>\n\nВведите ссылку-приглашение (например, https://t.me/+ABC123):"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_date_input(self, message, date_text):
        """Обработка ввода даты для ручного приема"""
        try:
            from datetime import datetime
            # Парсим дату в формате ДД.ММ.ГГГГ
            parsed_date = datetime.strptime(date_text, "%d.%m.%Y").strftime("%d.%m.%Y")

            # Находим заявки с этой датой
            matching_requests = [req for req in self.pending_requests if req["date"] == parsed_date]

            if not matching_requests:
                await self.send_message(message["chat"]["id"], f"❌ Заявок на дату {parsed_date} не найдено.")
                return

            # Показываем количество и предлагаем выбор
            text = f"📅 <b>Прием по дате</b>\n\n"
            text += f"Дата: {parsed_date}\n"
            text += f"Найдено заявок: {len(matching_requests)}\n\n"
            text += "Выберите действие:"

            keyboard = {
                "inline_keyboard": [
                    [{"text": f"✅ Принять всех ({len(matching_requests)})", "callback_data": f"date_accept_all_{parsed_date}"}],
                    [{"text": "✏️ Указать количество", "callback_data": f"date_accept_custom_{parsed_date}"}],
                    [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
                ]
            }

            await self.send_message(message["chat"]["id"], text, keyboard)

        except ValueError:
            await self.send_message(message["chat"]["id"], "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ (например, 15.12.2024)")

    async def handle_link_input(self, message, link_text):
        matching = [r for r in self.pending_requests if r.get("invite_link") == link_text.strip()]
        accepted = await self.approve_requests(matching)
        await self.send_message(message["chat"]["id"], f"✅ Принято по ссылке: {accepted}. Ошибочные заявки остаются в очереди.")

    async def handle_date_accept_all(self, message, data):
        date_str = data.replace("date_accept_all_", "")
        matching = [r for r in self.pending_requests if r.get("date") == date_str]
        accepted = await self.approve_requests(matching)
        await self.send_message(message["chat"]["id"], f"✅ Принято за {html.escape(date_str)}: {accepted}. Ошибочные заявки остаются в очереди.")

    async def handle_date_accept_custom(self, message, data):
        """Начать ввод количества для приема по дате"""
        date_str = data.replace("date_accept_custom_", "")

        # Находим заявки с этой датой
        matching_requests = [req for req in self.pending_requests if req["date"] == date_str]

        if not matching_requests:
            await self.send_message(message["chat"]["id"], f"❌ Заявок на дату {date_str} не найдено.")
            return

        self.waiting_for_date_count = True
        self.current_schedule_data = {"date": date_str, "max_count": len(matching_requests)}

        text = f"📅 <b>Прием по дате</b>\n\n"
        text += f"Дата: {date_str}\n"
        text += f"Доступно заявок: {len(matching_requests)}\n\n"
        text += f"Введите количество людей для приема (от 1 до {len(matching_requests)}):"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }

        await self.send_message(message["chat"]["id"], text, keyboard)

    async def handle_schedule_count_input(self, message, count_text):
        """Обработка ввода количества для расписания"""
        try:
            count = int(count_text)
            if count <= 0:
                await self.send_message(message["chat"]["id"], "❌ Количество должно быть больше 0.")
                return

            # Убираем проверку на текущее количество заявок
            # Заявки могут накопиться к моменту выполнения расписания

            # Сохраняем количество и активируем ожидание времени
            self.current_schedule_data["count"] = count
            self.waiting_for_schedule_time = True

            if self.current_schedule_data["day"] == "daily":
                day_name = "Ежедневно"
            else:
                days = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
                day_name = days[int(self.current_schedule_data["day"])]

            text = f"⏰ <b>Прием по расписанию</b>\n\n"
            text += f"День: {day_name}\n"
            text += f"Количество: {count}\n"
            text += f"В очереди: {len(self.pending_requests)} пользователей\n\n"
            text += "Введите время в формате ЧЧ:ММ (например, 14:30):"

            keyboard = {
                "inline_keyboard": [
                    [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
                ]
            }

            await self.send_message(message["chat"]["id"], text, keyboard)

        except ValueError:
            await self.send_message(message["chat"]["id"], "❌ Введите корректное число.")

    async def handle_date_count_input(self, message, count_text):
        try:
            count = int(count_text)
            if count <= 0:
                raise ValueError
        except ValueError:
            await self.send_message(message["chat"]["id"], "Введите целое число больше нуля.")
            self.waiting_for_date_count = True
            return
        date_str = (self.current_schedule_data or {}).get('date')
        matching = [r for r in self.pending_requests if r.get("date") == date_str][:count]
        accepted = await self.approve_requests(matching)
        await self.send_message(message["chat"]["id"], f"✅ Принято: {accepted}. Ошибочные заявки остаются в очереди.")

    async def handle_schedule_time_input(self, message, time_text):
        """Обработка ввода времени для расписания"""
        try:
            from datetime import datetime
            # Парсим время в формате ЧЧ:ММ
            parsed_time = datetime.strptime(time_text, "%H:%M").strftime("%H:%M")
            # Сохраняем/обновляем расписание для конкретного дня
            day_data = self.current_schedule_data["day"]
            count = self.current_schedule_data["count"]
            # Если расписание уже существует, сохраняем его статус
            existing_active = self.schedules.get(day_data, {}).get("active", True)
            self.schedules[day_data] = {
                "time": parsed_time,
                "count": count,
                "active": existing_active
            }
            self.save_settings()
            if day_data == "daily":
                day_name = "Ежедневно"
            else:
                days = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
                day_name = days[int(day_data)]
            text = f"✅ <b>Расписание настроено!</b>\n\n"
            text += f"День: {day_name}\n"
            text += f"Время: {parsed_time}\n"
            text += f"Количество: {'Все' if count == 'all' else count}\n\n"
            text += "Расписание будет выполняться автоматически."
            await self.send_message(message["chat"]["id"], text)
        except ValueError:
            await self.send_message(message["chat"]["id"], "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 14:30)")

    async def handle_schedule_day_selection(self, message, data):
        """Обработка выбора дня недели для расписания"""
        day_data = data.replace("schedule_day_", "")

        if day_data == "daily":
            day_name = "Ежедневно"
        else:
            days = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
            day_name = days[int(day_data)]

        text = f"⏰ <b>Прием по расписанию</b>\n\n"
        text += f"Выбран день: {day_name}\n"
        text += f"В очереди: {len(self.pending_requests)} пользователей\n\n"
        text += "Выберите количество для приема:"

        keyboard = {
            "inline_keyboard": [
                [{"text": "1", "callback_data": f"schedule_count_1_{day_data}"}, {"text": "5", "callback_data": f"schedule_count_5_{day_data}"}],
                [{"text": "10", "callback_data": f"schedule_count_10_{day_data}"}, {"text": "20", "callback_data": f"schedule_count_20_{day_data}"}],
                [{"text": "✅ Принять всех", "callback_data": f"schedule_count_all_{day_data}"}],
                [{"text": "✏️ Ввести свое", "callback_data": f"schedule_count_custom_{day_data}"}],
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_schedule_count_selection(self, message, data):
        """Обработка выбора количества для расписания"""
        # Парсим данные: schedule_count_5_daily или schedule_count_all_1 или schedule_count_custom_1
        parts = data.replace("schedule_count_", "").split("_")
        count = parts[0]  # "5", "all" или "custom"
        day_data = parts[1]  # "daily" или "1"

        if count == "custom":
            # Сохраняем данные и переходим в режим ввода количества
            self.waiting_for_schedule_count = True
            self.current_schedule_data = {"day": day_data}

            if day_data == "daily":
                day_name = "Ежедневно"
            else:
                days = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
                day_name = days[int(day_data)]

            text = f"⏰ <b>Прием по расписанию</b>\n\n"
            text += f"День: {day_name}\n"
            text += f"В очереди: {len(self.pending_requests)} пользователей\n\n"
            text += "Введите количество людей для приема:"

            keyboard = {
                "inline_keyboard": [
                    [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
                ]
            }

            await self.show_screen(message, text, keyboard)
            return

        if day_data == "daily":
            day_name = "Ежедневно"
        else:
            days = ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
            day_name = days[int(day_data)]

        # Сохраняем данные и активируем ожидание времени
        self.waiting_for_schedule_time = True
        self.current_schedule_data = {"day": day_data, "count": count if count != "all" else "all"}

        text = f"⏰ <b>Прием по расписанию</b>\n\n"
        text += f"День: {day_name}\n"
        text += f"Количество: {'Все' if count == 'all' else count}\n"
        text += f"В очереди: {len(self.pending_requests)} пользователей\n\n"
        text += "Введите время в формате ЧЧ:ММ (например, 14:30):"

        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def handle_delay_selection(self, message, data):
        """Обработка выбора задержки"""
        delay_text = data.replace("delay_", "")
        delay_seconds = self.parse_delay(delay_text)

        if delay_seconds is not None:
            self.auto_delay = delay_seconds
            self.save_settings()
            text = f"✅ Задержка автоприема установлена: {delay_text}"

            keyboard = {
                "inline_keyboard": [
                    [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
                ]
            }

            await self.show_screen(message, text, keyboard)

    def parse_delay(self, delay_text):
        """Парсинг времени задержки"""
        try:
            if delay_text == "30":
                return 30
            elif delay_text == "60":
                return 60
            elif delay_text == "120":
                return 120
            elif delay_text == "300":
                return 300
            elif delay_text == "600":
                return 600
            elif delay_text == "1800":
                return 1800
            elif delay_text == "3600":
                return 3600
            elif delay_text == "86400":
                return 86400
            else:
                return int(delay_text)
        except:
            return None

    async def handle_welcome_text(self, message, text):
        """Обработка текстового приветствия от админа"""
        self.waiting_for_welcome_text = False  # Сбрасываем состояние

        # Добавляем приветственное сообщение
        welcome_msg = {
            "text": text,
            "entities": message.get("entities", []),
            "type": "text",
            "delay": 0,  # Задержка в секундах (по умолчанию 0 - без задержки)
            "active": False,  # По умолчанию неактивно
            "url_buttons": [],
            "created_at": datetime.now().isoformat()
        }

        self.welcome_messages.append(welcome_msg)
        self.save_welcome_messages()

        await self.send_message(message["chat"]["id"],
                              f"✅ <b>Приветственное сообщение добавлено!</b>\n\n"
                              f"Текст: {html.escape(text[:100])}{'...' if len(text) > 100 else ''}\n"
                              f"Задержка: 0 секунд\n"
                              f"Статус: ❌ Выключено\n\n"
                              f"Включите сообщение в меню редактирования.",
                              self.get_main_menu())

    async def handle_welcome_photo(self, message):
        """Обработка фото приветствия от админа"""
        self.waiting_for_welcome_text = False  # Сбрасываем состояние
        photo = message["photo"][-1]  # Берем самое большое фото
        caption = message.get("caption", "")

        # Добавляем приветственное сообщение с фото
        welcome_msg = {
            "text": caption,
            "entities": message.get("caption_entities", []),
            "type": "photo",
            "photo_id": photo["file_id"],
            "delay": 0,  # Задержка в секундах (по умолчанию 0 - без задержки)
            "active": False,  # По умолчанию неактивно
            "url_buttons": [],
            "created_at": datetime.now().isoformat()
        }

        self.welcome_messages.append(welcome_msg)
        self.save_welcome_messages()

        await self.send_message(message["chat"]["id"],
                              f"✅ <b>Приветственное сообщение с фото добавлено!</b>\n\n"
                              f"Описание: {html.escape(caption[:100]) if caption else 'Без описания'}\n"
                              f"Задержка: 0 секунд\n"
                              f"Статус: ❌ Выключено\n\n"
                              f"Включите сообщение в меню редактирования.",
                              self.get_main_menu())

    async def handle_welcome_document(self, message):
        """Обработка документа приветствия от админа"""
        self.waiting_for_welcome_text = False  # Сбрасываем состояние
        document = message["document"]
        caption = message.get("caption", "")

        # Добавляем приветственное сообщение с документом
        welcome_msg = {
            "text": caption,
            "entities": message.get("caption_entities", []),
            "type": "document",
            "document_id": document["file_id"],
            "delay": 0,  # Задержка в секундах (по умолчанию 0 - без задержки)
            "active": False,  # По умолчанию неактивно
            "url_buttons": [],
            "created_at": datetime.now().isoformat()
        }

        self.welcome_messages.append(welcome_msg)
        self.save_welcome_messages()

        await self.send_message(message["chat"]["id"],
                              f"✅ <b>Приветственное сообщение с файлом добавлено!</b>\n\n"
                              f"Описание: {html.escape(caption[:100]) if caption else 'Без описания'}\n"
                              f"Файл: {document.get('file_name', 'Неизвестно')}\n"
                              f"Задержка: 0 секунд\n"
                              f"Статус: ❌ Выключено\n\n"
                              f"Включите сообщение в меню редактирования.",
                              self.get_main_menu())

    async def handle_welcome_video(self, message):
        """Обработка видео приветствия от админа"""
        self.waiting_for_welcome_text = False  # Сбрасываем состояние
        video = message["video"]
        caption = message.get("caption", "")

        # Добавляем приветственное сообщение с видео
        welcome_msg = {
            "text": caption,
            "entities": message.get("caption_entities", []),
            "type": "video",
            "video_id": video["file_id"],
            "delay": 0,  # Задержка в секундах (по умолчанию 0 - без задержки)
            "active": False,  # По умолчанию неактивно
            "url_buttons": [],
            "created_at": datetime.now().isoformat()
        }

        self.welcome_messages.append(welcome_msg)
        self.save_welcome_messages()

        await self.send_message(message["chat"]["id"],
                              f"✅ <b>Приветственное сообщение с видео добавлено!</b>\n\n"
                              f"Описание: {html.escape(caption[:100]) if caption else 'Без описания'}\n"
                              f"Видео: {video.get('file_name', 'GIF/Видео')}\n"
                              f"Задержка: 0 секунд\n"
                              f"Статус: ❌ Выключено\n\n"
                              f"Включите сообщение в меню редактирования.",
                              self.get_main_menu())

    async def show_welcome_edit_menu(self, message, data):
        """Показать меню редактирования конкретного приветствия"""
        try:
            welcome_id = int(data.replace("welcome_edit_", ""))
        except ValueError:
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        welcome_msg = self.welcome_messages[welcome_id]

        keyboard = {
            "inline_keyboard": [
                [{"text": "👁️ Предпросмотр", "callback_data": f"welcome_preview_{welcome_id}"}],
                [{"text": "✏️ Изменить текст", "callback_data": f"welcome_text_edit_{welcome_id}"}],
                [{"text": "⏱️ Задержка", "callback_data": f"welcome_delay_{welcome_id}"}],
                [{"text": "🔗 URL кнопки", "callback_data": f"welcome_buttons_{welcome_id}"}],
                [{"text": "🔄 Вкл/Выкл", "callback_data": f"welcome_toggle_{welcome_id}"}],
                [{"text": "🗑️ Удалить", "callback_data": f"welcome_delete_{welcome_id}"}],
                [{"text": "⬅️ Назад", "callback_data": "welcome_menu"}]
            ]
        }

        # Медиа-предпросмотр отдельным сообщением не шлём - для этого есть
        # кнопка "Предпросмотр". Меню рисуем поверх текущего экрана.
        menu_text = f"✏️ <b>Редактирование приветствия #{welcome_id + 1}</b>\n\n"
        menu_text += f"Задержка: {self.format_delay(welcome_msg.get('delay', 0))}\n"
        menu_text += f"URL кнопки: {'Есть' if welcome_msg.get('url_buttons') else 'Нет'}\n"
        menu_text += f"Статус: {'✅ Активно' if welcome_msg.get('active', True) else '❌ Выключено'}\n\n"
        menu_text += f"📋 <b>Текст:</b>\n{html.escape(welcome_msg['text'][:500])}"

        await self.show_screen(message, menu_text, keyboard)

    async def start_welcome_text_edit(self, message, data):
        """Запросить новый текст или подпись у существующего приветствия."""
        try:
            welcome_id = int(data.replace("welcome_text_edit_", ""))
        except ValueError:
            welcome_id = -1
        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        self.waiting_for_buttons = False
        self.waiting_for_welcome_text_edit = True
        self.current_welcome_text_id = welcome_id
        await self.edit_message(
            message["chat"]["id"], message["message_id"],
            "✏️ <b>Изменение приветствия</b>\n\n"
            "Отправьте новый текст. Можно использовать жирный, курсив, "
            "подчёркивание, скрытые ссылки и Premium-эмодзи Telegram. Просто вставьте эмодзи в сообщение.\n\n"
            "Для фото, видео или файла это заменит подпись.",
            {"inline_keyboard": [[
                {"text": "❌ Отмена", "callback_data": f"welcome_edit_{welcome_id}"}
            ]]},
        )

    async def handle_welcome_text_edit(self, message):
        """Сохранить текст вместе с исходными Telegram entities."""
        welcome_id = self.current_welcome_text_id
        if welcome_id is None or not 0 <= welcome_id < len(self.welcome_messages):
            self.waiting_for_welcome_text_edit = False
            self.current_welcome_text_id = None
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return

        new_text = message.get("text", message.get("caption", ""))
        if not new_text.strip():
            await self.send_message(message["chat"]["id"], "❌ Текст не может быть пустым.")
            return

        self.welcome_messages[welcome_id]["text"] = new_text
        self.welcome_messages[welcome_id]["entities"] = message.get("entities", message.get("caption_entities", []))
        self.save_welcome_messages()
        self.waiting_for_welcome_text_edit = False
        self.current_welcome_text_id = None
        await self.send_message(
            message["chat"]["id"],
            "✅ Текст обновлён. Форматирование и скрытые ссылки сохранены.",
        )
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")

    async def send_welcome_preview(self, chat_id, welcome_msg):
        return await self.send_welcome_preview_with_id(chat_id, welcome_msg)

    async def send_welcome_preview_with_id(self, chat_id, welcome_msg):
        return await self.send_single_welcome({"id": chat_id}, welcome_msg)

    async def show_welcome_preview(self, message, data):
        """Показать предпросмотр приветствия"""
        try:
            welcome_id = int(data.replace("welcome_preview_", ""))
        except ValueError:
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        welcome_msg = self.welcome_messages[welcome_id]

        # Отправляем предпросмотр как отдельное сообщение
        await self.send_welcome_preview_with_id(message["chat"]["id"], welcome_msg)

    async def show_welcome_delay_menu(self, message, data):
        """Показать меню настройки задержки для приветствия"""
        try:
            welcome_id = int(data.replace("welcome_delay_", ""))
        except ValueError:
            # Если не удается извлечь число, берем 0
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        text = "⏱️ <b>Выберите задержку отправки</b>"

        keyboard = {
            "inline_keyboard": [
                [{"text": "0 сек", "callback_data": f"set_delay_{welcome_id}_0"},
                 {"text": "30 сек", "callback_data": f"set_delay_{welcome_id}_30"}],
                [{"text": "1 мин", "callback_data": f"set_delay_{welcome_id}_60"},
                 {"text": "5 мин", "callback_data": f"set_delay_{welcome_id}_300"}],
                [{"text": "30 мин", "callback_data": f"set_delay_{welcome_id}_1800"},
                 {"text": "1 час", "callback_data": f"set_delay_{welcome_id}_3600"}],
                [{"text": "6 часов", "callback_data": f"set_delay_{welcome_id}_21600"},
                 {"text": "12 часов", "callback_data": f"set_delay_{welcome_id}_43200"}],
                [{"text": "1 день", "callback_data": f"set_delay_{welcome_id}_86400"},
                 {"text": "2 дня", "callback_data": f"set_delay_{welcome_id}_172800"}],
                [{"text": "3 дня", "callback_data": f"set_delay_{welcome_id}_259200"},
                 {"text": "7 дней", "callback_data": f"set_delay_{welcome_id}_604800"}],
                [{"text": "14 дней", "callback_data": f"set_delay_{welcome_id}_1209600"},
                 {"text": "30 дней", "callback_data": f"set_delay_{welcome_id}_2592000"}],
                [{"text": "✏️ Ввести свое", "callback_data": f"welcome_delay_custom_{welcome_id}"}],
                [{"text": "⬅️ Назад", "callback_data": f"welcome_edit_{welcome_id}"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def set_welcome_delay(self, message, data):
        """Установить задержку для приветствия"""
        parts = data.replace("set_delay_", "").split("_")
        welcome_id = int(parts[0])
        delay = int(parts[1])

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        welcome_msg = self.welcome_messages[welcome_id]
        welcome_msg["delay"] = delay
        # После сохранения список пересортируется по времени, поэтому номер
        # приветствия ищем заново по самому объекту.
        self.save_welcome_messages()
        welcome_id = self.welcome_messages.index(welcome_msg)

        await self.notify(f"Задержка: {self.format_delay(delay)}")
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")


    async def delete_welcome_message(self, message, data):
        """Удалить приветствие"""
        try:
            welcome_id = int(data.replace("welcome_delete_", ""))
        except ValueError:
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        removed = self.welcome_messages[welcome_id]
        removed_key = removed.get("id") or removed.get("created_at")
        del self.welcome_messages[welcome_id]
        self.save_welcome_messages()
        if removed_key:
            self.pending_welcome_jobs = [
                job for job in self.pending_welcome_jobs
                if job.get("welcome_key") != removed_key
            ]
            self.save_pending_welcome_jobs()

        await self.notify("Приветствие удалено")
        await self.show_welcome_menu(message)

    async def toggle_welcome_message(self, message, data):
        """Включить/выключить приветствие"""
        try:
            welcome_id = int(data.replace("welcome_toggle_", ""))
        except ValueError:
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        welcome_msg = self.welcome_messages[welcome_id]
        welcome_msg["active"] = not welcome_msg.get("active", True)
        self.save_welcome_messages()

        status = "включено" if welcome_msg["active"] else "выключено"
        await self.notify(f"Приветствие {status}")

        # Показываем обновленное меню редактирования
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")

    async def clear_welcome_buttons(self, message, data):
        """Очистить URL кнопки приветствия"""
        try:
            welcome_id = int(data.replace("clear_buttons_", ""))
        except ValueError:
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        self.welcome_messages[welcome_id]["url_buttons"] = []
        self.save_welcome_messages()

        await self.notify("URL кнопки очищены")

        # Возвращаемся к редактированию
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")

    async def handle_url_buttons_input(self, message, text):
        """Обработка ввода URL кнопок"""
        welcome_id = self.current_editing_id

        if welcome_id is None or welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Ошибка: приветствие не найдено!")
            self.waiting_for_buttons = False
            return

        try:
            url_buttons = parse_buttons(text, message.get("entities", []))
        except ValueError as error:
            await self.send_message(message["chat"]["id"], html.escape(str(error)) + "\n\n" + BUTTON_HELP)
            return

        # Сохраняем кнопки
        self.welcome_messages[welcome_id]["url_buttons"] = url_buttons
        self.save_welcome_messages()

        # Сбрасываем состояние
        self.waiting_for_buttons = False
        self.current_editing_id = None

        # Отправляем подтверждение
        await self.send_message(message["chat"]["id"], f"✅ Добавлено {len(url_buttons)} кнопок!")

        # Возвращаемся к редактированию
        # Создаем фиктивное сообщение для вызова метода
        fake_message = {
            "chat": {"id": message["chat"]["id"]},
            "message_id": message.get("message_id", 0)
        }
        await self.show_welcome_edit_menu(fake_message, f"welcome_edit_{welcome_id}")

    # Переопределяем метод показа кнопок для активации режима ввода
    async def show_welcome_buttons_menu(self, message, data):
        """Показать меню настройки URL кнопок"""
        try:
            welcome_id = int(data.replace("welcome_buttons_", ""))
        except ValueError:
            welcome_id = 0

        if not 0 <= welcome_id < len(self.welcome_messages):
            await self.welcome_not_found(message)
            return

        welcome_msg = self.welcome_messages[welcome_id]
        buttons = welcome_msg.get("url_buttons", [])

        text = f"🔗 <b>URL кнопки для приветствия #{welcome_id + 1}</b>\n\n"

        if buttons:
            text += "Текущие кнопки:\n"
            for i, btn in enumerate(buttons):
                text += f"{i+1}. {html.escape(btn['text'])} → {html.escape(btn['url'])} · {html.escape(btn.get('style', 'обычный'))}\n"
        else:
            text += "URL кнопки не настроены.\n"

        text += "\n" + BUTTON_HELP

        # Активируем режим ожидания кнопок
        self.waiting_for_buttons = True
        self.current_editing_id = welcome_id

        keyboard = {
            "inline_keyboard": [
                [{"text": "🗑️ Очистить все", "callback_data": f"clear_buttons_{welcome_id}"}],
                [{"text": "⬅️ Назад", "callback_data": f"welcome_edit_{welcome_id}"}]
            ]
        }

        await self.show_screen(message, text, keyboard)

    async def check_schedule(self):
        now = self.moscow_now()
        today = now.date().isoformat()
        for day, schedule in list(self.schedules.items()):
            if not schedule.get("active") or not self.valid_schedule_time(schedule.get("time", "")):
                continue
            if now.strftime("%H:%M") < schedule["time"] or schedule.get("last_run_date") == today:
                continue
            if day != "daily" and (6 if day == "0" else int(day) - 1) != now.weekday():
                continue
            schedule["last_run_date"] = today
            self.save_settings()
            count = schedule.get("count", "all")
            requests = list(self.pending_requests) if count == "all" else self.pending_requests[:max(0, int(count))]
            accepted = await self.approve_requests(requests)
            if accepted:
                for admin in self.admin_ids:
                    await self.send_message(admin, f"⏰ Расписание {schedule['time']} МСК: принято {accepted}.")

    async def check_daily_broadcast(self):
        schedule = self.daily_broadcast
        if not schedule.get("enabled") or not schedule.get("items") or not self.admin_ids:
            return
        now = self.moscow_now()
        today = now.date().isoformat()
        if not self.valid_schedule_time(schedule.get("time", "")) or now.strftime("%H:%M") < schedule["time"]:
            return
        if schedule.get("last_sent_date") == today:
            return
        data = {"items": copy.deepcopy(schedule["items"]), "buttons": copy.deepcopy(schedule.get("buttons", [])),
                "include_unknown": False, "audience_counts": self.audience_counts()}
        # Deterministic ID closes the crash window between journal commit and date save.
        self.journal.create(data, self.broadcast_targets(), self.admin_ids[0], job_id="daily-" + today)
        schedule["last_sent_date"] = today
        self.save_settings()

    async def send_video_with_markup(self, chat_id, video_id, caption, reply_markup=None, entities=None):
        """Отправить видео с разметкой"""
        url = f"{self.base_url}/sendVideo"
        data = {
            "chat_id": chat_id,
            "video": video_id,
            "caption": caption,
        }
        if entities is not None:
            data["caption_entities"] = json.dumps(entities, ensure_ascii=False)
        else:
            data["parse_mode"] = "HTML"
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def run(self):
        try:
            await self._run_connected()
        finally:
            self.is_running = False
            tasks = list(self.background_tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await self.close_session()

    async def _run_connected(self):
        """Запуск бота"""
        if not self.bot_token or not self.admin_ids:
            raise ValueError("Задайте BOT_TOKEN и ADMIN_IDS в .env или Environment хостинга")
        await self.initialize_session()
        me = await self.make_request_with_retry('GET', f"{self.base_url}/getMe")
        if not me.get("ok"):
            await self.close_session()
            raise RuntimeError(f"Telegram getMe: {me.get('error_code', 0)}. Проверьте BOT_TOKEN в Environment хостинга.")
        self.bot_username = me["result"]["username"]
        logger.info("Бот @%s запущен, токен проверен", self.bot_username)
        self.is_running = True
        self.spawn(self.delivery_tick())

        offset = None

        while self.is_running:
            try:
                updates = await self.get_updates(offset)

                if not updates.get("ok"):
                    if updates.get("error_code") in (401, 409):
                        self.is_running = False
                        logger.error("getUpdates: %s. Проверьте токен и отсутствие второй копии или webhook.", updates.get("error_code"))
                        raise RuntimeError("Опрос Telegram остановлен: токен отклонён или запущена вторая копия/webhook")
                    logger.error(f"Ошибка получения обновлений: {updates}")
                    await asyncio.sleep(5)
                    continue

                for update in updates.get("result", []):
                    offset = update["update_id"] + 1

                    if "message" in update:
                        message = update["message"]

                        # Обработка новых участников
                        if "new_chat_members" in message:
                            await self.handle_new_chat_members(message)
                        else:
                            await self.handle_message(message)
                    # Обработка заявок на вступление
                    elif "chat_join_request" in update:
                        req = update["chat_join_request"]
                        # Фильтруем по нашему каналу
                        if req["chat"]["id"] == self.channel_id:
                            user = req["from"]

                            # Оптимизированная проверка дубликатов
                            if user["id"] in self.user_requests:
                                # Пользователь уже подавал заявку - игнорируем
                                logger.debug(f"Повторная заявка от пользователя {user['id']} - игнорируем")
                                continue

                            # Создаем данные заявки эффективно
                            from datetime import datetime
                            current_time = datetime.fromtimestamp(req.get("date", time.time()), ZoneInfo("Europe/Moscow"))
                            request_data = {
                                "user_id": user["id"],
                                "username": user.get("username", ""),
                                "first_name": user.get("first_name", ""),
                                "date": current_time.strftime("%d.%m.%Y"),
                                "datetime": current_time.isoformat(),
                                "invite_link": req.get("invite_link", {}).get("invite_link", ""),
                                "request_time": current_time.timestamp()
                            }

                            # Добавляем в оба списка атомарно
                            self.pending_requests.append(request_data)
                            self.user_requests[user["id"]] = request_data
                            self.recipient(user["id"], join_seen_at=req.get("date", time.time()))
                            atomic_json("pending_requests.json", self.pending_requests)

                            # Логируем успешное добавление заявки
                            logger.info(f"Новая заявка от @{user.get('username', user['id'])} (всего: {len(self.pending_requests)})")

                            # Сохраняем всю цепочку в очередь. Первое письмо пробуем
                            # отправить через временный user_chat_id до обработки заявки.
                            try:
                                await self.send_welcome_to_user({
                                    "id": user["id"],
                                    "join_chat_id": req.get("user_chat_id"),
                                    "request_time": req.get("date", time.time()),
                                })
                            except Exception as e:
                                logger.error(f"Ошибка отправки приветствия пользователю {user['id']}: {e}")

                            # Автоприем при включенном режиме (асинхронно)
                            if self.auto_accept:
                                delay = self.auto_delay if self.auto_delay else 0
                                if delay > 0:
                                    # Создаем задачу с задержкой для автоприема
                                    self.spawn(self.delayed_auto_accept(user["id"], delay))
                                else:
                                    # Немедленный автоприем
                                    await self.delayed_auto_accept(user["id"], 0)
                            else:
                                logger.debug(f"Заявка сохранена в режиме ручного приема: {user['id']}")

                    elif "my_chat_member" in update:
                        await self.handle_membership(update["my_chat_member"])
                    elif "callback_query" in update:
                        await self.handle_callback_query(update["callback_query"])


                # Оптимизированный интервал для стабильной работы
                await asyncio.sleep(1.5)

            except RuntimeError:
                if not self.is_running:
                    raise
                logger.error("Ошибка обработки обновления")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {type(e).__name__}")
                await asyncio.sleep(5)

        # Cancel tasks before closing their shared HTTP session.
        for task in list(self.background_tasks):
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*list(self.background_tasks), return_exceptions=True)
        await self.close_session()
        logger.info("Бот остановлен!")

    async def stop(self):
        """Корректно остановить бота"""
        self.is_running = False

    def _remove_request_safely(self, user_id):
        """Безопасно удалить заявку из всех списков"""
        try:
            self.pending_requests = [r for r in self.pending_requests if r["user_id"] != user_id]
            if user_id in self.user_requests:
                del self.user_requests[user_id]
            if user_id in self.sent_welcomes:
                self.sent_welcomes.remove(user_id)
            atomic_json("pending_requests.json", self.pending_requests)
        except Exception as e:
            logger.error(f"Ошибка удаления заявки {user_id}: {e}")

    async def delayed_auto_accept(self, user_id, delay):
        if delay:
            await asyncio.sleep(delay)
        request = self.user_requests.get(user_id)
        if request:
            await self.approve_requests([request])

def main():
    from pathlib import Path
    from runtime_guard import ProcessLease
    source_dir = Path(__file__).resolve().parent
    load_dotenv(source_dir / '.env')
    data_dir = Path(os.getenv('BOT_DATA_DIR', str(source_dir))).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(data_dir)
    try:
        with ProcessLease(data_dir / 'bot.lock'):
            bot = SimpleBot()
            asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info('Бот остановлен пользователем')
    except Exception as error:
        logger.error('Запуск остановлен: %s', error)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
