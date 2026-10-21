#!/usr/bin/env python3
"""
Оптимизированный Telegram бот приветствия с улучшенной производительностью
"""

import asyncio
import logging
import aiohttp
import json
import re
from datetime import datetime
import os
import random

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = "8564056187:AAFp3wRhxS0QNZH4hmadjKLuYwsAbA2koG8"
CHANNEL_ID = -1001140244688
WELCOME_STORE_PATH = "welcome_messages.json"
SETTINGS_STORE_PATH = "bot_settings.json"
ADMIN_IDS = [8235497168, 375064528]

class SimpleBot:
    def __init__(self):
        self.bot_token = BOT_TOKEN
        self.channel_id = CHANNEL_ID
        self.admin_ids = ADMIN_IDS
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
        # Режим ожидания сообщения для рассылки
        self.waiting_for_broadcast = False
        # Сохраненное сообщение для предпросмотра рассылки
        self.broadcast_preview_data = None
        # Список фото для рассылки (для media group)
        self.broadcast_photos = []
        # Таймер для завершения сбора фото
        self.broadcast_photo_timer = None
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
        self.current_welcome_delay_id = None
        self.current_schedule_data = None  # Сохраняем данные расписания
        # Сохраненные расписания (словарь: день -> время и количество)
        self.schedules = {}
        # Оптимизация: словарь для отслеживания заявок по user_id
        self.user_requests = {}
        # Словарь для отслеживания отправленных приветствий
        self.sent_welcomes = set()

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
                ssl=False,  # Отключаем SSL верификацию для работы в локальной среде
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
        """Выполнить HTTP запрос с повторными попытками"""
        last_error = None

        for attempt in range(max_retries):
            try:
                async with self.rate_limiter:  # Ограничение скорости
                    await self.initialize_session()

                    try:
                        if method == 'POST':
                            async with self.session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=30)) as response:
                                result = await response.json()
                        else:  # GET
                            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                                result = await response.json()
                    except asyncio.TimeoutError:
                        last_error = "Timeout"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1 * (attempt + 1))
                            continue
                        else:
                            logger.error(f"Таймаут после {max_retries} попыток")
                            raise asyncio.TimeoutError("Timeout after retries")

                    # Проверяем на ошибки Telegram API
                    if not result.get("ok"):
                        error_code = result.get("error_code")
                        if error_code == 429:  # Too Many Requests
                            retry_after = result.get("parameters", {}).get("retry_after", 1)
                            if attempt < max_retries - 1:
                                await asyncio.sleep(retry_after)
                                continue
                        elif error_code in [400, 401, 403, 404]:
                            # Критические ошибки, не повторяем
                            return result

                    return result

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = str(e)
                if attempt == max_retries - 1:
                    logger.error(f"Ошибка HTTP запроса после {max_retries} попыток: {e}")
                    raise e

                # Exponential backoff с jitter
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Попытка {attempt + 1} неудачна, ждем {wait_time:.1f} сек: {e}")
                await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(f"Неожиданная ошибка при запросе: {e}")
                last_error = str(e)
                if attempt == max_retries - 1:
                    raise e
                await asyncio.sleep(1)

        # Если все попытки исчерпаны
        raise Exception(f"Все попытки неудачны. Последняя ошибка: {last_error}")

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
                        logger.info(f"Загружено приветствий: {len(self.welcome_messages)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки приветствий: {e}")

    def save_welcome_messages(self):
        """Сохранить приветствия в JSON файл"""
        try:
            with open(WELCOME_STORE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.welcome_messages, f, ensure_ascii=False, indent=2)
            logger.info("Приветствия сохранены")
        except Exception as e:
            logger.error(f"Ошибка сохранения приветствий: {e}")

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
                    logger.info("Настройки загружены")
        except Exception as e:
            logger.error(f"Ошибка загрузки настроек: {e}")

    def save_settings(self):
        """Сохранить настройки бота в JSON"""
        try:
            data = {
                "auto_accept": self.auto_accept,
                "auto_delay": self.auto_delay,
                "schedules": self.schedules,
            }
            with open(SETTINGS_STORE_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info("Настройки сохранены")
        except Exception as e:
            logger.error(f"Ошибка сохранения настроек: {e}")

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
        """Сохранить список подписчиков в JSON файл"""
        try:
            with open(self.subscribers_file, "w", encoding="utf-8") as f:
                json.dump(list(self.subscribers), f, ensure_ascii=False, indent=2)
            logger.info(f"Подписчики сохранены: {len(self.subscribers)}")
        except Exception as e:
            logger.error(f"Ошибка сохранения подписчиков: {e}")

    def add_subscriber(self, user_id):
        """Добавить подписчика и сохранить"""
        self.subscribers.add(user_id)
        self.save_subscribers()
        logger.info(f"Добавлен подписчик: {user_id}")

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
        params = {"timeout": 30}
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

    async def handle_message(self, message):
        """Обработка сообщений"""
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        
        # Обработка текстовых сообщений
        if "text" in message:
            text = message["text"]
            
            if text == "/start":
                if user_id in self.admin_ids:
                    await self.send_message(chat_id, "🔧 <b>Админ панель</b>\n\nВыберите действие:", 
                                          self.get_main_menu())
                else:
                    await self.send_message(chat_id, "Привет! Я бот для приветствия новых подписчиков.")
            
            elif text == "/admin" and user_id in self.admin_ids:
                await self.send_message(chat_id, "🔧 <b>Админ панель</b>\n\nВыберите действие:", 
                                      self.get_main_menu())
            
            # Обработка приветственных сообщений от админа
            elif user_id in self.admin_ids and chat_id == user_id:  # Личные сообщения админу
                # Режим рассылки (текст)
                if self.waiting_for_broadcast:
                    self.waiting_for_broadcast = False
                    # Сохраняем данные для предпросмотра
                    entities = message.get('entities', [])
                    formatted_text = self.convert_entities_to_html(text, entities)
                    self.broadcast_preview_data = {
                        'type': 'text',
                        'text': formatted_text,
                        'original_text': text,
                        'entities': entities
                    }
                    await self.show_broadcast_preview(chat_id)
                    return
                # Ожидание ввода даты для ручного приема
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
                    self.waiting_for_welcome_delay = False
                    await self.handle_welcome_delay_input(message, text)
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
        
        # Обработка фото
        elif "photo" in message and message["from"]["id"] in self.admin_ids:
            # Режим рассылки (фото с подписью)
            if self.waiting_for_broadcast and chat_id == user_id:
                # Добавляем фото в список
                photo_id = message["photo"][-1]["file_id"]
                caption = message.get("caption", "")
                entities = message.get('caption_entities', [])
                formatted_caption = self.convert_entities_to_html(caption, entities) if caption else ""
                
                self.broadcast_photos.append({
                    'photo_id': photo_id,
                    'caption': formatted_caption,
                    'original_caption': caption,
                    'entities': entities
                })
                
                # Отменяем предыдущий таймер если есть
                if self.broadcast_photo_timer:
                    self.broadcast_photo_timer.cancel()
                
                # Отправляем уведомление
                count_text = f"📸 Добавлено фото {len(self.broadcast_photos)}\n\n"
                count_text += "Отправьте еще фото или подождите 3 секунды для завершения."
                
                await self.send_message(chat_id, count_text)
                
                # Создаем таймер на 3 секунды
                async def finish_photos():
                    await asyncio.sleep(3)
                    if self.broadcast_photos:
                        self.waiting_for_broadcast = False
                        # Сохраняем данные для предпросмотра
                        if len(self.broadcast_photos) == 1:
                            # Одно фото - обычная отправка
                            self.broadcast_preview_data = {
                                'type': 'photo',
                                'photo_id': self.broadcast_photos[0]['photo_id'],
                                'caption': self.broadcast_photos[0]['caption'],
                                'original_caption': self.broadcast_photos[0]['original_caption'],
                                'entities': self.broadcast_photos[0]['entities']
                            }
                        else:
                            # Несколько фото - media group
                            self.broadcast_preview_data = {
                                'type': 'media_group',
                                'photos': self.broadcast_photos.copy()
                            }
                        
                        self.broadcast_photos = []
                        await self.show_broadcast_preview(chat_id)
                
                self.broadcast_photo_timer = asyncio.create_task(finish_photos())
                return
            else:
                await self.handle_welcome_photo(message)
        
        # Обработка документов (видео, файлы и т.д.)
        elif "document" in message and message["from"]["id"] in self.admin_ids:
            if self.waiting_for_broadcast and chat_id == user_id:
                self.waiting_for_broadcast = False
                # Сохраняем данные для предпросмотра
                document_id = message["document"]["file_id"]
                caption = message.get("caption", "")
                entities = message.get('caption_entities', [])
                formatted_caption = self.convert_entities_to_html(caption, entities) if caption else ""
                self.broadcast_preview_data = {
                    'type': 'document',
                    'document_id': document_id,
                    'caption': formatted_caption,
                    'original_caption': caption,
                    'entities': entities
                }
                await self.show_broadcast_preview(chat_id)
            else:
                await self.handle_welcome_document(message)
        
        # Обработка видео (включая GIF)
        elif "video" in message and message["from"]["id"] in self.admin_ids:
            if self.waiting_for_broadcast and chat_id == user_id:
                self.waiting_for_broadcast = False
                # Сохраняем данные для предпросмотра
                video_id = message["video"]["file_id"]
                caption = message.get("caption", "")
                entities = message.get('caption_entities', [])
                formatted_caption = self.convert_entities_to_html(caption, entities) if caption else ""
                self.broadcast_preview_data = {
                    'type': 'video',
                    'video_id': video_id,
                    'caption': formatted_caption,
                    'original_caption': caption,
                    'entities': entities
                }
                await self.show_broadcast_preview(chat_id)
            else:
                await self.handle_welcome_video(message)

    async def handle_callback_query(self, callback_query):
        """Обработка callback запросов"""
        query_id = callback_query["id"]
        data = callback_query["data"]
        logger.info(f"Обрабатываем callback: {data}")
        user_id = callback_query["from"]["id"]
        message = callback_query["message"]
        
        if user_id not in self.admin_ids:
            await self.answer_callback_query(query_id, "У вас нет прав администратора!")
            return
        
        await self.answer_callback_query(query_id)
        
        if data == "welcome_menu":
            await self.show_welcome_menu(message)
        elif data == "accept_menu":
            await self.show_accept_menu(message)
        elif data == "settings_menu":
            await self.show_settings_menu(message)
        elif data == "statistics":
            await self.show_statistics(message)
        elif data == "broadcast":
            # Включаем режим ожидания рассылки и просим прислать контент
            self.waiting_for_broadcast = True
            await self.edit_message(
                message["chat"]["id"],
                message["message_id"],
                "📢 <b>Рассылка</b>\n\nОтправьте сообщение с любым форматированием (жирный, курсив, ссылки и т.д.) — я сохраню всё форматирование!",
                {"inline_keyboard": [[{"text": "⬅️ Назад", "callback_data": "back_to_main"}]]}
            )
        elif data == "confirm_broadcast":
            # Подтверждение отправки рассылки
            await self.execute_broadcast(message)
        elif data == "cancel_broadcast":
            # Отмена рассылки
            self.broadcast_preview_data = None
            await self.edit_message(
                message["chat"]["id"],
                message["message_id"],
                "❌ Рассылка отменена.",
                {"inline_keyboard": [[{"text": "🔧 Админ панель", "callback_data": "back_to_main"}]]}
            )
        elif data == "back_to_main":
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
            await self.show_welcome_edit_menu(message, data)
        elif data.startswith("welcome_preview_"):
            await self.show_welcome_preview(message, data)
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

    async def send_welcome_to_user(self, user):
        """Отправить приветствие пользователю"""
        if not self.welcome_messages:
            logger.info("Нет приветственных сообщений")
            return
        
        # Проверяем, не отправляли ли уже приветствие этому пользователю
        if user["id"] in self.sent_welcomes:
            logger.info(f"Приветствие уже отправлено пользователю {user['id']} - пропускаем")
            return
        
        # Ищем активные сообщения
        active_messages = [msg for msg in self.welcome_messages if msg.get("active", False)]
        if not active_messages:
            # Если нет активных, берем первое сообщение (для обратной совместимости)
            if self.welcome_messages:
                active_messages = [self.welcome_messages[0]]
                logger.info(f"Используем первое приветствие (неактивное) для пользователя {user['id']}")
            else:
                logger.info("Нет приветственных сообщений для отправки")
                return
        else:
            logger.info(f"Найдено {len(active_messages)} активных приветствий для пользователя {user['id']}")
        
        # Отправляем ВСЕ активные приветствия с индивидуальными задержками
        for i, welcome_msg in enumerate(active_messages):
            logger.info(f"Отправляем приветствие {i+1}/{len(active_messages)} пользователю {user['id']}")
            # Получаем задержку для каждого приветствия индивидуально
            delay = welcome_msg.get("delay", 0)
            if delay > 0:
                logger.info(f"⏰ Применяем задержку {delay} секунд к приветствию {i+1} перед отправкой")
                await asyncio.sleep(delay)
            else:
                logger.info(f"🚀 Отправляем приветствие {i+1} без задержки")

            await self.send_single_welcome(user, welcome_msg)

            # Задержка между приветствиями (кроме последнего)
            if i < len(active_messages) - 1:
                logger.info(f"⏳ Пауза 1 секунда между приветствиями {i+1} и {i+2}")
                await asyncio.sleep(1)
        
        # Помечаем, что приветствие отправлено этому пользователю
        self.sent_welcomes.add(user["id"])
        logger.info(f"Пользователь {user['id']} добавлен в sent_welcomes")

    async def send_single_welcome(self, user, welcome_msg):
        """Отправить одно приветственное сообщение"""
        
        # Создаем URL кнопки если есть
        reply_markup = None
        if welcome_msg.get("url_buttons"):
            keyboard = []
            for button in welcome_msg["url_buttons"]:
                keyboard.append([{"text": button["text"], "url": button["url"]}])
            reply_markup = {"inline_keyboard": keyboard}
        
        # Отправляем сообщение в зависимости от типа
        try:
            if welcome_msg["type"] == "text":
                await self.send_message_with_markup(user["id"], welcome_msg["text"], reply_markup)
                logger.info(f"Отправлено текстовое приветствие пользователю {user['id']}")
            elif welcome_msg["type"] == "photo":
                await self.send_photo_with_markup(user["id"], welcome_msg["photo_id"], welcome_msg["text"], reply_markup)
                logger.info(f"Отправлено фото приветствие пользователю {user['id']}")
            elif welcome_msg["type"] == "document":
                await self.send_document_with_markup(user["id"], welcome_msg["document_id"], welcome_msg["text"], reply_markup)
                logger.info(f"Отправлено документ приветствие пользователю {user['id']}")
            elif welcome_msg["type"] == "video":
                await self.send_video_with_markup(user["id"], welcome_msg["video_id"], welcome_msg["text"], reply_markup)
                logger.info(f"Отправлено видео приветствие пользователю {user['id']}")
        except Exception as e:
            logger.error(f"Ошибка отправки приветствия пользователю {user['id']}: {e}")

    async def send_message_with_markup(self, chat_id, text, reply_markup=None):
        """Отправить сообщение с разметкой"""
        url = f"{self.base_url}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def send_photo_with_markup(self, chat_id, photo_id, caption=None, reply_markup=None):
        """Отправить фото с разметкой"""
        url = f"{self.base_url}/sendPhoto"
        data = {
            "chat_id": chat_id,
            "photo": photo_id
        }
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def send_document_with_markup(self, chat_id, document_id, caption=None, reply_markup=None):
        """Отправить документ с разметкой"""
        url = f"{self.base_url}/sendDocument"
        data = {
            "chat_id": chat_id,
            "document": document_id
        }
        if caption:
            data["caption"] = caption
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

    async def show_welcome_menu(self, message):
        """Показать меню приветствий"""
        text = "📝 <b>Управление приветствиями</b>\n\n"
        
        keyboard_buttons = []
        
        if self.welcome_messages:
            text += "Выберите приветствие для редактирования:\n\n"
            for i, msg in enumerate(self.welcome_messages):
                delay_text = f" (⏱️{msg.get('delay', 0)}с)" if msg.get('delay', 0) > 0 else ""
                type_emoji = "📝" if msg['type'] == 'text' else "📷" if msg['type'] == 'photo' else "📄"
                preview = msg['text'][:30] if msg['text'] else f"{msg['type'].upper()}"
                
                keyboard_buttons.append([{"text": f"{type_emoji} {i+1}. {preview}...{delay_text}", "callback_data": f"welcome_edit_{i}"}])
        else:
            text += "Приветственные сообщения не настроены.\n"
        
        keyboard_buttons.extend([
            [{"text": "➕ Добавить новое", "callback_data": "welcome_add"}],
            [{"text": "⬅️ Назад", "callback_data": "back_to_main"}]
        ])
        
        keyboard = {"inline_keyboard": keyboard_buttons}
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def show_statistics(self, message):
        """Показать статистику"""
        text = "📊 <b>Статистика</b>\n\n"
        text += f"Всего пользователей: {len(self.welcome_messages)}\n"
        text += f"Подписок сегодня: 0\n"
        text += f"Подписок за неделю: 0\n"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "⬅️ Назад", "callback_data": "back_to_main"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def start_broadcast(self, message):
        """Начать рассылку"""
        text = "📢 <b>Рассылка</b>\n\nОтправьте сообщение для рассылки:"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "back_to_main"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def show_broadcast_preview(self, chat_id):
        """Показать предпросмотр рассылки"""
        if not self.broadcast_preview_data:
            return
        
        # Подсчитываем количество получателей
        # Для демо используем админов, в реальности это будут все подписчики
        targets = list(self.subscribers) if self.subscribers else self.admin_ids
        recipient_count = len(targets)
        
        preview_text = f"📢 <b>Предпросмотр рассылки</b>\n\n"
        preview_text += f"👥 Получателей: <b>{recipient_count}</b>\n\n"
        preview_text += "━━━━━━━━━━━━━━━━━━━━\n"
        
        data = self.broadcast_preview_data
        
        # Отправляем предпросмотр в зависимости от типа
        try:
            if data['type'] == 'text':
                preview_text += f"{data['text']}\n"
                preview_text += "━━━━━━━━━━━━━━━━━━━━\n\n"
                preview_text += "Проверьте форматирование и нажмите кнопку для отправки."
                
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ Отправить рассылку", "callback_data": "confirm_broadcast"}],
                        [{"text": "❌ Отменить", "callback_data": "cancel_broadcast"}]
                    ]
                }
                await self.send_message(chat_id, preview_text, keyboard)
                
            elif data['type'] == 'media_group':
                # Отправляем группу фото
                media_list = []
                for i, photo in enumerate(data['photos']):
                    media_item = {
                        "type": "photo",
                        "media": photo['photo_id']
                    }
                    # Добавляем подпись только к первому фото
                    if i == 0 and photo['caption']:
                        media_item["caption"] = photo['caption']
                        media_item["parse_mode"] = "HTML"
                    media_list.append(media_item)
                
                await self.send_media_group(chat_id, media_list)
                
                # Отправляем кнопки отдельным сообщением
                button_text = f"{preview_text}Отправлено {len(data['photos'])} фото\n"
                button_text += "━━━━━━━━━━━━━━━━━━━━\n\n"
                button_text += "Проверьте и нажмите кнопку для отправки."
                
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ Отправить рассылку", "callback_data": "confirm_broadcast"}],
                        [{"text": "❌ Отменить", "callback_data": "cancel_broadcast"}]
                    ]
                }
                await self.send_message(chat_id, button_text, keyboard)
                
            elif data['type'] == 'photo':
                caption = f"{preview_text}"
                if data['caption']:
                    caption += f"\n{data['caption']}\n"
                caption += "━━━━━━━━━━━━━━━━━━━━\n\n"
                caption += "Проверьте форматирование и нажмите кнопку для отправки."
                
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ Отправить рассылку", "callback_data": "confirm_broadcast"}],
                        [{"text": "❌ Отменить", "callback_data": "cancel_broadcast"}]
                    ]
                }
                
                url = f"{self.base_url}/sendPhoto"
                send_data = {
                    "chat_id": chat_id,
                    "photo": data['photo_id'],
                    "caption": caption,
                    "parse_mode": "HTML",
                    "reply_markup": json.dumps(keyboard)
                }
                await self.make_request_with_retry('POST', url, send_data)
                
            elif data['type'] == 'video':
                caption = f"{preview_text}"
                if data['caption']:
                    caption += f"\n{data['caption']}\n"
                caption += "━━━━━━━━━━━━━━━━━━━━\n\n"
                caption += "Проверьте форматирование и нажмите кнопку для отправки."
                
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ Отправить рассылку", "callback_data": "confirm_broadcast"}],
                        [{"text": "❌ Отменить", "callback_data": "cancel_broadcast"}]
                    ]
                }
                
                url = f"{self.base_url}/sendVideo"
                send_data = {
                    "chat_id": chat_id,
                    "video": data['video_id'],
                    "caption": caption,
                    "parse_mode": "HTML",
                    "reply_markup": json.dumps(keyboard)
                }
                await self.make_request_with_retry('POST', url, send_data)
                
            elif data['type'] == 'document':
                caption = f"{preview_text}"
                if data['caption']:
                    caption += f"\n{data['caption']}\n"
                caption += "━━━━━━━━━━━━━━━━━━━━\n\n"
                caption += "Проверьте форматирование и нажмите кнопку для отправки."
                
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "✅ Отправить рассылку", "callback_data": "confirm_broadcast"}],
                        [{"text": "❌ Отменить", "callback_data": "cancel_broadcast"}]
                    ]
                }
                
                url = f"{self.base_url}/sendDocument"
                send_data = {
                    "chat_id": chat_id,
                    "document": data['document_id'],
                    "caption": caption,
                    "parse_mode": "HTML",
                    "reply_markup": json.dumps(keyboard)
                }
                await self.make_request_with_retry('POST', url, send_data)
                
        except Exception as e:
            logger.error(f"Ошибка показа предпросмотра: {e}")
            await self.send_message(chat_id, f"❌ Ошибка показа предпросмотра: {e}")

    async def execute_broadcast(self, message):
        """Выполнить рассылку"""
        if not self.broadcast_preview_data:
            await self.send_message(message["chat"]["id"], "❌ Нет данных для рассылки")
            return
        
        data = self.broadcast_preview_data
        targets = list(self.subscribers) if self.subscribers else self.admin_ids
        
        # Уведомляем о начале рассылки
        await self.edit_message(
            message["chat"]["id"],
            message["message_id"],
            f"📤 Отправка рассылки {len(targets)} получателям...",
            None
        )
        
        success_count = 0
        fail_count = 0
        
        try:
            for uid in targets:
                try:
                    if data['type'] == 'text':
                        await self.send_message(uid, data['text'])
                    elif data['type'] == 'media_group':
                        # Отправляем группу фото
                        media_list = []
                        for i, photo in enumerate(data['photos']):
                            media_item = {
                                "type": "photo",
                                "media": photo['photo_id']
                            }
                            # Добавляем подпись только к первому фото
                            if i == 0 and photo['caption']:
                                media_item["caption"] = photo['caption']
                                media_item["parse_mode"] = "HTML"
                            media_list.append(media_item)
                        await self.send_media_group(uid, media_list)
                    elif data['type'] == 'photo':
                        await self.send_photo(uid, data['photo_id'], data['caption'])
                    elif data['type'] == 'video':
                        await self.send_video(uid, data['video_id'], data['caption'])
                    elif data['type'] == 'document':
                        await self.send_document(uid, data['document_id'], data['caption'])
                    
                    success_count += 1
                    # Небольшая задержка между отправками
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.error(f"Ошибка отправки пользователю {uid}: {e}")
                    fail_count += 1
            
            # Отчет о рассылке
            result_text = f"✅ <b>Рассылка завершена!</b>\n\n"
            result_text += f"📤 Отправлено: {success_count}\n"
            if fail_count > 0:
                result_text += f"❌ Ошибок: {fail_count}\n"
            
            keyboard = {
                "inline_keyboard": [
                    [{"text": "🔧 Админ панель", "callback_data": "back_to_main"}]
                ]
            }
            
            await self.edit_message(
                message["chat"]["id"],
                message["message_id"],
                result_text,
                keyboard
            )
            
        except Exception as e:
            logger.error(f"Ошибка выполнения рассылки: {e}")
            await self.edit_message(
                message["chat"]["id"],
                message["message_id"],
                f"❌ Ошибка рассылки: {e}",
                {"inline_keyboard": [[{"text": "🔧 Админ панель", "callback_data": "back_to_main"}]]}
            )
        finally:
            # Очищаем данные предпросмотра
            self.broadcast_preview_data = None

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def start_add_welcome(self, message):
        """Начать добавление приветствия"""
        self.waiting_for_welcome_text = True
        text = "📝 <b>Добавление приветствия</b>\n\nОтправьте текст, фото, документ или видео для приветствия:"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "welcome_menu"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        """Парсинг пользовательского ввода задержки"""
        try:
            delay_text = delay_text.strip().lower()
            total_seconds = 0
            
            # Обработка различных форматов
            import re
            
            # Секунды: 30, 60, 120
            if re.match(r'^\d+$', delay_text):
                return int(delay_text)
            
            # Минуты: 2м, 5м, 10м
            if delay_text.endswith('м'):
                minutes = int(delay_text[:-1])
                return minutes * 60
            
            # Часы: 1ч, 2ч, 24ч
            if delay_text.endswith('ч'):
                hours = int(delay_text[:-1])
                return hours * 3600
            
            # Комбинированный: 1ч 30м, 2ч 15м 30с
            parts = delay_text.split()
            for part in parts:
                if part.endswith('ч'):
                    hours = int(part[:-1])
                    total_seconds += hours * 3600
                elif part.endswith('м'):
                    minutes = int(part[:-1])
                    total_seconds += minutes * 60
                elif part.endswith('с'):
                    seconds = int(part[:-1])
                    total_seconds += seconds
                else:
                    # Просто число - считаем секундами
                    total_seconds += int(part)
            
            return total_seconds if total_seconds > 0 else None
            
        except:
            return None

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
        text += "• Часы: 1ч, 2ч, 3ч\n"
        text += "• Комбинированный: 1ч 30м, 2ч 15м 30с\n\n"
        text += "Примеры: 30, 2м, 1ч, 1ч 30м"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": f"welcome_delay_{welcome_id}"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
            
            if delay_seconds > 86400:  # Максимум 24 часа для приветствий
                await self.send_message(message["chat"]["id"], "❌ Максимальная задержка для приветствий: 24 часа.")
                return
            
            welcome_id = self.current_welcome_delay_id
            if welcome_id is not None and welcome_id < len(self.welcome_messages):
                self.welcome_messages[welcome_id]["delay"] = delay_seconds
                
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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def accept_all_users(self, message):
        """Принять всех пользователей"""
        if not self.pending_requests:
            text = "❌ В очереди нет заявок."
        else:
            # Принимаем всех пользователей
            accepted_count = 0
            for req in self.pending_requests:
                try:
                    await self.approve_chat_join_request(self.channel_id, req["user_id"])
                    # Добавляем в подписчики
                    self.add_subscriber(req["user_id"])
                    # Приветствие уже отправлено при подаче заявки
                    accepted_count += 1
                except Exception as e:
                    logger.error(f"Ошибка приема пользователя {req['user_id']}: {e}")
            
            # Очищаем все списки заявок
            self.pending_requests = []
            self.user_requests = {}
            self.sent_welcomes = set()
            text = f"✅ Принято {accepted_count} пользователей."
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def start_manual_accept_count(self, message):
        """Начать ручной прием по количеству"""
        text = "📊 <b>Прием по количеству</b>\n\nВведите количество пользователей для приема:"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def start_manual_accept_date(self, message):
        """Начать ручной прием по дате"""
        self.waiting_for_date = True
        text = "📅 <b>Прием по дате</b>\n\nВведите дату в формате ДД.ММ.ГГГГ (например, 15.12.2024):"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def start_manual_accept_link(self, message):
        """Начать ручной прием по ссылке"""
        self.waiting_for_link = True
        text = "🔗 <b>Прием по ссылке</b>\n\nВведите ссылку-приглашение (например, https://t.me/+ABC123):"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "❌ Отмена", "callback_data": "accept_menu"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
        """Обработка ввода ссылки для ручного приема"""
        # Находим заявки с этой ссылкой
        matching_requests = [req for req in self.pending_requests if req["invite_link"] == link_text]
        
        if not matching_requests:
            await self.send_message(message["chat"]["id"], f"❌ Заявок по ссылке {link_text} не найдено.")
            return
        
        # Принимаем всех с этой ссылкой
        accepted_count = 0
        for req in matching_requests:
            try:
                await self.approve_chat_join_request(self.channel_id, req["user_id"])
                # Добавляем в подписчики
                self.add_subscriber(req["user_id"])
                await self.send_welcome_to_user({"id": req["user_id"]})
                accepted_count += 1
            except Exception as e:
                logger.error(f"Ошибка приема пользователя {req['user_id']}: {e}")
        
        # Удаляем принятых из всех списков
        self.pending_requests = [req for req in self.pending_requests if req["invite_link"] != link_text]
        for req in matching_requests:
            if req["user_id"] in self.user_requests:
                del self.user_requests[req["user_id"]]
            if req["user_id"] in self.sent_welcomes:
                self.sent_welcomes.remove(req["user_id"])
        
        await self.send_message(message["chat"]["id"], f"✅ Принято {accepted_count} пользователей по ссылке {link_text}")

    async def handle_date_accept_all(self, message, data):
        """Принять всех пользователей с указанной даты"""
        date_str = data.replace("date_accept_all_", "")
        
        # Находим заявки с этой датой
        matching_requests = [req for req in self.pending_requests if req["date"] == date_str]
        
        if not matching_requests:
            await self.send_message(message["chat"]["id"], f"❌ Заявок на дату {date_str} не найдено.")
            return
        
        # Принимаем всех с этой датой
        accepted_count = 0
        for req in matching_requests:
            try:
                await self.approve_chat_join_request(self.channel_id, req["user_id"])
                # Добавляем в подписчики
                self.add_subscriber(req["user_id"])
                await self.send_welcome_to_user({"id": req["user_id"]})
                accepted_count += 1
            except Exception as e:
                logger.error(f"Ошибка приема пользователя {req['user_id']}: {e}")
        
        # Удаляем принятых из всех списков
        self.pending_requests = [req for req in self.pending_requests if req["date"] != date_str]
        for req in matching_requests:
            if req["user_id"] in self.user_requests:
                del self.user_requests[req["user_id"]]
            if req["user_id"] in self.sent_welcomes:
                self.sent_welcomes.remove(req["user_id"])
        
        await self.send_message(message["chat"]["id"], f"✅ Принято {accepted_count} пользователей с даты {date_str}")

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
        """Обработка ввода количества для приема по дате"""
        try:
            count = int(count_text)
            max_count = self.current_schedule_data["max_count"]
            date_str = self.current_schedule_data["date"]
            
            if count <= 0 or count > max_count:
                await self.send_message(message["chat"]["id"], f"❌ Количество должно быть от 1 до {max_count}.")
                return
            
            # Находим заявки с этой датой
            matching_requests = [req for req in self.pending_requests if req["date"] == date_str]
            
            # Принимаем указанное количество
            accepted_count = 0
            for i, req in enumerate(matching_requests[:count]):
                try:
                    await self.approve_chat_join_request(self.channel_id, req["user_id"])
                    # Добавляем в подписчики
                    self.add_subscriber(req["user_id"])
                    # Приветствие уже отправлено при подаче заявки
                    accepted_count += 1
                except Exception as e:
                    logger.error(f"Ошибка приема пользователя {req['user_id']}: {e}")
            
            # Удаляем принятых из всех списков
            for req in matching_requests[:count]:
                self.pending_requests = [r for r in self.pending_requests if r["user_id"] != req["user_id"]]
                if req["user_id"] in self.user_requests:
                    del self.user_requests[req["user_id"]]
                if req["user_id"] in self.sent_welcomes:
                    self.sent_welcomes.remove(req["user_id"])
            
            await self.send_message(message["chat"]["id"], f"✅ Принято {accepted_count} пользователей с даты {date_str}")
            
        except ValueError:
            await self.send_message(message["chat"]["id"], "❌ Введите корректное число.")

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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
            
            await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)
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
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def handle_delay_selection(self, message, data):
        """Обработка выбора задержки"""
        delay_text = data.replace("delay_", "")
        delay_seconds = self.parse_delay(delay_text)
        
        if delay_seconds is not None:
            self.auto_delay = delay_seconds
            text = f"✅ Задержка автоприема установлена: {delay_text}"
            
            keyboard = {
                "inline_keyboard": [
                    [{"text": "⬅️ Назад", "callback_data": "accept_menu"}]
                ]
            }
            
            await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

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
                              f"Текст: {text[:100]}{'...' if len(text) > 100 else ''}\n"
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
                              f"Описание: {caption[:100] if caption else 'Без описания'}\n"
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
                              f"Описание: {caption[:100] if caption else 'Без описания'}\n"
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
                              f"Описание: {caption[:100] if caption else 'Без описания'}\n"
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
        
        if welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return
        
        welcome_msg = self.welcome_messages[welcome_id]
        
        # Формируем текст с предпросмотром (без строки Тип)
        text = f"✏️ <b>Редактирование приветствия #{welcome_id + 1}</b>\n\n"
        text += f"Задержка: {welcome_msg.get('delay', 0)} секунд\n"
        text += f"URL кнопки: {'Есть' if welcome_msg.get('url_buttons') else 'Нет'}\n"
        text += f"Статус: {'✅ Активно' if welcome_msg.get('active', True) else '❌ Выключено'}\n\n"
        text += f"📋 <b>Предпросмотр:</b>\n{welcome_msg['text']}"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "👁️ Предпросмотр", "callback_data": f"welcome_preview_{welcome_id}"}],
                [{"text": "⏱️ Задержка", "callback_data": f"welcome_delay_{welcome_id}"}],
                [{"text": "🔗 URL кнопки", "callback_data": f"welcome_buttons_{welcome_id}"}],
                [{"text": "🔄 Вкл/Выкл", "callback_data": f"welcome_toggle_{welcome_id}"}],
                [{"text": "🗑️ Удалить", "callback_data": f"welcome_delete_{welcome_id}"}],
                [{"text": "⬅️ Назад", "callback_data": "welcome_menu"}]
            ]
        }
        
        # Сначала показываем предпросмотр с кнопками (фото/документ если есть)
        if welcome_msg["type"] == "photo":
            # Создаем URL кнопки если есть
            reply_markup = None
            if welcome_msg.get("url_buttons"):
                keyboard_preview = []
                for button in welcome_msg["url_buttons"]:
                    keyboard_preview.append([{"text": button["text"], "url": button["url"]}])
                reply_markup = {"inline_keyboard": keyboard_preview}
            
            await self.send_photo_with_markup(message["chat"]["id"], welcome_msg["photo_id"], 
                                            f"📋 <b>Предпросмотр:</b>\n{welcome_msg['text']}", reply_markup)
        elif welcome_msg["type"] == "document":
            # Создаем URL кнопки если есть
            reply_markup = None
            if welcome_msg.get("url_buttons"):
                keyboard_preview = []
                for button in welcome_msg["url_buttons"]:
                    keyboard_preview.append([{"text": button["text"], "url": button["url"]}])
                reply_markup = {"inline_keyboard": keyboard_preview}
            
            await self.send_document_with_markup(message["chat"]["id"], welcome_msg["document_id"], 
                                               f"📋 <b>Предпросмотр:</b>\n{welcome_msg['text']}", reply_markup)
        
        # Затем отправляем меню редактирования БЕЗ предпросмотра
        menu_text = f"✏️ <b>Редактирование приветствия #{welcome_id + 1}</b>\n\n"
        menu_text += f"Задержка: {welcome_msg.get('delay', 0)} секунд\n"
        menu_text += f"URL кнопки: {'Есть' if welcome_msg.get('url_buttons') else 'Нет'}\n"
        menu_text += f"Статус: {'✅ Активно' if welcome_msg.get('active', True) else '❌ Выключено'}"
        
        await self.send_message(message["chat"]["id"], menu_text, keyboard)

    async def send_welcome_preview(self, chat_id, welcome_msg):
        """Отправить предпросмотр приветствия"""
        try:
            if welcome_msg["type"] == "text":
                preview_text = f"📋 <b>Предпросмотр:</b>\n\n{welcome_msg['text']}"
                await self.send_message(chat_id, preview_text)
            elif welcome_msg["type"] == "photo":
                await self.send_photo(chat_id, welcome_msg["photo_id"], 
                                    f"📋 <b>Предпросмотр фото:</b>\n\n{welcome_msg['text']}")
            elif welcome_msg["type"] == "document":
                await self.send_document(chat_id, welcome_msg["document_id"], 
                                       f"📋 <b>Предпросмотр файла:</b>\n\n{welcome_msg['text']}")
        except Exception as e:
            await self.send_message(chat_id, f"❌ Ошибка предпросмотра: {e}")

    async def send_welcome_preview_with_id(self, chat_id, welcome_msg):
        """Отправить предпросмотр приветствия и вернуть response с ID"""
        try:
            # Создаем URL кнопки если есть
            reply_markup = None
            if welcome_msg.get("url_buttons"):
                keyboard = []
                for button in welcome_msg["url_buttons"]:
                    keyboard.append([{"text": button["text"], "url": button["url"]}])
                reply_markup = {"inline_keyboard": keyboard}
            
            if welcome_msg["type"] == "text":
                preview_text = f"📋 <b>Предпросмотр:</b>\n\n{welcome_msg['text']}"
                return await self.send_message_with_markup(chat_id, preview_text, reply_markup)
            elif welcome_msg["type"] == "photo":
                caption = f"📋 <b>Предпросмотр фото:</b>\n\n{welcome_msg['text']}"
                return await self.send_photo_with_markup(chat_id, welcome_msg["photo_id"], caption, reply_markup)
            elif welcome_msg["type"] == "document":
                caption = f"📋 <b>Предпросмотр файла:</b>\n\n{welcome_msg['text']}"
                return await self.send_document_with_markup(chat_id, welcome_msg["document_id"], caption, reply_markup)
            elif welcome_msg["type"] == "video":
                caption = f"📋 <b>Предпросмотр видео:</b>\n\n{welcome_msg['text']}"
                return await self.send_video_with_markup(chat_id, welcome_msg["video_id"], caption, reply_markup)
        except Exception as e:
            return await self.send_message(chat_id, f"❌ Ошибка предпросмотра: {e}")

    async def show_welcome_preview(self, message, data):
        """Показать предпросмотр приветствия"""
        try:
            welcome_id = int(data.replace("welcome_preview_", ""))
        except ValueError:
            welcome_id = 0
        
        if welcome_id >= len(self.welcome_messages):
            await self.answer_callback_query(message.get("message_id", ""), "❌ Приветствие не найдено!")
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
        
        text = "⏱️ <b>Выберите задержку отправки</b>"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "0 сек", "callback_data": f"set_delay_{welcome_id}_0"}, 
                 {"text": "30 сек", "callback_data": f"set_delay_{welcome_id}_30"}],
                [{"text": "1 мин", "callback_data": f"set_delay_{welcome_id}_60"}, 
                 {"text": "2 мин", "callback_data": f"set_delay_{welcome_id}_120"}],
                [{"text": "5 мин", "callback_data": f"set_delay_{welcome_id}_300"}, 
                 {"text": "10 мин", "callback_data": f"set_delay_{welcome_id}_600"}],
                [{"text": "30 мин", "callback_data": f"set_delay_{welcome_id}_1800"}, 
                 {"text": "1 час", "callback_data": f"set_delay_{welcome_id}_3600"}],
                [{"text": "✏️ Ввести свое", "callback_data": f"welcome_delay_custom_{welcome_id}"}],
                [{"text": "⬅️ Назад", "callback_data": f"welcome_edit_{welcome_id}"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def set_welcome_delay(self, message, data):
        """Установить задержку для приветствия"""
        parts = data.replace("set_delay_", "").split("_")
        welcome_id = int(parts[0])
        delay = int(parts[1])
        
        if welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return
        
        self.welcome_messages[welcome_id]["delay"] = delay
        self.save_welcome_messages()
        
        # Показываем обновленное меню редактирования
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")


    async def delete_welcome_message(self, message, data):
        """Удалить приветствие"""
        try:
            welcome_id = int(data.replace("welcome_delete_", ""))
        except ValueError:
            welcome_id = 0
        
        if welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return
        
        del self.welcome_messages[welcome_id]
        self.save_welcome_messages()
        
        await self.edit_message(message["chat"]["id"], message["message_id"], 
                              "✅ Приветствие удалено!", 
                              {"inline_keyboard": [[{"text": "⬅️ К приветствиям", "callback_data": "welcome_menu"}]]})

    async def toggle_welcome_message(self, message, data):
        """Включить/выключить приветствие"""
        try:
            welcome_id = int(data.replace("welcome_toggle_", ""))
        except ValueError:
            welcome_id = 0
        
        if welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return
        
        welcome_msg = self.welcome_messages[welcome_id]
        welcome_msg["active"] = not welcome_msg.get("active", True)
        self.save_welcome_messages()
        
        status = "включено" if welcome_msg["active"] else "выключено"
        
        # Отправляем уведомление
        await self.send_message(message["chat"]["id"], f"✅ Приветствие {status}!")
        
        # Показываем обновленное меню редактирования
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")

    async def clear_welcome_buttons(self, message, data):
        """Очистить URL кнопки приветствия"""
        try:
            welcome_id = int(data.replace("clear_buttons_", ""))
        except ValueError:
            welcome_id = 0
        
        if welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return
        
        self.welcome_messages[welcome_id]["url_buttons"] = []
        self.save_welcome_messages()
        
        # Отправляем уведомление
        await self.send_message(message["chat"]["id"], "✅ URL кнопки очищены!")
        
        # Возвращаемся к редактированию
        await self.show_welcome_edit_menu(message, f"welcome_edit_{welcome_id}")

    async def handle_url_buttons_input(self, message, text):
        """Обработка ввода URL кнопок"""
        welcome_id = self.current_editing_id
        
        if welcome_id is None or welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Ошибка: приветствие не найдено!")
            self.waiting_for_buttons = False
            return
        
        # Парсим кнопки
        url_buttons = []
        lines = text.split('\n')
        for line in lines:
            if '|' in line:
                parts = line.split('|', 1)
                if len(parts) == 2:
                    button_text = parts[0].strip()
                    button_url = parts[1].strip()
                    url_buttons.append({"text": button_text, "url": button_url})
        
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
        
        if welcome_id >= len(self.welcome_messages):
            await self.send_message(message["chat"]["id"], "❌ Приветствие не найдено!")
            return
        
        welcome_msg = self.welcome_messages[welcome_id]
        buttons = welcome_msg.get("url_buttons", [])
        
        text = f"🔗 <b>URL кнопки для приветствия #{welcome_id + 1}</b>\n\n"
        
        if buttons:
            text += "Текущие кнопки:\n"
            for i, btn in enumerate(buttons):
                text += f"{i+1}. {btn['text']} → {btn['url']}\n"
        else:
            text += "URL кнопки не настроены.\n"
        
        text += "\n💡 Отправьте кнопки в формате:\nТекст кнопки | URL\nТекст кнопки 2 | URL2"
        
        # Активируем режим ожидания кнопок
        self.waiting_for_buttons = True
        self.current_editing_id = welcome_id
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🗑️ Очистить все", "callback_data": f"clear_buttons_{welcome_id}"}],
                [{"text": "⬅️ Назад", "callback_data": f"welcome_edit_{welcome_id}"}]
            ]
        }
        
        await self.edit_message(message["chat"]["id"], message["message_id"], text, keyboard)

    async def check_schedule(self):
        """Проверка и выполнение расписания"""
        if not self.schedules:
            return
        
        from datetime import datetime
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        current_weekday = now.weekday()  # 0=Понедельник, 6=Воскресенье
        
        # Проверяем все расписания
        for day_data, schedule in self.schedules.items():
            if not schedule.get("active", False):
                continue
            
            # Проверяем время
            if current_time != schedule["time"]:
                continue
            
            # Проверяем день недели
            if day_data != "daily":
                # Конвертируем в наш формат (0=Воскресенье, 1=Понедельник...)
                if day_data == "0":  # Воскресенье
                    target_weekday = 6
                else:
                    target_weekday = int(day_data) - 1
                
                if current_weekday != target_weekday:
                    continue
            
            # Выполняем прием для этого дня
            count = schedule["count"]
            if count == "all":
                target_count = len(self.pending_requests)
            else:
                target_count = min(int(count), len(self.pending_requests))
            
            if target_count == 0:
                continue
            
            # Принимаем пользователей
            accepted_count = 0
            for i, req in enumerate(self.pending_requests[:target_count]):
                try:
                    await self.approve_chat_join_request(self.channel_id, req["user_id"])
                    # Добавляем в подписчики
                    self.add_subscriber(req["user_id"])
                    # Приветствие уже отправлено при подаче заявки
                    accepted_count += 1
                except Exception as e:
                    logger.error(f"Ошибка приема пользователя {req['user_id']}: {e}")
            
            # Удаляем принятых из всех списков
            for i in range(target_count):
                if i < len(self.pending_requests):
                    req = self.pending_requests[i]
                    if req["user_id"] in self.user_requests:
                        del self.user_requests[req["user_id"]]
                    if req["user_id"] in self.sent_welcomes:
                        self.sent_welcomes.remove(req["user_id"])
            self.pending_requests = self.pending_requests[target_count:]
            
            # Уведомляем админа
            if accepted_count > 0:
                day_name = "Ежедневно" if day_data == "daily" else ["Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"][int(day_data)]
                for admin_id in self.admin_ids:
                    await self.send_message(admin_id, f"⏰ <b>Расписание выполнено!</b>\n\nДень: {day_name}\nВремя: {schedule['time']}\nПринято пользователей: {accepted_count}")
            
            logger.info(f"Расписание выполнено для {day_data}: принято {accepted_count} пользователей")

    async def send_video_with_markup(self, chat_id, video_id, caption, reply_markup=None):
        """Отправить видео с разметкой"""
        url = f"{self.base_url}/sendVideo"
        data = {
            "chat_id": chat_id,
            "video": video_id,
            "caption": caption,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)

        return await self.make_request_with_retry('POST', url, data)

    async def run(self):
        """Запуск бота"""
        logger.info("Бот запущен!")
        self.is_running = True

        # Инициализируем HTTP сессию
        await self.initialize_session()

        offset = None

        while self.is_running:
            try:
                updates = await self.get_updates(offset)
                
                if not updates.get("ok"):
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
                            current_time = datetime.now()
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

                            # Логируем успешное добавление заявки
                            logger.info(f"Новая заявка от @{user.get('username', user['id'])} (всего: {len(self.pending_requests)})")

                            # Отправляем приветствие асинхронно без блокировки основного цикла
                            try:
                                asyncio.create_task(self.send_welcome_to_user({"id": user["id"]}))
                            except Exception as e:
                                logger.error(f"Ошибка отправки приветствия пользователю {user['id']}: {e}")

                            # Автоприем при включенном режиме (асинхронно)
                            if self.auto_accept:
                                delay = self.auto_delay if self.auto_delay else 0
                                if delay > 0:
                                    # Создаем задачу с задержкой для автоприема
                                    asyncio.create_task(self.delayed_auto_accept(user["id"], delay))
                                else:
                                    # Немедленный автоприем
                                    asyncio.create_task(self.approve_chat_join_request(self.channel_id, user["id"]))
                                    # Добавляем в подписчики
                                    self.add_subscriber(user["id"])
                                    # Удаляем из списков после одобрения
                                    self._remove_request_safely(user["id"])
                            else:
                                logger.debug(f"Заявка сохранена в режиме ручного приема: {user['id']}")

                    elif "callback_query" in update:
                        await self.handle_callback_query(update["callback_query"])
                
                # Проверяем расписание
                await self.check_schedule()
                
                # Оптимизированный интервал для стабильной работы
                await asyncio.sleep(1.5)
                
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}")
                await asyncio.sleep(5)

        # Очищаем ресурсы при завершении
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
        except Exception as e:
            logger.error(f"Ошибка удаления заявки {user_id}: {e}")

    async def delayed_auto_accept(self, user_id, delay):
        """Выполнить автоприем с задержкой асинхронно"""
        try:
            await asyncio.sleep(delay)
            # Проверяем, не была ли уже обработана заявка
            if user_id in self.user_requests:
                await self.approve_chat_join_request(self.channel_id, user_id)
                # Добавляем в подписчики
                self.add_subscriber(user_id)
                self._remove_request_safely(user_id)
                logger.info(f"Автоприем выполнен для пользователя {user_id} с задержкой {delay}с")
        except Exception as e:
            logger.error(f"Ошибка автоприема пользователя {user_id}: {e}")

if __name__ == "__main__":
    print("🚀 Запуск оптимизированного бота приветствий...")
    print("📊 Проверяем настройки...")

    bot = SimpleBot()

    # Проверяем токен и настройки
    print(f"🔑 Токен: {bot.bot_token[:10]}...")
    print(f"📢 Канал ID: {bot.channel_id}")
    print(f"👤 Админы ID: {bot.admin_ids}")

    # Проверяем приветственные сообщения
    if bot.welcome_messages:
        print(f"✅ Загружено приветствий: {len(bot.welcome_messages)}")
    else:
        print("⚠️  Приветственные сообщения не найдены")

    print("🔧 Настройки автоприема:", "ВКЛ" if bot.auto_accept else "ВЫКЛ")

    try:
        print("🌐 Подключение к Telegram API...")
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n🛑 Получен сигнал завершения...")
        asyncio.run(bot.stop())
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка при запуске: {e}")
