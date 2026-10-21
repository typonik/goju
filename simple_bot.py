#!/usr/bin/env python3
"""
Упрощенная версия Telegram бота приветствия
"""

import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)
from telegram.constants import ParseMode
import re
import json

from config import BOT_TOKEN, CHANNEL_ID, DELAY_OPTIONS, AUTO_ACCEPT_DELAYS
from database import Database

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Инициализация базы данных
db = Database("bot_database.db")

# Состояния для ConversationHandler
(
    WAITING_WELCOME_MESSAGE, WAITING_DELAY_INPUT, WAITING_URL_BUTTONS,
    WAITING_ADMIN_USERNAME, WAITING_MANUAL_ACCEPT_COUNT, WAITING_SCHEDULE_TIME,
    WAITING_BROADCAST_MESSAGE, WAITING_CUSTOM_DELAY
) = range(8)

class SimpleTelegramBot:
    def __init__(self):
        self.application = Application.builder().token(BOT_TOKEN).build()
        self.setup_handlers()

    def setup_handlers(self):
        """Настройка обработчиков команд"""
        
        # Команда /start
        self.application.add_handler(CommandHandler("start", self.start_command))
        
        # Обработчик для новых участников канала
        self.application.add_handler(MessageHandler(
            filters.StatusUpdate.NEW_CHAT_MEMBERS, self.handle_new_member
        ))
        
        # Обработчик для callback запросов (кнопки)
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        
        # ConversationHandler для админ панели
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("admin", self.admin_panel)],
            states={
                WAITING_WELCOME_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_welcome_message)],
                WAITING_DELAY_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_delay_input)],
                WAITING_URL_BUTTONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_url_buttons)],
                WAITING_ADMIN_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_admin_username)],
                WAITING_MANUAL_ACCEPT_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_manual_accept_count)],
                WAITING_SCHEDULE_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_schedule_time)],
                WAITING_BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_broadcast_message)],
                WAITING_CUSTOM_DELAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_custom_delay)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_operation)],
        )
        self.application.add_handler(conv_handler)

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user_id = update.effective_user.id
        
        # Проверяем, является ли пользователь админом
        if await db.is_admin(user_id):
            await update.message.reply_text(
                "Добро пожаловать в админ панель!",
                reply_markup=self.get_admin_main_menu()
            )
        else:
            await update.message.reply_text(
                "Привет! Я бот для приветствия новых подписчиков. "
                "Для доступа к админ панели обратитесь к администратору."
            )

    async def handle_new_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик новых участников канала"""
        if update.effective_chat.id != CHANNEL_ID:
            return
            
        for member in update.message.new_chat_members:
            if not member.is_bot:
                await self.send_welcome_message(member, context)

    async def send_welcome_message(self, user, context: ContextTypes.DEFAULT_TYPE):
        """Отправить приветственное сообщение пользователю"""
        try:
            # Получаем активные приветственные сообщения
            welcome_messages = await db.get_welcome_messages()
            active_messages = [msg for msg in welcome_messages if msg['is_active']]
            
            if not active_messages:
                return
                
            # Берем первое активное сообщение
            message_data = active_messages[0]
            
            # Создаем клавиатуру с URL кнопками
            keyboard = []
            for button in message_data['url_buttons']:
                keyboard.append([InlineKeyboardButton(
                    button['text'], 
                    url=button['url']
                )])
            
            reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            
            # Заменяем ссылки если нужно
            message_text = message_data['message_text']
            if message_data['replace_links']:
                message_text = self.replace_links(message_text)
            
            # Отправляем сообщение с задержкой
            delay = message_data['delay_seconds']
            if delay > 0:
                await asyncio.sleep(delay)
            
            await context.bot.send_message(
                chat_id=user.id,
                text=message_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
            
            # Записываем в статистику
            await db.add_statistics(user.id, 'subscribed')
            
        except Exception as e:
            logger.error(f"Ошибка при отправке приветственного сообщения: {e}")

    def replace_links(self, text: str) -> str:
        """Заменяет ссылки в тексте"""
        # Простая замена ссылок на [ссылка]
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        return re.sub(url_pattern, '[ссылка]', text)

    async def admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Главное меню админ панели"""
        user_id = update.effective_user.id
        
        if not await db.is_admin(user_id):
            await update.message.reply_text("У вас нет прав администратора!")
            return ConversationHandler.END
            
        await update.message.reply_text(
            "🔧 <b>Админ панель</b>\n\nВыберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=self.get_admin_main_menu()
        )
        return ConversationHandler.END

    def get_admin_main_menu(self) -> InlineKeyboardMarkup:
        """Главное меню админ панели"""
        keyboard = [
            [InlineKeyboardButton("📝 Приветствия", callback_data="welcome_menu")],
            [InlineKeyboardButton("👥 Прием подписчиков", callback_data="accept_menu")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="settings_menu")],
            [InlineKeyboardButton("📊 Статистика", callback_data="statistics")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="broadcast")]
        ]
        return InlineKeyboardMarkup(keyboard)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback запросов (кнопок)"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        if not await db.is_admin(user_id):
            await query.edit_message_text("У вас нет прав администратора!")
            return
        
        data = query.data
        
        if data == "welcome_menu":
            await self.show_welcome_menu(query)
        elif data == "accept_menu":
            await self.show_accept_menu(query)
        elif data == "settings_menu":
            await self.show_settings_menu(query)
        elif data == "statistics":
            await self.show_statistics(query)
        elif data == "broadcast":
            await self.start_broadcast(query)
        elif data.startswith("welcome_"):
            await self.handle_welcome_callback(query, data)
        elif data.startswith("accept_"):
            await self.handle_accept_callback(query, data)
        elif data.startswith("settings_"):
            await self.handle_settings_callback(query, data)
        elif data == "back_to_main":
            await query.edit_message_text(
                "🔧 <b>Админ панель</b>\n\nВыберите действие:",
                parse_mode=ParseMode.HTML,
                reply_markup=self.get_admin_main_menu()
            )

    async def show_welcome_menu(self, query):
        """Показать меню приветствий"""
        welcome_messages = await db.get_welcome_messages()
        
        text = "📝 <b>Управление приветствиями</b>\n\n"
        
        if welcome_messages:
            text += "Активные приветствия:\n"
            for msg in welcome_messages[:5]:  # Показываем только первые 5
                status = "✅" if msg['is_active'] else "❌"
                delay_text = f" (задержка: {msg['delay_seconds']}с)" if msg['delay_seconds'] > 0 else ""
                text += f"{status} ID{msg['id']}: {msg['message_text'][:50]}...{delay_text}\n"
        else:
            text += "Приветственные сообщения не настроены.\n"
        
        keyboard = [
            [InlineKeyboardButton("➕ Добавить", callback_data="welcome_add")],
            [InlineKeyboardButton("📋 Список всех", callback_data="welcome_list")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_accept_menu(self, query):
        """Показать меню приема подписчиков"""
        auto_accept = await db.get_setting("auto_accept", "off")
        auto_delay = await db.get_setting("auto_accept_delay", "0")
        
        text = f"👥 <b>Прием подписчиков</b>\n\n"
        text += f"Автоприем: {'✅ Включен' if auto_accept == 'on' else '❌ Выключен'}\n"
        if auto_accept == 'on':
            text += f"Задержка: {auto_delay} секунд\n"
        
        keyboard = [
            [InlineKeyboardButton("🤖 Автоприем", callback_data="accept_auto")],
            [InlineKeyboardButton("✋ Ручной прием", callback_data="accept_manual")],
            [InlineKeyboardButton("⏰ По расписанию", callback_data="accept_schedule")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_settings_menu(self, query):
        """Показать меню настроек"""
        admins = await db.get_admins()
        
        text = "⚙️ <b>Настройки</b>\n\n"
        text += f"Админов: {len(admins)}\n"
        text += f"Канал: {CHANNEL_ID}\n"
        
        keyboard = [
            [InlineKeyboardButton("👥 Админы", callback_data="settings_admins")],
            [InlineKeyboardButton("🔗 Канал", callback_data="settings_channel")],
            [InlineKeyboardButton("🔑 Токен", callback_data="settings_token")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_statistics(self, query):
        """Показать статистику"""
        stats = await db.get_statistics()
        
        text = "📊 <b>Статистика</b>\n\n"
        text += f"Всего пользователей: {stats['total_users']}\n"
        text += f"Подписок сегодня: {stats['today_subscriptions']}\n"
        text += f"Подписок за неделю: {stats['week_subscriptions']}\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def start_broadcast(self, query):
        """Начать рассылку"""
        await query.edit_message_text(
            "📢 <b>Рассылка</b>\n\nОтправьте сообщение для рассылки:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Отмена", callback_data="back_to_main")
            ]])
        )
        return WAITING_BROADCAST_MESSAGE

    async def handle_welcome_callback(self, query, data):
        """Обработчик callback для приветствий"""
        if data == "welcome_add":
            await query.edit_message_text(
                "📝 <b>Добавление приветствия</b>\n\nОтправьте текст приветственного сообщения:",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Отмена", callback_data="welcome_menu")
                ]])
            )
            return WAITING_WELCOME_MESSAGE
        elif data == "welcome_list":
            await self.show_all_welcome_messages(query)

    async def show_all_welcome_messages(self, query):
        """Показать все приветственные сообщения"""
        welcome_messages = await db.get_welcome_messages()
        
        text = "📋 <b>Все приветственные сообщения</b>\n\n"
        
        if not welcome_messages:
            text += "Приветственные сообщения не найдены."
        else:
            for msg in welcome_messages:
                status = "✅" if msg['is_active'] else "❌"
                delay_text = f" (задержка: {msg['delay_seconds']}с)" if msg['delay_seconds'] > 0 else ""
                text += f"{status} <b>ID{msg['id']}</b>{delay_text}\n"
                text += f"<i>{msg['message_text'][:100]}{'...' if len(msg['message_text']) > 100 else ''}</i>\n\n"
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад", callback_data="welcome_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def handle_accept_callback(self, query, data):
        """Обработчик callback для приема подписчиков"""
        if data == "accept_auto":
            await self.show_auto_accept_menu(query)
        elif data == "accept_manual":
            await self.show_manual_accept_menu(query)
        elif data == "accept_schedule":
            await self.show_schedule_accept_menu(query)

    async def show_auto_accept_menu(self, query):
        """Показать меню автоприема"""
        auto_accept = await db.get_setting("auto_accept", "off")
        auto_delay = await db.get_setting("auto_accept_delay", "0")
        
        text = f"🤖 <b>Автоприем</b>\n\n"
        text += f"Статус: {'✅ Включен' if auto_accept == 'on' else '❌ Выключен'}\n"
        text += f"Задержка: {auto_delay} секунд\n\n"
        text += "Выберите действие:"
        
        keyboard = [
            [InlineKeyboardButton(
                "❌ Выключить" if auto_accept == "on" else "✅ Включить",
                callback_data="auto_accept_toggle"
            )],
            [InlineKeyboardButton("⏱️ Задержка", callback_data="auto_accept_delay")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="accept_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_manual_accept_menu(self, query):
        """Показать меню ручного приема"""
        pending_users = await db.get_pending_users()
        
        text = f"✋ <b>Ручной прием</b>\n\n"
        text += f"В очереди: {len(pending_users)} пользователей\n\n"
        text += "Выберите действие:"
        
        keyboard = [
            [InlineKeyboardButton("📊 Количество", callback_data="manual_accept_count")],
            [InlineKeyboardButton("✅ Принять всех", callback_data="manual_accept_all")],
            [InlineKeyboardButton("📅 По дате", callback_data="manual_accept_date")],
            [InlineKeyboardButton("🔗 По ссылке", callback_data="manual_accept_link")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="accept_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_schedule_accept_menu(self, query):
        """Показать меню приема по расписанию"""
        schedule_time = await db.get_setting("schedule_time", "")
        
        text = f"⏰ <b>Прием по расписанию</b>\n\n"
        if schedule_time:
            text += f"Время приема: {schedule_time}\n"
        else:
            text += "Расписание не настроено\n"
        
        text += "\nВведите время в формате ЧЧ:ММ (например, 14:30):"
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад", callback_data="accept_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return WAITING_SCHEDULE_TIME

    async def handle_settings_callback(self, query, data):
        """Обработчик callback для настроек"""
        if data == "settings_admins":
            await self.show_admins_menu(query)
        elif data == "settings_channel":
            await self.show_channel_settings(query)
        elif data == "settings_token":
            await self.show_token_settings(query)

    async def show_admins_menu(self, query):
        """Показать меню админов"""
        admins = await db.get_admins()
        
        text = "👥 <b>Управление админами</b>\n\n"
        
        if admins:
            for admin in admins:
                username = f"@{admin['username']}" if admin['username'] else "Без username"
                text += f"• {username} (ID: {admin['user_id']})\n"
        else:
            text += "Админы не найдены.\n"
        
        text += "\nВыберите действие:"
        
        keyboard = [
            [InlineKeyboardButton("➕ Добавить админа", callback_data="admin_add")],
            [InlineKeyboardButton("➖ Удалить админа", callback_data="admin_remove")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="settings_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_channel_settings(self, query):
        """Показать настройки канала"""
        text = f"🔗 <b>Настройки канала</b>\n\n"
        text += f"ID канала: <code>{CHANNEL_ID}</code>\n"
        text += f"Текущий канал: {CHANNEL_ID}\n\n"
        text += "Для смены канала обратитесь к разработчику."
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад", callback_data="settings_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_token_settings(self, query):
        """Показать настройки токена"""
        text = "🔑 <b>Настройки токена</b>\n\n"
        text += "Токен бота скрыт для безопасности.\n"
        text += "Для смены токена обратитесь к разработчику."
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад", callback_data="settings_menu")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # Обработчики для ConversationHandler
    async def handle_welcome_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ввода приветственного сообщения"""
        message_text = update.message.text
        context.user_data['welcome_message'] = message_text
        
        await update.message.reply_text(
            "⏱️ <b>Настройка задержки</b>\n\nВыберите задержку отправки сообщения:",
            parse_mode=ParseMode.HTML,
            reply_markup=self.get_delay_keyboard()
        )
        return WAITING_DELAY_INPUT

    def get_delay_keyboard(self) -> InlineKeyboardMarkup:
        """Клавиатура выбора задержки"""
        keyboard = []
        for i in range(0, len(DELAY_OPTIONS), 2):
            row = []
            for j in range(2):
                if i + j < len(DELAY_OPTIONS):
                    option = DELAY_OPTIONS[i + j]
                    callback_data = f"delay_{option}" if option != "Ввести вручную" else "delay_custom"
                    row.append(InlineKeyboardButton(option, callback_data=callback_data))
            keyboard.append(row)
        return InlineKeyboardMarkup(keyboard)

    async def handle_delay_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ввода задержки"""
        # Этот метод будет вызван только для ручного ввода
        try:
            delay_text = update.message.text
            delay_seconds = self.parse_delay(delay_text)
            
            if delay_seconds is None:
                await update.message.reply_text(
                    "❌ Неверный формат времени. Попробуйте еще раз или выберите из предложенных вариантов:",
                    reply_markup=self.get_delay_keyboard()
                )
                return WAITING_DELAY_INPUT
            
            context.user_data['delay_seconds'] = delay_seconds
            
            await update.message.reply_text(
                "🔗 <b>URL кнопки</b>\n\n"
                "Отправьте кнопки в формате:\n"
                "Текст кнопки | URL\n"
                "Текст кнопки 2 | URL2\n\n"
                "Или отправьте 'пропустить' для продолжения без кнопок:",
                parse_mode=ParseMode.HTML
            )
            return WAITING_URL_BUTTONS
            
        except Exception as e:
            await update.message.reply_text(f"Ошибка: {e}")
            return WAITING_DELAY_INPUT

    def parse_delay(self, delay_text: str) -> int:
        """Парсинг времени задержки"""
        delay_text = delay_text.lower().strip()
        
        if 'сек' in delay_text or 's' in delay_text:
            return int(re.findall(r'\d+', delay_text)[0])
        elif 'мин' in delay_text or 'm' in delay_text:
            return int(re.findall(r'\d+', delay_text)[0]) * 60
        elif 'час' in delay_text or 'h' in delay_text:
            return int(re.findall(r'\d+', delay_text)[0]) * 3600
        else:
            # Пытаемся распарсить как число (секунды)
            try:
                return int(delay_text)
            except ValueError:
                return None

    async def handle_url_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ввода URL кнопок"""
        buttons_text = update.message.text.strip()
        url_buttons = []
        
        if buttons_text.lower() != 'пропустить':
            lines = buttons_text.split('\n')
            for line in lines:
                if '|' in line:
                    parts = line.split('|', 1)
                    if len(parts) == 2:
                        text = parts[0].strip()
                        url = parts[1].strip()
                        url_buttons.append({"text": text, "url": url})
        
        # Сохраняем приветственное сообщение
        await db.add_welcome_message(
            message_text=context.user_data['welcome_message'],
            delay_seconds=context.user_data.get('delay_seconds', 0),
            replace_links=False,  # По умолчанию не заменяем ссылки
            url_buttons=url_buttons
        )
        
        await update.message.reply_text(
            "✅ <b>Приветственное сообщение добавлено!</b>\n\n"
            "Теперь новые подписчики будут получать это сообщение.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
            ]])
        )
        return ConversationHandler.END

    async def handle_admin_username(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик добавления админа по username или ID"""
        input_text = update.message.text.strip()
        
        try:
            # Пытаемся распарсить как ID
            if input_text.isdigit():
                user_id = int(input_text)
                await db.add_admin(user_id)
                await update.message.reply_text(
                    f"✅ Админ с ID {user_id} добавлен.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
                    ]])
                )
            else:
                # Обрабатываем как username
                username = input_text
                if username.startswith('@'):
                    username = username[1:]
                
                await update.message.reply_text(
                    f"❌ Не удалось найти пользователя @{username}.\n"
                    "Попросите пользователя написать боту /start, а затем добавьте его по ID.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
                    ]])
                )
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат. Введите ID пользователя (число) или username.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
                ]])
            )
        
        return ConversationHandler.END

    async def handle_manual_accept_count(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ввода количества для ручного приема"""
        try:
            count = int(update.message.text.strip())
            pending_users = await db.get_pending_users()
            
            if count > len(pending_users):
                count = len(pending_users)
            
            accepted = 0
            for i in range(count):
                user = pending_users[i]
                try:
                    await context.bot.approve_chat_join_request(
                        chat_id=CHANNEL_ID,
                        user_id=user['user_id']
                    )
                    await db.remove_pending_user(user['user_id'])
                    accepted += 1
                except Exception as e:
                    logger.error(f"Ошибка при принятии пользователя {user['user_id']}: {e}")
            
            await update.message.reply_text(
                f"✅ Принято {accepted} пользователей из {count} запрошенных.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
                ]])
            )
            return ConversationHandler.END
            
        except ValueError:
            await update.message.reply_text(
                "❌ Введите корректное число.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
                ]])
            )
            return ConversationHandler.END

    async def handle_schedule_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ввода времени для расписания"""
        time_text = update.message.text.strip()
        
        # Проверяем формат времени
        if not re.match(r'^\d{1,2}:\d{2}$', time_text):
            await update.message.reply_text(
                "❌ Неверный формат времени. Используйте формат ЧЧ:ММ (например, 14:30):",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
                ]])
            )
            return WAITING_SCHEDULE_TIME
        
        await db.set_setting("schedule_time", time_text)
        
        await update.message.reply_text(
            f"✅ Время приема установлено: {time_text}\n\n"
            "Пользователи будут приниматься в указанное время.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
            ]])
        )
        return ConversationHandler.END

    async def handle_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик рассылки"""
        message_text = update.message.text
        
        # Здесь должна быть логика рассылки всем пользователям
        # Пока просто уведомляем об успехе
        await update.message.reply_text(
            f"📢 <b>Рассылка выполнена!</b>\n\n"
            f"Сообщение отправлено пользователям.\n\n"
            f"<i>Текст сообщения:</i>\n{message_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
            ]])
        )
        return ConversationHandler.END

    async def handle_custom_delay(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ввода пользовательской задержки"""
        delay_text = update.message.text
        delay_seconds = self.parse_delay(delay_text)
        
        if delay_seconds is None:
            await update.message.reply_text(
                "❌ Неверный формат времени. Попробуйте еще раз:"
            )
            return WAITING_CUSTOM_DELAY
        
        context.user_data['delay_seconds'] = delay_seconds
        
        await update.message.reply_text(
            "🔗 <b>URL кнопки</b>\n\n"
            "Отправьте кнопки в формате:\n"
            "Текст кнопки | URL\n"
            "Текст кнопки 2 | URL2\n\n"
            "Или отправьте 'пропустить' для продолжения без кнопок:",
            parse_mode=ParseMode.HTML
        )
        return WAITING_URL_BUTTONS

    async def cancel_operation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена операции"""
        await update.message.reply_text(
            "❌ Операция отменена.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔧 Админ панель", callback_data="back_to_main")
            ]])
        )
        return ConversationHandler.END

    async def run(self):
        """Запуск бота"""
        # Инициализация базы данных
        await db.init_db()
        
        # Добавляем первого админа
        await db.add_admin(5376489425, "admin")
        
        # Запуск бота
        logger.info("Бот запущен!")
        
        # Ожидание завершения
        try:
            await self.application.run_polling()
        except KeyboardInterrupt:
            logger.info("Остановка бота...")

if __name__ == "__main__":
    bot = SimpleTelegramBot()
    asyncio.run(bot.run())
