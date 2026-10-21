import aiosqlite
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
import json

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self):
        """Инициализация базы данных"""
        async with aiosqlite.connect(self.db_path) as db:
            # Таблица для хранения приветственных сообщений
            await db.execute("""
                CREATE TABLE IF NOT EXISTS welcome_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_text TEXT NOT NULL,
                    delay_seconds INTEGER DEFAULT 0,
                    replace_links BOOLEAN DEFAULT FALSE,
                    url_buttons TEXT DEFAULT '[]',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица для хранения админов
            await db.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица для настроек бота
            await db.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            # Таблица для статистики
            await db.execute("""
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица для очереди приема
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pending_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source TEXT DEFAULT 'manual'
                )
            """)
            
            await db.commit()

    async def add_admin(self, user_id: int, username: str = None):
        """Добавить админа"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO admins (user_id, username) VALUES (?, ?)",
                (user_id, username)
            )
            await db.commit()

    async def remove_admin(self, user_id: int):
        """Удалить админа"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
            await db.commit()

    async def is_admin(self, user_id: int) -> bool:
        """Проверить, является ли пользователь админом"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
            return await cursor.fetchone() is not None

    async def get_admins(self) -> List[Dict]:
        """Получить список всех админов"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT user_id, username FROM admins")
            rows = await cursor.fetchall()
            return [{"user_id": row[0], "username": row[1]} for row in rows]

    async def add_welcome_message(self, message_text: str, delay_seconds: int = 0, 
                                replace_links: bool = False, url_buttons: List[Dict] = None):
        """Добавить приветственное сообщение"""
        if url_buttons is None:
            url_buttons = []
            
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO welcome_messages (message_text, delay_seconds, replace_links, url_buttons)
                VALUES (?, ?, ?, ?)
            """, (message_text, delay_seconds, replace_links, json.dumps(url_buttons)))
            await db.commit()

    async def get_welcome_messages(self) -> List[Dict]:
        """Получить все приветственные сообщения"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT id, message_text, delay_seconds, replace_links, url_buttons, is_active
                FROM welcome_messages ORDER BY created_at DESC
            """)
            rows = await cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "message_text": row[1],
                    "delay_seconds": row[2],
                    "replace_links": bool(row[3]),
                    "url_buttons": json.loads(row[4]) if row[4] else [],
                    "is_active": bool(row[5])
                }
                for row in rows
            ]

    async def update_welcome_message(self, message_id: int, **kwargs):
        """Обновить приветственное сообщение"""
        if not kwargs:
            return
            
        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        values = list(kwargs.values()) + [message_id]
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(f"UPDATE welcome_messages SET {set_clause} WHERE id = ?", values)
            await db.commit()

    async def delete_welcome_message(self, message_id: int):
        """Удалить приветственное сообщение"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM welcome_messages WHERE id = ?", (message_id,))
            await db.commit()

    async def set_setting(self, key: str, value: str):
        """Установить настройку"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
                (key, value)
            )
            await db.commit()

    async def get_setting(self, key: str, default: str = None) -> str:
        """Получить настройку"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
            row = await cursor.fetchone()
            return row[0] if row else default

    async def add_statistics(self, user_id: int, action: str):
        """Добавить запись в статистику"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO statistics (user_id, action) VALUES (?, ?)",
                (user_id, action)
            )
            await db.commit()

    async def get_statistics(self) -> Dict:
        """Получить статистику"""
        async with aiosqlite.connect(self.db_path) as db:
            # Общее количество пользователей
            cursor = await db.execute("SELECT COUNT(DISTINCT user_id) FROM statistics")
            total_users = (await cursor.fetchone())[0]
            
            # Количество подписок сегодня
            cursor = await db.execute("""
                SELECT COUNT(*) FROM statistics 
                WHERE action = 'subscribed' AND DATE(timestamp) = DATE('now')
            """)
            today_subscriptions = (await cursor.fetchone())[0]
            
            # Количество подписок за неделю
            cursor = await db.execute("""
                SELECT COUNT(*) FROM statistics 
                WHERE action = 'subscribed' AND timestamp >= datetime('now', '-7 days')
            """)
            week_subscriptions = (await cursor.fetchone())[0]
            
            return {
                "total_users": total_users,
                "today_subscriptions": today_subscriptions,
                "week_subscriptions": week_subscriptions
            }

    async def add_pending_user(self, user_id: int, username: str = None, 
                             first_name: str = None, last_name: str = None, source: str = "manual"):
        """Добавить пользователя в очередь ожидания"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO pending_users (user_id, username, first_name, last_name, source)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, username, first_name, last_name, source))
            await db.commit()

    async def get_pending_users(self) -> List[Dict]:
        """Получить список пользователей в очереди"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT user_id, username, first_name, last_name, added_at, source
                FROM pending_users ORDER BY added_at DESC
            """)
            rows = await cursor.fetchall()
            return [
                {
                    "user_id": row[0],
                    "username": row[1],
                    "first_name": row[2],
                    "last_name": row[3],
                    "added_at": row[4],
                    "source": row[5]
                }
                for row in rows
            ]

    async def remove_pending_user(self, user_id: int):
        """Удалить пользователя из очереди"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM pending_users WHERE user_id = ?", (user_id,))
            await db.commit()
