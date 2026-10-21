#!/usr/bin/env python3
"""
Скрипт для тестирования функций бота
"""

import asyncio
from database import Database

async def test_database():
    """Тестирование функций базы данных"""
    print("🧪 Тестирование базы данных...")
    
    db = Database("test_database.db")
    await db.init_db()
    
    # Тест добавления админа
    await db.add_admin(123456789, "test_admin")
    print("✅ Админ добавлен")
    
    # Тест проверки админа
    is_admin = await db.is_admin(123456789)
    print(f"✅ Проверка админа: {is_admin}")
    
    # Тест добавления приветственного сообщения
    await db.add_welcome_message(
        "Добро пожаловать! 🎉",
        delay_seconds=30,
        replace_links=True,
        url_buttons=[{"text": "Наш сайт", "url": "https://example.com"}]
    )
    print("✅ Приветственное сообщение добавлено")
    
    # Тест получения приветственных сообщений
    messages = await db.get_welcome_messages()
    print(f"✅ Получено сообщений: {len(messages)}")
    
    # Тест настроек
    await db.set_setting("auto_accept", "on")
    auto_accept = await db.get_setting("auto_accept")
    print(f"✅ Настройка auto_accept: {auto_accept}")
    
    # Тест статистики
    await db.add_statistics(123456789, "subscribed")
    stats = await db.get_statistics()
    print(f"✅ Статистика: {stats}")
    
    print("🎉 Все тесты пройдены успешно!")

if __name__ == "__main__":
    asyncio.run(test_database())
