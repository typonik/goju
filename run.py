#!/usr/bin/env python3
"""
Скрипт для запуска Telegram бота приветствия
"""

import sys
from bot import TelegramBot

def main():
    """Главная функция запуска бота"""
    print("🚀 Запуск Telegram бота приветствия...")
    bot = TelegramBot()
    bot.run()

if __name__ == "__main__":
    main()
