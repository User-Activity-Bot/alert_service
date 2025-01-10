import os
import asyncio
from dotenv import load_dotenv, find_dotenv
from telethon import TelegramClient
from telethon.tl.types import UserStatusOnline, UserStatusOffline, UserStatusRecently

# Загрузка переменных окружения
load_dotenv(find_dotenv())

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")

# Функция-обертка для запуска асинхронных задач в синхронном режиме
def run_sync(coroutine):
    return asyncio.run(coroutine)

# Синхронная функция для получения статуса пользователя
def get_user_status(username):
    async def fetch_user_status():
        try:
            # Инициализация клиента Telegram
            client = TelegramClient('user_status_bot', api_id, api_hash)

            # Подключение к клиенту
            await client.start()
            # Получение информации о пользователе
            user = await client.get_entity(username)

            if isinstance(user.status, UserStatusOnline):
                print(f"Пользователь {username} сейчас онлайн.")
                return "online"
            elif isinstance(user.status, UserStatusOffline):
                print(f"Пользователь {username} был оффлайн в {user.status.was_online}.")
                return "offline"
            elif isinstance(user.status, UserStatusRecently):
                print(f"Пользователь {username} сейчас офлайн. Последний онлайн скрыт.")
                return "offline"
            else:
                print(f"Статус пользователя {username} неизвестен или скрыт.")
        except Exception as e:
            print(f"Ошибка: {e}")
        finally:
            await client.disconnect()

    # Запуск асинхронной функции
    return run_sync(fetch_user_status())
