import os
import requests

def send_telegram_message( chat_id: int, text: str) -> dict:
    """
    Отправляет сообщение через Telegram-бота.
    
    :param token: Токен Telegram-бота.
    :param chat_id: ID чата, в который нужно отправить сообщение.
    :param text: Текст сообщения.
    :return: Ответ Telegram API в формате JSON.
    """
    
    token = os.getenv("BOT_TOKEN")
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()
