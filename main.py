import os
import time
import psycopg2
import psycopg2.extras

from dotenv import load_dotenv, find_dotenv

from scally_client import ZMQClient

from utils import get_user_status
from sending_alerts import send_telegram_message

load_dotenv(find_dotenv())

DB_NAME = os.getenv('NAME')
DB_USER = os.getenv('USER')
DB_PASSWORD = os.getenv('PASSWORD')
DB_HOST = os.getenv('HOST')
DB_PORT = os.getenv('PORT')

def get_actions_with_payments():
    """
    Функция коннектится к базе PostgreSQL и делает выборку:
    - track_id, plan, status из таблицы actions_action
    - status из связанной payment_payment
    - user_id (кастомное поле) из таблицы user_activity_user
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
            SELECT 
                a.track_id,
                a.plan,
                a.status AS action_status,
                p.status AS payment_status,
                u.user_id AS custom_user_id
            FROM actions_action a
            LEFT JOIN payment_payment p ON a.payment_id = p.id
            LEFT JOIN users_user u ON a.user_id = u.id;
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        scally_client = ZMQClient()
        
        for row in rows:
            track_id = row['track_id']
            plan = row['plan']
            action_status = row['action_status']
            payment_status = row['payment_status']
            custom_user_id = row['custom_user_id']
            
            current_status = get_user_status(track_id)
            
            if plan == "alert" and payment_status == "success" and action_status == "active":
                document = scally_client.get_last_document(track_id).get("documents")
                
                if len(document) >= 1:
                    legacy_status = document[0].get("status")
                    if not legacy_status == current_status:
                        send_telegram_message(chat_id=custom_user_id, text=f"Статус пользователя {track_id} изменился. Сейчас пользователь {current_status}")
            
            scally_client.create_document(username=f"{track_id}", status=f"{current_status}")

    except psycopg2.Error as e:
        print("Ошибка при работе с PostgreSQL:", e)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

while True:
    get_actions_with_payments()
    time.sleep(1)
