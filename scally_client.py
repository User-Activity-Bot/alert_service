import os
import zmq
import datetime

from datetime import time

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class ZMQClient:
    """
    Класс для взаимодействия с ZeroMQ-сервером по REQ/REP паттерну.
    """

    def __init__(self, host="localhost", port=5555):
        """
        Инициализирует контекст и сокет REQ, подключается к заданному адресу.
        
        :param host: Хост, на котором запущен сервер (по умолчанию localhost)
        :param port: Порт, на котором слушает сервер (по умолчанию 5555)
        """
        
        if os.getenv("ZMQ_HOST"):
            host = os.getenv("ZMQ_HOST")
        if os.getenv("ZMQ_PORT"):
            port = os.getenv("ZMQ_PORT")
        
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        connect_str = f"tcp://{host}:{port}"
        self.socket.connect(connect_str)
        print(f"[ZMQClient] Connected to {connect_str}")

    def send_request(self, data: dict) -> dict:
        """
        Универсальный метод отправки запроса в виде JSON и получения ответа.
        
        :param data: словарь с данными для отправки
        :return: словарь-ответ от сервера
        """
        self.socket.send_json(data, default=str)
        response = self.socket.recv_json()
        return response

    def create_document(self, username: str, status: str) -> dict:
        """
        Отправляет запрос на создание документа (action='create_document').
        
        :param username: имя пользователя
        :param status: статус пользователя
        :return: словарь-ответ от сервера
        """
        start = datetime.datetime.now()
        request_data = {
            "action": "create_document",
            "username": username,
            "status": status,
        }
        response = self.send_request(request_data)
        elapsed = datetime.datetime.now() - start
        print(f"[create_document] Затраченное время: {elapsed}")
        return response

    def get_document(self, 
                    username: str = None, 
                    status: str = None, 
                    creation_date_start=None, 
                    creation_date_end=None,
                    order_by=None,
                    limit=None) -> dict:
        """
        Отправляет запрос на получение документа (action='get_document').
        """
        start = datetime.datetime.now()
        request_data = {
            "action": "get_document",
        }
        if username:
            request_data["username"] = username
        if status:
            request_data["status"] = status
        if creation_date_start:
            request_data["creation_date_start"] = creation_date_start
        if creation_date_end:
            request_data["creation_date_end"] = creation_date_end
        
        # Новые поля:
        if order_by:
            request_data["order_by"] = order_by
        if limit:
            request_data["limit"] = limit

        response = self.send_request(request_data)
        elapsed = datetime.datetime.now() - start
        print(f"[get_document] Затраченное время: {elapsed}")
        return response

    def get_last_document(self, username: str) -> dict:
        """
        Запрашивает у сервера последнюю запись (одну) по указанному username.
        """
        start = datetime.datetime.now()
        request_data = {
            "action": "get_last_document",
            "username": username,
        }
        
        response = self.send_request(request_data)
        elapsed = datetime.datetime.now() - start
        print(f"[get_last_document] Затраченное время: {elapsed}")
        return response

    def upsert_daily_report(self, username : str, most_active_hour : time, total : time) -> dict:
        """
        Отправляет запрос на создание документа (action='create_document').
        
        :param username: имя пользователя
        :param status: статус пользователя
        :return: словарь-ответ от сервера
        """
        start = datetime.datetime.now()
        request_data = {
            "action": "upsert_daily_report",
            "username": username,
            "most_visited_hour": most_active_hour,
            "total": total,
        }
        response = self.send_request(request_data)
        elapsed = datetime.datetime.now() - start
        print(f"[upsert_daily_report] Затраченное время: {elapsed}")
        return response

    def get_daily_report(self, username = None, most_active_hour = None, total = None, creation_date_start = None, creation_date_end = None):
        """
        Отправляет запрос на получение документа (action='get_daily_report').
        
        :param username: Имя пользователя.
        :param most_visited_hour: Время, которое нужно записать в most_visited_hour (тип TIME).
        :param total: Общее количество (тип TIME).
        :param creation_date_start: Начальная дата от сортироваки (тип TIME)
        :param creation_date_end: Конечная дата от сортироваки (тип TIME)
        
        :return: словарь-ответ от сервера
        """
        start = datetime.datetime.now()
        request_data = {
            "action": "get_daily_report",
            "username": username,
            "most_visited_hour": most_active_hour,
            "creation_date_start": creation_date_start,
            "creation_date_end" : creation_date_end,
            "total": total,
        }
        response = self.send_request(request_data)
        elapsed = datetime.datetime.now() - start
        print(f"[upsert_daily_report] Затраченное время: {elapsed}")
        return response

    def close(self):
        """
        Закрывает сокет и контекст ZMQ.
        """
        if self.socket:
            self.socket.close()
        if self.context:
            self.context.term()
        print("[ZMQClient] Соединение закрыто.")