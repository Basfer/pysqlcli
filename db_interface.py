import sys
import logging
from abc import ABC, abstractmethod

# Настройка логгера для вывода в stderr
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)

class DatabaseInterface(ABC):
    """Абстрактный базовый класс для работы с базами данных"""

    def __init__(self, connection_string, user, password):
        self.connection_string = connection_string
        self.user = user
        self.password = password
        self.connection = None

    @abstractmethod
    def connect(self):
        """Подключение к базе данных"""
        pass

    @abstractmethod
    def disconnect(self):
        """Отключение от базы данных"""
        pass

    @abstractmethod
    def execute_query(self, query):
        """Выполнение SQL запроса"""
        pass

    @abstractmethod
    def execute_statements(self, statements):
        """Выполнение нескольких SQL запросов"""
        pass

    def is_oracle(self):
        """Проверка, является ли это Oracle базой данных"""
        return False

    def is_postgres(self):
        """Проверка, является ли это PostgreSQL базой данных"""
        return False

class DatabaseFactory:
    """Фабрика для создания экземпляров баз данных"""

    _instances = {}

    @classmethod
    def create_database(cls, db_type, connection_string, user, password):
        """Создание экземпляра базы данных"""
        if db_type.lower() == 'oracle':
            from db_oracle import OracleDatabase
            return OracleDatabase(connection_string, user, password)
        elif db_type.lower() == 'postgres':
            from db_postgres import PostgresDatabase
            return PostgresDatabase(connection_string, user, password)
        else:
            raise ValueError(f"Неизвестный тип базы данных: {db_type}")