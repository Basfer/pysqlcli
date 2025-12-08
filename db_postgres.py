import psycopg2
import sys
import logging
from db_interface import DatabaseInterface

class PostgresDatabase(DatabaseInterface):
    """Класс для работы с PostgreSQL базой данных"""

    def __init__(self, connection_string, user, password):
        super().__init__(connection_string, user, password)
        self.connection = None

    def connect(self):
        """Подключение к PostgreSQL базе данных"""
        try:
            host, port, database = self.connection_string.replace(':', '/').split('/')
            self.connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=self.user,
                password=self.password
            )
            return True
        except psycopg2.Error as error:
            logging.critical(f"Ошибка подключения к PostgreSQL: {error}")
            return False
        except Exception as e:
            logging.critical(f"Ошибка разбора строки подключения к PostgreSQL '{self.connection_string}': {e}")
            return False

    def disconnect(self):
        """Отключение от PostgreSQL базы данных"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query):
        """Выполнение запроса к PostgreSQL"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)

            # Если это SELECT запрос
            if query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                # Вывод заголовков
                print(" | ".join(columns))
                print("-" * (len(" | ".join(columns))))

                # Вывод данных
                for row in rows:
                    print(" | ".join(str(item) for item in row))
            else:
                # Для других запросов (INSERT, UPDATE, DELETE)
                print(f"Запрос выполнен. Затронуто строк: {cursor.rowcount}")

            cursor.close()
        except psycopg2.Error as error:
            logging.error(f"Ошибка выполнения запроса в PostgreSQL: {error}")

    def execute_statements(self, statements):
        """Выполнение нескольких запросов к PostgreSQL"""
        if isinstance(statements, str):
            statements = [statements]

        for statement in statements:
            self.execute_query(statement.strip().strip(';'))

    def is_postgres(self):
        """Проверка, является ли это PostgreSQL базой данных"""
        return True