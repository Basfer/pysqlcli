import oracledb
import sys
import logging
from db_interface import DatabaseInterface

class OracleDatabase(DatabaseInterface):
    """Класс для работы с Oracle базой данных"""

    def __init__(self, connection_string, user, password):
        super().__init__(connection_string, user, password)
        self.connection = None

    def connect(self):
        """Подключение к Oracle базе данных"""
        try:
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.connection_string
            )
            return True
        except oracledb.Error as error:
            logging.critical(f"Ошибка подключения к Oracle: {error}")
            return False

    def disconnect(self):
        """Отключение от Oracle базы данных"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, enc='utf-8'):
        """Выполнение запроса к Oracle"""
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
        except oracledb.Error as error:
            logging.error(f"Ошибка выполнения запроса в Oracle: {error}")

    def execute_statements(self, statements, enc='utf-8'):
        """Выполнение нескольких запросов к Oracle"""
        if isinstance(statements, str):
            statements = [statements]

        for statement in statements:
            self.execute_query(statement.strip().strip(';'), enc=enc)

    def is_oracle(self):
        """Проверка, является ли это Oracle базой данных"""
        return True