import os
import argparse
import logging
import sys

import oracledb
import psycopg2
from dotenv import load_dotenv

import tokenizer
from tokenizer import split_statements


# Настройка логгера для вывода в stderr
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)

ARGS = [
    {'name_or_args': ['--type', '-t'], 'required': True, 'choices': ['oracle', 'postgres'], 'help': 'Тип базы данных: oracle или postgres'},
    {'name_or_args': ['--query', '-q'], 'help': 'SQL запрос для выполнения'},
    {'name_or_args': ['--files', '-f'], 'nargs': '*', 'help': 'Файлы с SQL запросами для выполнения'},
    {'name_or_args': ['--connectionstring', '-c'], 'required': True, 'help': 'строка подключения в виде Хост:порт/БД'},
    {'name_or_args': ['--user', '-u'], 'required': True, 'help': 'Имя пользователя'},
    {'name_or_args': ['--password', '-p'], 'required': True, 'help': 'Пароль'}
]

# Загружаем переменные окружения из файла .env
load_dotenv()
# print(os.environ['user'])


def get_console_encoding_windows():
    print(__dict__)

    ret = 0
    if sys.platform == "win32":
        try:
            # Пытаемся получить кодировку консоли
            import ctypes
            from ctypes import wintypes

            # Получаем кодовую страницу консоли
            kernel32 = ctypes.windll.kernel32
            print(kernel32.GetConsoleCP())
            console_handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            code_page = wintypes.DWORD()
            print(code_page.value)

            if kernel32.GetConsoleCP(byref(code_page)):
                cp = code_page.value
                if cp == 65001:  # UTF-8
                    ret = 'utf-8'
                elif cp == 1251:  # Windows-1251
                    ret = 'cp1251'
                else:
                    ret = f'cp{cp}'
        except:
            pass

    encodings = {
        'stdout': getattr(sys.stdout, 'encoding', None),
        'stderr': getattr(sys.stderr, 'encoding', None),
        'stdin': getattr(sys.stdin, 'encoding', None),
        'locale.getpreferredencoding': locale.getpreferredencoding(),
        'sys.getdefaultencoding': sys.getdefaultencoding(),
        ''
    }


    return ret or sys.stdout.encoding or 'utf-8'

import sys
import locale

print("Default encoding:", sys.getdefaultencoding())
print("Stdout encoding:", sys.stdout.encoding)
print("Stderr encoding:", sys.stderr.encoding)
print("Stdin encoding:", sys.stdin.encoding)
print("Locale preferred encoding:", locale.getpreferredencoding())
print("Locale preferred encoding (False):", locale.getpreferredencoding(False))
print(os.system("chcp"))

encoding = get_console_encoding_windows()
print(f"Кодировка консоли: {encoding}")
s = 'йцук'
s = input()
print(s)
try:
    print(s.encode('cp1251').decode())
except Exception as e: print(e)

try:
    print(s.encode('cp1251').decode('cp866'))
except Exception as e: print(e)

exit(0)

def connect_oracle(connstr, user, password):
    """Подключение к Oracle базе данных с использованием python-oracledb"""
    try:
        connection = oracledb.connect(
            user=user,
            password=password,
            dsn=connstr
        )
        return connection
    except oracledb.Error as error:
        logging.critical(f"Ошибка подключения к Oracle: {error}")
        return None

def connect_postgres(connstr, user, password):
    """Подключение к PostgreSQL базе данных"""
    try:
        host, port, database = connstr.replace(':', '/').split('/')
    except Exception as e:
        logging.critical(f"Ошибка разбора строки подключения к PostgreSQL '{connstr}': {error}")
        return None
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return connection
    except psycopg2.Error as error:
        logging.critical(f"Ошибка подключения к PostgreSQL '{connstr}': {error}")
        return None

def connect_db(connectionstring, user, password, type):
    connect_func = connect_oracle if type.lower() == 'oracle' else connect_postgres
    return connect_func(connectionstring, user, password)

def execute_query_oracle(connection, query):
    """Выполнение запроса к Oracle"""
    try:
        cursor = connection.cursor()
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

def execute_query_postgresql(connection, query):
    """Выполнение запроса к PostgreSQL"""
    try:
        cursor = connection.cursor()
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


def execute_statements(connection, statements):
    execute_func = execute_query_oracle if isinstance(connection, oracledb.Connection) else execute_query_postgresql
    if isinstance(statements, str): statements = [statements]
    for statement in statements:
        execute_func(connection, statement.strip().strip(';'))


def main():
    parser = argparse.ArgumentParser(description='Консольный клиент для Oracle и PostgreSQL')
    for arg in ARGS:
        name_or_args = arg['name_or_args']
        del arg['name_or_args']
        if argval := os.environ.get(name_or_args[0].strip('-')):
            arg['required'] = False
            arg['default'] = argval
        parser.add_argument(*name_or_args, **arg)

    args = parser.parse_args()

    # Проверка обязательных параметров


    connection = connect_db(args.connectionstring, args.user, args.password, type=args.type)
    # Enable autocommit
    connection.autocommit = True

    if not connection:
        sys.exit(1)

    # Выполнение основного запроса из параметра
    if args.query:
        execute_statements(connection, args.query)
        if not args.files: exit(0)

    # Выполнение запросов из файлов (если переданы) или из ввода
    files = args.files if args.files else [sys.stdin]
    original_stdin = sys.stdin

    for inpfile in files:
        if isinstance(inpfile, str):
            try:
                sys.stdin = open(inpfile, 'r', encoding='utf-8')
            except FileNotFoundError:
                logging.error(f"Файл {inpfile} не найден")
            except Exception as e:
                logging.error(f"Ошибка при чтении файла {inpfile}: {e}")

        sql = ''
        l = 0
        eof = False
        while not eof:
            l += 1
            line = ''
            try:
                line = input('SQL: ' if l == 1 else format(l, '>3')+': ')
            except EOFError: eof = True
            except KeyboardInterrupt: exit(0)

            sql += ('\n' if sql else '') + (line if sys.stdin.isatty() else line.strip(' "'))
            statements = tokenizer.split_statements(sql)
            if not eof and line and not statements[len(statements)-1][3]:
                continue

            l, sql = 0, ''
            execute_statements(connection, [s[0] for s in statements])

    sys.stdin = original_stdin

    connection.close()

if __name__ == '__main__':
    main()