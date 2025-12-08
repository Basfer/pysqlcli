import os
import argparse
import sys

from dotenv import load_dotenv
import tokenizer
from db_interface import DatabaseFactory

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

def get_console_encoding():
    import locale
    encodings = {
        'stdout': getattr(sys.stdout, 'encoding', None),
        'stderr': getattr(sys.stderr, 'encoding', None),
        'stdin': getattr(sys.stdin, 'encoding', None),
        'locale.getpreferredencoding': locale.getpreferredencoding(),
        'sys.getdefaultencoding': sys.getdefaultencoding()
    }

    ret = 0
    if sys.platform == "win32":
        try:
            # Пытаемся получить кодировку консоли
            import ctypes
            from ctypes import wintypes, byref
            kernel32 = ctypes.windll.kernel32
            code_page = wintypes.DWORD()
            if kernel32.GetConsoleCP(byref(code_page)):
                cp = code_page.value
                if cp == 65001:  # UTF-8
                    ret = 'utf-8'
                elif cp == 1251:  # Windows-1251
                    ret = 'cp1251'
                else:
                    ret = f'cp{cp}'
        except Exception as e:
            print(f"Ошибка получения кодировки консоли: {e}", file=sys.stderr)

    return ret or sys.stdin.encoding or 'utf-8'

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

    # Создаем экземпляр нужной базы данных
    db = DatabaseFactory.create_database(args.type, args.connectionstring, args.user, args.password)

    # Подключаемся к базе данных
    if not db.connect():
        sys.exit(1)

    enc = get_console_encoding()
    print(f'enc={enc}')

    # Enable autocommit
    if hasattr(db.connection, 'autocommit'):
        db.connection.autocommit = True

    # Выполнение основного запроса из параметра
    if args.query:
        db.execute_statements(args.query)
        if not args.files:
            db.disconnect()
            exit(0)

    # Выполнение запросов из файлов (если переданы) или из ввода
    files = args.files if args.files else [sys.stdin]
    original_stdin = sys.stdin

    for inpfile in files:
        # если передано имя файла - открываем на чтение; иначе читаем stdin
        if isinstance(inpfile, str):
            try:
                sys.stdin = open(inpfile, 'r', encoding='utf-8')
            except FileNotFoundError:
                print(f"Файл {inpfile} не найден", file=sys.stderr)
            except Exception as e:
                print(f"Ошибка при чтении файла {inpfile}: {e}", file=sys.stderr)

        sql = ''
        l = 0
        eof = False
        while not eof:
            l += 1
            line = ''
            try:
                line = input('SQL: ' if l == 1 else format(l, '>3')+': ')
            except EOFError:
                eof = True
            except KeyboardInterrupt:
                db.disconnect()
                exit(0)

            sql += ('\n' if sql else '') + (line if sys.stdin.isatty() else line.strip(' "'))
            statements = tokenizer.split_statements(sql)
            if not eof and line and not statements[len(statements)-1][3]:
                continue

            l, sql = 0, ''
            db.execute_statements([s[0] for s in statements], enc=enc)

    sys.stdin = original_stdin
    db.disconnect()

if __name__ == '__main__':
    main()