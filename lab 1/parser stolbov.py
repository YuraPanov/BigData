import sqlite3

def auto_analyze_collisions(db_path):
    """Автоматический анализ ДТП без знания структуры"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Найти все таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]

        if not tables:
            print(f"В базе данных {db_path} нет таблиц!")
            conn.close()
            return

        print("Найденные таблицы:", tables)

        # Будем искать таблицу с данными о ДТП
        target_table = None
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]

            # Ищем таблицу с географическими данными
            geographic_columns = [col for col in columns if any(geo in col.lower() for geo in
                                                                ['county', 'city', 'location', 'address',
                                                                 'jurisdiction'])]

            if geographic_columns:
                print(f"\nНайдена таблица с географическими данными: {table}")
                print("Колонки:", geographic_columns)
                target_table = table
                break

        # Получаем все колонки целевой таблицы
        cursor.execute(f"PRAGMA table_info({target_table})")
        all_columns = [col[1] for col in cursor.fetchall()]

        print(f"\nВсе колонки таблицы {target_table}:")
        for col in all_columns:
            print(f"  - {col}")

        conn.close()

    except sqlite3.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")

db_file = 'switrs.sqlite'
auto_analyze_collisions(db_file)