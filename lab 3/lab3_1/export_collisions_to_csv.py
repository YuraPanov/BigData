import sqlite3
import pandas as pd
import os

DB_PATH = r"E:\Python_LabsAndProjekt\mathmod\Veresk\switrs.sqlite"
CSV_PATH = "collisions_small.csv"
TABLE = "collisions"

def export_small():
    print("Подключение к базе")
    conn = sqlite3.connect(DB_PATH)

    print("Проверяем наличие таблицы")
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    print("Найдены таблицы:", tables)

    if TABLE not in tables:
        print(f"Таблица {TABLE} не найдена!")
        return

    print("Экспортируем только нужные колонки")

    query = f"""
        SELECT 
            county_location,
            county_city_location
        FROM {TABLE};
    """

    df = pd.read_sql_query(query, conn)

    save_path = os.path.abspath(CSV_PATH)
    df.to_csv(save_path, index=False)

    conn.close()

    print("Экспорт завершён.")
    print(f"CSV создан по пути:\n{save_path}")

if __name__ == "__main__":
    export_small()
