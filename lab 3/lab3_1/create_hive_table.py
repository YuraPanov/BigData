from pyhive import hive

# Пути и настройки
CSV_PATH = "/tmp/collisions_small.csv"
HIVE_DB = "switrs"
HIVE_TABLE = "collisions_small"

# Подключение к Hive
conn = hive.Connection(
    host="localhost",
    port=10000,
    username="hive",
    database="default"
)
cursor = conn.cursor()

try:
    # Создаём базу (если ещё не существует)
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
    cursor.execute(f"USE {HIVE_DB}")
    print(f"База {HIVE_DB} выбрана.")

    # Удаляем таблицу если существует (отдельный запрос)
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {HIVE_TABLE}")
        print(f"  Старая таблица удалена (если существовала)")
    except Exception as drop_error:
        print(f"  Таблица не существовала: {drop_error}")

    # Создаём таблицу Hive (отдельный запрос без точки с запятой)
    create_table_query = f"""
CREATE TABLE {HIVE_TABLE} (
    county_location STRING,
    county_city_location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
"""
    # ROW FORMAT DELIMITED указывает что строки разделены опр. разделителем
    # FIELDS TERMINATED BY ',' указываем каким разделитиелем
    # STORED AS TEXTFILE указываем тип хранения дынных, текстовый документ

    cursor.execute(create_table_query)
    print(f" Таблица {HIVE_TABLE} создана.")

    # Загружаем CSV в Hive
    # Смотрим что файл существует в контейнере
    load_query = f"""
LOAD DATA LOCAL INPATH '{CSV_PATH}'
OVERWRITE INTO TABLE {HIVE_TABLE}
"""
    # OVERWRITE очищаем таблицу перед загрузкой
    cursor.execute(load_query)
    print(f" CSV {CSV_PATH} загружен в таблицу {HIVE_TABLE}.")

    # Проверим сколько строк загружено
    cursor.execute(f"SELECT COUNT(*) FROM {HIVE_TABLE}")
    count = cursor.fetchone()[0]
    print(f"Количество строк в таблице {HIVE_TABLE}: {count}")

    # ТОП 10 округов по количеству аварий
    cursor.execute(f"""
SELECT county_location, COUNT(*) AS accidents
FROM {HIVE_TABLE}
GROUP BY county_location
ORDER BY accidents DESC
LIMIT 10
""")
    print("\n ТОП 10 округов по числу аварий:")
    for row in cursor.fetchall():
        print(row)

except Exception as e:
    print(f" Ошибка: {e}")
    # Вывод дополнительной информации об ошибке
    import traceback
    traceback.print_exc()

finally:
    # Закрываем соединение
    if cursor:
        cursor.close()
    if conn:
        conn.close()