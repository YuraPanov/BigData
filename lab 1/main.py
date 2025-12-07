import sqlite3
from typing import List, Tuple, Generator


def shuffler(mapped_words: List[Tuple[str, int]]) -> Generator:
    """Функция для группировки данных по ключу"""
    mapped_words = sorted(mapped_words)

    buffer: List[int] = []
    prev_word: str or None = None
    for word, value in mapped_words:
        if prev_word == word:
            buffer.append(value)
        else:
            if prev_word is not None:
                yield prev_word, buffer
            buffer = [value]
        prev_word = word

    if prev_word is not None:
        yield prev_word, buffer


def mapper_counties(db_path: str, table_name: str = 'collisions') -> List[Tuple[str, int]]:
    """Map: извлекает округа из базы данных и создает пары (округ, 1)"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Получаем все округа из базы данных
    cursor.execute(
        f"SELECT county_location FROM {table_name} WHERE county_location IS NOT NULL AND county_location != ''")
    counties = cursor.fetchall()
    conn.close()

    # Создаем пары (округ, 1) для каждого ДТП
    mapped_data = []
    for county in counties:
        county_name = county[0].strip().lower()
        mapped_data.append((county_name, 1))

    return mapped_data


def reducer_counties(grouped_data: Generator) -> List[Tuple[str, int]]:
    """Reduce: подсчитывает количество ДТП для каждого округа"""
    reduced_data = []
    for county, counts in grouped_data:
        total_accidents = sum(counts)
        reduced_data.append((county, total_accidents))

    # Сортируем по убыванию количества ДТП
    reduced_data.sort(key=lambda x: x[1], reverse=True)
    return reduced_data


def get_top_counties(db_path: str = 'switrs.sqlite',
                     table_name: str = 'collisions',
                     top_n: int = 15) -> List[Tuple[str, int]]:
    """Основная функция для получения ТОП-15 округов по ДТП"""
    print("=" * 60)
    print("ТОП-15 ОКРУГОВ КАЛИФОРНИИ ПО КОЛИЧЕСТВУ ДТП")
    print("=" * 60)

    # Шаг 1: Map
    print("1. Извлечение данных из базы...")
    mapped_data = mapper_counties(db_path, table_name)
    print(f"   Обработано записей: {len(mapped_data):,}")

    # Шаг 2: Shuffle & Sort
    print("2. Группировка данных...")
    grouped_data = shuffler(mapped_data)

    # Шаг 3: Reduce
    print("3. Подсчет количества ДТП...")
    reduced_data = reducer_counties(grouped_data)

    # Вывод результатов
    print("\n" + "=" * 60)
    print(f"ТОП-{top_n} ОКРУГОВ КАЛИФОРНИИ ПО КОЛИЧЕСТВУ ДТП")
    print("=" * 60)

    for i, (county, count) in enumerate(reduced_data[:top_n], 1):
        print(f"{i:2d}. {county.title()}: {count:,} ДТП")

    # Общая статистика
    total_accidents = sum(count for _, count in reduced_data)
    unique_counties = len(reduced_data)
    print(f"\nВсего ДТП в анализе: {total_accidents:,}")
    print(f"Всего уникальных округов: {unique_counties}")

    return reduced_data[:top_n]


def main():
    db_path = 'switrs.sqlite'
    table_name = 'collisions'

    print("АНАЛИЗ ДТП В КАЛИФОРНИИ ПО ОКРУГАМ")
    print("=" * 60)

    try:
        top_counties = get_top_counties(db_path, table_name)
    except sqlite3.OperationalError as e:
        print(f"\nОшибка доступа к базе данных: {e}")

if __name__ == "__main__":
    main()