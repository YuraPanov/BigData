import subprocess
import sys

PYTHON = sys.executable


def run_script(script_name: str) -> None:
    """Запускает скрипт Python"""
    print(f"\n>>> Запуск {script_name}...")
    subprocess.call([PYTHON, script_name])
    print(f"<<< Завершён {script_name}\n")


def main_menu() -> None:
    """Главное меню системы"""
    while True:
        print("\n=======================================")
        print("        ПОИСКОВАЯ СИСТЕМА")
        print("=======================================")
        print("1. Парсинг документов")
        print("2. PageRank (MapReduce)")
        print("3. PageRank (Pregel)")
        print("4. Поиск (Document-at-a-Time)")
        print("0. Выход")
        print("=======================================")

        choice = input("Выберите пункт меню: ").strip()

        if choice == "1":
            run_script("parser_and_db.py")
        elif choice == "2":
            run_script("pagerank_mapreduce.py")
        elif choice == "3":
            run_script("pagerank_pregel.py")
        elif choice == "4":
            run_script("doc_at_a_time.py")
        elif choice == "0":
            print("Выход.")
            break
        else:
            print("Неверный выбор.")


if __name__ == "__main__":
    main_menu()