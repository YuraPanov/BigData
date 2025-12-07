import sqlite3
import math
from collections import defaultdict

DB_PATH = "old/search_engine.db"


def build_inverted_index():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT w.word, d.id, dw.tf
        FROM doc_words dw
        JOIN documents d ON dw.doc_id = d.id
        JOIN words w ON dw.word_id = w.id
    """)

    index = defaultdict(list)
    for word, doc_id, tf in cur.fetchall():
        index[word].append((doc_id, tf))

    conn.close()
    return index


def document_at_a_time(query, index, total_docs):
    """DAAT-обработка запроса с TF-IDF"""
    terms = query.lower().split()

    lists = []
    for term in terms:
        if term in index:
            lists.append(sorted(index[term], key=lambda x: x[0]))

    if not lists:
        return []

    # Вычисляем IDF
    idf_cache = {}
    for term in terms:
        if term in index:
            df = len(index[term])
            idf = math.log(total_docs / df)
            idf_cache[term] = idf

    pointers = [0] * len(lists)
    scores = defaultdict(float)

    while True:
        current_docs = []
        for i in range(len(lists)):
            if pointers[i] < len(lists[i]):
                current_docs.append(lists[i][pointers[i]][0])
            else:
                current_docs.append(None)

        if all(doc is None for doc in current_docs):
            break

        min_doc = float('inf')
        for doc in current_docs:
            if doc is not None and doc < min_doc:
                min_doc = doc

        for i in range(len(lists)):
            if pointers[i] < len(lists[i]) and lists[i][pointers[i]][0] == min_doc:
                doc_id, tf = lists[i][pointers[i]]
                term = terms[i]
                if term in idf_cache:
                    scores[doc_id] += tf * idf_cache[term]
                pointers[i] += 1

    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def term_at_a_time(query, index, total_docs):
    terms = query.lower().split()

    idf_cache = {}
    for term in terms:
        if term in index:
            df = len(index[term])
            idf = math.log(total_docs / df)
            idf_cache[term] = idf

    scores = defaultdict(float)

    for term in terms:
        if term in idf_cache:
            idf = idf_cache[term]
            for doc_id, tf in index[term]:
                scores[doc_id] += tf * idf

    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def main():
    index = build_inverted_index()

    # Получаем общее количество документов
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM documents")
    total_docs = cur.fetchone()[0]
    cur.execute("SELECT id, url FROM documents")
    url_map = {doc_id: url for doc_id, url in cur.fetchall()}
    conn.close()

    query = input("Введите поисковый запрос: ")

    print("\n--- Document-at-a-Time ---")
    daat_results = document_at_a_time(query, index, total_docs)
    for doc_id, score in daat_results[:10]:
        url = url_map.get(doc_id, "Unknown URL")
        print(f"DocID={doc_id}, Score={score:.2f}, URL={url}")

    print("\n--- Term-at-a-Time ---")
    taat_results = term_at_a_time(query, index, total_docs)
    for doc_id, score in taat_results[:10]:
        url = url_map.get(doc_id, "Unknown URL")
        print(f"DocID={doc_id}, Score={score:.2f}, URL={url}")


if __name__ == "__main__":
    main()