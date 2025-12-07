import sqlite3

DB_NAME = 'search_engine.db'
DAMPING = 0.85
ITERATIONS = 20

def load_graph():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Загружаем все документы из базы
    c.execute('SELECT id, url FROM documents')
    docs = {row[0]: row[1] for row in c.fetchall()}

    pagerank = {doc_id: 1.0 / len(docs) for doc_id in docs}

    # Загружаем все ссылки и оставляем только те, которые соединяют документы из базы
    c.execute('SELECT src_doc_id, dest_doc_id FROM links')
    graph = {}
    for src, dst in c.fetchall():
        if src in docs and dst in docs:
            graph.setdefault(src, []).append(dst)

    conn.close()
    return docs, graph, pagerank

def pagerank_mapreduce():
    docs, graph, pagerank = load_graph()
    N = len(docs)

    for it in range(ITERATIONS):
        new_rank = {doc: (1 - DAMPING) / N for doc in docs}
        for doc, rank in pagerank.items():
            out_links = graph.get(doc, [])
            if out_links:
                share = rank / len(out_links)
                for dst in out_links:
                    new_rank[dst] += DAMPING * share
            else:
                # Для "висячих" страниц (без исходящих ссылок)
                for dst in docs:
                    new_rank[dst] += DAMPING * (rank / N)
        pagerank = new_rank

    print("PageRank (MapReduce) для документов из базы:")
    for doc_id, url in docs.items():
        print(f"ID={doc_id}  PR={pagerank[doc_id]:.6f}  URL={url}")

if __name__ == '__main__':
    pagerank_mapreduce()
