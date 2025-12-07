import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import time
import re
from nltk.corpus import stopwords

stop_words = set(stopwords.words('russian')) | set(stopwords.words('english'))
DB_NAME = 'search_engine.db'

ALLOWED_DOCS = [
    'https://ru.wikipedia.org/wiki/Большие_данные',
    'https://ru.wikipedia.org/wiki/MapReduce',
    'https://ru.wikipedia.org/wiki/Python',
    'https://ru.wikipedia.org/wiki/Fallout',
    'https://ru.wikipedia.org/wiki/Fallout:_New_Vegas',
    'https://ru.wikipedia.org/wiki/Fallout_4'
]


def create_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS documents(id INTEGER PRIMARY KEY, url TEXT UNIQUE)')
    c.execute('CREATE TABLE IF NOT EXISTS words(id INTEGER PRIMARY KEY, word TEXT UNIQUE)')
    c.execute('CREATE TABLE IF NOT EXISTS doc_words(doc_id INTEGER, word_id INTEGER, PRIMARY KEY (doc_id, word_id))')
    c.execute('CREATE TABLE IF NOT EXISTS links(src_doc_id INTEGER, dest_doc_id INTEGER)')
    conn.commit()
    return conn, c


def tokenize_text(text):
    tokens = re.findall(r'\b\w+\b', text.lower())
    return [w for w in tokens if w not in stop_words]


def parse_document(url):
    headers = {'User-Agent': 'mini-search-engine-bot/0.1'}
    response = requests.get(url, headers=headers)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    content_div = soup.find('div', id='bodyContent') or soup.find('div', id='mw-content-text') or soup

    text = content_div.get_text()
    tokens = tokenize_text(text)

    links = []
    for a in content_div.find_all('a', href=True):
        href = a['href']
        if href.startswith('/wiki/') and ':' not in href:
            full = urljoin('https://ru.wikipedia.org', href)
            if full in ALLOWED_DOCS:
                links.append(full)

    return tokens, links


def save_to_db(c, conn, url, words, links):
    c.execute('INSERT OR IGNORE INTO documents(url) VALUES (?)', (url,))
    c.execute('SELECT id FROM documents WHERE url = ?', (url,))
    doc_id = c.fetchone()[0]

    for word in set(words):
        c.execute('INSERT OR IGNORE INTO words(word) VALUES (?)', (word,))
        c.execute('SELECT id FROM words WHERE word = ?', (word,))
        word_id = c.fetchone()[0]
        c.execute('INSERT OR IGNORE INTO doc_words(doc_id, word_id) VALUES (?, ?)', (doc_id, word_id))

    for link in set(links):
        c.execute('INSERT OR IGNORE INTO documents(url) VALUES (?)', (link,))
        c.execute('SELECT id FROM documents WHERE url = ?', (link,))
        dest_id = c.fetchone()[0]
        c.execute('INSERT OR IGNORE INTO links(src_doc_id, dest_doc_id) VALUES (?, ?)', (doc_id, dest_id))

    conn.commit()


def main():
    conn, c = create_db()

    for url in ALLOWED_DOCS:
        print(f"Парсим {url}")
        words, links = parse_document(url)
        print(f"  Всего слов: {len(words)}, Уникальных слов: {len(set(words))}")
        save_to_db(c, conn, url, words, links)
        time.sleep(1)

    conn.close()
    print("Готово.")


if __name__ == '__main__':
    main()