import requests
import sqlite3
from datetime import datetime, timedelta
import hashlib
from typing import Optional, Tuple, List, Dict
import argparse
from contextlib import closing

def create_tables(cursor: sqlite3.Cursor) -> None:
    create_website_table_query = '''
    CREATE TABLE IF NOT EXISTS websites (
        url TEXT PRIMARY KEY,
        timestamp DATETIME,
        content_hash TEXT,
        content TEXT
    )
    '''
    create_urls_table_query = '''
    CREATE TABLE IF NOT EXISTS urls (
        url TEXT PRIMARY KEY
    )
    '''
    create_website_archive_table_query = '''
    CREATE TABLE IF NOT EXISTS website_archive (
        url TEXT,
        timestamp DATETIME,
        content_hash TEXT,
        content TEXT
    )
    '''
    cursor.execute(create_website_table_query)
    cursor.execute(create_website_archive_table_query)
    cursor.execute(create_urls_table_query)

def update_urls(cursor: sqlite3.Cursor, new_urls: List[str]) -> None:
    timestamp = datetime.now()
    for url in new_urls:
        try:
            cursor.execute('INSERT OR IGNORE INTO urls (url, last_checked) VALUES (?, ?)', (url, timestamp))
            cursor.execute('UPDATE urls SET last_checked = ? WHERE url = ?', (timestamp, url))
        except sqlite3.Error as e:
            print(f"Database error occurred while updating URL {url}: {e}")
    print(f"Added {len(new_urls)} url(s).")


def remove_urls(cursor: sqlite3.Cursor, del_urls: List[str]) -> None:
    del_cnt = 0
    for url in del_urls:
        try:
            cursor.execute('DELETE FROM urls WHERE url = ?', (url,))
            del_cnt += 1
        except sqlite3.Error as e:
            print(f"Database error occurred while deleting URL {url}: {e}")
    print(f"Deleted {del_cnt} url(s).")

def get_all_urls(cursor: sqlite3.Cursor) -> List[str]:
    cursor.execute('SELECT url FROM urls')
    return [row[0] for row in cursor.fetchall()]

def get_website_content(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(f"Request error occurred with {url}:", err)
        return ''
    return response.text

def get_content_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()

def should_fetch_content(cursor: sqlite3.Cursor, url: str, time_delta_minutes: int) -> Tuple[bool, Optional[str]]:
    cursor.execute('SELECT content_hash FROM websites WHERE url = ?', (url,))
    hash_result = cursor.fetchone()
    cursor.execute('SELECT last_checked FROM urls WHERE url = ?', (url,))
    timestamp = cursor.fetchone()[0]

    if hash_result is not None:
        old_content_hash = hash_result[0]
    else:
        return True, None

    datetime_last_checked = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    timestamp_now_minus_window = datetime.now() - timedelta(minutes=time_delta_minutes)
    if datetime_last_checked < timestamp_now_minus_window:
        # print(f'URL {url} has not been checked in {time_delta_minutes} minutes.')
        return True, old_content_hash
    else:
        print(f'URL {url} has been checked in last {time_delta_minutes} minutes.')

    return False, None

def store_website_content(cursor: sqlite3.Cursor, url: str, content: str) -> None:
    timestamp = datetime.now()
    content_hash = get_content_hash(content)

    cursor.execute('INSERT INTO websites (url, timestamp, content_hash, content) VALUES (?, ?, ?, ?)', (url, timestamp, content_hash, content))

def archive_old_website_content(cursor: sqlite3.Cursor, url: str) -> str:

    cursor.execute('SELECT * FROM websites WHERE url = ?', (url,))
    data = cursor.fetchone()
    website_content = data[3]

    assert data is not None, "Attempted to archive old record, but couldn't find data to archive."

    cursor.execute('INSERT INTO website_archive (url, timestamp, content_hash, content) VALUES (?, ?, ?, ?)', data)
    cursor.execute('DELETE FROM websites WHERE url = ?', (url,))
    return website_content

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Monitor changes in websites.')
    parser.add_argument('--new_urls', nargs='+', default=[],
                        help='List of new URLs to monitor.')
    parser.add_argument('--del_urls', nargs='+', default=[],
                        help='List of URLs to stop monitoring.')
    parser.add_argument('--time_delta_minutes', type=int, default=60,
                        help='Time window in minutes for checking updates.')
    return parser.parse_args()

def highlight_diffs(old_content: str, new_content: str) -> str:
    from difflib import unified_diff
    from pygments import highlight
    from pygments.lexers import DiffLexer
    from pygments.formatters import TerminalFormatter

    diff = unified_diff(old_content.splitlines(keepends=True), new_content.splitlines(keepends=True), fromfile='old', tofile='new')
    changed_lines = [line for line in diff if line.startswith('-') or line.startswith('+')]
    return highlight(''.join(changed_lines), DiffLexer(), TerminalFormatter())

def monitor_website_changes(cursor: sqlite3.Cursor, urls: List[str], time_delta_minutes: int) -> Dict[str, int]:
    stats = {'num_errors': 0, 'num_fetches': 0, 'num_changes': 0, 'num_new_pages': 0}

    for url in urls:
        should_fetch, old_content_hash = should_fetch_content(cursor, url, time_delta_minutes)
        if should_fetch:
            stats['num_fetches'] += 1
            print(f'Evaluating URL: {url}')
            current_content = get_website_content(url)
            if not current_content:
                stats['num_errors'] += 1
                continue
            current_content_hash = get_content_hash(current_content)
            if old_content_hash is None:
                print(f'Adding new page: {url}')
                stats['num_new_pages'] += 1
                store_website_content(cursor, url, current_content)
            elif old_content_hash != current_content_hash:
                print(f'Page has changed: {url}')
                stats['num_changes'] += 1
                old_content = archive_old_website_content(cursor, url)
                print(highlight_diffs(current_content, old_content))
                store_website_content(cursor, url, current_content)
            cursor.execute('UPDATE urls SET last_checked = ? WHERE url = ?', (datetime.now(), url))

    return stats


def main() -> None:
    args = parse_args()

    with sqlite3.connect('website_content.db') as conn, closing(conn.cursor()) as cursor:
        create_tables(cursor)
        update_urls(cursor, args.new_urls)
        remove_urls(cursor, args.del_urls)
        urls = get_all_urls(cursor)

        print(f'Number of URLs to evaluate: {len(urls)}')

        stats = monitor_website_changes(cursor, urls, args.time_delta_minutes)
        
        conn.commit()

    if stats['num_fetches'] > 0:
        print(f'Total new pages added: {stats["num_new_pages"]}')
        print(f'Total pages changed: {stats["num_changes"]}')
        print(f'Total errors: {stats["num_errors"]}')

if __name__ == '__main__':
    main()
