import unittest
import sqlite3
from app import create_tables, update_urls, remove_urls, get_all_urls, should_fetch_content, get_website_content, get_content_hash, store_website_content, archive_old_website_content

class TestApp(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        create_tables(self.cursor)

    def tearDown(self):
        self.conn.close()

    def test_update_urls(self):
        update_urls(self.cursor, ['http://example.com'])
        self.cursor.execute('SELECT * FROM urls')
        result = self.cursor.fetchone()
        self.assertEqual(result[0], 'http://example.com')

    def test_remove_urls(self):
        update_urls(self.cursor, ['http://example.com'])
        remove_urls(self.cursor, ['http://example.com'])
        self.cursor.execute('SELECT * FROM urls')
        result = self.cursor.fetchone()
        self.assertIsNone(result)

    def test_get_all_urls(self):
        urls = ['http://example.com','http://example2.com']
        update_urls(self.cursor, urls)
        result = get_all_urls(self.cursor)
        self.assertEqual(result, urls)

    def test_should_fetch_content(self):
        store_website_content(self.cursor, 'http://example.com', 'old content')
        result = should_fetch_content(self.cursor, 'http://example.com', 60)
        self.assertEqual(result, (True, get_content_hash('old content')))

    def test_get_website_content(self):
        result = get_website_content('http://example.com')
        self.assertIsNotNone(result)

    def test_get_content_hash(self):
        result = get_content_hash('example content')
        self.assertEqual(result, 'a9c4a6f9f8d6d7eae8f7f1c9e5d9d5d9')

    def test_store_website_content(self):
        store_website_content(self.cursor, 'http://example.com', 'example content')
        self.cursor.execute('SELECT * FROM website_content')
        result = self.cursor.fetchone()
        self.assertEqual(result[0], 'http://example.com')
        self.assertEqual(result[1], 'example content')

    def test_archive_old_website_content(self):
        store_website_content(self.cursor, 'http://example.com', 'old content')
        archive_old_website_content(self.cursor, 'http://example.com')
        self.cursor.execute('SELECT * FROM website_content_archive')
        result = self.cursor.fetchone()
        self.assertEqual(result[0], 'http://example.com')
        self.assertEqual(result[1], 'old content')

if __name__ == '__main__':
    unittest.main()