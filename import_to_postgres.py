"""
Импорт цитат из JSON файлов в PostgreSQL.
Выполняет дедупликацию и загружает в таблицу quotations.
"""

import json
import os
import logging
from typing import List, Dict, Set
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Получение подключения к БД.

    Returns:
        Подключение к PostgreSQL
    """
    db_url = os.getenv('DB_URL')
    if not db_url:
        raise ValueError("DB_URL not set in environment")

    return psycopg2.connect(db_url)


def init_quotations_table(conn):
    """
    Инициализация таблицы quotations если не существует.

    Args:
        conn: Подключение к БД
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quotations (
            id SERIAL PRIMARY KEY,
            text_original TEXT NOT NULL,
            language_original VARCHAR(10) NOT NULL,
            text_translated TEXT,
            language_translated VARCHAR(10),
            author VARCHAR(255),
            source_url VARCHAR(500),
            tags TEXT[],
            is_validated BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(text_original, language_original)
        )
    """)
    conn.commit()
    cur.close()
    logger.info("Quotations table initialized")


def is_valid_quotation(text: str) -> bool:
    """
    Проверка валидности цитаты (строгие фильтры).

    Args:
        text: Текст цитаты

    Returns:
        True если цитата валидна
    """
    import re

    if not text or len(text.strip()) < 20:
        return False

    # Убираем лишние пробелы
    text = ' '.join(text.split())

    # НЕ допускаем:
    # - Цифры (арабские и римские)
    if re.search(r'\d', text):
        return False

    # - Римские цифры (кроме 'I' как местоимения)
    roman_numerals = re.search(r'\b[IVXLCDM]{2,}\b', text, re.IGNORECASE)
    if roman_numerals:
        return False

    # - Имена людей (два подряд заглавных слова)
    if re.search(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+', text):
        # Исключаем начало предложения
        if not text[0].isupper():
            return False

    # - Адреса, места
    place_keywords = [
        r'\bstreet\b', r'\bavenue\b', r'\broad\b', r'\bdrive\b',
        r'\bулица\b', r'\bпроспект\b', r'\bпереулок\b',
        r'\bmoscow\b', r'\blondon\b', r'\bparis\b'
    ]
    for keyword in place_keywords:
        if re.search(keyword, text, re.IGNORECASE):
            return False

    # - Названия книг, фильмов (в кавычках с заглавными буквами)
    if re.search(r'["«»""][A-Z][a-z]+.*[A-Z][a-z]+["«»""]', text):
        return False

    # - URL и email
    if re.search(r'http[s]?://|www\.|@', text, re.IGNORECASE):
        return False

    # - Даты
    month_names = [
        r'\bjanuary\b', r'\bfebruary\b', r'\bmarch\b', r'\bapril\b',
        r'\bmay\b', r'\bjune\b', r'\bjuly\b', r'\baugust\b',
        r'\bseptember\b', r'\boctober\b', r'\bnovember\b', r'\bdecember\b',
        r'\bянварь\b', r'\bфевраль\b', r'\bмарт\b', r'\bапрель\b',
        r'\bмай\b', r'\bиюнь\b', r'\bиюль\b', r'\bавгуст\b',
        r'\bсентябрь\b', r'\bоктябрь\b', r'\bноябрь\b', r'\bдекабрь\b'
    ]
    for month in month_names:
        if re.search(month, text, re.IGNORECASE):
            return False

    # - Театральные ссылки
    theater_keywords = [
        r'\bact\b', r'\bscene\b', r'\bpage\b', r'\bchapter\b',
        r'\bакт\b', r'\bсцена\b', r'\bстраница\b', r'\bглава\b'
    ]
    for keyword in theater_keywords:
        if re.search(keyword, text, re.IGNORECASE):
            return False

    # - Спам паттерны (повторяющиеся символы)
    if re.search(r'(.)\1{4,}', text):
        return False

    return True


def get_db_statistics() -> Dict[str, any]:
    """
    Получение статистики из БД.

    Returns:
        Словарь со статистикой из БД
    """
    stats = {
        "total_quotes": 0,
        "by_language": {},
        "by_author": {},
        "total_authors": 0,
        "authors_en": 0,
        "authors_ru": 0
    }

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Общее количество цитат
        cur.execute("SELECT COUNT(*) FROM quotations")
        stats["total_quotes"] = cur.fetchone()[0]

        # По языкам
        cur.execute("""
            SELECT language_original, COUNT(*) as count
            FROM quotations
            GROUP BY language_original
            ORDER BY count DESC
        """)
        for row in cur.fetchall():
            lang, count = row
            stats["by_language"][lang] = count

        # По авторам (топ 20)
        cur.execute("""
            SELECT author, COUNT(*) as count
            FROM quotations
            WHERE author IS NOT NULL
            GROUP BY author
            ORDER BY count DESC
            LIMIT 20
        """)
        for row in cur.fetchall():
            author, count = row
            stats["by_author"][author] = count

        # Общее количество уникальных авторов
        cur.execute("""
            SELECT COUNT(DISTINCT author)
            FROM quotations
            WHERE author IS NOT NULL
        """)
        stats["total_authors"] = cur.fetchone()[0]

        # Авторы по языкам
        cur.execute("""
            SELECT language_original, COUNT(DISTINCT author) as count
            FROM quotations
            WHERE author IS NOT NULL
            GROUP BY language_original
        """)
        for row in cur.fetchall():
            lang, count = row
            if lang == 'en':
                stats["authors_en"] = count
            elif lang == 'ru':
                stats["authors_ru"] = count

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return stats

    return stats


def print_db_statistics():
    """
    Вывод статистики из БД.
    """
    logger.info("=" * 60)
    logger.info("Database Statistics")
    logger.info("=" * 60)

    stats = get_db_statistics()

    logger.info(f"Total quotes: {stats['total_quotes']}")
    logger.info("")

    logger.info("By language:")
    for lang, count in sorted(
        stats['by_language'].items(),
        key=lambda x: x[1],
        reverse=True
    ):
        lang_name = "English" if lang == 'en' else "Russian" if lang == 'ru' else lang
        logger.info(f"  {lang_name} ({lang}): {count} quotes")

    logger.info("")
    logger.info(f"Total authors: {stats['total_authors']}")
    if stats['authors_en'] > 0:
        logger.info(f"  English authors: {stats['authors_en']}")
    if stats['authors_ru'] > 0:
        logger.info(f"  Russian authors: {stats['authors_ru']}")

    if stats['by_author']:
        logger.info("")
        logger.info("Top authors:")
        for author, count in list(stats['by_author'].items())[:10]:
            logger.info(f"  {author}: {count} quotes")

    logger.info("=" * 60)

    return stats


def import_to_postgres(
    input_file: str = "ALL_QUOTES.json",
    clear_existing: bool = False
) -> Dict[str, any]:
    """
    Импорт цитат из JSON файла в PostgreSQL.

    Args:
        input_file: Путь к JSON файлу с цитатами
        clear_existing: Очистить существующие цитаты перед импортом

    Returns:
        Словарь со статистикой импорта
    """
    stats = {
        "loaded": 0,
        "saved": 0,
        "skipped": 0,
        "errors": 0,
        "final_stats": {}
    }

    # Загружаем цитаты из файла
    logger.info(f"Loading quotes from {input_file}...")
    try:
        with open(input_file, encoding="utf-8") as f:
            quotes = json.load(f)
        stats["loaded"] = len(quotes)
        logger.info(f"Loaded {len(quotes)} quotes from file")
    except FileNotFoundError:
        logger.error(f"File {input_file} not found")
        return stats
    except Exception as e:
        logger.error(f"Error loading file: {e}")
        return stats

    # Подключаемся к БД
    try:
        conn = get_db_connection()
        init_quotations_table(conn)
        cur = conn.cursor()

        # Очищаем существующие цитаты если нужно
        if clear_existing:
            logger.warning("Clearing existing quotations...")
            cur.execute("DELETE FROM quotations")
            conn.commit()
            logger.info("Existing quotations cleared")

        # Импортируем цитаты
        logger.info("Importing quotes to PostgreSQL...")
        for quote in quotes:
            try:
                text = quote.get("text", "").strip()
                author = quote.get("author")
                source = quote.get("source", "")
                lang = quote.get("lang", "en")

                # Пропускаем пустые цитаты
                if not text:
                    stats["skipped"] += 1
                    continue

                # Применяем строгие фильтры
                if not is_valid_quotation(text):
                    stats["skipped"] += 1
                    continue

                # Проверяем на дубликаты
                cur.execute("""
                    SELECT id FROM quotations
                    WHERE text_original = %s AND language_original = %s
                """, (text, lang))

                existing = cur.fetchone()
                if existing:
                    stats["skipped"] += 1
                    continue

                # Вставляем новую цитату
                cur.execute("""
                    INSERT INTO quotations
                    (text_original, language_original, author, source_url, tags)
                    VALUES (%s, %s, %s, %s, %s)
                """, (text, lang, author, source, ['general']))

                stats["saved"] += 1

                if stats["saved"] % 100 == 0:
                    conn.commit()
                    logger.info(f"Saved {stats['saved']} quotes...")

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error importing quote: {e}")
                conn.rollback()
                continue

        conn.commit()

        # Получаем финальную статистику из БД
        final_stats = get_db_statistics()
        stats["final_stats"] = final_stats

        cur.close()
        conn.close()

        logger.info("=" * 60)
        logger.info("Import completed:")
        logger.info(f"  Loaded from file: {stats['loaded']}")
        logger.info(f"  Saved to DB: {stats['saved']}")
        logger.info(f"  Skipped (duplicates/invalid): {stats['skipped']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info("")
        logger.info("Final database statistics:")
        logger.info(f"  Total quotes in DB: {final_stats['total_quotes']}")
        logger.info("  By language:")
        for lang, count in sorted(
            final_stats['by_language'].items(),
            key=lambda x: x[1],
            reverse=True
        ):
            lang_name = "English" if lang == 'en' else "Russian" if lang == 'ru' else lang
            logger.info(f"    {lang_name} ({lang}): {count} quotes")
        logger.info(f"  Total authors: {final_stats['total_authors']}")
        if final_stats['authors_en'] > 0:
            logger.info(f"    English authors: {final_stats['authors_en']}")
        if final_stats['authors_ru'] > 0:
            logger.info(f"    Russian authors: {final_stats['authors_ru']}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return stats

    return stats


if __name__ == "__main__":
    import sys

    # Проверяем опцию статистики
    if "--stats" in sys.argv or "--statistics" in sys.argv or "-s" in sys.argv:
        print_db_statistics()
        sys.exit(0)

    clear = "--clear" in sys.argv or "-c" in sys.argv
    input_file = "ALL_QUOTES.json"

    # Парсим аргументы
    for i, arg in enumerate(sys.argv):
        if arg == "--file" and i + 1 < len(sys.argv):
            input_file = sys.argv[i + 1]

    import_to_postgres(input_file=input_file, clear_existing=clear)
