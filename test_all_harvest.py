"""
Тестовый скрипт для проверки всех harvest скриптов.
Запускает каждый скрипт с ограничением на 1 цитату для быстрой проверки.
"""

import sys
import os
import json
import logging
from pathlib import Path

# Добавляем load в путь для импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'load'))

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Создаем директорию data если не существует
os.makedirs('data', exist_ok=True)


def test_harvest_quotable():
    """Тест harvest_quotable.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_quotable.py")
    logger.info("=" * 60)
    try:
        from harvest_quotable import harvest_quotable
        quotes = harvest_quotable(output_file="data/test_quotable.json")
        logger.info(f"✓ harvest_quotable: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_quotable failed: {e}")
        return False, []


def test_harvest_zenquotes():
    """Тест harvest_zenquotes.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_zenquotes.py")
    logger.info("=" * 60)
    try:
        from harvest_zenquotes import harvest_zenquotes
        quotes = harvest_zenquotes(max_quotes=1, output_file="data/test_zenquotes.json")
        logger.info(f"✓ harvest_zenquotes: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_zenquotes failed: {e}")
        return False, []


def test_harvest_goodreads():
    """Тест harvest_goodreads.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_goodreads.py")
    logger.info("=" * 60)
    try:
        from harvest_goodreads import harvest_goodreads
        quotes = harvest_goodreads(max_pages=1, output_file="data/test_goodreads.json")
        logger.info(f"✓ harvest_goodreads: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_goodreads failed: {e}")
        return False, []


def test_harvest_brainyquote():
    """Тест harvest_brainyquote.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_brainyquote.py")
    logger.info("=" * 60)
    try:
        from harvest_brainyquote import harvest_brainyquote
        quotes = harvest_brainyquote(topics=["motivational"], output_file="data/test_brainyquote.json")
        logger.info(f"✓ harvest_brainyquote: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_brainyquote failed: {e}")
        return False, []


def test_harvest_forismatic():
    """Тест harvest_forismatic.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_forismatic.py")
    logger.info("=" * 60)
    try:
        from harvest_forismatic import harvest_forismatic
        quotes = harvest_forismatic(max_quotes=1, languages=['en'], output_file="data/test_forismatic.json")
        logger.info(f"✓ harvest_forismatic: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_forismatic failed: {e}")
        return False, []


def test_harvest_citaty_net():
    """Тест harvest_citaty_net.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_citaty_net.py")
    logger.info("=" * 60)
    try:
        from harvest_citaty_net import harvest_citaty_net
        quotes = harvest_citaty_net(max_pages=1, output_file="data/test_citaty_net.json")
        logger.info(f"✓ harvest_citaty_net: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_citaty_net failed: {e}")
        return False, []


def test_harvest_citaty_info():
    """Тест harvest_citaty_info.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_citaty_info.py")
    logger.info("=" * 60)
    try:
        from harvest_citaty_info import harvest_citaty_info
        quotes = harvest_citaty_info(max_pages=1, output_file="data/test_citaty_info.json")
        logger.info(f"✓ harvest_citaty_info: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_citaty_info failed: {e}")
        return False, []


def test_harvest_aphorizm_ru():
    """Тест harvest_aphorizm_ru.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_aphorizm_ru.py")
    logger.info("=" * 60)
    try:
        from harvest_aphorizm_ru import harvest_aphorizm_ru
        quotes = harvest_aphorizm_ru(max_pages=1, output_file="data/test_aphorizm_ru.json")
        logger.info(f"✓ harvest_aphorizm_ru: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_aphorizm_ru failed: {e}")
        return False, []


def test_harvest_anecdot_ru():
    """Тест harvest_anecdot_ru.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_anecdot_ru.py")
    logger.info("=" * 60)
    try:
        from harvest_anecdot_ru import harvest_anecdot_ru
        quotes = harvest_anecdot_ru(max_pages=1, output_file="data/test_anecdot_ru.json")
        logger.info(f"✓ harvest_anecdot_ru: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_anecdot_ru failed: {e}")
        return False, []


def test_harvest_wikiquote_ru():
    """Тест harvest_wikiquote_ru.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_wikiquote_ru.py")
    logger.info("=" * 60)
    try:
        from harvest_wikiquote_ru import harvest_wikiquote_ru
        quotes = harvest_wikiquote_ru(authors=["Лев Толстой"], output_file="data/test_wikiquote_ru.json")
        logger.info(f"✓ harvest_wikiquote_ru: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_wikiquote_ru failed: {e}")
        return False, []


def test_harvest_doc_files():
    """Тест harvest_doc_files.py"""
    logger.info("=" * 60)
    logger.info("Testing harvest_doc_files.py")
    logger.info("=" * 60)
    try:
        from harvest_doc_files import harvest_doc_files
        quotes = harvest_doc_files(folder_path="data", output_file="data/test_doc_files.json")
        logger.info(f"✓ harvest_doc_files: {len(quotes)} quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ harvest_doc_files failed: {e}")
        return False, []


def test_merge_quotes():
    """Тест merge_quotes.py"""
    logger.info("=" * 60)
    logger.info("Testing merge_quotes.py")
    logger.info("=" * 60)
    try:
        from merge_quotes import merge_quotes
        quotes = merge_quotes(
            input_pattern="data/test_*.json",
            exclude_files=[],
            output_json="data/test_ALL_QUOTES.json",
            output_txt="data/test_ALL_QUOTES.txt"
        )
        logger.info(f"✓ merge_quotes: {len(quotes)} unique quotes")
        return len(quotes) > 0, quotes
    except Exception as e:
        logger.error(f"✗ merge_quotes failed: {e}")
        return False, []


def test_import_to_postgres():
    """Тест import_to_postgres.py"""
    logger.info("=" * 60)
    logger.info("Testing import_to_postgres.py")
    logger.info("=" * 60)
    try:
        from import_to_postgres import import_to_postgres
        stats = import_to_postgres(
            input_file="data/test_ALL_QUOTES.json",
            clear_existing=False
        )
        logger.info(f"✓ import_to_postgres: {stats.get('saved', 0)} saved, {stats.get('skipped', 0)} skipped")
        return stats.get('saved', 0) > 0 or stats.get('skipped', 0) > 0, stats
    except Exception as e:
        logger.error(f"✗ import_to_postgres failed: {e}")
        return False, {}


def main():
    """Главная функция тестирования"""
    logger.info("=" * 60)
    logger.info("Starting comprehensive harvest scripts test")
    logger.info("=" * 60)
    
    results = {}
    all_quotes = []
    
    # Тестируем все harvest скрипты
    tests = [
        ("harvest_quotable", test_harvest_quotable),
        ("harvest_zenquotes", test_harvest_zenquotes),
        ("harvest_goodreads", test_harvest_goodreads),
        ("harvest_brainyquote", test_harvest_brainyquote),
        ("harvest_forismatic", test_harvest_forismatic),
        ("harvest_citaty_net", test_harvest_citaty_net),
        ("harvest_citaty_info", test_harvest_citaty_info),
        ("harvest_aphorizm_ru", test_harvest_aphorizm_ru),
        ("harvest_anecdot_ru", test_harvest_anecdot_ru),
        ("harvest_wikiquote_ru", test_harvest_wikiquote_ru),
        ("harvest_doc_files", test_harvest_doc_files),
    ]
    
    for test_name, test_func in tests:
        success, quotes = test_func()
        results[test_name] = {
            "success": success,
            "quotes_count": len(quotes) if quotes else 0
        }
        if quotes:
            all_quotes.extend(quotes)
    
    # Тестируем merge
    merge_success, merged_quotes = test_merge_quotes()
    results["merge_quotes"] = {
        "success": merge_success,
        "quotes_count": len(merged_quotes) if merged_quotes else 0
    }
    
    # Тестируем импорт
    import_success, import_stats = test_import_to_postgres()
    results["import_to_postgres"] = {
        "success": import_success,
        "stats": import_stats
    }
    
    # Итоговый отчет
    logger.info("=" * 60)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 60)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result["success"] else "✗ FAIL"
        if "quotes_count" in result:
            logger.info(f"{status} {test_name}: {result['quotes_count']} quotes")
        else:
            logger.info(f"{status} {test_name}")
    
    total_harvested = sum(r.get("quotes_count", 0) for r in results.values() if "quotes_count" in r)
    logger.info(f"\nTotal quotes harvested: {total_harvested}")
    logger.info(f"Total unique after merge: {len(merged_quotes) if merged_quotes else 0}")
    
    if import_stats:
        logger.info(f"Imported to DB: {import_stats.get('saved', 0)}")
        logger.info(f"Skipped (duplicates): {import_stats.get('skipped', 0)}")
    
    logger.info("=" * 60)
    
    # Проверяем наличие дубликатов
    if merged_quotes:
        texts = [q.get("text", "").lower().strip() for q in merged_quotes if q.get("text")]
        unique_texts = set(texts)
        duplicates = len(texts) - len(unique_texts)
        logger.info(f"Deduplication check: {duplicates} duplicates found in {len(texts)} quotes")
    
    return all(results[test_name]["success"] for test_name, _ in tests)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
