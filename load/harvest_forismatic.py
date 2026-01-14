"""
Сбор цитат с Forismatic API (английский и русский).
API: http://api.forismatic.com/api/1.0/
"""

import requests
import json
import time
import logging
from tqdm import tqdm
from typing import List, Dict
import re

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

FORISMATIC_URL = "http://api.forismatic.com/api/1.0/"


def is_valid_quotation(text: str) -> bool:
    """
    Проверка валидности цитаты (строгие фильтры).

    Args:
        text: Текст цитаты

    Returns:
        True если цитата валидна
    """
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


def harvest_forismatic(
    max_quotes: int = 1000,
    languages: List[str] = None,
    output_file: str = "data/forismatic.json"
) -> List[Dict]:
    """
    Сбор цитат с Forismatic API.

    Args:
        max_quotes: Максимальное количество цитат (на каждый язык)
        languages: Список языков ['en', 'ru'] или None для обоих
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    if languages is None:
        languages = ['en', 'ru']

    quotes = []
    seen = set()

    logger.info(
        f"Starting Forismatic API harvest "
        f"(max quotes per language: {max_quotes}, languages: {languages})..."
    )
    logger.warning("IMPORTANT: Using 1 second delay between requests!")

    try:
        total_requests = max_quotes * len(languages)
        with tqdm(total=total_requests, desc="Harvesting Forismatic") as pbar:
            for lang in languages:
                lang_quotes = 0
                attempts = 0
                max_attempts = max_quotes * 3  # Пробуем больше раз для уникальных

                while lang_quotes < max_quotes and attempts < max_attempts:
                    try:
                        # Forismatic API параметры
                        params = {
                            'method': 'getQuote',
                            'format': 'json',
                            'lang': lang,
                            'key': attempts  # Используем номер попытки как ключ
                        }

                        response = requests.get(
                            FORISMATIC_URL,
                            params=params,
                            timeout=10
                        )
                        response.raise_for_status()
                        data = response.json()

                        quote_text = data.get('quoteText', '').strip()
                        quote_author = data.get('quoteAuthor', '').strip()

                        if not quote_text:
                            attempts += 1
                            pbar.update(1)
                            time.sleep(1)
                            continue

                        # Проверяем уникальность
                        text_key = quote_text.lower()
                        if text_key in seen:
                            attempts += 1
                            pbar.update(1)
                            time.sleep(1)
                            continue

                        # Применяем строгие фильтры
                        if is_valid_quotation(quote_text):
                            seen.add(text_key)
                            quotes.append({
                                "text": quote_text,
                                "author": quote_author if quote_author else None,
                                "source": "forismatic",
                                "lang": lang
                            })
                            lang_quotes += 1

                        attempts += 1
                        pbar.update(1)
                        pbar.set_postfix({
                            "lang": lang,
                            f"{lang}_quotes": lang_quotes,
                            "total": len(quotes)
                        })
                        time.sleep(1)  # ОБЯЗАТЕЛЬНАЯ задержка

                    except requests.exceptions.RequestException as e:
                        logger.error(f"Error on request {attempts}: {e}")
                        attempts += 1
                        pbar.update(1)
                        time.sleep(2)  # Увеличиваем задержку при ошибке
                        continue
                    except Exception as e:
                        logger.error(f"Error parsing request {attempts}: {e}")
                        attempts += 1
                        pbar.update(1)
                        continue

                logger.info(
                    f"Harvested {lang_quotes} quotes for language '{lang}'"
                )

    except Exception as e:
        logger.error(f"Error in harvest_forismatic: {e}")

    # Сохраняем результаты
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(quotes, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    harvest_forismatic()
