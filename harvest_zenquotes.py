"""
Сбор цитат с ZenQuotes API (английский).
API: https://zenquotes.io/api/random
"""

import requests
import json
import time
import logging
from tqdm import tqdm
from typing import List, Dict, Set

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

ZEN_URL = "https://zenquotes.io/api/random"


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


def harvest_zenquotes(
    max_quotes: int = 5000,
    output_file: str = "zenquotes.json"
) -> List[Dict]:
    """
    Сбор цитат с ZenQuotes API.

    Args:
        max_quotes: Максимальное количество цитат
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    seen: Set[str] = set()
    quotes = []

    logger.info(f"Starting ZenQuotes API harvest (max: {max_quotes})...")

    try:
        with tqdm(total=max_quotes, desc="Harvesting ZenQuotes") as pbar:
            for i in range(max_quotes):
                try:
                    # Пробуем с проверкой SSL
                    try:
                        r = requests.get(ZEN_URL, timeout=10, verify=True)
                    except requests.exceptions.SSLError:
                        # Если SSL ошибка, пробуем без проверки
                        logger.warning(
                            "SSL verification failed, trying without "
                            "verification (not recommended for production)"
                        )
                        import urllib3
                        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                        r = requests.get(ZEN_URL, timeout=10, verify=False)
                    
                    r.raise_for_status()
                    data = r.json()

                    if not data or len(data) == 0:
                        continue

                    quote_data = data[0]
                    text = quote_data.get("q", "").strip()
                    author = quote_data.get("a", "").strip()

                    # Проверяем на дубликаты
                    if text in seen:
                        pbar.update(1)
                        continue

                    # Применяем строгие фильтры
                    if is_valid_quotation(text):
                        seen.add(text)
                        quotes.append({
                            "text": text,
                            "author": author if author else None,
                            "source": "zenquotes",
                            "lang": "en"
                        })

                    pbar.update(1)
                    pbar.set_postfix({"unique": len(quotes)})

                    # ОБЯЗАТЕЛЬНАЯ задержка (требование API)
                    time.sleep(1)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error on request {i}: {e}")
                    time.sleep(5)  # Увеличиваем задержку при ошибке
                    continue
                except Exception as e:
                    logger.error(f"Error parsing request {i}: {e}")
                    continue

    except Exception as e:
        logger.error(f"Error in harvest_zenquotes: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} unique quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    harvest_zenquotes()
