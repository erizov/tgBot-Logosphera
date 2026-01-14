"""
Сбор цитат с BrainyQuote (английский).
Веб-скрапинг: https://www.brainyquote.com/topics
"""

import requests
import json
import time
import logging
from bs4 import BeautifulSoup
from tqdm import tqdm
from typing import List, Dict
import re

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.brainyquote.com/topics"
TOPICS = [
    "motivational", "wisdom", "success", "life", "love",
    "happiness", "friendship", "inspirational", "courage",
    "time", "work", "dreams", "hope", "faith", "peace"
]


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


def harvest_brainyquote(
    topics: List[str] = None,
    output_file: str = "data/brainyquote.json"
) -> List[Dict]:
    """
    Сбор цитат с BrainyQuote.

    Args:
        topics: Список тем для сбора (по умолчанию использует TOPICS)
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    if topics is None:
        topics = TOPICS

    quotes = []

    logger.info(f"Starting BrainyQuote harvest (topics: {len(topics)})...")
    logger.warning("IMPORTANT: Using 15 second delay between requests!")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        with tqdm(total=len(topics), desc="Harvesting BrainyQuote") as pbar:
            for topic in topics:
                try:
                    url = f"{BASE_URL}/{topic}-quotes"
                    # Пробуем с проверкой SSL
                    try:
                        response = requests.get(
                            url, headers=headers, timeout=15, verify=True
                        )
                    except requests.exceptions.SSLError:
                        # Если SSL ошибка, пробуем без проверки
                        logger.warning(
                            f"SSL verification failed for topic '{topic}', "
                            "trying without verification"
                        )
                        import urllib3
                        urllib3.disable_warnings(
                            urllib3.exceptions.InsecureRequestWarning
                        )
                        response = requests.get(
                            url, headers=headers, timeout=15, verify=False
                        )
                    
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, "html.parser")

                    # Ищем цитаты по селектору
                    quote_elements = soup.select("a[title^='view quote']")

                    if not quote_elements:
                        # Пробуем альтернативные селекторы
                        quote_elements = soup.select(".bqQuoteLink")
                        if not quote_elements:
                            quote_elements = soup.select("a.oncl_q")

                    for q in quote_elements:
                        text = q.text.strip()
                        if text:
                            # Применяем строгие фильтры
                            if is_valid_quotation(text):
                                quotes.append({
                                    "text": text,
                                    "author": None,  # BrainyQuote часто не содержит автора в ссылке
                                    "source": "brainyquote",
                                    "lang": "en"
                                })

                    pbar.update(1)
                    pbar.set_postfix({"total": len(quotes), "topic": topic})

                    # ОЧЕНЬ ВАЖНО: задержка между запросами
                    time.sleep(15)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error on topic '{topic}': {e}")
                    time.sleep(30)  # Увеличиваем задержку при ошибке
                    continue
                except Exception as e:
                    logger.error(f"Error parsing topic '{topic}': {e}")
                    continue

    except Exception as e:
        logger.error(f"Error in harvest_brainyquote: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    harvest_brainyquote()
