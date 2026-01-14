"""
Сбор цитат с Anecdot.ru/aphorizm (русский).
Веб-скрапинг: https://anecdot.ru/aphorizm/
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

BASE_URL = "https://anecdot.ru/aphorizm"
# Альтернативные URL на случай недоступности основного
ALTERNATIVE_URLS = [
    "http://anecdot.ru/aphorizm",  # HTTP вместо HTTPS
    "https://www.anecdot.ru/aphorizm",  # С www
    "http://www.anecdot.ru/aphorizm",  # HTTP с www
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
    if re.search(r'\b[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+', text):
        # Исключаем начало предложения
        if not text[0].isupper():
            return False

    # - Адреса, места
    place_keywords = [
        r'\bулица\b', r'\bпроспект\b', r'\bпереулок\b',
        r'\bмосква\b', r'\bлондон\b', r'\bпариж\b',
        r'\bstreet\b', r'\bavenue\b', r'\broad\b'
    ]
    for keyword in place_keywords:
        if re.search(keyword, text, re.IGNORECASE):
            return False

    # - Названия книг, фильмов (в кавычках с заглавными буквами)
    if re.search(r'["«»""][А-ЯЁ][а-яё]+.*[А-ЯЁ][а-яё]+["«»""]', text):
        return False

    # - URL и email
    if re.search(r'http[s]?://|www\.|@', text, re.IGNORECASE):
        return False

    # - Даты
    month_names = [
        r'\bянварь\b', r'\bфевраль\b', r'\bмарт\b', r'\bапрель\b',
        r'\bмай\b', r'\bиюнь\b', r'\bиюль\b', r'\bавгуст\b',
        r'\bсентябрь\b', r'\bоктябрь\b', r'\bноябрь\b', r'\bдекабрь\b',
        r'\bjanuary\b', r'\bfebruary\b', r'\bmarch\b', r'\bapril\b'
    ]
    for month in month_names:
        if re.search(month, text, re.IGNORECASE):
            return False

    # - Театральные ссылки
    theater_keywords = [
        r'\bакт\b', r'\bсцена\b', r'\bстраница\b', r'\bглава\b',
        r'\bact\b', r'\bscene\b', r'\bpage\b', r'\bchapter\b'
    ]
    for keyword in theater_keywords:
        if re.search(keyword, text, re.IGNORECASE):
            return False

    # - Спам паттерны (повторяющиеся символы)
    if re.search(r'(.)\1{4,}', text):
        return False

    return True


def clean_text(text: str) -> str:
    """
    Очистка текста цитаты.

    Args:
        text: Исходный текст

    Returns:
        Очищенный текст
    """
    if not text:
        return ''

    # Удаление лишних пробелов
    text = ' '.join(text.split())

    # Удаление HTML тегов если остались
    text = re.sub(r'<[^>]+>', '', text)

    # Удаление лишних переносов строк
    text = re.sub(r'\n+', ' ', text)

    return text.strip()


def harvest_anecdot_ru(
    max_pages: int = 50,
    output_file: str = "data/anecdot_ru.json"
) -> List[Dict]:
    """
    Сбор цитат с Anecdot.ru/aphorizm.

    Args:
        max_pages: Максимальное количество страниц
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    quotes = []

    logger.info(f"Starting Anecdot.ru harvest (max pages: {max_pages})...")
    logger.warning("IMPORTANT: Using 5 second delay between requests!")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/91.0.4472.124 Safari/537.36"
    }

    # Сначала проверяем доступность сайта
    logger.info("Checking site availability...")
    site_available = False
    working_base_url = BASE_URL
    
    for test_url in [BASE_URL] + ALTERNATIVE_URLS:
        try:
            test_response = requests.get(
                test_url, headers=headers, timeout=10, verify=False
            )
            if test_response.status_code == 200:
                site_available = True
                working_base_url = test_url
                logger.info(f"Site is available at: {test_url}")
                break
        except Exception:
            continue
    
    if not site_available:
        logger.error(
            "Anecdot.ru/aphorizm is not accessible. Possible reasons:\n"
            "  1. Site is down or blocked\n"
            "  2. Network connectivity issues\n"
            "  3. Firewall/proxy blocking access\n"
            "  4. Site may have changed URL structure\n"
            "\n"
            "Skipping this source. Try again later or use other sources."
        )
        # Сохраняем пустой файл
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        logger.info(f"Saved empty file: {output_file}")
        return []

    try:
        with tqdm(total=max_pages, desc="Harvesting Anecdot.ru") as pbar:
            for page in range(1, max_pages + 1):
                try:
                    # Пробуем разные варианты URL
                    url_patterns = [
                        f'{working_base_url}/?page={page}',
                        f'{working_base_url}/page/{page}/',
                        f'{working_base_url}/?p={page}',
                    ]

                    success = False
                    for url in url_patterns:
                        try:
                            # Пробуем с проверкой SSL
                            try:
                                response = requests.get(
                                    url, headers=headers, timeout=15, verify=True
                                )
                            except requests.exceptions.SSLError:
                                logger.warning(
                                    f"SSL verification failed for {url}, "
                                    "trying without verification"
                                )
                                import urllib3
                                urllib3.disable_warnings(
                                    urllib3.exceptions.InsecureRequestWarning
                                )
                                response = requests.get(
                                    url, headers=headers, timeout=15, verify=False
                                )

                            if response.status_code == 404:
                                continue

                            response.raise_for_status()
                            soup = BeautifulSoup(response.text, "html.parser")

                            # Ищем цитаты
                            quote_elements = soup.find_all(
                                ['div', 'p', 'blockquote', 'span'],
                                class_=re.compile(r'quote|text|aphorism|aphorizm|citata')
                            )
                            if not quote_elements:
                                quote_elements = soup.find_all('blockquote')
                            if not quote_elements:
                                quote_elements = soup.find_all('p',
                                                               string=re.compile(r'.{20,}'))

                            if quote_elements:
                                success = True
                                break

                        except requests.exceptions.RequestException:
                            continue

                    if not success:
                        logger.warning(
                            f"No quotes found on page {page}\n"
                            "  Possible reasons:\n"
                            "  1. Site structure changed\n"
                            "  2. Site is down or blocked\n"
                            "  3. Different URL pattern needed"
                        )
                        break

                    # Обрабатываем найденные цитаты
                    for elem in quote_elements:
                        text = clean_text(elem.get_text())

                        # Проверяем, что это русский текст
                        if not re.search(r'[а-яёА-ЯЁ]', text):
                            continue

                        if len(text) >= 20 and is_valid_quotation(text):
                            quotes.append({
                                "text": text,
                                "author": None,
                                "source": "anecdot_ru",
                                "lang": "ru"
                            })

                    pbar.update(1)
                    pbar.set_postfix({"total": len(quotes)})

                    # Задержка между запросами
                    time.sleep(5)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error on page {page}: {e}")
                    time.sleep(10)  # Увеличиваем задержку при ошибке
                    continue
                except Exception as e:
                    logger.error(f"Error parsing page {page}: {e}")
                    continue

    except Exception as e:
        logger.error(f"Error in harvest_anecdot_ru: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    # РЕКОМЕНДУЕТСЯ НИЗКОЕ КОЛИЧЕСТВО СТРАНИЦ
    harvest_anecdot_ru(max_pages=20)
