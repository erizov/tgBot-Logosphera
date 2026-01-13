"""
Сбор цитат с Goodreads (английский).
Веб-скрапинг: https://www.goodreads.com/quotes
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

BASE_URL = "https://www.goodreads.com/quotes"


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


def harvest_goodreads(
    max_pages: int = 200,
    output_file: str = "goodreads.json"
) -> List[Dict]:
    """
    Сбор цитат с Goodreads.

    Args:
        max_pages: Максимальное количество страниц (РЕКОМЕНДУЕТСЯ НИЗКОЕ)
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    quotes = []

    logger.info(f"Starting Goodreads harvest (max pages: {max_pages})...")
    logger.warning("IMPORTANT: Using 10 second delay between requests!")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        with tqdm(total=max_pages, desc="Harvesting Goodreads") as pbar:
            for page in range(1, max_pages + 1):
                try:
                    url = f"{BASE_URL}?page={page}"
                    # Пробуем с проверкой SSL (увеличенный таймаут)
                    # Примечание: Goodreads может быть заблокирован в некоторых странах
                    # Если возникают таймауты, попробуйте использовать VPN
                    max_retries = 3
                    retry_delay = 5
                    response = None
                    
                    for attempt in range(max_retries):
                        try:
                            try:
                                response = requests.get(
                                    url, headers=headers, timeout=60, verify=True
                                )
                                break  # Успешно, выходим из цикла retry
                            except (requests.exceptions.SSLError, 
                                    requests.exceptions.Timeout,
                                    requests.exceptions.ConnectionError) as e:
                                if attempt < max_retries - 1:
                                    logger.warning(
                                        f"Attempt {attempt + 1} failed for page {page}: "
                                        f"{type(e).__name__}. Retrying in {retry_delay}s..."
                                    )
                                    time.sleep(retry_delay)
                                    # Пробуем без проверки SSL
                                    import urllib3
                                    urllib3.disable_warnings(
                                        urllib3.exceptions.InsecureRequestWarning
                                    )
                                    try:
                                        response = requests.get(
                                            url, headers=headers, timeout=60, verify=False
                                        )
                                        break  # Успешно, выходим из цикла retry
                                    except Exception:
                                        if attempt < max_retries - 1:
                                            continue
                                        raise
                                else:
                                    # Последняя попытка
                                    import urllib3
                                    urllib3.disable_warnings(
                                        urllib3.exceptions.InsecureRequestWarning
                                    )
                                    response = requests.get(
                                        url, headers=headers, timeout=60, verify=False
                                    )
                                    break
                        except requests.exceptions.Timeout:
                            if attempt < max_retries - 1:
                                logger.warning(
                                    f"Timeout on page {page}, attempt {attempt + 1}. "
                                    f"Retrying in {retry_delay * 2}s..."
                                )
                                time.sleep(retry_delay * 2)
                                continue
                            else:
                                logger.error(
                                    f"Failed to load page {page} after {max_retries} "
                                    "attempts. This might be due to:"
                                    "\n  1. Geo-blocking (try using VPN)"
                                    "\n  2. Network issues"
                                    "\n  3. Goodreads server problems"
                                )
                                raise
                        except requests.exceptions.ConnectionError as e:
                            if attempt < max_retries - 1:
                                logger.warning(
                                    f"Connection error on page {page}, attempt "
                                    f"{attempt + 1}. Retrying in {retry_delay * 2}s..."
                                )
                                time.sleep(retry_delay * 2)
                                continue
                            else:
                                logger.error(
                                    f"Connection failed for page {page} after "
                                    f"{max_retries} attempts: {e}"
                                    "\n  Tip: If you're in Russia, try using VPN"
                                )
                                raise
                    
                    if response is None:
                        logger.error(f"Failed to get response for page {page}")
                        continue
                    
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, "html.parser")

                    # Ищем цитаты
                    quote_elements = soup.select(".quoteText")
                    
                    if not quote_elements:
                        logger.warning(f"No quotes found on page {page}")
                        break

                    for q in quote_elements:
                        # Извлекаем текст цитаты
                        text = q.get_text(" ", strip=True)
                        # Разделяем по символу "―" (длинное тире)
                        parts = text.split("―")
                        if parts:
                            text = parts[0].strip()

                        # Извлекаем автора
                        author = None
                        author_elem = q.find_next("span", class_="authorOrTitle")
                        if author_elem:
                            author = author_elem.text.strip()

                        # Применяем строгие фильтры
                        if is_valid_quotation(text):
                            quotes.append({
                                "text": text,
                                "author": author,
                                "source": "goodreads",
                                "lang": "en"
                            })

                    pbar.update(1)
                    pbar.set_postfix({"total": len(quotes)})

                    # ОЧЕНЬ ВАЖНО: задержка между запросами
                    time.sleep(10)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error on page {page}: {e}")
                    time.sleep(30)  # Увеличиваем задержку при ошибке
                    continue
                except Exception as e:
                    logger.error(f"Error parsing page {page}: {e}")
                    continue

    except Exception as e:
        logger.error(f"Error in harvest_goodreads: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    # РЕКОМЕНДУЕТСЯ НИЗКОЕ КОЛИЧЕСТВО СТРАНИЦ
    harvest_goodreads(max_pages=50)
