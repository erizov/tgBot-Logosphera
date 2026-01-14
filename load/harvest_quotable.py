"""
Сбор цитат с Quotable API (английский).
API: https://api.quotable.io/quotes
"""

import requests
import json
import time
import logging
from tqdm import tqdm
from typing import List, Dict

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

QUOTABLE_URL = "https://api.quotable.io/quotes"


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


def harvest_quotable(output_file: str = "data/quotable.json") -> List[Dict]:
    """
    Сбор цитат с Quotable API.

    Args:
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    quotes = []
    page = 1

    logger.info("Starting Quotable API harvest...")

    try:
        with tqdm(desc="Harvesting Quotable quotes") as pbar:
            while True:
                try:
                    # Пробуем с проверкой SSL
                    try:
                        r = requests.get(
                            QUOTABLE_URL,
                            params={"page": page, "limit": 150},
                            timeout=15,
                            verify=True
                        )
                    except requests.exceptions.SSLError:
                        # Если SSL ошибка, пробуем без проверки (только для разработки)
                        logger.warning(
                            "SSL verification failed, trying without "
                            "verification (not recommended for production)"
                        )
                        import urllib3
                        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                        r = requests.get(
                            QUOTABLE_URL,
                            params={"page": page, "limit": 150},
                            timeout=15,
                            verify=False
                        )
                    
                    r.raise_for_status()
                    data = r.json()

                    if "results" not in data or not data["results"]:
                        break

                    for q in data["results"]:
                        text = q.get("content", "").strip()
                        author = q.get("author", "").strip()

                        # Применяем строгие фильтры
                        if is_valid_quotation(text):
                            quotes.append({
                                "text": text,
                                "author": author if author else None,
                                "source": "quotable",
                                "lang": "en"
                            })

                    pbar.update(len(data["results"]))
                    pbar.set_postfix({"total": len(quotes)})

                    # Проверяем, есть ли еще страницы
                    # API может возвращать hasNext как None, поэтому проверяем
                    # наличие результатов и общее количество
                    has_next = data.get("hasNext")
                    total_count = data.get("totalCount", 0)
                    current_results = len(data.get("results", []))
                    
                    # Если нет результатов на текущей странице, останавливаемся
                    if current_results == 0:
                        logger.info(
                            f"No more results. Total quotes collected: "
                            f"{len(quotes)}"
                        )
                        break
                    
                    # Если hasNext явно False, останавливаемся
                    if has_next is False:
                        logger.info(
                            f"Reached end of API. Total quotes collected: "
                            f"{len(quotes)}"
                        )
                        break
                    
                    # Если hasNext None, но есть totalCount, проверяем по нему
                    if has_next is None and total_count > 0:
                        # Вычисляем, сколько страниц должно быть
                        estimated_pages = (total_count + 149) // 150
                        if page >= estimated_pages:
                            logger.info(
                                f"Reached estimated end ({estimated_pages} pages). "
                                f"Total quotes collected: {len(quotes)}"
                            )
                            break

                    page += 1
                    time.sleep(0.5)  # Задержка между запросами
                    
                    # Защита от бесконечного цикла
                    if page > 100:  # Максимальное количество страниц (2127/150 ≈ 15)
                        logger.warning(
                            f"Reached maximum page limit ({page}). "
                            f"Total quotes collected: {len(quotes)}"
                        )
                        break

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error on page {page}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error parsing page {page}: {e}")
                    break

    except Exception as e:
        logger.error(f"Error in harvest_quotable: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    harvest_quotable()
