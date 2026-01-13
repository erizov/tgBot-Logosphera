"""
Сбор цитат с Citaty.net (русский).
Веб-скрапинг: https://ru.citaty.net
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

BASE_URL = "https://ru.citaty.net"


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


def harvest_citaty_net(
    max_pages: int = 50,
    output_file: str = "citaty_net.json"
) -> List[Dict]:
    """
    Сбор цитат с Citaty.net.

    Args:
        max_pages: Максимальное количество страниц
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    quotes = []

    logger.info(f"Starting Citaty.net harvest (max pages: {max_pages})...")
    logger.warning("IMPORTANT: Using 5 second delay between requests!")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        with tqdm(total=max_pages, desc="Harvesting Citaty.net") as pbar:
            for page in range(1, max_pages + 1):
                try:
                    # Пробуем разные варианты URL
                    url_patterns = [
                        f'{BASE_URL}/tsitaty/?page={page}',
                        f'{BASE_URL}/tsitaty/page/{page}/',
                        f'{BASE_URL}/?page={page}',
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
                                ['div', 'p', 'blockquote'],
                                class_=re.compile(r'quote|text')
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
                        logger.warning(f"No quotes found on page {page}")
                        break

                    # Обрабатываем найденные цитаты
                    for elem in quote_elements:
                        text = clean_text(elem.get_text())

                        # Проверяем, что это русский текст
                        if not re.search(r'[а-яёА-ЯЁ]', text):
                            continue

                        if len(text) >= 20 and is_valid_quotation(text):
                            # Пробуем извлечь автора
                            author = None
                            author_elem = elem.find_next(['span', 'div', 'p'],
                                                         class_=re.compile(r'author'))
                            if author_elem:
                                author = clean_text(author_elem.get_text())

                            quotes.append({
                                "text": text,
                                "author": author,
                                "source": "citaty_net",
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
        logger.error(f"Error in harvest_citaty_net: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    # РЕКОМЕНДУЕТСЯ НИЗКОЕ КОЛИЧЕСТВО СТРАНИЦ
    harvest_citaty_net(max_pages=20)
