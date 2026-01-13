"""
Сбор цитат с Wikiquote (русский).
Веб-скрапинг: https://ru.wikiquote.org
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

BASE_URL = "https://ru.wikiquote.org/wiki"


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

    # Удаление ссылок на источники [1], [2], etc.
    text = re.sub(r'\[\d+\]', '', text)
    # Удаление всех ссылок в квадратных скобках
    text = re.sub(r'\[.*?\]', '', text)
    # Удаление лишних пробелов
    text = re.sub(r'\s+', ' ', text)

    # Удаление HTML тегов если остались
    text = re.sub(r'<[^>]+>', '', text)

    # Удаление лишних переносов строк
    text = re.sub(r'\n+', ' ', text)

    return text.strip()


def harvest_wikiquote_ru(
    authors: List[str] = None,
    output_file: str = "wikiquote_ru.json"
) -> List[Dict]:
    """
    Сбор цитат с Wikiquote (русский).

    Args:
        authors: Список авторов для сбора (по умолчанию использует популярных)
        output_file: Имя файла для сохранения

    Returns:
        Список цитат
    """
    if authors is None:
        authors = [
            'Лев Толстой', 'Фёдор Достоевский', 'Антон Чехов',
            'Александр Пушкин', 'Иван Тургенев', 'Николай Гоголь',
            'Михаил Лермонтов', 'Александр Блок', 'Сергей Есенин',
            'Владимир Маяковский', 'Анна Ахматова', 'Марина Цветаева',
            'Борис Пастернак', 'Иосиф Бродский', 'Александр Солженицын',
            'Михаил Булгаков', 'Иван Бунин', 'Максим Горький',
            'Владимир Набоков', 'Александр Грибоедов',
            'Аристотель', 'Платон', 'Сократ', 'Конфуций',
            'Фридрих Ницше', 'Иммануил Кант', 'Карл Маркс'
        ]

    quotes = []

    logger.info(f"Starting Wikiquote RU harvest (authors: {len(authors)})...")
    logger.warning("IMPORTANT: Using 5 second delay between requests!")
    logger.warning(
        "Note: Wikiquote has strict rate limiting. "
        "If you get 403 errors, wait longer between requests."
    )

    # User-Agent согласно политике Wiki (https://meta.wikimedia.org/wiki/User-Agent_policy)
    headers = {
        "User-Agent": (
            "QuoteHarvester/1.0 "
            "(https://github.com/yourusername/quotebot; "
            "contact@example.com) "
            "Python-requests/2.31.0"
        )
    }

    try:
        with tqdm(total=len(authors), desc="Harvesting Wikiquote RU") as pbar:
            for author in authors:
                try:
                    # Формируем URL страницы автора
                    author_url = f"{BASE_URL}/{author.replace(' ', '_')}"

                    # Пробуем с проверкой SSL
                    try:
                        response = requests.get(
                            author_url, headers=headers, timeout=15, verify=True
                        )
                    except requests.exceptions.SSLError:
                        logger.warning(
                            f"SSL verification failed for {author_url}, "
                            "trying without verification"
                        )
                        import urllib3
                        urllib3.disable_warnings(
                            urllib3.exceptions.InsecureRequestWarning
                        )
                        response = requests.get(
                            author_url, headers=headers, timeout=15, verify=False
                        )

                    if response.status_code == 404:
                        logger.debug(f"Author page not found: {author}")
                        pbar.update(1)
                        time.sleep(5)  # Задержка даже при 404
                        continue

                    # Обработка 403 (Too Many Requests)
                    if response.status_code == 403:
                        logger.warning(
                            f"403 Forbidden for {author}. "
                            "Rate limit exceeded. Waiting 30 seconds..."
                        )
                        time.sleep(30)  # Долгая задержка при 403
                        # Пробуем еще раз
                        try:
                            response = requests.get(
                                author_url, headers=headers, timeout=15, verify=False
                            )
                            if response.status_code == 403:
                                logger.error(
                                    f"Skipping {author} - still rate limited. "
                                    "Consider reducing request frequency."
                                )
                                pbar.update(1)
                                time.sleep(5)
                                continue
                        except Exception as e:
                            logger.error(f"Error retrying {author}: {e}")
                            pbar.update(1)
                            time.sleep(5)
                            continue

                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Ищем основной контент
                    content = soup.find('div', class_='mw-parser-output')
                    if not content:
                        content = soup.find('div', id='mw-content-text')
                    if not content:
                        content = soup

                    # Ищем все li элементы (цитаты в списках)
                    quote_elements = content.find_all('li')

                    # Фильтруем: пропускаем навигацию и короткие элементы
                    for elem in quote_elements:
                        text = clean_text(elem.get_text())

                        # Пропускаем навигацию
                        if re.search(r'^(edit|править|ссылки|links|see also|'
                                    r'категории|categories)',
                                    text, re.IGNORECASE):
                            continue

                        # Пропускаем слишком короткие
                        if len(text) < 40:
                            continue

                        # Проверяем, что это русский текст
                        if not re.search(r'[а-яёА-ЯЁ]', text):
                            continue

                        # Применяем строгие фильтры
                        if is_valid_quotation(text):
                            quotes.append({
                                "text": text,
                                "author": author,
                                "source": "wikiquote_ru",
                                "lang": "ru"
                            })

                    # Также ищем в других структурах
                    quote_divs = content.find_all(
                        ['div', 'p', 'blockquote'],
                        class_=re.compile(r'quote|quotation|цитата')
                    )

                    for elem in quote_divs:
                        text = clean_text(elem.get_text())

                        if len(text) >= 40:
                            # Проверяем, что это русский текст
                            if not re.search(r'[а-яёА-ЯЁ]', text):
                                continue

                            if is_valid_quotation(text):
                                quotes.append({
                                    "text": text,
                                    "author": author,
                                    "source": "wikiquote_ru",
                                    "lang": "ru"
                                })

                    pbar.update(1)
                    pbar.set_postfix({"total": len(quotes), "author": author})

                    # Задержка между запросами (увеличена для избежания 403)
                    time.sleep(5)

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error loading author '{author}': {e}")
                    pbar.update(1)
                    continue
                except Exception as e:
                    logger.error(f"Error parsing author '{author}': {e}")
                    pbar.update(1)
                    continue

    except Exception as e:
        logger.error(f"Error in harvest_wikiquote_ru: {e}")

    # Сохраняем в файл
    if quotes:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(quotes, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(quotes)} quotes to {output_file}")

    return quotes


if __name__ == "__main__":
    harvest_wikiquote_ru()
