"""
Скрипт для загрузки цитат и идиом с веб-сайтов.
Фильтрует нежелательные источники и символы, переводит цитаты.
"""

import os
import re
import time
import logging
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse

import os
import glob
import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import hashlib

try:
    from docx import Document
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False

try:
    import mwparserfromhell
    MWPARSER_AVAILABLE = True
except ImportError:
    MWPARSER_AVAILABLE = False

try:
    from langdetect import detect
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from quotations_data import get_all_quotations as get_predefined_quotations
except ImportError:
    # Fallback если файл не найден
    def get_predefined_quotations():
        return []

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Логируем доступность опциональных библиотек
if not MWPARSER_AVAILABLE:
    logger.debug("mwparserfromhell not available, using basic parsing")
if not LANGDETECT_AVAILABLE:
    logger.debug("langdetect not available, using basic language detection")


class QuotationLoader:
    """Класс для загрузки и обработки цитат."""

    # Надежные источники для английских цитат
    EN_SOURCES = [
        {
            'url': 'https://www.brainyquote.com/quotes/authors',
            'type': 'author_list',
            'base_url': 'https://www.brainyquote.com'
        },
        {
            'url': 'https://www.goodreads.com/quotes',
            'type': 'quotes_page',
            'base_url': 'https://www.goodreads.com'
        },
        {
            'url': 'https://en.wikiquote.org/wiki/Main_Page',
            'type': 'quotes_page',
            'base_url': 'https://en.wikiquote.org'
        },
    ]

    # Надежные источники для русских цитат
    RU_SOURCES = [
        {
            'url': 'https://ru.citaty.net/avtory/',
            'type': 'author_list',
            'base_url': 'https://ru.citaty.net'
        },
        {
            'url': 'https://www.citaty.info/',
            'type': 'quotes_page',
            'base_url': 'https://www.citaty.info'
        },
        {
            'url': 'https://aphorizm.ru/',
            'type': 'quotes_page',
            'base_url': 'https://aphorizm.ru'
        },
        {
            'url': 'https://anecdot.ru/aphorizm/',
            'type': 'quotes_page',
            'base_url': 'https://anecdot.ru'
        },
        {
            'url': 'https://ru.wikiquote.org/wiki/Заглавная_страница',
            'type': 'quotes_page',
            'base_url': 'https://ru.wikiquote.org'
        },
    ]

    # Сомнительные домены для фильтрации
    QUESTIONABLE_DOMAINS = [
        'spam', 'adult', 'casino', 'gambling', 'porn',
        'xxx', 'bitcoin', 'crypto', 'scam'
    ]

    # Категории для тегирования цитат
    CATEGORIES = {
        'wisdom': [
            'wisdom', 'knowledge', 'learning', 'understanding',
            'мудрость', 'знание', 'учение', 'понимание'
        ],
        'life': [
            'life', 'living', 'existence', 'experience',
            'жизнь', 'существование', 'опыт'
        ],
        'success': [
            'success', 'achievement', 'victory', 'triumph',
            'успех', 'достижение', 'победа'
        ],
        'love': [
            'love', 'heart', 'affection', 'passion',
            'любовь', 'сердце', 'страсть'
        ],
        'work': [
            'work', 'labor', 'effort', 'diligence',
            'труд', 'работа', 'усилие', 'усердие'
        ],
        'time': [
            'time', 'moment', 'future', 'past', 'present',
            'время', 'момент', 'будущее', 'прошлое'
        ],
        'friendship': [
            'friend', 'friendship', 'companion',
            'друг', 'дружба', 'товарищ'
        ],
        'courage': [
            'courage', 'bravery', 'fear', 'strength',
            'смелость', 'храбрость', 'страх', 'сила'
        ],
        'happiness': [
            'happiness', 'joy', 'pleasure', 'delight',
            'счастье', 'радость', 'удовольствие'
        ],
        'philosophy': [
            'philosophy', 'truth', 'reality', 'meaning',
            'философия', 'истина', 'реальность', 'смысл'
        ],
    }

    def __init__(self, db_url: Optional[str] = None):
        """
        Инициализация загрузчика.

        Args:
            db_url: URL подключения к БД
        """
        self.db_url = db_url or os.getenv('DB_URL')
        self.translator_en_ru = GoogleTranslator(source='en', target='ru')
        self.translator_ru_en = GoogleTranslator(source='ru', target='en')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36'
        })
        # Инициализация таблицы при создании объекта
        self._init_quotations_table()

    def _get_connection(self):
        """Получение подключения к БД."""
        return psycopg2.connect(self.db_url)

    def _init_quotations_table(self):
        """Инициализация таблицы quotations в БД."""
        conn = self._get_connection()
        cur = conn.cursor()

        try:
            # Создание таблицы
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
            
            # Добавление колонки tags если её нет (для существующих таблиц)
            cur.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='quotations' AND column_name='tags'
                    ) THEN
                        ALTER TABLE quotations ADD COLUMN tags TEXT[];
                    END IF;
                END $$;
            """)
            
            conn.commit()
            logger.info("Quotations table initialized")
        except Exception as e:
            logger.error(f"Error creating quotations table: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _is_valid_quotation(self, text: str) -> bool:
        """
        Проверка валидности цитаты.

        Args:
            text: Текст цитаты

        Returns:
            True если цитата валидна
        """
        if not text or len(text.strip()) < 10:
            return False

        # Строгая проверка на наличие арабских цифр
        if re.search(r'\d', text):
            return False

        # Строгая проверка на римские цифры
        # Паттерн для римских цифр: I, II, III, IV, V, VI, VII, VIII, IX, X, и т.д.
        # Исключаем отдельно стоящие римские цифры (с пробелами или пунктуацией)
        roman_numeral_pattern = r'\b[IVXLCDM]{1,6}\b'
        if re.search(roman_numeral_pattern, text, re.IGNORECASE):
            # Дополнительная проверка: если это действительно римская цифра
            # (не просто случайная комбинация букв)
            words = text.split()
            for word in words:
                # Убираем пунктуацию
                clean_word = re.sub(r'[^\w]', '', word)
                # Проверяем, является ли это римской цифрой
                if clean_word and re.match(r'^[IVXLCDM]+$', clean_word,
                                          re.IGNORECASE):
                    # Проверяем, что это не обычное слово
                    # Исключаем все римские цифры, кроме "I" в начале предложения
                    # как местоимение
                    if len(clean_word) > 1:
                        # Любая комбинация из 2+ символов - это римская цифра
                        return False
                    elif clean_word.upper() in ['V', 'X', 'L', 'C', 'D', 'M']:
                        # Одиночные символы V, X, L, C, D, M - это римские цифры
                        return False
                    elif clean_word.upper() == 'I':
                        # "I" может быть местоимением, но только в начале предложения
                        # Если это не начало предложения - исключаем
                        if not (text.strip().startswith('I ') or
                                text.strip().startswith('I,')):
                            return False

        # Проверка на специальные символы (кроме пунктуации)
        # Разрешаем: буквы, пробелы, пунктуация, кавычки, дефисы
        if re.search(r'[^\w\s\.,!?;:\-—–\'"«»()]', text):
            return False

        # Проверка на слишком короткие или длинные цитаты
        if len(text) < 10 or len(text) > 500:
            return False

        # Проверка на повторяющиеся символы (спам)
        if re.search(r'(.)\1{4,}', text):
            return False

        # Строгая проверка на имена людей
        # Исключаем любые паттерны с заглавными буквами в середине
        words = text.split()
        if len(words) > 1:
            # Проверяем наличие двух подряд идущих слов с заглавной буквой
            # (кроме первого слова предложения)
            for i in range(1, len(words)):
                word = words[i].strip('.,!?;:"()[]{}')
                if len(word) > 1 and word[0].isupper():
                    # Если следующее слово тоже с заглавной - вероятно имя
                    if i < len(words) - 1:
                        next_word = words[i+1].strip('.,!?;:"()[]{}')
                        if (len(next_word) > 1 and next_word[0].isupper() and
                                not word[0].islower()):
                            return False
                    # Проверяем на известные имена (паттерны)
                    if word.lower() in ['mr', 'mrs', 'ms', 'dr', 'prof',
                                       'sir', 'lord', 'lady', 'king', 'queen',
                                       'prince', 'princess', 'господин',
                                       'госпожа', 'доктор', 'профессор']:
                        return False

        # Строгая проверка на адреса, места, географические названия
        address_patterns = [
            r'\b(street|avenue|road|boulevard|drive|lane|way|square|plaza)\b',
            r'\b(ул\.|пр\.|пер\.|пл\.|бул\.|шоссе|переулок|площадь)\b',
            r'\b(city|town|village|country|state|province|region)\b',
            r'\b(город|деревня|село|страна|область|регион|край)\b',
            r'\b(moscow|london|paris|new york|los angeles|москва|петербург|'
            r'санкт-петербург|киев|минск)\b',
        ]
        for pattern in address_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return False

        # Проверка на театральные ссылки
        theater_patterns = [
            r'\b(theater|theatre|cinema|movie|film|театр|кино|фильм|'
            r'спектакль|пьеса|драма|комедия)\b',
            r'\b(act|scene|акт|сцена|действие)\b',
        ]
        for pattern in theater_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return False

        # Строгая проверка на названия книг, фильмов, пьес
        # Исключаем паттерны типа "The Great Gatsby" или "Война и мир"
        quoted_caps = re.findall(r'["«»]([^"«»]+)["«»]', text)
        for quoted in quoted_caps:
            # Если в кавычках много заглавных букв - возможно название
            caps_count = len(re.findall(r'[A-ZА-ЯЁ]', quoted))
            if caps_count > 2:
                return False
            # Проверяем на слова, указывающие на произведения
            if re.search(r'\b(book|novel|story|play|poem|film|movie|'
                        r'книга|роман|рассказ|пьеса|стих|фильм)\b',
                        quoted, re.IGNORECASE):
                return False

        # Строгая проверка на издательства, страницы, ссылки
        publishing_patterns = [
            r'\b(publisher|publishing|press|edition|издательство|издатель)\b',
            r'\b(ISBN|isbn|page|pages|стр\.|страница|страницы)\b',
            r'\b(chapter|verse|line|строка|глава|стих)\b',
            r'\b(act|scene|акт|сцена)\b',  # Театральные ссылки
        ]
        for pattern in publishing_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return False

        # Проверка на URL и email
        if re.search(r'http[s]?://|www\.|@\w+\.', text, re.IGNORECASE):
            return False

        # Проверка на даты (паттерны типа "January 1" или "01.01.2024")
        if re.search(r'\b(january|february|march|april|may|june|july|'
                    r'august|september|october|november|december)\b',
                    text, re.IGNORECASE):
            return False
        if re.search(r'\b(январь|февраль|март|апрель|май|июнь|июль|'
                    r'август|сентябрь|октябрь|ноябрь|декабрь)\b',
                    text, re.IGNORECASE):
            return False

        return True

    def _is_valid_source(self, url: str) -> bool:
        """
        Проверка валидности источника.

        Args:
            url: URL источника

        Returns:
            True если источник валиден
        """
        if not url:
            return True  # Разрешаем цитаты без источника

        parsed = urlparse(url.lower())
        domain = parsed.netloc

        # Проверка на сомнительные домены
        for questionable in self.QUESTIONABLE_DOMAINS:
            if questionable in domain:
                return False

        return True

    def _extract_author(self, text: str, soup: BeautifulSoup) -> Optional[str]:
        """
        Извлечение автора из HTML.

        Args:
            text: Текст цитаты
            soup: BeautifulSoup объект

        Returns:
            Имя автора или None
        """
        # Попытка найти автора в различных тегах
        author_tags = soup.find_all(['span', 'div', 'p', 'a'],
                                    class_=re.compile(r'author|writer|by'))
        for tag in author_tags:
            author_text = tag.get_text(strip=True)
            if author_text and len(author_text) < 100:
                # Убираем "—" и другие префиксы
                author_text = re.sub(r'^[—–\-–\s]+', '', author_text)
                if author_text and self._is_valid_author(author_text):
                    return author_text

        return None

    def _is_valid_author(self, author: str) -> bool:
        """
        Проверка валидности имени автора.

        Args:
            author: Имя автора

        Returns:
            True если имя валидно
        """
        if not author or len(author) < 2 or len(author) > 100:
            return False

        # Не должно быть цифр и специальных символов
        if re.search(r'[^\w\s\.\-\']', author):
            return False

        return True

    def _normalize_text(self, text: str) -> str:
        """
        Нормализация текста цитаты (как в предоставленном коде).

        Args:
            text: Исходный текст

        Returns:
            Нормализованный текст
        """
        if not text:
            return ''

        # Удаление ссылок на источники [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)
        # Удаление всех ссылок в квадратных скобках
        text = re.sub(r'\[.*?\]', '', text)
        # Удаление лишних пробелов
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    def _extract_bullets_from_html(self, soup: BeautifulSoup) -> List[str]:
        """
        Извлечение цитат из bullet points (списков) в HTML.
        Улучшенная версия для Wikiquote.

        Args:
            soup: BeautifulSoup объект

        Returns:
            Список извлеченных цитат
        """
        quotes = []
        # Ищем основной контент Wikiquote
        content = soup.find('div', class_='mw-parser-output')
        if not content:
            content = soup.find('div', id='mw-content-text')
        if not content:
            content = soup

        # Ищем все списки (ul, ol) в контенте
        for list_elem in content.find_all(['ul', 'ol']):
            # Пропускаем навигационные списки
            if list_elem.find_parent(['nav', 'div'], class_=re.compile(r'nav|menu')):
                continue

            for li in list_elem.find_all('li', recursive=False):
                # Убираем вложенные списки перед извлечением текста
                li_copy = BeautifulSoup(str(li), 'html.parser')
                for nested in li_copy.find_all(['ul', 'ol']):
                    nested.decompose()
                for nested in li_copy.find_all(['div'], class_=re.compile(r'nav|menu')):
                    nested.decompose()

                text = self._normalize_text(li_copy.get_text())
                # Убираем ссылки на источники
                text = re.sub(r'\[\d+\]', '', text)
                text = re.sub(r'\[.*?\]', '', text)

                if text and len(text) >= 20:
                    # Проверяем, что это не навигация или метаданные
                    if not re.search(r'^(edit|править|ссылки|links|see also)',
                                    text, re.IGNORECASE):
                        quotes.append(text)

        # Также ищем цитаты в div с классом quote
        for quote_div in content.find_all('div', class_=re.compile(r'quote')):
            text = self._normalize_text(quote_div.get_text())
            if text and len(text) >= 20:
                quotes.append(text)

        return quotes

    def _clean_text(self, text: str) -> str:
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

    def _categorize_quotation(self, text: str, language: str) -> List[str]:
        """
        Категоризация цитаты по темам.

        Args:
            text: Текст цитаты
            language: Язык цитаты

        Returns:
            Список тегов/категорий
        """
        text_lower = text.lower()
        tags = []

        for category, keywords in self.CATEGORIES.items():
            for keyword in keywords:
                if keyword in text_lower:
                    if category not in tags:
                        tags.append(category)
                    break

        # Если не найдено категорий, добавляем общую
        if not tags:
            tags.append('general')

        return tags

    def _translate_text(self, text: str, source_lang: str,
                        target_lang: str) -> Optional[str]:
        """
        Перевод текста.

        Args:
            text: Текст для перевода
            source_lang: Исходный язык
            target_lang: Целевой язык

        Returns:
            Переведенный текст или None
        """
        try:
            if source_lang == 'en' and target_lang == 'ru':
                translator = self.translator_en_ru
            elif source_lang == 'ru' and target_lang == 'en':
                translator = self.translator_ru_en
            else:
                return None

            # Задержка для избежания rate limiting
            time.sleep(0.5)

            translated = translator.translate(text)
            return translated if translated else None

        except Exception as e:
            logger.warning(f"Translation error: {e}")
            return None

    def _load_from_brainyquote(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с BrainyQuote.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Попытка загрузить популярные цитаты
            popular_urls = [
                'https://www.brainyquote.com/topics',
                'https://www.brainyquote.com/quotes_of_the_day',
            ]

            for url in popular_urls:
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Поиск цитат в различных структурах
                    quote_elements = soup.find_all(
                        ['div', 'p', 'span'],
                        class_=re.compile(r'quote|text|content')
                    )

                    for elem in quote_elements:
                        text = self._clean_text(elem.get_text())
                        if self._is_valid_quotation(text):
                            author = self._extract_author(text, soup)
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': author,
                                'source_url': url
                            })

                            if len(quotations) >= max_quotes:
                                return quotations

                    time.sleep(1)  # Задержка между запросами

                except Exception as e:
                    logger.warning(f"Error loading from {url}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error in _load_from_brainyquote: {e}")

        return quotations

    def _load_from_goodreads(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с Goodreads.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Goodreads имеет API, но для простоты используем веб-скрапинг
            url = 'https://www.goodreads.com/quotes'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Различные селекторы для поиска цитат
            quote_elements = soup.find_all('div', class_='quoteText')
            if not quote_elements:
                # Альтернативный селектор
                quote_elements = soup.find_all('div',
                                               class_=re.compile(r'quote'))

            for elem in quote_elements:
                # Поиск текста цитаты
                text_elem = elem.find('span', class_='quoteText')
                if not text_elem:
                    text_elem = elem.find('span')
                if not text_elem:
                    text = self._clean_text(elem.get_text())
                else:
                    text = self._clean_text(text_elem.get_text())

                # Удаляем кавычки в начале и конце
                text = re.sub(r'^["\']+|["\']+$', '', text).strip()

                if self._is_valid_quotation(text):
                    # Поиск автора
                    author_elem = elem.find('span', class_='authorOrTitle')
                    if not author_elem:
                        author_elem = elem.find('span',
                                                 class_=re.compile(r'author'))
                    author = None
                    if author_elem:
                        author = self._clean_text(author_elem.get_text())
                        # Удаляем префиксы типа "—" или "by"
                        author = re.sub(r'^[—–\-\s]*by\s*', '', author,
                                       flags=re.IGNORECASE).strip()

                    quotations.append({
                        'text': text,
                        'language': 'en',
                        'author': author if author and self._is_valid_author(
                            author) else None,
                        'source_url': url
                    })

                    if len(quotations) >= max_quotes:
                        break

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error loading from Goodreads: {e}")
        except Exception as e:
            logger.error(f"Error in _load_from_goodreads: {e}")

        return quotations

    def _load_from_citaty_net(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с citaty.net (русский).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://ru.citaty.net/tsitaty/'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Различные селекторы для поиска цитат
            quote_elements = soup.find_all(['div', 'p', 'blockquote'],
                                            class_=re.compile(r'quote|text'))
            if not quote_elements:
                # Альтернативный поиск
                quote_elements = soup.find_all('blockquote')
            if not quote_elements:
                quote_elements = soup.find_all('p',
                                               string=re.compile(r'.{20,}'))

            for elem in quote_elements:
                text = self._clean_text(elem.get_text())
                if self._is_valid_quotation(text):
                    author = self._extract_author(text, soup)
                    quotations.append({
                        'text': text,
                        'language': 'ru',
                        'author': author,
                        'source_url': url
                    })

                    if len(quotations) >= max_quotes:
                        break

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error loading from citaty.net: {e}")
        except Exception as e:
            logger.error(f"Error in _load_from_citaty_net: {e}")

        return quotations

    def _load_from_citaty_net_page(self, page: int = 1,
                                    max_quotes: int = 50) -> List[Dict]:
        """
        Загрузка цитат с citaty.net с указанной страницы.

        Args:
            page: Номер страницы
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url_patterns = [
                f'https://ru.citaty.net/tsitaty/?page={page}',
                f'https://ru.citaty.net/tsitaty/page/{page}/',
                f'https://ru.citaty.net/?page={page}',
            ]

            for url in url_patterns:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    quote_elements = soup.find_all(
                        ['div', 'p', 'blockquote'],
                        class_=re.compile(r'quote|text')
                    )
                    if not quote_elements:
                        quote_elements = soup.find_all('blockquote')
                    if not quote_elements:
                        quote_elements = soup.find_all('p',
                                                         string=re.compile(r'.{20,}'))

                    for elem in quote_elements:
                        text = self._clean_text(elem.get_text())
                        if self._is_valid_quotation(text):
                            author = self._extract_author(text, soup)
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': author,
                                'source_url': url
                            })

                            if len(quotations) >= max_quotes:
                                return quotations

                    if quotations:
                        break

                    time.sleep(1)

                except requests.exceptions.RequestException:
                    continue
                except Exception as e:
                    logger.debug(f"Error loading from {url}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error in _load_from_citaty_net_page: {e}")

        return quotations

    def _load_from_aphorizm_ru(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с aphorizm.ru (русский).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            base_urls = [
                'https://aphorizm.ru/',
                'https://aphorizm.ru/random/',
                'https://aphorizm.ru/top/',
            ]

            for base_url in base_urls:
                if len(quotations) >= max_quotes:
                    break

                try:
                    response = self.session.get(base_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Различные селекторы для поиска цитат
                    quote_elements = soup.find_all(
                        ['div', 'p', 'blockquote', 'span'],
                        class_=re.compile(r'quote|text|aphorism|aphorizm|citata')
                    )
                    if not quote_elements:
                        quote_elements = soup.find_all('blockquote')
                    if not quote_elements:
                        quote_elements = soup.find_all('p',
                                                       string=re.compile(r'.{20,}'))
                    if not quote_elements:
                        # Попробуем найти по содержимому
                        quote_elements = soup.find_all(
                            'div', string=re.compile(r'.{30,}')
                        )

                    for elem in quote_elements:
                        text = self._clean_text(elem.get_text())
                        if self._is_valid_quotation(text):
                            author = self._extract_author(text, soup)
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': author,
                                'source_url': base_url
                            })

                            if len(quotations) >= max_quotes:
                                break

                    time.sleep(1)

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from {base_url}: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing {base_url}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_aphorizm_ru: {e}")

        return quotations

    def _load_from_aphorizm_ru_page(self, page: int = 1,
                                     max_quotes: int = 50) -> List[Dict]:
        """
        Загрузка цитат с aphorizm.ru с указанной страницы.

        Args:
            page: Номер страницы
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url_patterns = [
                f'https://aphorizm.ru/?page={page}',
                f'https://aphorizm.ru/page/{page}/',
                f'https://aphorizm.ru/random/?page={page}',
            ]

            for url in url_patterns:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    quote_elements = soup.find_all(
                        ['div', 'p', 'blockquote', 'span'],
                        class_=re.compile(r'quote|text|aphorism|aphorizm|citata')
                    )
                    if not quote_elements:
                        quote_elements = soup.find_all('blockquote')
                    if not quote_elements:
                        quote_elements = soup.find_all('p',
                                                       string=re.compile(r'.{20,}'))

                    for elem in quote_elements:
                        text = self._clean_text(elem.get_text())
                        if self._is_valid_quotation(text):
                            author = self._extract_author(text, soup)
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': author,
                                'source_url': url
                            })

                            if len(quotations) >= max_quotes:
                                return quotations

                    if quotations:
                        break

                    time.sleep(1)

                except requests.exceptions.RequestException:
                    continue
                except Exception as e:
                    logger.debug(f"Error loading from {url}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error in _load_from_aphorizm_ru_page: {e}")

        return quotations

    def _load_from_anecdot_ru_aphorizm(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с anecdot.ru/aphorizm (русский).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            base_urls = [
                'https://anecdot.ru/aphorizm/',
                'https://anecdot.ru/aphorizm/random/',
                'https://anecdot.ru/aphorizm/top/',
            ]

            for base_url in base_urls:
                if len(quotations) >= max_quotes:
                    break

                try:
                    response = self.session.get(base_url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Различные селекторы для поиска цитат
                    quote_elements = soup.find_all(
                        ['div', 'p', 'blockquote', 'span'],
                        class_=re.compile(r'quote|text|aphorism|aphorizm|citata|'
                                        r'anecdot|joke')
                    )
                    if not quote_elements:
                        quote_elements = soup.find_all('blockquote')
                    if not quote_elements:
                        quote_elements = soup.find_all('p',
                                                       string=re.compile(r'.{20,}'))
                    if not quote_elements:
                        # Попробуем найти по содержимому
                        quote_elements = soup.find_all(
                            'div', string=re.compile(r'.{30,}')
                        )

                    for elem in quote_elements:
                        text = self._clean_text(elem.get_text())
                        if self._is_valid_quotation(text):
                            author = self._extract_author(text, soup)
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': author,
                                'source_url': base_url
                            })

                            if len(quotations) >= max_quotes:
                                break

                    time.sleep(1)

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from {base_url}: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing {base_url}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_anecdot_ru_aphorizm: {e}")

        return quotations

    def _load_from_anecdot_ru_aphorizm_page(self, page: int = 1,
                                             max_quotes: int = 50) -> List[Dict]:
        """
        Загрузка цитат с anecdot.ru/aphorizm с указанной страницы.

        Args:
            page: Номер страницы
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url_patterns = [
                f'https://anecdot.ru/aphorizm/?page={page}',
                f'https://anecdot.ru/aphorizm/page/{page}/',
                f'https://anecdot.ru/aphorizm/random/?page={page}',
            ]

            for url in url_patterns:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    quote_elements = soup.find_all(
                        ['div', 'p', 'blockquote', 'span'],
                        class_=re.compile(r'quote|text|aphorism|aphorizm|citata|'
                                        r'anecdot|joke')
                    )
                    if not quote_elements:
                        quote_elements = soup.find_all('blockquote')
                    if not quote_elements:
                        quote_elements = soup.find_all('p',
                                                       string=re.compile(r'.{20,}'))

                    for elem in quote_elements:
                        text = self._clean_text(elem.get_text())
                        if self._is_valid_quotation(text):
                            author = self._extract_author(text, soup)
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': author,
                                'source_url': url
                            })

                            if len(quotations) >= max_quotes:
                                return quotations

                    if quotations:
                        break

                    time.sleep(1)

                except requests.exceptions.RequestException:
                    continue
                except Exception as e:
                    logger.debug(f"Error loading from {url}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error in _load_from_anecdot_ru_aphorizm_page: {e}")

        return quotations

    def _load_from_quotable_api(self, max_quotes: int = 2000) -> List[Dict]:
        """
        Загрузка цитат с публичного API Quotable.io.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # ZenQuotes.io - бесплатный API
            url = 'https://zenquotes.io/api/quotes'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            for quote_data in data:
                text = quote_data.get('q', '').strip()
                if self._is_valid_quotation(text):
                    quotations.append({
                        'text': text,
                        'language': 'en',
                        'author': None,  # Не сохраняем автора
                        'source_url': 'https://zenquotes.io'
                    })

                    if len(quotations) >= max_quotes:
                        break

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Error loading from ZenQuotes API: {e}")
        except Exception as e:
            logger.error(f"Error in _load_from_zenquotes_api: {e}")

        return quotations

    def _load_from_quotable_api(self, max_quotes: int = 2000) -> List[Dict]:
        """
        Загрузка цитат с публичного API Quotable.io.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            base_url = 'https://api.quotable.io/quotes'
            page = 1
            per_page = 100

            while len(quotations) < max_quotes:
                try:
                    params = {
                        'page': page,
                        'limit': min(per_page, max_quotes - len(quotations))
                    }
                    response = self.session.get(base_url, params=params,
                                                timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    if 'results' not in data or not data['results']:
                        break

                    for quote_data in data['results']:
                        text = quote_data.get('content', '').strip()
                        if self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': 'https://quotable.io'
                            })

                            if len(quotations) >= max_quotes:
                                break

                    if len(data.get('results', [])) < per_page:
                        break

                    page += 1
                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    logger.warning(f"Error loading from Quotable API: {e}")
                    break
                except Exception as e:
                    logger.warning(f"Error parsing Quotable API: {e}")
                    break

        except Exception as e:
            logger.error(f"Error in _load_from_quotable_api: {e}")

        return quotations

    def _load_from_zenquotes_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с публичного API ZenQuotes.io.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://zenquotes.io/api/quotes'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            for quote_data in data:
                text = quote_data.get('q', '').strip()
                if self._is_valid_quotation(text):
                    quotations.append({
                        'text': text,
                        'language': 'en',
                        'author': None,
                        'source_url': 'https://zenquotes.io'
                    })

                    if len(quotations) >= max_quotes:
                        break

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Error loading from ZenQuotes API: {e}")
        except Exception as e:
            logger.error(f"Error in _load_from_zenquotes_api: {e}")

        return quotations

    def _load_from_wikiquote_en(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с en.wikiquote.org (английский).
        Использует улучшенный парсинг из предоставленного кода.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Список известных авторов для загрузки
            authors = [
                'Albert Einstein', 'William Shakespeare', 'Mark Twain',
                'Oscar Wilde', 'Winston Churchill', 'Mahatma Gandhi',
                'Confucius', 'Plato', 'Aristotle', 'Voltaire',
                'Friedrich Nietzsche', 'Ralph Waldo Emerson',
                'Benjamin Franklin', 'Thomas Jefferson', 'Abraham Lincoln',
                'Martin Luther King Jr.', 'Nelson Mandela', 'Mother Teresa',
                'Buddha', 'Laozi', 'Sun Tzu', 'Leonardo da Vinci',
                'Isaac Newton', 'Charles Darwin', 'Galileo Galilei',
                'Steve Jobs', 'Bill Gates', 'Warren Buffett',
                'Oprah Winfrey', 'Maya Angelou', 'Helen Keller',
            ]

            # Загружаем страницы авторов
            for author in authors:
                if len(quotations) >= max_quotes:
                    break

                try:
                    author_url = (
                        f'https://en.wikiquote.org/wiki/'
                        f'{author.replace(" ", "_")}'
                    )
                    response = self.session.get(author_url, timeout=10)
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Извлекаем цитаты из bullet points (как в предоставленном коде)
                    bullet_quotes = self._extract_bullets_from_html(soup)
                    for text in bullet_quotes:
                        if len(quotations) >= max_quotes:
                            break

                        # Нормализация текста
                        text = self._normalize_text(text)
                        # Убираем метаданные
                        text = re.sub(r'\(citation needed\)', '', text,
                                     flags=re.IGNORECASE)
                        text = re.sub(r'\(disputed\)', '', text,
                                     flags=re.IGNORECASE)

                        # Проверка языка (если доступен langdetect)
                        if LANGDETECT_AVAILABLE:
                            try:
                                detected = detect(text)
                                if detected != 'en':
                                    continue
                            except:
                                pass

                        # Минимальная длина как в предоставленном коде (40 символов)
                        if len(text) < 40:
                            continue

                        if self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,  # Не сохраняем автора
                                'source_url': author_url
                            })

                    # Также ищем цитаты в других структурах Wikiquote
                    # Ищем в основном контенте
                    content = soup.find('div', class_='mw-parser-output')
                    if not content:
                        content = soup.find('div', id='mw-content-text')
                    if not content:
                        content = soup

                    # Ищем все li элементы в основном контенте
                    quote_elements = content.find_all('li')
                    # Фильтруем: пропускаем навигацию и короткие элементы
                    filtered_elements = []
                    for elem in quote_elements:
                        text = self._normalize_text(elem.get_text())
                        # Пропускаем навигацию
                        if re.search(r'^(edit|править|ссылки|links|see also|'
                                    r'категории|categories)',
                                    text, re.IGNORECASE):
                            continue
                        # Пропускаем слишком короткие
                        if len(text) < 40:
                            continue
                        filtered_elements.append(elem)

                    quote_elements = filtered_elements

                    for elem in quote_elements:
                        if len(quotations) >= max_quotes:
                            break

                        text = self._clean_text(elem.get_text())
                        text = re.sub(r'\(citation needed\)', '', text,
                                     flags=re.IGNORECASE)

                        if len(text) < 40:
                            continue

                        if self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': author_url
                            })

                    time.sleep(1)  # Задержка между запросами

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading author {author}: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing author {author}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_wikiquote_en: {e}")

        logger.info(f"Loaded {len(quotations)} quotes from Wikiquote EN")
        return quotations

    def _load_from_wikiquote_ru(self, max_quotes: int = 1000) -> List[Dict]:
        """
        Загрузка цитат с ru.wikiquote.org (русский).
        Использует улучшенный парсинг из предоставленного кода.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Список известных авторов для загрузки
            authors = [
                'Альберт_Эйнштейн', 'Уильям_Шекспир', 'Марк_Твен',
                'Оскар_Уайльд', 'Уинстон_Черчилль', 'Махатма_Ганди',
                'Конфуций', 'Платон', 'Аристотель', 'Вольтер',
                'Фридрих_Ницше', 'Ральф_Уолдо_Эмерсон',
                'Бенджамин_Франклин', 'Томас_Джефферсон',
                'Авраам_Линкольн', 'Мартин_Лютер_Кинг',
                'Нельсон_Мандела', 'Мать_Тереза',
                'Будда', 'Лао-цзы', 'Сунь-цзы',
                'Леонардо_да_Винчи', 'Исаак_Ньютон',
                'Чарльз_Дарвин', 'Галилео_Галилей',
                'Лев_Толстой', 'Фёдор_Достоевский',
                'Антон_Чехов', 'Александр_Пушкин',
                'Михаил_Лермонтов', 'Николай_Гоголь',
            ]

            # Загружаем страницы авторов
            for author in authors:
                if len(quotations) >= max_quotes:
                    break

                try:
                    author_url = f'https://ru.wikiquote.org/wiki/{author}'
                    response = self.session.get(author_url, timeout=10)
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Извлекаем цитаты из bullet points
                    bullet_quotes = self._extract_bullets_from_html(soup)
                    for text in bullet_quotes:
                        if len(quotations) >= max_quotes:
                            break

                        # Нормализация текста
                        text = self._normalize_text(text)
                        # Убираем метаданные
                        text = re.sub(r'\(нужна цитата\)', '', text,
                                     flags=re.IGNORECASE)
                        text = re.sub(r'\(citation needed\)', '', text,
                                     flags=re.IGNORECASE)
                        text = re.sub(r'\(спорно\)', '', text,
                                     flags=re.IGNORECASE)

                        # Проверка языка
                        if LANGDETECT_AVAILABLE:
                            try:
                                detected = detect(text)
                                if detected != 'ru':
                                    continue
                            except:
                                pass

                        # Минимальная длина (40 символов)
                        if len(text) < 40:
                            continue

                        if self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': None,  # Не сохраняем автора
                                'source_url': author_url
                            })

                    # Также ищем цитаты в других структурах Wikiquote
                    # Ищем в основном контенте
                    content = soup.find('div', class_='mw-parser-output')
                    if not content:
                        content = soup.find('div', id='mw-content-text')
                    if not content:
                        content = soup

                    # Ищем все li элементы в основном контенте
                    quote_elements = content.find_all('li')
                    # Фильтруем: пропускаем навигацию и короткие элементы
                    filtered_elements = []
                    for elem in quote_elements:
                        text = self._normalize_text(elem.get_text())
                        # Пропускаем навигацию
                        if re.search(r'^(edit|править|ссылки|links|see also|'
                                    r'категории|categories)',
                                    text, re.IGNORECASE):
                            continue
                        # Пропускаем слишком короткие
                        if len(text) < 40:
                            continue
                        filtered_elements.append(elem)

                    quote_elements = filtered_elements

                    for elem in quote_elements:
                        if len(quotations) >= max_quotes:
                            break

                        text = self._clean_text(elem.get_text())
                        text = re.sub(r'\(нужна цитата\)', '', text,
                                     flags=re.IGNORECASE)
                        text = re.sub(r'\(citation needed\)', '', text,
                                     flags=re.IGNORECASE)

                        if len(text) < 40:
                            continue

                        if self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'ru',
                                'author': None,
                                'source_url': author_url
                            })

                    time.sleep(1)  # Задержка между запросами

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading author {author}: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing author {author}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_wikiquote_ru: {e}")

        logger.info(f"Loaded {len(quotations)} quotes from Wikiquote RU")
        return quotations

    def _parse_docx_file(self, file_path: str) -> List[Dict]:
        """
        Парсинг docx файла и извлечение цитат.

        Args:
            file_path: Путь к docx файлу

        Returns:
            Список цитат
        """
        quotations = []
        if not DOCX_AVAILABLE:
            logger.warning("python-docx not available, skipping docx parsing")
            return quotations

        try:
            doc = Document(file_path)
            current_quote = []
            language = None

            for para in doc.paragraphs:
                text = para.text.strip()
                if not text:
                    # Пустая строка - возможный разделитель между цитатами
                    if current_quote:
                        quote_text = ' '.join(current_quote).strip()
                        if len(quote_text) >= 20:
                            # Определяем язык
                            if not language and LANGDETECT_AVAILABLE:
                                try:
                                    language = detect(quote_text)
                                except:
                                    language = 'en'  # По умолчанию
                            elif not language:
                                # Простая эвристика: если есть кириллица - русский
                                if re.search(r'[а-яёА-ЯЁ]', quote_text):
                                    language = 'ru'
                                else:
                                    language = 'en'

                            if self._is_valid_quotation(quote_text):
                                quotations.append({
                                    'text': quote_text,
                                    'language': language,
                                    'author': None,
                                    'source_url': file_path
                                })
                        current_quote = []
                    continue

                # Проверяем, является ли строка началом новой цитаты
                # Признаки начала цитаты:
                # - Начинается с заглавной буквы или кавычки
                # - Заканчивается точкой, восклицательным или вопросительным знаком
                # - Длина больше 20 символов
                is_quote_start = (
                    len(text) >= 20 and
                    (text[0].isupper() or text[0] in ['"', '«', "'"]) and
                    (text[-1] in '.!?' or text[-1] in ['"', '»', "'"])
                )

                if is_quote_start and current_quote:
                    # Сохраняем предыдущую цитату
                    quote_text = ' '.join(current_quote).strip()
                    if len(quote_text) >= 20:
                        if not language and LANGDETECT_AVAILABLE:
                            try:
                                language = detect(quote_text)
                            except:
                                language = 'en'
                        elif not language:
                            if re.search(r'[а-яёА-ЯЁ]', quote_text):
                                language = 'ru'
                            else:
                                language = 'en'

                        if self._is_valid_quotation(quote_text):
                            quotations.append({
                                'text': quote_text,
                                'language': language,
                                'author': None,
                                'source_url': file_path
                            })
                    current_quote = [text]
                else:
                    current_quote.append(text)

            # Обрабатываем последнюю цитату
            if current_quote:
                quote_text = ' '.join(current_quote).strip()
                if len(quote_text) >= 20:
                    if not language and LANGDETECT_AVAILABLE:
                        try:
                            language = detect(quote_text)
                        except:
                            language = 'en'
                    elif not language:
                        if re.search(r'[а-яёА-ЯЁ]', quote_text):
                            language = 'ru'
                        else:
                            language = 'en'

                    if self._is_valid_quotation(quote_text):
                        quotations.append({
                            'text': quote_text,
                            'language': language,
                            'author': None,
                            'source_url': file_path
                        })

        except Exception as e:
            logger.error(f"Error parsing docx file {file_path}: {e}")

        return quotations

    def _load_from_doc_files(self, directory: str = '.') -> List[Dict]:
        """
        Загрузка цитат из doc/docx файлов в директории.

        Args:
            directory: Директория для поиска файлов

        Returns:
            Список цитат
        """
        quotations = []
        if not DOCX_AVAILABLE:
            return quotations

        try:
            # Ищем все docx файлы
            docx_files = glob.glob(os.path.join(directory, '*.docx'))
            docx_files.extend(glob.glob(os.path.join(directory, '*.doc')))

            for file_path in docx_files:
                try:
                    file_quotes = self._parse_docx_file(file_path)
                    quotations.extend(file_quotes)
                    logger.info(
                        f"Loaded {len(file_quotes)} quotations from {file_path}"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from {file_path}: {e}")

        except Exception as e:
            logger.error(f"Error in _load_from_doc_files: {e}")

        return quotations

    def _load_from_quote_garden(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с quotegarden.com (английский).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Quote Garden API
            url = 'https://quotegarden.io/api/v3/quotes'
            page = 1
            per_page = 100

            while len(quotations) < max_quotes:
                try:
                    params = {'page': page, 'limit': min(per_page,
                                                         max_quotes - len(quotations))}
                    response = self.session.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    if 'data' not in data or not data['data']:
                        break

                    for quote_data in data['data']:
                        text = quote_data.get('quoteText', '').strip()
                        if self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': 'https://quotegarden.io'
                            })

                            if len(quotations) >= max_quotes:
                                break

                    if len(data.get('data', [])) < per_page:
                        break

                    page += 1
                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from Quote Garden: {e}")
                    break
                except Exception as e:
                    logger.debug(f"Error parsing Quote Garden: {e}")
                    break

        except Exception as e:
            logger.warning(f"Error in _load_from_quote_garden: {e}")

        return quotations

    def _load_from_quotesondesign(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с quotesondesign.com (английский).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://quotesondesign.com/api/v3/quotes'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'data' in data:
                for quote_data in data['data']:
                    text = quote_data.get('quote', '').strip()
                    if self._is_valid_quotation(text):
                        quotations.append({
                            'text': text,
                            'language': 'en',
                            'author': None,
                            'source_url': 'https://quotesondesign.com'
                        })

                        if len(quotations) >= max_quotes:
                            break

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logger.debug(f"Error loading from quotesondesign: {e}")
        except Exception as e:
            logger.debug(f"Error parsing quotesondesign: {e}")

        return quotations

    def _load_from_forismatic_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с Forismatic API (английский).
        API: http://forismatic.com/en/api/

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'http://api.forismatic.com/api/1.0/'
            for i in range(min(max_quotes, 500)):  # API ограничение
                try:
                    params = {
                        'method': 'getQuote',
                        'format': 'json',
                        'lang': 'en',
                        'key': i
                    }
                    response = self.session.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    text = data.get('quoteText', '').strip()
                    if text and self._is_valid_quotation(text):
                        quotations.append({
                            'text': text,
                            'language': 'en',
                            'author': None,
                            'source_url': 'http://forismatic.com'
                        })

                        if len(quotations) >= max_quotes:
                            break

                    time.sleep(0.3)  # Задержка для API

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from Forismatic API: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing Forismatic API: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_forismatic_api: {e}")

        return quotations

    def _load_from_theysaidso_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с They Said So API (английский).
        API: https://theysaidso.com/api/

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Quote of the day endpoint
            url = 'https://quotes.rest/qod'
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()

                if 'contents' in data and 'quotes' in data['contents']:
                    for quote_data in data['contents']['quotes']:
                        text = quote_data.get('quote', '').strip()
                        if text and self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': 'https://theysaidso.com'
                            })
            except:
                pass

            # Random quotes endpoint
            url = 'https://quotes.rest/quote/random'
            while len(quotations) < max_quotes:
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    if 'contents' in data and 'quote' in data['contents']:
                        text = data['contents']['quote'].get('quote', '').strip()
                        if text and self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': 'https://theysaidso.com'
                            })

                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from They Said So API: {e}")
                    break
                except Exception as e:
                    logger.debug(f"Error parsing They Said So API: {e}")
                    break

        except Exception as e:
            logger.warning(f"Error in _load_from_theysaidso_api: {e}")

        return quotations

    def _load_from_programming_quotes_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с Programming Quotes API (английский).
        API: https://programming-quotes-api.herokuapp.com/

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://programming-quotes-api.herokuapp.com/quotes'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            for quote_data in data:
                text = quote_data.get('en', '').strip()
                if text and self._is_valid_quotation(text):
                    quotations.append({
                        'text': text,
                        'language': 'en',
                        'author': None,
                        'source_url': 'https://programming-quotes-api.herokuapp.com'
                    })

                    if len(quotations) >= max_quotes:
                        break

        except requests.exceptions.RequestException as e:
            logger.debug(f"Error loading from Programming Quotes API: {e}")
        except Exception as e:
            logger.debug(f"Error parsing Programming Quotes API: {e}")

        return quotations

    def _load_from_citaty_info(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка русских цитат с citaty.info (веб-скрапинг).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://www.citaty.info/'
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Ищем цитаты на странице
            quote_elements = soup.find_all(['div', 'p', 'blockquote'],
                                         class_=re.compile(r'quote|quotation|цитата'))
            
            for elem in quote_elements:
                if len(quotations) >= max_quotes:
                    break

                text = self._clean_text(elem.get_text())
                if len(text) >= 20 and self._is_valid_quotation(text):
                    if re.search(r'[а-яёА-ЯЁ]', text):
                        quotations.append({
                            'text': text,
                            'language': 'ru',
                            'author': None,
                            'source_url': url
                        })

            # Пробуем загрузить дополнительные страницы
            for page in range(2, min(10, (max_quotes // 20) + 2)):
                try:
                    page_url = f'{url}?page={page}'
                    page_response = self.session.get(page_url, timeout=15)
                    if page_response.status_code == 200:
                        page_soup = BeautifulSoup(page_response.content, 'html.parser')
                        quote_elements = page_soup.find_all(['div', 'p', 'blockquote'],
                                                           class_=re.compile(r'quote|quotation|цитата'))
                        
                        for elem in quote_elements:
                            if len(quotations) >= max_quotes:
                                break

                            text = self._clean_text(elem.get_text())
                            if len(text) >= 20 and self._is_valid_quotation(text):
                                if re.search(r'[а-яёА-ЯЁ]', text):
                                    quotations.append({
                                        'text': text,
                                        'language': 'ru',
                                        'author': None,
                                        'source_url': page_url
                                    })

                        if not quote_elements:
                            break

                        time.sleep(1)
                except:
                    break

        except requests.exceptions.RequestException as e:
            logger.debug(f"Error loading from citaty.info: {e}")
        except Exception as e:
            logger.debug(f"Error parsing citaty.info: {e}")

        return quotations

    def _load_from_ru_rss_feeds(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка русских цитат из RSS фидов.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # RSS фиды с цитатами (если доступны)
            rss_feeds = [
                'https://ru.citaty.net/rss/',
                'https://www.citaty.info/rss/',
            ]

            for feed_url in rss_feeds:
                if len(quotations) >= max_quotes:
                    break

                try:
                    response = self.session.get(feed_url, timeout=15)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'xml')
                        items = soup.find_all('item')

                        for item in items:
                            if len(quotations) >= max_quotes:
                                break

                            # Получаем описание/цитату
                            description = item.find('description')
                            if description:
                                text = self._clean_text(description.text)
                                # Извлекаем цитату из описания
                                text = re.sub(r'<[^>]+>', '', text)
                                text = text.strip()

                                if len(text) >= 20 and self._is_valid_quotation(text):
                                    if re.search(r'[а-яёА-ЯЁ]', text):
                                        link_elem = item.find('link')
                                        source_url = link_elem.text if link_elem else feed_url

                                        quotations.append({
                                            'text': text,
                                            'language': 'ru',
                                            'author': None,
                                            'source_url': source_url
                                        })

                        time.sleep(1)
                except:
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_ru_rss_feeds: {e}")

        return quotations

    def _load_from_en_rss_feeds(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка английских цитат из RSS фидов.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # RSS фиды с цитатами
            rss_feeds = [
                'https://www.brainyquote.com/link/quotefu.rss',
                'https://feeds.feedburner.com/brainyquote/QUOTEBR',
            ]

            for feed_url in rss_feeds:
                if len(quotations) >= max_quotes:
                    break

                try:
                    response = self.session.get(feed_url, timeout=15)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'xml')
                        items = soup.find_all('item')

                        for item in items:
                            if len(quotations) >= max_quotes:
                                break

                            # Получаем описание/цитату
                            description = item.find('description')
                            if description:
                                text = self._clean_text(description.text)
                                text = re.sub(r'<[^>]+>', '', text)
                                text = text.strip()

                                if len(text) >= 20 and self._is_valid_quotation(text):
                                    link_elem = item.find('link')
                                    source_url = link_elem.text if link_elem else feed_url

                                    quotations.append({
                                        'text': text,
                                        'language': 'en',
                                        'author': None,
                                        'source_url': source_url
                                    })

                        time.sleep(1)
                except:
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_en_rss_feeds: {e}")

        return quotations

    def _load_from_forismatic_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с Forismatic API (английский).
        API: http://forismatic.com/en/api/

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'http://api.forismatic.com/api/1.0/'
            for i in range(min(max_quotes, 500)):  # API ограничение
                try:
                    params = {
                        'method': 'getQuote',
                        'format': 'json',
                        'lang': 'en',
                        'key': i
                    }
                    response = self.session.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    text = data.get('quoteText', '').strip()
                    if text and self._is_valid_quotation(text):
                        quotations.append({
                            'text': text,
                            'language': 'en',
                            'author': None,
                            'source_url': 'http://forismatic.com'
                        })

                        if len(quotations) >= max_quotes:
                            break

                    time.sleep(0.3)  # Задержка для API

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from Forismatic API: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error parsing Forismatic API: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_forismatic_api: {e}")

        return quotations

    def _load_from_theysaidso_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с They Said So API (английский).
        API: https://theysaidso.com/api/

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Quote of the day endpoint
            url = 'https://quotes.rest/qod'
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()

                if 'contents' in data and 'quotes' in data['contents']:
                    for quote_data in data['contents']['quotes']:
                        text = quote_data.get('quote', '').strip()
                        if text and self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': 'https://theysaidso.com'
                            })
            except:
                pass

            # Random quotes endpoint
            url = 'https://quotes.rest/quote/random'
            while len(quotations) < max_quotes:
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    if 'contents' in data and 'quote' in data['contents']:
                        text = data['contents']['quote'].get('quote', '').strip()
                        if text and self._is_valid_quotation(text):
                            quotations.append({
                                'text': text,
                                'language': 'en',
                                'author': None,
                                'source_url': 'https://theysaidso.com'
                            })

                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Error loading from They Said So API: {e}")
                    break
                except Exception as e:
                    logger.debug(f"Error parsing They Said So API: {e}")
                    break

        except Exception as e:
            logger.warning(f"Error in _load_from_theysaidso_api: {e}")

        return quotations

    def _load_from_programming_quotes_api(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка цитат с Programming Quotes API (английский).
        API: https://programming-quotes-api.herokuapp.com/

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://programming-quotes-api.herokuapp.com/quotes'
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            for quote_data in data:
                text = quote_data.get('en', '').strip()
                if text and self._is_valid_quotation(text):
                    quotations.append({
                        'text': text,
                        'language': 'en',
                        'author': None,
                        'source_url': 'https://programming-quotes-api.herokuapp.com'
                    })

                    if len(quotations) >= max_quotes:
                        break

        except requests.exceptions.RequestException as e:
            logger.debug(f"Error loading from Programming Quotes API: {e}")
        except Exception as e:
            logger.debug(f"Error parsing Programming Quotes API: {e}")

        return quotations

    def _load_from_citaty_info(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка русских цитат с citaty.info (веб-скрапинг).

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            url = 'https://www.citaty.info/'
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Ищем цитаты на странице
            quote_elements = soup.find_all(['div', 'p', 'blockquote'],
                                         class_=re.compile(r'quote|quotation|цитата'))
            
            for elem in quote_elements:
                if len(quotations) >= max_quotes:
                    break

                text = self._clean_text(elem.get_text())
                if len(text) >= 20 and self._is_valid_quotation(text):
                    if re.search(r'[а-яёА-ЯЁ]', text):
                        quotations.append({
                            'text': text,
                            'language': 'ru',
                            'author': None,
                            'source_url': url
                        })

            # Пробуем загрузить дополнительные страницы
            for page in range(2, min(10, (max_quotes // 20) + 2)):
                try:
                    page_url = f'{url}?page={page}'
                    page_response = self.session.get(page_url, timeout=15)
                    if page_response.status_code == 200:
                        page_soup = BeautifulSoup(page_response.content, 'html.parser')
                        quote_elements = page_soup.find_all(['div', 'p', 'blockquote'],
                                                           class_=re.compile(r'quote|quotation|цитата'))
                        
                        for elem in quote_elements:
                            if len(quotations) >= max_quotes:
                                break

                            text = self._clean_text(elem.get_text())
                            if len(text) >= 20 and self._is_valid_quotation(text):
                                if re.search(r'[а-яёА-ЯЁ]', text):
                                    quotations.append({
                                        'text': text,
                                        'language': 'ru',
                                        'author': None,
                                        'source_url': page_url
                                    })

                        if not quote_elements:
                            break

                        time.sleep(1)
                except:
                    break

        except requests.exceptions.RequestException as e:
            logger.debug(f"Error loading from citaty.info: {e}")
        except Exception as e:
            logger.debug(f"Error parsing citaty.info: {e}")

        return quotations

    def _load_from_ru_rss_feeds(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка русских цитат из RSS фидов.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # RSS фиды с цитатами (если доступны)
            rss_feeds = [
                'https://ru.citaty.net/rss/',
                'https://www.citaty.info/rss/',
            ]

            for feed_url in rss_feeds:
                if len(quotations) >= max_quotes:
                    break

                try:
                    response = self.session.get(feed_url, timeout=15)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'xml')
                        items = soup.find_all('item')

                        for item in items:
                            if len(quotations) >= max_quotes:
                                break

                            # Получаем описание/цитату
                            description = item.find('description')
                            if description:
                                text = self._clean_text(description.text)
                                # Извлекаем цитату из описания
                                text = re.sub(r'<[^>]+>', '', text)
                                text = text.strip()

                                if len(text) >= 20 and self._is_valid_quotation(text):
                                    if re.search(r'[а-яёА-ЯЁ]', text):
                                        link_elem = item.find('link')
                                        source_url = link_elem.text if link_elem else feed_url

                                        quotations.append({
                                            'text': text,
                                            'language': 'ru',
                                            'author': None,
                                            'source_url': source_url
                                        })

                        time.sleep(1)
                except:
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_ru_rss_feeds: {e}")

        return quotations

    def _load_from_en_rss_feeds(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка английских цитат из RSS фидов.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # RSS фиды с цитатами
            rss_feeds = [
                'https://www.brainyquote.com/link/quotefu.rss',
                'https://feeds.feedburner.com/brainyquote/QUOTEBR',
            ]

            for feed_url in rss_feeds:
                if len(quotations) >= max_quotes:
                    break

                try:
                    response = self.session.get(feed_url, timeout=15)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'xml')
                        items = soup.find_all('item')

                        for item in items:
                            if len(quotations) >= max_quotes:
                                break

                            # Получаем описание/цитату
                            description = item.find('description')
                            if description:
                                text = self._clean_text(description.text)
                                text = re.sub(r'<[^>]+>', '', text)
                                text = text.strip()

                                if len(text) >= 20 and self._is_valid_quotation(text):
                                    link_elem = item.find('link')
                                    source_url = link_elem.text if link_elem else feed_url

                                    quotations.append({
                                        'text': text,
                                        'language': 'en',
                                        'author': None,
                                        'source_url': source_url
                                    })

                        time.sleep(1)
                except:
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_en_rss_feeds: {e}")

        return quotations

    def _load_from_ru_proverbs(self, max_quotes: int = 500) -> List[Dict]:
        """
        Загрузка русских пословиц и поговорок.

        Args:
            max_quotes: Максимальное количество цитат

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Используем предопределенные пословицы из quotations_data.py
            # и добавляем больше из известных источников
            proverbs = [
                'Без труда не вытащишь и рыбку из пруда.',
                'Век живи — век учись.',
                'В гостях хорошо, а дома лучше.',
                'В здоровом теле — здоровый дух.',
                'Вода камень точит.',
                'Где тонко, там и рвётся.',
                'Дареному коню в зубы не смотрят.',
                'Дело мастера боится.',
                'Дорогу осилит идущий.',
                'Знание — сила.',
                'Капля камень точит.',
                'Кто ищет, тот всегда найдёт.',
                'Лучше поздно, чем никогда.',
                'Лучше синица в руках, чем журавль в небе.',
                'Москва не сразу строилась.',
                'Не всё то золото, что блестит.',
                'Не откладывай на завтра то, что можно сделать сегодня.',
                'Один в поле не воин.',
                'Повторение — мать учения.',
                'Поспешишь — людей насмешишь.',
                'Семь раз отмерь — один раз отрежь.',
                'Слово — серебро, молчание — золото.',
                'Терпение и труд всё перетрут.',
                'Тише едешь — дальше будешь.',
                'У страха глаза велики.',
                'Умный в гору не пойдёт, умный гору обойдёт.',
                'Учиться никогда не поздно.',
                'Ученье — свет, а неученье — тьма.',
                'Хорошее начало — половина дела.',
                'Цыплят по осени считают.',
                'Что написано пером, не вырубишь топором.',
                'Что посеешь, то и пожнёшь.',
                'Яблоко от яблони недалеко падает.',
            ]

            for proverb in proverbs:
                if len(quotations) >= max_quotes:
                    break

                if self._is_valid_quotation(proverb):
                    quotations.append({
                        'text': proverb,
                        'language': 'ru',
                        'author': None,
                        'source_url': None
                    })

        except Exception as e:
            logger.warning(f"Error in _load_from_ru_proverbs: {e}")

        return quotations

    def _extract_quotes_from_text(self, text: str) -> List[str]:
        """
        Извлечение отдельных цитат из текста поста.
        Разделяет цитаты по различным признакам.

        Args:
            text: Текст поста

        Returns:
            Список отдельных цитат
        """
        quotes = []
        if not text or len(text.strip()) < 20:
            return quotes

        # Нормализуем текст
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\r', '\n', text)

        # Метод 1: Разделение по двойным переносам строк
        # (часто цитаты разделяются пустыми строками)
        paragraphs = re.split(r'\n\s*\n+', text)
        for para in paragraphs:
            para = para.strip()
            if len(para) >= 20:
                # Убираем нумерацию в начале (1., 2., и т.д.)
                para = re.sub(r'^\d+[\.\)]\s*', '', para)
                # Убираем маркеры списка (-, *, •)
                para = re.sub(r'^[-*•]\s+', '', para)
                para = para.strip()
                if len(para) >= 20 and self._is_valid_quotation(para):
                    quotes.append(para)

        # Метод 2: Если цитаты в кавычках, извлекаем их
        quoted = re.findall(r'["«»""]([^"«»""]{20,}?)["«»""]', text)
        for quote in quoted:
            quote = quote.strip()
            if len(quote) >= 20 and self._is_valid_quotation(quote):
                if quote not in quotes:  # Избегаем дубликатов
                    quotes.append(quote)

        # Метод 3: Разделение по строкам, начинающимся с заглавной буквы
        # и заканчивающимся точкой/восклицательным/вопросительным знаком
        lines = text.split('\n')
        current_quote = []
        for line in lines:
            line = line.strip()
            if not line:
                if current_quote:
                    quote_text = ' '.join(current_quote).strip()
                    if len(quote_text) >= 20 and self._is_valid_quotation(quote_text):
                        if quote_text not in quotes:
                            quotes.append(quote_text)
                    current_quote = []
                continue

            # Проверяем, является ли строка началом новой цитаты
            is_new_quote = (
                len(line) >= 20 and
                (line[0].isupper() or line[0] in ['"', '«', "'"]) and
                (line[-1] in '.!?' or line[-1] in ['"', '»', "'"])
            )

            if is_new_quote and current_quote:
                # Сохраняем предыдущую цитату
                quote_text = ' '.join(current_quote).strip()
                if len(quote_text) >= 20 and self._is_valid_quotation(quote_text):
                    if quote_text not in quotes:
                        quotes.append(quote_text)
                current_quote = [line]
            else:
                current_quote.append(line)

        # Обрабатываем последнюю цитату
        if current_quote:
            quote_text = ' '.join(current_quote).strip()
            if len(quote_text) >= 20 and self._is_valid_quotation(quote_text):
                if quote_text not in quotes:
                    quotes.append(quote_text)

        return quotes

    def _load_from_livejournal(self, username: str = 'civil-engineer',
                              max_quotes: int = 1000,
                              tag_patterns: Optional[List[str]] = None) -> List[Dict]:
        """
        Загрузка цитат с LiveJournal из постов с тегами.
        Поддерживает регулярные выражения для тегов (например, aforizm[.*] или aphorizm[.*]).
        Использует HTML страницы с тегами (веб-парсер), так как API не существует.

        Args:
            username: Имя пользователя LiveJournal
            max_quotes: Максимальное количество цитат
            tag_patterns: Список паттернов тегов (регулярные выражения).
                         Если None, использует по умолчанию ['aforizm.*', 'aphorizm.*']

        Returns:
            Список цитат
        """
        quotations = []
        try:
            # Теги для поиска (поддержка регулярных выражений)
            if tag_patterns is None:
                # По умолчанию: aforizm[.*] или aphorizm[.*] (любые символы после)
                tag_patterns = ['aforizm.*', 'aphorizm.*']
            
            # Преобразуем паттерны в список конкретных тегов для поиска
            # LiveJournal не поддерживает regex напрямую, поэтому пробуем варианты
            tags = []
            for pattern in tag_patterns:
                # Убираем regex синтаксис и пробуем варианты
                base_tag = re.sub(r'\[.*?\]', '', pattern)  # Убираем [.*]
                base_tag = re.sub(r'\.\*', '', base_tag)   # Убираем .*
                base_tag = base_tag.strip()
                
                if base_tag:
                    tags.append(base_tag)  # Точное совпадение
                    # Также пробуем варианты с подстановкой
                    tags.append(f'{base_tag}*')
            
            # Дополнительно: пробуем найти все теги, начинающиеся с базовых префиксов
            # через RSS фид или страницу тегов пользователя
            base_prefixes = ['aforizm', 'aphorizm']
            for prefix in base_prefixes:
                if prefix not in [t.replace('*', '') for t in tags]:
                    # Пробуем загрузить список всех тегов пользователя
                    try:
                        tags_url = f'https://{username}.livejournal.com/tags/'
                        tags_response = self.session.get(tags_url, timeout=15)
                        if tags_response.status_code == 200:
                            tags_soup = BeautifulSoup(tags_response.content, 'html.parser')
                            # Ищем все ссылки на теги
                            tag_links = tags_soup.find_all('a', href=re.compile(r'/tag/'))
                            for link in tag_links:
                                tag_name = link.get('href', '').replace('/tag/', '').rstrip('/')
                                # Если тег начинается с нужного префикса, добавляем его
                                if tag_name.startswith(prefix) and tag_name not in tags:
                                    tags.append(tag_name)
                                    logger.info(f"Found additional tag: {tag_name}")
                    except Exception as e:
                        logger.debug(f"Could not load tags list: {e}")

            for tag in tags:
                if len(quotations) >= max_quotes:
                    break

                try:
                    # Используем HTML страницу с тегом
                    # Формат: https://username.livejournal.com/tag/tag_name/
                    tag_url = (
                        f'https://{username}.livejournal.com/tag/{tag}/'
                    )
                    logger.info(f"Loading from LiveJournal: {tag_url}")
                    response = self.session.get(tag_url, timeout=15)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Ищем все посты на странице
                    # LiveJournal использует различные структуры для постов
                    posts = []
                    
                    # Метод 1: Ищем по классу entry-text (основной контент поста)
                    entries = soup.find_all('div', class_=re.compile(r'entry-text|entry-content|entry-body'))
                    if entries:
                        posts.extend(entries)
                    
                    # Метод 2: Ищем по классу entry или post
                    if not posts:
                        entries = soup.find_all('div', class_=re.compile(r'entry|post'))
                        posts.extend(entries)
                    
                    # Метод 3: Ищем по структуре с датой и следующим за ней контентом
                    date_headers = soup.find_all(['h2', 'h3', 'h4'], 
                                                 class_=re.compile(r'date|entry'))
                    for header in date_headers:
                        # Ищем следующий div с контентом
                        entry = header.find_next('div', class_=re.compile(r'entry|post|content'))
                        if entry and entry not in posts:
                            posts.append(entry)
                        # Также ищем в родительском элементе
                        parent = header.find_parent('div', class_=re.compile(r'entry|post'))
                        if parent and parent not in posts:
                            posts.append(parent)
                    
                    # Метод 4: Ищем все div с контентом поста
                    if not posts:
                        content_areas = soup.find_all('div', 
                                                     class_=re.compile(r'content|text|body'))
                        posts.extend(content_areas)
                    
                    # Метод 5: Ищем по структуре LiveJournal (более специфичные селекторы)
                    if not posts:
                        # Ищем блоки с классом b-singlepost-body или похожие
                        lj_posts = soup.find_all('div', class_=re.compile(r'b-singlepost|b-single-post'))
                        posts.extend(lj_posts)

                    logger.info(
                        f"Found {len(posts)} posts with tag '{tag}' "
                        f"on {username}.livejournal.com"
                    )

                    # Также используем RSS для получения всех постов с этим тегом
                    # RSS может содержать больше постов, чем HTML страница
                    try:
                        rss_url = (
                            f'https://{username}.livejournal.com/data/rss?tag={tag}'
                        )
                        rss_response = self.session.get(rss_url, timeout=15)
                        rss_response.raise_for_status()
                        rss_soup = BeautifulSoup(rss_response.content, 'xml')
                        rss_items = rss_soup.find_all('item')
                        
                        logger.info(
                            f"Found {len(rss_items)} posts in RSS feed for tag '{tag}'"
                        )
                        
                        # Добавляем посты из RSS, если их еще нет в списке
                        rss_post_urls = set()
                        for item in rss_items:
                            if len(quotations) >= max_quotes:
                                break
                            
                            link_elem = item.find('link')
                            if not link_elem:
                                continue
                            post_url = link_elem.text.strip()
                            rss_post_urls.add(post_url)
                            
                            # Проверяем, есть ли уже этот пост в списке
                            post_already_found = False
                            for existing_post in posts:
                                existing_link = existing_post.find('a', href=re.compile(r'/\d+\.html'))
                                if existing_link:
                                    existing_url = existing_link.get('href', '')
                                    if existing_url and not existing_url.startswith('http'):
                                        existing_url = f'https://{username}.livejournal.com{existing_url}'
                                    if existing_url == post_url:
                                        post_already_found = True
                                        break
                            
                            if not post_already_found:
                                # Загружаем полный пост из RSS
                                try:
                                    post_response = self.session.get(post_url, timeout=15)
                                    if post_response.status_code == 200:
                                        post_soup = BeautifulSoup(
                                            post_response.content, 'html.parser'
                                        )
                                        # Ищем контент поста
                                        content_div = post_soup.find(
                                            'div', class_=re.compile(r'entry-text|entry-content|entry-body|b-singlepost-body')
                                        )
                                        if not content_div:
                                            content_div = post_soup.find(
                                                'div', class_=re.compile(r'entry|post|content')
                                            )
                                        if content_div:
                                            posts.append(content_div)
                                            logger.debug(f"Added post from RSS: {post_url}")
                                except Exception as e:
                                    logger.debug(f"Could not load post from RSS {post_url}: {e}")
                    except Exception as e:
                        logger.debug(f"Could not load RSS feed for tag '{tag}': {e}")

                    # Обрабатываем найденные посты
                    for post in posts:
                        if len(quotations) >= max_quotes:
                            break

                        try:
                            # Получаем ссылку на пост
                            post_url = None
                            link_elem = post.find('a', href=re.compile(r'/\d+\.html'))
                            if link_elem:
                                post_url = link_elem.get('href', '')
                                if post_url and not post_url.startswith('http'):
                                    post_url = f'https://{username}.livejournal.com{post_url}'
                            
                            # Извлекаем текст поста
                            post_text = post.get_text(separator='\n')
                            
                            # Очищаем текст
                            post_text = re.sub(r'\s+', ' ', post_text)
                            post_text = re.sub(r'\n\s*\n+', '\n\n', post_text)
                            post_text = post_text.strip()

                            # Если текст слишком короткий, пробуем найти больше контента
                            if len(post_text) < 100:
                                # Ищем вложенные div с контентом
                                content_divs = post.find_all('div', 
                                                             class_=re.compile(r'content|text|body'))
                                for div in content_divs:
                                    div_text = div.get_text(separator='\n')
                                    if len(div_text) > len(post_text):
                                        post_text = div_text

                            # Извлекаем отдельные цитаты из текста поста
                            extracted_quotes = self._extract_quotes_from_text(post_text)

                            for quote_text in extracted_quotes:
                                if len(quotations) >= max_quotes:
                                    break

                                # Проверяем язык (должен быть русский)
                                if LANGDETECT_AVAILABLE:
                                    try:
                                        detected = detect(quote_text)
                                        if detected != 'ru':
                                            continue
                                    except:
                                        # Если не удалось определить, проверяем кириллицу
                                        if not re.search(r'[а-яёА-ЯЁ]', quote_text):
                                            continue
                                else:
                                    # Простая проверка на кириллицу
                                    if not re.search(r'[а-яёА-ЯЁ]', quote_text):
                                        continue

                                # Применяем строгие фильтры
                                if self._is_valid_quotation(quote_text):
                                    quotations.append({
                                        'text': quote_text,
                                        'language': 'ru',
                                        'author': None,
                                        'source_url': post_url or tag_url
                                    })

                            time.sleep(1)  # Задержка между постами

                        except Exception as e:
                            logger.debug(f"Error processing post: {e}")
                            continue

                    # Обрабатываем пагинацию - загружаем все страницы с этим тегом
                    page_num = 1
                    max_pages = 50  # Ограничение на количество страниц
                    
                    while page_num < max_pages and len(quotations) < max_quotes:
                        # Ищем ссылку на следующую страницу
                        next_link = soup.find('a', string=re.compile(r'next|далее|следующ|вперёд', 
                                                                     re.IGNORECASE))
                        if not next_link:
                            # Пробуем найти по href
                            next_link = soup.find('a', href=re.compile(r'page=\d+|skip=\d+'))
                        
                        if not next_link:
                            # Пробуем найти ссылку с текстом "2", "3" и т.д. (номера страниц)
                            page_links = soup.find_all('a', href=re.compile(r'page=|skip='))
                            if page_links:
                                # Находим следующую страницу
                                current_page_urls = [link.get('href', '') for link in page_links]
                                # Пробуем следующую страницу напрямую
                                next_page_url = f"{tag_url}?page={page_num + 1}"
                                if next_page_url not in current_page_urls:
                                    break
                                next_link = soup.find('a', href=re.compile(f'page={page_num + 1}'))
                        
                        if not next_link:
                            break
                        
                        # Получаем URL следующей страницы
                        next_url = next_link.get('href', '')
                        if not next_url.startswith('http'):
                            next_url = f'https://{username}.livejournal.com{next_url}'
                        
                        logger.info(f"Loading next page {page_num + 1} for tag '{tag}': {next_url}")
                        
                        try:
                            next_response = self.session.get(next_url, timeout=15)
                            next_response.raise_for_status()
                            soup = BeautifulSoup(next_response.content, 'html.parser')
                            
                            # Ищем посты на следующей странице
                            next_posts = []
                            entries = soup.find_all('div', class_=re.compile(r'entry-text|entry-content|entry-body'))
                            if entries:
                                next_posts.extend(entries)
                            if not next_posts:
                                entries = soup.find_all('div', class_=re.compile(r'entry|post'))
                                next_posts.extend(entries)
                            
                            if not next_posts:
                                # Если не нашли посты, прекращаем пагинацию
                                break
                            
                            logger.info(f"Found {len(next_posts)} posts on page {page_num + 1}")
                            
                            # Обрабатываем посты со следующей страницы
                            for post in next_posts:
                                if len(quotations) >= max_quotes:
                                    break
                                
                                try:
                                    # Получаем ссылку на пост
                                    post_url = None
                                    link_elem = post.find('a', href=re.compile(r'/\d+\.html'))
                                    if link_elem:
                                        post_url = link_elem.get('href', '')
                                        if post_url and not post_url.startswith('http'):
                                            post_url = f'https://{username}.livejournal.com{post_url}'
                                    
                                    # Извлекаем текст поста
                                    entry_text = post.find('div', class_=re.compile(r'entry-text|entry-content|entry-body'))
                                    if entry_text:
                                        post_text = entry_text.get_text(separator='\n')
                                    else:
                                        post_text = post.get_text(separator='\n')
                                    
                                    post_text = re.sub(r'\s+', ' ', post_text)
                                    post_text = re.sub(r'\n\s*\n+', '\n\n', post_text)
                                    post_text = post_text.strip()
                                    
                                    # Если текст короткий, загружаем полный пост
                                    if len(post_text) < 100 and post_url:
                                        try:
                                            post_response = self.session.get(post_url, timeout=15)
                                            if post_response.status_code == 200:
                                                post_soup = BeautifulSoup(
                                                    post_response.content, 'html.parser'
                                                )
                                                content_div = post_soup.find(
                                                    'div', class_=re.compile(r'entry-text|entry-content|entry-body|b-singlepost-body')
                                                )
                                                if not content_div:
                                                    content_div = post_soup.find(
                                                        'div', class_=re.compile(r'entry|post|content')
                                                    )
                                                if content_div:
                                                    post_text = content_div.get_text(separator='\n')
                                                    post_text = re.sub(r'\s+', ' ', post_text)
                                                    post_text = re.sub(r'\n\s*\n+', '\n\n', post_text)
                                                    post_text = post_text.strip()
                                        except Exception as e:
                                            logger.debug(f"Could not load full post {post_url}: {e}")
                                    
                                    # Извлекаем цитаты
                                    extracted_quotes = self._extract_quotes_from_text(post_text)
                                    
                                    for quote_text in extracted_quotes:
                                        if len(quotations) >= max_quotes:
                                            break
                                        
                                        # Проверяем язык
                                        if LANGDETECT_AVAILABLE:
                                            try:
                                                detected = detect(quote_text)
                                                if detected != 'ru':
                                                    continue
                                            except:
                                                if not re.search(r'[а-яёА-ЯЁ]', quote_text):
                                                    continue
                                        else:
                                            if not re.search(r'[а-яёА-ЯЁ]', quote_text):
                                                continue
                                        
                                        # Применяем фильтры
                                        if self._is_valid_quotation(quote_text):
                                            quotations.append({
                                                'text': quote_text,
                                                'language': 'ru',
                                                'author': None,
                                                'source_url': post_url or next_url
                                            })
                                    
                                    time.sleep(0.5)  # Задержка между постами
                                
                                except Exception as e:
                                    logger.debug(f"Error processing post on page {page_num + 1}: {e}")
                                    continue
                            
                            page_num += 1
                            time.sleep(1)  # Задержка между страницами
                            
                        except Exception as e:
                            logger.debug(f"Error loading page {page_num + 1}: {e}")
                            break

                    time.sleep(2)  # Задержка между тегами

                except requests.exceptions.RequestException as e:
                    logger.warning(
                        f"Error loading from LiveJournal tag '{tag}': {e}"
                    )
                    continue
                except Exception as e:
                    logger.warning(
                        f"Error parsing LiveJournal tag '{tag}': {e}"
                    )
                    continue

        except Exception as e:
            logger.warning(f"Error in _load_from_livejournal: {e}")

        logger.info(
            f"Loaded {len(quotations)} quotes from "
            f"{username}.livejournal.com"
        )
        return quotations

    def _load_manual_quotations(self) -> List[Dict]:
        """
        Загрузка предопределенных цитат (fallback).

        Returns:
            Список цитат
        """
        # Загружаем из внешнего файла если доступен
        try:
            predefined = get_predefined_quotations()
            if predefined:
                logger.info(
                    f"Loaded {len(predefined)} predefined quotations "
                    f"from quotations_data.py"
                )
                return predefined
        except Exception as e:
            logger.debug(f"Could not load from quotations_data.py: {e}")

        # Fallback на встроенные цитаты
        en_quotations = [
            {
                'text': 'The only way to do great work is to love what you do.',
                'language': 'en',
                'author': 'Steve Jobs',
                'source_url': None
            },
            {
                'text': 'Innovation distinguishes between a leader and a '
                       'follower.',
                'language': 'en',
                'author': 'Steve Jobs',
                'source_url': None
            },
            {
                'text': 'Life is what happens to you while you are busy '
                       'making other plans.',
                'language': 'en',
                'author': 'John Lennon',
                'source_url': None
            },
            {
                'text': 'The future belongs to those who believe in the '
                       'beauty of their dreams.',
                'language': 'en',
                'author': 'Eleanor Roosevelt',
                'source_url': None
            },
            {
                'text': 'It is during our darkest moments that we must focus '
                       'to see the light.',
                'language': 'en',
                'author': 'Aristotle',
                'source_url': None
            },
            {
                'text': 'Be yourself; everyone else is already taken.',
                'language': 'en',
                'author': 'Oscar Wilde',
                'source_url': None
            },
            {
                'text': 'Two things are infinite: the universe and human '
                       'stupidity; and I am not sure about the universe.',
                'language': 'en',
                'author': 'Albert Einstein',
                'source_url': None
            },
            {
                'text': 'So many books, so little time.',
                'language': 'en',
                'author': 'Frank Zappa',
                'source_url': None
            },
            {
                'text': 'A room without books is like a body without a soul.',
                'language': 'en',
                'author': 'Marcus Tullius Cicero',
                'source_url': None
            },
            {
                'text': 'You only live once, but if you do it right, once is '
                       'enough.',
                'language': 'en',
                'author': 'Mae West',
                'source_url': None
            },
            {
                'text': 'Be the change that you wish to see in the world.',
                'language': 'en',
                'author': 'Mahatma Gandhi',
                'source_url': None
            },
            {
                'text': 'In three words I can sum up everything I have learned '
                       'about life: it goes on.',
                'language': 'en',
                'author': 'Robert Frost',
                'source_url': None
            },
            {
                'text': 'If you tell the truth, you do not have to remember '
                       'anything.',
                'language': 'en',
                'author': 'Mark Twain',
                'source_url': None
            },
            {
                'text': 'A friend is someone who knows all about you and still '
                       'loves you.',
                'language': 'en',
                'author': 'Elbert Hubbard',
                'source_url': None
            },
            {
                'text': 'To live is the rarest thing in the world. Most people '
                       'just exist.',
                'language': 'en',
                'author': 'Oscar Wilde',
                'source_url': None
            },
            {
                'text': 'Without music, life would be a mistake.',
                'language': 'en',
                'author': 'Friedrich Nietzsche',
                'source_url': None
            },
            {
                'text': 'It is better to be hated for what you are than to be '
                       'loved for what you are not.',
                'language': 'en',
                'author': 'Andre Gide',
                'source_url': None
            },
            {
                'text': 'We accept the love we think we deserve.',
                'language': 'en',
                'author': 'Stephen Chbosky',
                'source_url': None
            },
            {
                'text': 'The person, be it gentleman or lady, who has not '
                       'pleasure in a good novel, must be intolerably stupid.',
                'language': 'en',
                'author': 'Jane Austen',
                'source_url': None
            },
            {
                'text': 'Imperfection is beauty, madness is genius and it is '
                       'better to be absolutely ridiculous than absolutely '
                       'boring.',
                'language': 'en',
                'author': 'Marilyn Monroe',
                'source_url': None
            },
        ]

        ru_quotations = [
            {
                'text': 'Век живи — век учись.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Ученье — свет, а неученье — тьма.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Терпение и труд всё перетрут.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Без труда не вытащишь и рыбку из пруда.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Что посеешь, то и пожнёшь.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Семь раз отмерь — один раз отрежь.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Тише едешь — дальше будешь.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Не откладывай на завтра то, что можно сделать сегодня.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Кто не работает, тот не ест.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'В гостях хорошо, а дома лучше.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Лучше синица в руках, чем журавль в небе.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Не всё то золото, что блестит.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Дареному коню в зубы не смотрят.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Любопытство — не порок, а большое безобразие.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Слово — серебро, молчание — золото.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'В здоровом теле — здоровый дух.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Ученье — свет, а неученье — тьма.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Повторение — мать учения.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Знание — сила.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
            {
                'text': 'Учиться никогда не поздно.',
                'language': 'ru',
                'author': None,
                'source_url': None
            },
        ]

        # Дополнительные английские цитаты для расширения базы
        additional_en = [
            {'text': 'Success is not final, failure is not fatal.',
             'language': 'en', 'author': 'Winston Churchill',
             'source_url': None},
            {'text': 'The only impossible journey is the one you never begin.',
             'language': 'en', 'author': 'Tony Robbins',
             'source_url': None},
            {'text': 'In the middle of difficulty lies opportunity.',
             'language': 'en', 'author': 'Albert Einstein',
             'source_url': None},
            {'text': 'The way to get started is to quit talking and begin doing.',
             'language': 'en', 'author': 'Walt Disney',
             'source_url': None},
            {'text': 'Don\'t let yesterday take up too much of today.',
             'language': 'en', 'author': 'Will Rogers',
             'source_url': None},
            {'text': 'You learn more from failure than from success.',
             'language': 'en', 'author': None,
             'source_url': None},
            {'text': 'If you are working on something exciting, you don\'t have to be pushed.',
             'language': 'en', 'author': 'Steve Jobs',
             'source_url': None},
            {'text': 'People who are crazy enough to think they can change the world, are the ones who do.',
             'language': 'en', 'author': 'Rob Siltanen',
             'source_url': None},
            {'text': 'Failure will never overtake me if my determination to succeed is strong enough.',
             'language': 'en', 'author': 'Og Mandino',
             'source_url': None},
            {'text': 'Entrepreneurs are great at dealing with uncertainty and also very good at minimizing risk.',
             'language': 'en', 'author': None,
             'source_url': None},
            {'text': 'The successful warrior is the average man with laser-like focus.',
             'language': 'en', 'author': 'Bruce Lee',
             'source_url': None},
            {'text': 'Take up one idea. Make that one idea your life.',
             'language': 'en', 'author': 'Swami Vivekananda',
             'source_url': None},
            {'text': 'I find that the harder I work, the more luck I seem to have.',
             'language': 'en', 'author': 'Thomas Jefferson',
             'source_url': None},
            {'text': 'The way to get started is to quit talking and begin doing.',
             'language': 'en', 'author': 'Walt Disney',
             'source_url': None},
            {'text': 'Don\'t be afraid to give up the good to go for the great.',
             'language': 'en', 'author': 'John D. Rockefeller',
             'source_url': None},
        ]

        # Дополнительные русские цитаты
        additional_ru = [
            {'text': 'Дорогу осилит идущий.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Тише едешь — дальше будешь.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Поспешишь — людей насмешишь.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'У страха глаза велики.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Лучше поздно, чем никогда.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Вода камень точит.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Терпение и труд всё перетрут.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Где тонко, там и рвётся.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Не всё то золото, что блестит.',
             'language': 'ru', 'author': None,
             'source_url': None},
            {'text': 'Семь бед — один ответ.',
             'language': 'ru', 'author': None,
             'source_url': None},
        ]

        return en_quotations + ru_quotations + additional_en + additional_ru

    def _generate_more_quotations(self) -> List[Dict]:
        """
        Генерация дополнительных цитат для достижения целевого количества.

        Returns:
            Список дополнительных цитат
        """
        # Большой набор английских цитат
        large_en_set = [
            {'text': 'The journey of a thousand miles begins with one step.',
             'language': 'en', 'author': 'Lao Tzu', 'source_url': None},
            {'text': 'That which does not kill us makes us stronger.',
             'language': 'en', 'author': 'Friedrich Nietzsche',
             'source_url': None},
            {'text': 'Life is what happens to you while you are busy making other plans.',
             'language': 'en', 'author': 'John Lennon', 'source_url': None},
            {'text': 'When the going gets tough, the tough get going.',
             'language': 'en', 'author': None, 'source_url': None},
            {'text': 'You must be the change you wish to see in the world.',
             'language': 'en', 'author': 'Mahatma Gandhi', 'source_url': None},
            {'text': 'Twenty years from now you will be more disappointed by the things that you did not do than by the ones you did do.',
             'language': 'en', 'author': 'Mark Twain', 'source_url': None},
            {'text': 'The difference between ordinary and extraordinary is that little extra.',
             'language': 'en', 'author': 'Jimmy Johnson', 'source_url': None},
            {'text': 'Great minds discuss ideas; average minds discuss events; small minds discuss people.',
             'language': 'en', 'author': 'Eleanor Roosevelt', 'source_url': None},
            {'text': 'A person who never made a mistake never tried anything new.',
             'language': 'en', 'author': 'Albert Einstein', 'source_url': None},
            {'text': 'Education is the most powerful weapon which you can use to change the world.',
             'language': 'en', 'author': 'Nelson Mandela', 'source_url': None},
            {'text': 'The only person you are destined to become is the person you decide to be.',
             'language': 'en', 'author': 'Ralph Waldo Emerson', 'source_url': None},
            {'text': 'Go confidently in the direction of your dreams. Live the life you have imagined.',
             'language': 'en', 'author': 'Henry David Thoreau', 'source_url': None},
            {'text': 'The two most important days in your life are the day you are born and the day you find out why.',
             'language': 'en', 'author': 'Mark Twain', 'source_url': None},
            {'text': 'Whether you think you can or you think you cannot, you are right.',
             'language': 'en', 'author': 'Henry Ford', 'source_url': None},
            {'text': 'People often say that motivation does not last. Well, neither does bathing. That is why we recommend it daily.',
             'language': 'en', 'author': 'Zig Ziglar', 'source_url': None},
            {'text': 'There are only two ways to live your life. One is as though nothing is a miracle. The other is as though everything is a miracle.',
             'language': 'en', 'author': 'Albert Einstein', 'source_url': None},
            {'text': 'The person who says it cannot be done should not interrupt the person who is doing it.',
             'language': 'en', 'author': None, 'source_url': None},
            {'text': 'Learn from yesterday, live for today, hope for tomorrow.',
             'language': 'en', 'author': 'Albert Einstein', 'source_url': None},
            {'text': 'You have to learn the rules of the game. And then you have to play better than anyone else.',
             'language': 'en', 'author': 'Albert Einstein', 'source_url': None},
            {'text': 'The future belongs to those who believe in the beauty of their dreams.',
             'language': 'en', 'author': 'Eleanor Roosevelt', 'source_url': None},
        ]

        # Большой набор русских цитат и пословиц
        large_ru_set = [
            {'text': 'Умный в гору не пойдёт, умный гору обойдёт.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Кто ищет, тот всегда найдёт.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Всякое дело мастера боится.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Где родился, там и пригодился.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Дело мастера боится.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Капля камень точит.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Москва не сразу строилась.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Не откладывай на завтра то, что можно сделать сегодня.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Один в поле не воин.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Повторение — мать учения.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Семь раз отмерь — один раз отрежь.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Тише едешь — дальше будешь.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Ученье — свет, а неученье — тьма.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Хорошее начало — половина дела.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Цыплят по осени считают.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Что написано пером, не вырубишь топором.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Яблоко от яблони недалеко падает.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Без труда не вытащишь и рыбку из пруда.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'Век живи — век учись.',
             'language': 'ru', 'author': None, 'source_url': None},
            {'text': 'В гостях хорошо, а дома лучше.',
             'language': 'ru', 'author': None, 'source_url': None},
        ]

        return large_en_set + large_ru_set

    def _expand_quotations(self, base_quotations: List[Dict],
                          target_count: int) -> List[Dict]:
        """
        Расширение списка цитат до целевого количества.

        Args:
            base_quotations: Базовый список цитат
            target_count: Целевое количество

        Returns:
            Расширенный список цитат
        """
        if len(base_quotations) >= target_count:
            return base_quotations[:target_count]

        # Добавляем дополнительные цитаты
        expanded = base_quotations.copy()
        additional = self._generate_more_quotations()
        expanded.extend(additional)

        # Если всё ещё недостаточно, добавляем вариации
        # Вместо точных дубликатов, создаем уникальные комбинации
        if len(expanded) < target_count:
            base_list = base_quotations + additional
            base_count = len(base_list)
            
            if base_count > 0:
                needed = target_count - len(expanded)
                # Используем разные комбинации для создания уникальности
                # Добавляем суффикс к тексту для создания вариантов
                for i in range(needed):
                    quote = base_list[i % base_count].copy()
                    # Создаем уникальный вариант, добавляя невидимый маркер
                    # Это позволит сохранить больше цитат
                    quote['_variant_id'] = i // base_count
                    expanded.append(quote)

        return expanded[:target_count]

    def load_quotations_livejournal_only(self, username: str = 'civil-engineer',
                                         target_count: int = 10000,
                                         tag_patterns: Optional[List[str]] = None) -> List[Dict]:
        """
        Загрузка цитат ТОЛЬКО с LiveJournal (без других источников).

        Args:
            username: Имя пользователя LiveJournal
            target_count: Целевое количество цитат
            tag_patterns: Список паттернов тегов (регулярные выражения).
                         Если None, использует ['aforizm.*', 'aphorizm.*']

        Returns:
            Список цитат
        """
        all_quotations = []
        logger.info(
            f"Loading quotations ONLY from LiveJournal "
            f"({username}.livejournal.com)..."
        )
        logger.info(f"Target count: {target_count}")

        # Загружаем только с LiveJournal
        try:
            lj_quotes = self._load_from_livejournal(
                username=username,
                max_quotes=target_count,
                tag_patterns=tag_patterns
            )
            all_quotations.extend(lj_quotes)
            logger.info(
                f"Loaded {len(lj_quotes)} quotations from "
                f"{username}.livejournal.com"
            )
        except Exception as e:
            logger.error(f"Error loading from LiveJournal: {e}")

        # Фильтрация
        logger.info("Filtering quotations...")
        filtered = []
        seen_texts = set()
        invalid_count = 0
        
        for quote in all_quotations:
            text_key = (quote['text'].lower(), quote['language'])
            if text_key in seen_texts:
                continue
            seen_texts.add(text_key)

            if (self._is_valid_quotation(quote['text']) and
                    self._is_valid_source(quote.get('source_url', ''))):
                filtered.append(quote)
            else:
                invalid_count += 1

        logger.info(
            f"Filtered to {len(filtered)} unique valid quotations "
            f"({invalid_count} invalid removed)"
        )

        if len(filtered) < target_count:
            logger.warning(
                f"Only {len(filtered)} unique quotations found, "
                f"target was {target_count}."
            )

        return filtered

    def load_quotations(self, target_count: int = 10000) -> List[Dict]:
        """
        Загрузка цитат из различных источников.

        Args:
            target_count: Целевое количество цитат

        Returns:
            Список цитат
        """
        all_quotations = []
        logger.info(f"Starting to load {target_count} quotations...")

        # Загружаем цитаты из docx файлов
        logger.info("Loading quotations from docx files...")
        doc_quotes = self._load_from_doc_files()
        all_quotations.extend(doc_quotes)
        logger.info(f"Loaded {len(doc_quotes)} quotations from docx files")

        # Начинаем с предопределенных цитат
        logger.info("Loading predefined quotations...")
        manual = self._load_manual_quotations()
        all_quotations.extend(manual)
        logger.info(f"Loaded {len(manual)} predefined quotations")

        # Определяем целевое количество для каждого языка
        target_en = target_count // 2
        target_ru = target_count // 2

        # Загрузка английских цитат с веб-сайтов
        logger.info(f"Loading English quotations from web (target: {target_en})...")
        en_loaded = sum(1 for q in all_quotations if q['language'] == 'en')
        en_needed = max(0, target_en - en_loaded)
        
        if en_needed > 0:
            # Пробуем публичные API сначала (быстрее и надежнее)
            try:
                quotable_quotes = self._load_from_quotable_api(
                    max_quotes=min(2000, en_needed)
                )
                all_quotations.extend(quotable_quotes)
                logger.info(
                    f"Loaded {len(quotable_quotes)} English quotations "
                    f"from Quotable API"
                )
            except Exception as e:
                logger.warning(f"Error loading from Quotable API: {e}")

            # Пробуем ZenQuotes API
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    zen_quotes = self._load_from_zenquotes_api(
                        max_quotes=min(500, en_still_needed)
                    )
                    all_quotations.extend(zen_quotes)
                    logger.info(
                        f"Loaded {len(zen_quotes)} English quotations "
                        f"from ZenQuotes API"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from ZenQuotes API: {e}")

            # Пробуем веб-скрапинг как резерв
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    en_quotes = self._load_from_goodreads(
                        max_quotes=min(500, en_still_needed)
                    )
                    all_quotations.extend(en_quotes)
                    logger.info(
                        f"Loaded {len(en_quotes)} English quotations "
                        f"from Goodreads"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Goodreads: {e}")

            # Пробуем BrainyQuote
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    brainy_quotes = self._load_from_brainyquote(
                        max_quotes=min(500, en_still_needed)
                    )
                    all_quotations.extend(brainy_quotes)
                    logger.info(
                        f"Loaded {len(brainy_quotes)} English quotations "
                        f"from BrainyQuote"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from BrainyQuote: {e}")

            # Пробуем Wikiquote (английский)
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    wikiquote_quotes = self._load_from_wikiquote_en(
                        max_quotes=min(1000, en_still_needed)
                    )
                    all_quotations.extend(wikiquote_quotes)
                    logger.info(
                        f"Loaded {len(wikiquote_quotes)} English quotations "
                        f"from Wikiquote"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Wikiquote EN: {e}")

            # Пробуем Quote Garden API
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    quote_garden_quotes = self._load_from_quote_garden(
                        max_quotes=min(500, en_still_needed)
                    )
                    all_quotations.extend(quote_garden_quotes)
                    logger.info(
                        f"Loaded {len(quote_garden_quotes)} English quotations "
                        f"from Quote Garden"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Quote Garden: {e}")

            # Пробуем Quotes on Design
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    design_quotes = self._load_from_quotesondesign(
                        max_quotes=min(300, en_still_needed)
                    )
                    all_quotations.extend(design_quotes)
                    logger.info(
                        f"Loaded {len(design_quotes)} English quotations "
                        f"from Quotes on Design"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Quotes on Design: {e}")

            # Пробуем Forismatic API
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    forismatic_quotes = self._load_from_forismatic_api(
                        max_quotes=min(500, en_still_needed)
                    )
                    all_quotations.extend(forismatic_quotes)
                    logger.info(
                        f"Loaded {len(forismatic_quotes)} English quotations "
                        f"from Forismatic API"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Forismatic API: {e}")

            # Пробуем They Said So API
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    theysaidso_quotes = self._load_from_theysaidso_api(
                        max_quotes=min(300, en_still_needed)
                    )
                    all_quotations.extend(theysaidso_quotes)
                    logger.info(
                        f"Loaded {len(theysaidso_quotes)} English quotations "
                        f"from They Said So API"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from They Said So API: {e}")

            # Пробуем Programming Quotes API
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    prog_quotes = self._load_from_programming_quotes_api(
                        max_quotes=min(200, en_still_needed)
                    )
                    all_quotations.extend(prog_quotes)
                    logger.info(
                        f"Loaded {len(prog_quotes)} English quotations "
                        f"from Programming Quotes API"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Programming Quotes API: {e}")

            # Пробуем RSS фиды (английские)
            en_loaded_now = sum(1 for q in all_quotations if q['language'] == 'en')
            en_still_needed = max(0, target_en - en_loaded_now)
            if en_still_needed > 0:
                try:
                    rss_quotes = self._load_from_en_rss_feeds(
                        max_quotes=min(500, en_still_needed)
                    )
                    all_quotations.extend(rss_quotes)
                    logger.info(
                        f"Loaded {len(rss_quotes)} English quotations "
                        f"from RSS feeds"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from English RSS feeds: {e}")

        # Загрузка русских цитат с веб-сайтов
        logger.info(f"Loading Russian quotations from web (target: {target_ru})...")
        ru_loaded = sum(1 for q in all_quotations if q['language'] == 'ru')
        ru_needed = max(0, target_ru - ru_loaded)
        
        if ru_needed > 0:
            # Пробуем несколько источников
            # 1. Citaty.net
            try:
                ru_quotes = self._load_from_citaty_net(
                    max_quotes=min(500, ru_needed)
                )
                all_quotations.extend(ru_quotes)
                logger.info(
                    f"Loaded {len(ru_quotes)} Russian quotations from citaty.net"
                )
            except Exception as e:
                logger.warning(f"Error loading from citaty.net: {e}")

            # 2. Aphorizm.ru
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                try:
                    aphorizm_quotes = self._load_from_aphorizm_ru(
                        max_quotes=min(500, ru_still_needed)
                    )
                    all_quotations.extend(aphorizm_quotes)
                    logger.info(
                        f"Loaded {len(aphorizm_quotes)} Russian quotations "
                        f"from aphorizm.ru"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from aphorizm.ru: {e}")

            # 3. Anecdot.ru/aphorizm
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                try:
                    anecdot_quotes = self._load_from_anecdot_ru_aphorizm(
                        max_quotes=min(500, ru_still_needed)
                    )
                    all_quotations.extend(anecdot_quotes)
                    logger.info(
                        f"Loaded {len(anecdot_quotes)} Russian quotations "
                        f"from anecdot.ru/aphorizm"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from anecdot.ru: {e}")

            # 4. Wikiquote (русский)
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                try:
                    wikiquote_ru_quotes = self._load_from_wikiquote_ru(
                        max_quotes=min(500, ru_still_needed)
                    )
                    all_quotations.extend(wikiquote_ru_quotes)
                    logger.info(
                        f"Loaded {len(wikiquote_ru_quotes)} Russian quotations "
                        f"from Wikiquote"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Wikiquote RU: {e}")

            # 5. LiveJournal (civil-engineer)
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                try:
                    lj_quotes = self._load_from_livejournal(
                        username='civil-engineer',
                        max_quotes=min(1000, ru_still_needed)
                    )
                    all_quotations.extend(lj_quotes)
                    logger.info(
                        f"Loaded {len(lj_quotes)} Russian quotations "
                        f"from civil-engineer.livejournal.com"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from LiveJournal: {e}")

            # 6. Citaty.info (веб-скрапинг)
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                try:
                    citaty_info_quotes = self._load_from_citaty_info(
                        max_quotes=min(500, ru_still_needed)
                    )
                    all_quotations.extend(citaty_info_quotes)
                    logger.info(
                        f"Loaded {len(citaty_info_quotes)} Russian quotations "
                        f"from citaty.info"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from citaty.info: {e}")

            # 7. RSS фиды (русские)
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                try:
                    rss_ru_quotes = self._load_from_ru_rss_feeds(
                        max_quotes=min(500, ru_still_needed)
                    )
                    all_quotations.extend(rss_ru_quotes)
                    logger.info(
                        f"Loaded {len(rss_ru_quotes)} Russian quotations "
                        f"from RSS feeds"
                    )
                except Exception as e:
                    logger.warning(f"Error loading from Russian RSS feeds: {e}")

            # Пробуем загрузить с нескольких страниц каждого источника
            ru_loaded_now = sum(1 for q in all_quotations
                               if q['language'] == 'ru')
            ru_still_needed = max(0, target_ru - ru_loaded_now)
            if ru_still_needed > 0:
                # Страницы с citaty.net
                for page in range(2, min(6, (ru_still_needed // 50) + 2)):
                    try:
                        page_quotes = self._load_from_citaty_net_page(
                            page=page, max_quotes=50
                        )
                        all_quotations.extend(page_quotes)
                        logger.info(
                            f"Loaded {len(page_quotes)} Russian quotations "
                            f"from citaty.net page {page}"
                        )
                        if len(page_quotes) == 0:
                            break
                    except Exception as e:
                        logger.debug(f"Error loading citaty.net page {page}: {e}")
                        break

                # Страницы с aphorizm.ru
                ru_loaded_now = sum(1 for q in all_quotations
                                   if q['language'] == 'ru')
                ru_still_needed = max(0, target_ru - ru_loaded_now)
                if ru_still_needed > 0:
                    for page in range(2, min(6, (ru_still_needed // 50) + 2)):
                        try:
                            page_quotes = self._load_from_aphorizm_ru_page(
                                page=page, max_quotes=50
                            )
                            all_quotations.extend(page_quotes)
                            logger.info(
                                f"Loaded {len(page_quotes)} Russian quotations "
                                f"from aphorizm.ru page {page}"
                            )
                            if len(page_quotes) == 0:
                                break
                        except Exception as e:
                            logger.debug(
                                f"Error loading aphorizm.ru page {page}: {e}"
                            )
                            break

                # Страницы с anecdot.ru/aphorizm
                ru_loaded_now = sum(1 for q in all_quotations
                                   if q['language'] == 'ru')
                ru_still_needed = max(0, target_ru - ru_loaded_now)
                if ru_still_needed > 0:
                    for page in range(2, min(6, (ru_still_needed // 50) + 2)):
                        try:
                            page_quotes = (
                                self._load_from_anecdot_ru_aphorizm_page(
                                    page=page, max_quotes=50
                                )
                            )
                            all_quotations.extend(page_quotes)
                            logger.info(
                                f"Loaded {len(page_quotes)} Russian quotations "
                                f"from anecdot.ru/aphorizm page {page}"
                            )
                            if len(page_quotes) == 0:
                                break
                        except Exception as e:
                            logger.debug(
                                f"Error loading anecdot.ru page {page}: {e}"
                            )
                            break

        # Фильтрация
        logger.info("Filtering quotations...")
        filtered = []
        seen_texts = set()
        invalid_count = 0
        
        for quote in all_quotations:
            text_key = (quote['text'].lower(), quote['language'])
            if text_key in seen_texts:
                continue
            seen_texts.add(text_key)

            if (self._is_valid_quotation(quote['text']) and
                    self._is_valid_source(quote.get('source_url', ''))):
                filtered.append(quote)
            else:
                invalid_count += 1

        logger.info(
            f"Filtered to {len(filtered)} unique valid quotations "
            f"({invalid_count} invalid removed)"
        )

        # Если недостаточно уникальных цитат, предупреждаем
        if len(filtered) < target_count:
            logger.warning(
                f"Only {len(filtered)} unique quotations found, "
                f"target was {target_count}. "
                f"To get more, improve web scraping or add more sources."
            )
            # НЕ расширяем дубликатами - возвращаем только уникальные
            logger.info(
                f"Returning {len(filtered)} unique quotations "
                f"(not expanding with duplicates)"
            )

        return filtered[:target_count]

    def save_quotations(self, quotations: List[Dict]) -> int:
        """
        Сохранение цитат в БД с переводами.

        Args:
            quotations: Список цитат

        Returns:
            Количество сохраненных цитат
        """
        # Убеждаемся, что таблица существует
        self._init_quotations_table()

        conn = self._get_connection()
        cur = conn.cursor()
        saved_count = 0

        logger.info(f"Saving {len(quotations)} quotations to database...")
        
        skipped_count = 0
        error_count = 0

        for idx, quote in enumerate(quotations, 1):
            try:
                text = quote['text']
                lang = quote['language']
                author = quote.get('author')
                source_url = quote.get('source_url')

                # Перевод и категоризация (делаем до проверки дубликатов)
                target_lang = 'ru' if lang == 'en' else 'en'
                
                if idx % 50 == 0:
                    logger.info(f"Processing {idx}/{len(quotations)}: {text[:50]}...")
                
                translated = self._translate_text(text, lang, target_lang)
                tags = self._categorize_quotation(text, lang)

                # Проверка на дубликаты
                cur.execute("""
                    SELECT id FROM quotations
                    WHERE text_original = %s AND language_original = %s
                """, (text, lang))

                existing = cur.fetchone()
                if existing:
                    # Обновляем существующую запись (перевод и теги)
                    cur.execute("""
                        UPDATE quotations
                        SET text_translated = COALESCE(%s, text_translated),
                            language_translated = COALESCE(%s, language_translated),
                            tags = COALESCE(%s, tags),
                            author = COALESCE(%s, author)
                        WHERE id = %s
                    """, (translated, target_lang, tags, author, existing[0]))
                    skipped_count += 1
                    if skipped_count % 100 == 0:
                        logger.info(f"Updated {skipped_count} existing quotations...")
                    continue

                # Сохранение
                cur.execute("""
                    INSERT INTO quotations
                    (text_original, language_original, text_translated,
                     language_translated, author, source_url, tags)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (text, lang, translated, target_lang, author, source_url,
                      tags))

                saved_count += 1

                if saved_count % 100 == 0:
                    conn.commit()
                    logger.info(f"Saved {saved_count} quotations...")

            except Exception as e:
                error_count += 1
                logger.error(f"Error saving quotation {idx}: {e}")
                conn.rollback()
                continue

        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            f"Save complete: {saved_count} saved, "
            f"{skipped_count} updated (existing), "
            f"{error_count} errors"
        )
        return saved_count

    def get_statistics(self) -> Dict[str, Any]:
        """
        Получение статистики по цитатам в БД.

        Returns:
            Словарь со статистикой
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        stats = {}

        # Общее количество
        cur.execute("SELECT COUNT(*) as total FROM quotations")
        stats['total'] = cur.fetchone()['total']

        # По языкам
        cur.execute("""
            SELECT language_original, COUNT(*) as count
            FROM quotations
            GROUP BY language_original
            ORDER BY count DESC
        """)
        stats['by_language'] = {row['language_original']: row['count']
                                for row in cur.fetchall()}

        # С авторами и без
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE author IS NOT NULL) as with_author,
                COUNT(*) FILTER (WHERE author IS NULL) as without_author
            FROM quotations
        """)
        author_stats = cur.fetchone()
        stats['authors'] = {
            'with_author': author_stats['with_author'],
            'without_author': author_stats['without_author']
        }

        # С переводами и без
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE text_translated IS NOT NULL) 
                    as with_translation,
                COUNT(*) FILTER (WHERE text_translated IS NULL) 
                    as without_translation
            FROM quotations
        """)
        trans_stats = cur.fetchone()
        stats['translations'] = {
            'with_translation': trans_stats['with_translation'],
            'without_translation': trans_stats['without_translation']
        }

        # По тегам (топ 10)
        cur.execute("""
            SELECT unnest(tags) as tag, COUNT(*) as count
            FROM quotations
            WHERE tags IS NOT NULL AND array_length(tags, 1) > 0
            GROUP BY tag
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_tags'] = {row['tag']: row['count']
                            for row in cur.fetchall()}

        # Топ авторов (топ 10)
        cur.execute("""
            SELECT author, COUNT(*) as count
            FROM quotations
            WHERE author IS NOT NULL
            GROUP BY author
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_authors'] = {row['author']: row['count']
                               for row in cur.fetchall()}

        # По дате создания (последние 7 дней)
        cur.execute("""
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM quotations
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        """)
        stats['recent_additions'] = {str(row['date']): row['count']
                                     for row in cur.fetchall()}

        # Валидированные и невалидированные
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_validated = TRUE) as validated,
                COUNT(*) FILTER (WHERE is_validated = FALSE) as not_validated
            FROM quotations
        """)
        valid_stats = cur.fetchone()
        stats['validation'] = {
            'validated': valid_stats['validated'],
            'not_validated': valid_stats['not_validated']
        }

        cur.close()
        conn.close()

        return stats

    def print_statistics_report(self):
        """
        Вывод отчета по статистике цитат.
        """
        logger.info("=" * 60)
        logger.info("QUOTATIONS STATISTICS REPORT")
        logger.info("=" * 60)

        try:
            stats = self.get_statistics()

            # Общая статистика
            logger.info(f"\n📊 Total Quotations: {stats['total']}")

            # По языкам
            logger.info("\n🌍 By Language:")
            for lang, count in stats['by_language'].items():
                percentage = (count / stats['total'] * 100) if stats['total'] > 0 else 0
                logger.info(f"  {lang.upper()}: {count} ({percentage:.1f}%)")

            # Авторы
            logger.info("\n✍️  Authors:")
            logger.info(f"  With author: {stats['authors']['with_author']}")
            logger.info(f"  Without author: {stats['authors']['without_author']}")

            # Переводы
            logger.info("\n🔄 Translations:")
            logger.info(f"  With translation: {stats['translations']['with_translation']}")
            logger.info(
                f"  Without translation: "
                f"{stats['translations']['without_translation']}"
            )

            # Топ теги
            if stats['top_tags']:
                logger.info("\n🏷️  Top Tags:")
                for tag, count in list(stats['top_tags'].items())[:10]:
                    logger.info(f"  {tag}: {count}")

            # Топ авторы
            if stats['top_authors']:
                logger.info("\n👤 Top Authors:")
                for author, count in list(stats['top_authors'].items())[:10]:
                    logger.info(f"  {author}: {count}")

            # Недавние добавления
            if stats['recent_additions']:
                logger.info("\n📅 Recent Additions (last 7 days):")
                for date, count in stats['recent_additions'].items():
                    logger.info(f"  {date}: {count}")

            # Валидация
            logger.info("\n✅ Validation:")
            logger.info(f"  Validated: {stats['validation']['validated']}")
            logger.info(
                f"  Not validated: {stats['validation']['not_validated']}"
            )

            logger.info("\n" + "=" * 60)

        except Exception as e:
            logger.error(f"Error generating statistics: {e}")


def main():
    """Главная функция."""
    import sys
    
    db_url = os.getenv('DB_URL')
    if not db_url:
        logger.error("DB_URL not set in environment")
        return

    loader = QuotationLoader(db_url)

    # Опция для отчета по статистике
    if '--stats' in sys.argv or '--statistics' in sys.argv or '-s' in sys.argv:
        loader.print_statistics_report()
        return

    # Опция для очистки существующих цитат
    clear_existing = '--clear' in sys.argv or '-c' in sys.argv
    
    if clear_existing:
        logger.warning("Clearing existing quotations...")
        conn = loader._get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM quotations")
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Existing quotations cleared.")

    # Опция для загрузки ТОЛЬКО с LiveJournal
    lj_only = ('--livejournal-only' in sys.argv or 
               '--lj-only' in sys.argv or 
               '--livejournal' in sys.argv)
    
    if lj_only:
        logger.info("=" * 60)
        logger.info("Loading quotations ONLY from LiveJournal...")
        logger.info("=" * 60)
        
        # Парсим теги из аргументов, если указаны
        tag_patterns = None
        username = 'civil-engineer'
        
        # Ищем аргументы --tags или --username
        for i, arg in enumerate(sys.argv):
            if arg == '--tags' and i + 1 < len(sys.argv):
                tag_patterns = sys.argv[i + 1].split(',')
            elif arg == '--username' and i + 1 < len(sys.argv):
                username = sys.argv[i + 1]
        
        # Если теги не указаны, используем по умолчанию
        if tag_patterns is None:
            tag_patterns = ['aforizm.*', 'aphorizm.*']
        
        logger.info(f"Username: {username}")
        logger.info(f"Tag patterns: {tag_patterns}")
        
        quotations = loader.load_quotations_livejournal_only(
            username=username,
            target_count=10000,
            tag_patterns=tag_patterns
        )
    else:
        logger.info("=" * 60)
        logger.info("Starting quotation loading process...")
        logger.info("=" * 60)
        
        quotations = loader.load_quotations(target_count=10000)
    
    logger.info(f"Loaded {len(quotations)} quotations for saving")
    
    # Проверяем распределение по языкам
    en_count = sum(1 for q in quotations if q['language'] == 'en')
    ru_count = sum(1 for q in quotations if q['language'] == 'ru')
    logger.info(f"English: {en_count}, Russian: {ru_count}")
    
    saved = loader.save_quotations(quotations)

    logger.info("=" * 60)
    logger.info(f"Process completed. Saved {saved} quotations.")
    logger.info("=" * 60)
    
    # Показываем статистику после загрузки
    loader.print_statistics_report()
    
    if saved == 0:
        logger.warning(
            "No new quotations saved. This might be because all quotations "
            "already exist in the database.\n"
            "Run with --clear flag to clear existing quotations: "
            "python load_quotations.py --clear"
        )


if __name__ == '__main__':
    main()
