# Загрузчик цитат и идиом

Скрипт для загрузки цитат и идиом с веб-сайтов, их фильтрации и сохранения в PostgreSQL с переводами.

## Возможности

- Загрузка цитат с надежных веб-сайтов (английский и русский)
- Фильтрация нежелательных источников и символов
- Автоматический перевод цитат (en↔ru)
- Сохранение в PostgreSQL с авторами и источниками
- Валидация цитат (без цифр, специальных символов)

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Настройка

Убедитесь, что в файле `.env` указан `DB_URL`:

```env
DB_URL=postgres://admin:password@localhost:5432/logosphera
```

## Использование

### Базовое использование

```bash
python load_quotations.py
```

Скрипт загрузит до 10000 цитат по умолчанию.

### Программное использование

```python
from load_quotations import QuotationLoader

loader = QuotationLoader()
quotations = loader.load_quotations(target_count=10000)
saved = loader.save_quotations(quotations)
print(f"Saved {saved} quotations")
```

## Источники данных

### Английские цитаты
- Goodreads (https://www.goodreads.com/quotes)
- Предопределенный набор известных цитат

### Русские цитаты
- Citaty.net (https://ru.citaty.net)
- Предопределенный набор русских пословиц и цитат

## Фильтрация

Цитаты фильтруются по следующим критериям:
- Нет цифр в тексте
- Нет специальных символов (кроме пунктуации)
- Длина от 10 до 500 символов
- Нет повторяющихся символов (спам)
- Источник не в списке сомнительных доменов

## Перевод

Используется бесплатный сервис Google Translate через библиотеку `deep-translator`.

**Важно:** При большом количестве переводов могут быть задержки из-за rate limiting.

## Структура таблицы

```sql
CREATE TABLE quotations (
    id SERIAL PRIMARY KEY,
    text_original TEXT NOT NULL,
    language_original VARCHAR(10) NOT NULL,
    text_translated TEXT,
    language_translated VARCHAR(10),
    author VARCHAR(255),
    source_url VARCHAR(500),
    is_validated BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(text_original, language_original)
);
```

## Проверка результатов

```bash
# Подсчет цитат
docker-compose exec db psql -U admin -d logosphera -c \
  "SELECT COUNT(*) FROM quotations;"

# Просмотр примеров
docker-compose exec db psql -U admin -d logosphera -c \
  "SELECT text_original, language_original, author FROM quotations LIMIT 10;"

# Статистика по языкам
docker-compose exec db psql -U admin -d logosphera -c \
  "SELECT language_original, COUNT(*) FROM quotations GROUP BY language_original;"
```

## Ограничения

- Веб-скрапинг может быть заблокирован некоторыми сайтами
- Rate limiting при переводе (задержка 0.5 сек между запросами)
- Некоторые сайты требуют JavaScript (не поддерживается)

## Рекомендации

1. Запускайте скрипт в нерабочее время для веб-скрапинга
2. При ошибках перевода скрипт продолжит работу
3. Проверяйте результаты после загрузки
4. При необходимости запустите скрипт несколько раз для накопления данных
