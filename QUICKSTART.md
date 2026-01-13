# Быстрый старт

## Шаг 1: Получение токена бота

1. Найдите [@BotFather](https://t.me/BotFather) в Telegram
2. Отправьте команду `/newbot`
3. Следуйте инструкциям для создания бота
4. Сохраните полученный токен

## Шаг 2: Настройка окружения

Создайте файл `.env` в корне проекта:

```env
TELEGRAM_TOKEN=your_telegram_bot_token_here
DB_URL=postgres://admin:password@localhost:5432/logosphera
```

## Шаг 3: Запуск через Docker (рекомендуется)

```bash
# Запуск всех сервисов
docker-compose up -d

# Просмотр логов
docker-compose logs -f bot

# Остановка
docker-compose down
```

## Шаг 4: Запуск локально

```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск PostgreSQL через Docker
docker-compose up -d db

# Запуск бота
python bot.py
```

## Шаг 5: Тестирование

### Как найти вашего бота в Telegram:

**Способ 1: Через BotFather (если знаете имя бота)**
1. Откройте [@BotFather](https://t.me/BotFather) в Telegram
2. Отправьте команду `/mybots`
3. Выберите вашего бота из списка
4. Нажмите "Open Bot" или используйте кнопку с ссылкой

**Способ 2: Поиск по username**
1. Откройте поиск в Telegram (иконка лупы)
2. Введите username вашего бота (например: `@my_logosphera_bot`)
3. Нажмите на найденного бота

**Способ 3: Прямая ссылка**
Если вы знаете username бота, откройте ссылку:
```
https://t.me/your_bot_username
```

**Способ 4: Через BotFather - получение ссылки**
1. Откройте [@BotFather](https://t.me/BotFather)
2. Отправьте `/mybots`
3. Выберите вашего бота
4. Нажмите "Edit Bot" → "Edit Username" (если username не установлен)
5. Или используйте кнопку "Open Bot" для перехода к боту

### После того, как нашли бота:

1. Откройте чат с ботом
2. Отправьте команду `/start`
3. Начните изучение идиом!

## Решение проблем

### Ошибка подключения к БД

Убедитесь, что PostgreSQL запущен:
```bash
docker-compose ps
```

### Ошибка токена

Проверьте, что токен правильно указан в файле `.env`:
```bash
cat .env
```

### Порт занят

Измените порт в `docker-compose.yml`:
```yaml
ports:
  - "5433:5432"  # Вместо 5432
```
