# PowerShell скрипт с командами для работы с базой данных Logosphera
# Использование: скопируйте нужную команду и выполните в терминале

# Переход в директорию проекта (обязательно!)
cd E:\Python\GptEngineer\tgBot\tgBot-Logosphera

# ============================================
# ПРОСМОТР ДАННЫХ
# ============================================

# Просмотреть всех пользователей
docker-compose exec db psql -U admin -d logosphera -c "SELECT * FROM users;"

# Просмотреть идиомы
docker-compose exec db psql -U admin -d logosphera -c "SELECT id, expression, topic FROM idioms;"

# Просмотреть достижения
docker-compose exec db psql -U admin -d logosphera -c "SELECT name, icon, threshold FROM achievements ORDER BY threshold;"

# Просмотреть прогресс пользователей
docker-compose exec db psql -U admin -d logosphera -c "SELECT u.telegram_id, u.username, COUNT(up.idiom_id) as learned FROM users u LEFT JOIN user_progress up ON u.id = up.user_id WHERE up.status = 'completed' GROUP BY u.telegram_id, u.username;"

# Просмотреть все таблицы
docker-compose exec db psql -U admin -d logosphera -c "\dt"

# Просмотреть структуру таблицы
docker-compose exec db psql -U admin -d logosphera -c "\d users"
docker-compose exec db psql -U admin -d logosphera -c "\d idioms"

# ============================================
# ПОДСЧЕТЫ
# ============================================

# Количество пользователей
docker-compose exec db psql -U admin -d logosphera -c "SELECT COUNT(*) as total_users FROM users;"

# Количество идиом
docker-compose exec db psql -U admin -d logosphera -c "SELECT COUNT(*) as total_idioms FROM idioms;"

# Количество изученных идиом по пользователям
docker-compose exec db psql -U admin -d logosphera -c "SELECT u.telegram_id, COUNT(up.idiom_id) as learned FROM users u LEFT JOIN user_progress up ON u.id = up.user_id WHERE up.status = 'completed' GROUP BY u.telegram_id;"

# Количество идиом по темам
docker-compose exec db psql -U admin -d logosphera -c "SELECT topic, COUNT(*) as count FROM idioms GROUP BY topic ORDER BY count DESC;"

# ============================================
# ИНТЕРАКТИВНЫЙ РЕЖИМ
# ============================================

# Войти в интерактивный режим psql
docker-compose exec db psql -U admin -d logosphera

# В интерактивном режиме можно использовать:
# \dt - список таблиц
# \d table_name - структура таблицы
# \q - выход
# SELECT * FROM users; - SQL запросы

# ============================================
# ДОБАВЛЕНИЕ ДАННЫХ
# ============================================

# Добавить новую идиому (пример)
docker-compose exec db psql -U admin -d logosphera -c "INSERT INTO idioms (expression, explanation, example, philosophical_meaning, topic) VALUES ('Test idiom', 'Test explanation', 'Test example', 'Test meaning', 'Communication');"

# ============================================
# РЕЗЕРВНОЕ КОПИРОВАНИЕ
# ============================================

# Создать резервную копию
docker-compose exec db pg_dump -U admin logosphera > backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').sql

# Восстановить из резервной копии
# docker-compose exec -T db psql -U admin logosphera < backup.sql

# ============================================
# ОЧИСТКА
# ============================================

# Удалить всех пользователей (ОСТОРОЖНО!)
# docker-compose exec db psql -U admin -d logosphera -c "DELETE FROM users;"

# Удалить прогресс пользователя (ОСТОРОЖНО!)
# docker-compose exec db psql -U admin -d logosphera -c "DELETE FROM user_progress WHERE user_id = 1;"
