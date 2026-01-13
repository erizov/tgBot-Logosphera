"""
Модуль для работы с базой данных PostgreSQL.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, List, Any
import os
import logging

logger = logging.getLogger(__name__)


class Database:
    """Класс для работы с базой данных."""

    def __init__(self, db_url: Optional[str] = None):
        """
        Инициализация подключения к БД.

        Args:
            db_url: URL подключения к БД
        """
        self.db_url = db_url or os.getenv('DB_URL')
        self.conn = None
        self._init_db()

    def _get_connection(self):
        """Получение подключения к БД."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.db_url)
        return self.conn

    def _init_db(self):
        """Инициализация структуры БД."""
        conn = self._get_connection()
        cur = conn.cursor()

        # Таблица пользователей
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                username VARCHAR(255),
                progress INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблица достижений
        cur.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                icon VARCHAR(10) NOT NULL,
                description TEXT,
                threshold INTEGER NOT NULL
            )
        """)

        # Таблица идиом
        cur.execute("""
            CREATE TABLE IF NOT EXISTS idioms (
                id SERIAL PRIMARY KEY,
                expression VARCHAR(255) NOT NULL,
                explanation TEXT NOT NULL,
                example TEXT NOT NULL,
                philosophical_meaning TEXT,
                topic VARCHAR(100)
            )
        """)

        # Таблица прогресса пользователя
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_progress (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                idiom_id INTEGER REFERENCES idioms(id),
                status VARCHAR(50) DEFAULT 'started',
                completed_at TIMESTAMP,
                UNIQUE(user_id, idiom_id)
            )
        """)

        # Таблица достижений пользователя
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_achievements (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                achievement_id INTEGER REFERENCES achievements(id),
                unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, achievement_id)
            )
        """)

        # Таблица рефлексии
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_reflections (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                idiom_id INTEGER REFERENCES idioms(id),
                reflection_type VARCHAR(50),
                answer TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблица цитат/идиом
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
        
        # Добавление колонки tags если её нет
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
        cur.close()

        # Инициализация базовых данных
        self._init_achievements()
        self._init_idioms()

    def _init_achievements(self):
        """Инициализация базовых достижений."""
        conn = self._get_connection()
        cur = conn.cursor()

        achievements = [
            ('Мудрец дня', '🌟', 'Изучено 5 идиом', 5),
            ('Философский путь', '💫', 'Изучено 10 идиом', 10),
            ('Языковой мастер', '🔮', 'Изучено 20 идиом', 20),
            ('Хранитель мудрости', '🏰', 'Изучено 50 идиом', 50),
            ('Верховный мудрец', '🐉', 'Изучено 100 идиом', 100),
        ]

        for name, icon, desc, threshold in achievements:
            cur.execute("""
                INSERT INTO achievements (name, icon, description, threshold)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (name, icon, desc, threshold))

        conn.commit()
        cur.close()

    def _init_idioms(self):
        """Инициализация базовых идиом."""
        conn = self._get_connection()
        cur = conn.cursor()

        idioms = [
            (
                'Break the ice',
                'Начать разговор, преодолеть неловкость',
                'He told a joke to break the ice at the meeting.',
                'Как первый шаг в путешествии, начало диалога открывает '
                'новые горизонты понимания.',
                'Communication'
            ),
            (
                'Piece of cake',
                'Очень легко',
                'The exam was a piece of cake for her.',
                'Простота часто скрывает глубину мастерства.',
                'Difficulty'
            ),
            (
                'Once in a blue moon',
                'Очень редко',
                'I see him once in a blue moon.',
                'Редкие моменты обладают особой ценностью.',
                'Frequency'
            ),
        ]

        for expr, expl, ex, phil, topic in idioms:
            cur.execute("""
                INSERT INTO idioms (expression, explanation, example,
                                  philosophical_meaning, topic)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (expr, expl, ex, phil, topic))

        conn.commit()
        cur.close()

    def get_or_create_user(self, telegram_id: int,
                           username: Optional[str] = None) -> Dict[str, Any]:
        """
        Получение или создание пользователя.

        Args:
            telegram_id: ID пользователя в Telegram
            username: Имя пользователя

        Returns:
            Словарь с данными пользователя
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            INSERT INTO users (telegram_id, username)
            VALUES (%s, %s)
            ON CONFLICT (telegram_id)
            DO UPDATE SET username = EXCLUDED.username
            RETURNING *
        """, (telegram_id, username))

        user = cur.fetchone()
        conn.commit()
        cur.close()
        return dict(user) if user else {}

    def get_user_progress(self, user_id: int) -> int:
        """
        Получение прогресса пользователя.

        Args:
            user_id: ID пользователя

        Returns:
            Количество изученных идиом
        """
        conn = self._get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) FROM user_progress
            WHERE user_id = %s AND status = 'completed'
        """, (user_id,))

        count = cur.fetchone()[0]
        cur.close()
        return count

    def update_user_progress(self, user_id: int, idiom_id: int,
                             status: str = 'completed'):
        """
        Обновление прогресса пользователя.

        Args:
            user_id: ID пользователя
            idiom_id: ID идиомы
            status: Статус изучения
        """
        conn = self._get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO user_progress (user_id, idiom_id, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, idiom_id)
            DO UPDATE SET status = EXCLUDED.status,
                         completed_at = CURRENT_TIMESTAMP
        """, (user_id, idiom_id, status))

        conn.commit()
        cur.close()

        # Проверка достижений
        self._check_achievements(user_id)

    def _check_achievements(self, user_id: int):
        """Проверка и разблокировка достижений."""
        progress = self.get_user_progress(user_id)
        conn = self._get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT id FROM achievements
            WHERE threshold <= %s
            AND id NOT IN (
                SELECT achievement_id FROM user_achievements
                WHERE user_id = %s
            )
        """, (progress, user_id))

        new_achievements = cur.fetchall()
        for ach_id, in new_achievements:
            cur.execute("""
                INSERT INTO user_achievements (user_id, achievement_id)
                VALUES (%s, %s)
            """, (user_id, ach_id))

        conn.commit()
        cur.close()

    def get_user_achievements(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Получение достижений пользователя.

        Args:
            user_id: ID пользователя

        Returns:
            Список достижений
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT a.*, ua.unlocked_at
            FROM achievements a
            JOIN user_achievements ua ON a.id = ua.achievement_id
            WHERE ua.user_id = %s
            ORDER BY a.threshold
        """, (user_id,))

        achievements = cur.fetchall()
        cur.close()
        return [dict(ach) for ach in achievements]

    def get_idioms_by_topic(self, topic: Optional[str] = None
                            ) -> List[Dict[str, Any]]:
        """
        Получение идиом по теме.

        Args:
            topic: Название темы (опционально)

        Returns:
            Список идиом
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if topic:
            cur.execute("""
                SELECT * FROM idioms WHERE topic = %s
            """, (topic,))
        else:
            cur.execute("SELECT * FROM idioms")

        idioms = cur.fetchall()
        cur.close()
        return [dict(idiom) for idiom in idioms]

    def get_random_idiom(self, user_id: Optional[int] = None
                         ) -> Optional[Dict[str, Any]]:
        """
        Получение случайной идиомы.

        Args:
            user_id: ID пользователя (для исключения изученных)

        Returns:
            Словарь с данными идиомы
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if user_id:
            cur.execute("""
                SELECT * FROM idioms
                WHERE id NOT IN (
                    SELECT idiom_id FROM user_progress
                    WHERE user_id = %s AND status = 'completed'
                )
                ORDER BY RANDOM()
                LIMIT 1
            """, (user_id,))
        else:
            cur.execute("""
                SELECT * FROM idioms
                ORDER BY RANDOM()
                LIMIT 1
            """)

        idiom = cur.fetchone()
        cur.close()
        return dict(idiom) if idiom else None

    def save_reflection(self, user_id: int, idiom_id: int,
                        reflection_type: str, answer: str):
        """
        Сохранение ответа рефлексии.

        Args:
            user_id: ID пользователя
            idiom_id: ID идиомы
            reflection_type: Тип рефлексии
            answer: Ответ пользователя
        """
        conn = self._get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO user_reflections
            (user_id, idiom_id, reflection_type, answer)
            VALUES (%s, %s, %s, %s)
        """, (user_id, idiom_id, reflection_type, answer))

        conn.commit()
        cur.close()

    def get_reflection_stats(self, user_id: int) -> Dict[str, Any]:
        """
        Получение статистики рефлексии.

        Args:
            user_id: ID пользователя

        Returns:
            Словарь со статистикой
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT reflection_type, COUNT(*) as count
            FROM user_reflections
            WHERE user_id = %s
            GROUP BY reflection_type
        """, (user_id,))

        stats = cur.fetchall()
        cur.close()
        return {row['reflection_type']: row['count'] for row in stats}

    def get_quotations(self, language: Optional[str] = None,
                      limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Получение цитат из БД.

        Args:
            language: Язык цитат (опционально)
            limit: Ограничение количества (опционально)

        Returns:
            Список цитат
        """
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = "SELECT * FROM quotations WHERE is_validated = TRUE"
        params = []

        if language:
            query += " AND language_original = %s"
            params.append(language)

        query += " ORDER BY created_at DESC"

        if limit:
            query += " LIMIT %s"
            params.append(limit)

        cur.execute(query, params)
        quotations = cur.fetchall()
        cur.close()
        return [dict(q) for q in quotations]

    def get_quotation_count(self) -> int:
        """
        Получение количества цитат в БД.

        Returns:
            Количество цитат
        """
        conn = self._get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) FROM quotations WHERE is_validated = TRUE
        """)

        count = cur.fetchone()[0]
        cur.close()
        return count

    def close(self):
        """Закрытие подключения к БД."""
        if self.conn and not self.conn.closed:
            self.conn.close()
