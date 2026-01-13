"""
Модуль выбора темы для изучения.
"""

from typing import List, Dict, Any
from database import Database


class TopicSelectorModule:
    """Модуль выбора темы изучения."""

    TOPICS = [
        ("Communication", "💬 Общение"),
        ("Difficulty", "⚡ Сложность"),
        ("Frequency", "⏰ Частота"),
        ("Emotions", "❤️ Эмоции"),
        ("Success", "🎯 Успех"),
        ("Time", "🕐 Время"),
    ]

    def __init__(self, db: Database):
        """
        Инициализация модуля.

        Args:
            db: Экземпляр базы данных
        """
        self.db = db

    def get_topics_keyboard(self) -> list:
        """
        Получение клавиатуры с темами.

        Returns:
            Список кнопок с темами
        """
        keyboard = []
        for topic_key, topic_name in self.TOPICS:
            keyboard.append([{
                "text": topic_name,
                "callback_data": f"topic_{topic_key}"
            }])
        keyboard.append([{
            "text": "🔙 Назад",
            "callback_data": "main_menu"
        }])
        return keyboard

    def get_idioms_for_topic(self, topic: str) -> List[Dict[str, Any]]:
        """
        Получение идиом по теме.

        Args:
            topic: Название темы

        Returns:
            Список идиом
        """
        return self.db.get_idioms_by_topic(topic)
