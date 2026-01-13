"""
Модуль объяснения идиом.
"""

from typing import Dict, Any


class IdiomExplainerModule:
    """Модуль объяснения идиом с философским подходом."""

    def format_idiom_explanation(self, idiom: Dict[str, Any]) -> str:
        """
        Форматирование объяснения идиомы.

        Args:
            idiom: Словарь с данными идиомы

        Returns:
            Отформатированный текст объяснения
        """
        text = f"📖 *{idiom['expression']}*\n\n"
        text += f"*Значение:* {idiom['explanation']}\n\n"
        text += f"*Пример:*\n_{idiom['example']}_\n\n"

        if idiom.get('philosophical_meaning'):
            text += f"💭 *Философский смысл:*\n"
            text += f"{idiom['philosophical_meaning']}\n\n"

        text += "─" * 30 + "\n"
        text += "Готовы к практическому заданию?"

        return text

    def get_practice_keyboard(self, idiom_id: int) -> list:
        """
        Получение клавиатуры для практики.

        Args:
            idiom_id: ID идиомы

        Returns:
            Список кнопок
        """
        return [
            [
                {
                    "text": "✅ Готов к заданию",
                    "callback_data": f"practice_{idiom_id}"
                },
                {
                    "text": "🔄 Другая идиома",
                    "callback_data": "random_idiom"
                },
            ],
            [
                {"text": "🔙 Главное меню", "callback_data": "main_menu"}
            ],
        ]
