"""
Модуль практических заданий.
"""

from typing import Dict, Any, List, Tuple
import random


class PracticeModule:
    """Модуль практических заданий."""

    def __init__(self):
        """Инициализация модуля."""
        self.exercise_types = [
            self._create_fill_blank,
            self._create_translation,
            self._create_choice,
        ]

    def generate_exercise(self, idiom: Dict[str, Any]
                          ) -> Tuple[str, str, List[List[Dict[str, str]]]]:
        """
        Генерация практического задания.

        Args:
            idiom: Словарь с данными идиомы

        Returns:
            Кортеж: (вопрос, правильный ответ, клавиатура)
        """
        exercise_func = random.choice(self.exercise_types)
        return exercise_func(idiom)

    def _create_fill_blank(self, idiom: Dict[str, Any]
                           ) -> Tuple[str, str, List[List[Dict[str, str]]]]:
        """Создание задания на заполнение пропуска."""
        expression = idiom['expression']
        words = expression.split()
        if len(words) > 1:
            blank_word = random.choice(words)
            question = expression.replace(blank_word, "______")
        else:
            question = "______"

        text = f"📝 Заполните пропуск:\n\n*{question}*\n\n"
        text += f"Пример: {idiom['example']}"

        keyboard = [
            [{"text": "Показать ответ", "callback_data": f"answer_{idiom['id']}"}],
            [{"text": "🔙 Назад", "callback_data": "main_menu"}],
        ]

        return text, expression, keyboard

    def _create_translation(self, idiom: Dict[str, Any]
                            ) -> Tuple[str, str, List[List[Dict[str, str]]]]:
        """Создание задания на перевод."""
        text = f"🌐 Переведите идиому:\n\n*{idiom['expression']}*\n\n"
        text += "Выберите правильный вариант:"

        correct = idiom['explanation']
        wrong_answers = [
            "Очень сложно",
            "Часто встречается",
            "Начать разговор",
            "Легко и просто",
        ]

        # Убираем правильный ответ из неправильных
        wrong_answers = [a for a in wrong_answers if a != correct]
        answers = [correct] + random.sample(wrong_answers, 2)
        random.shuffle(answers)

        keyboard = []
        for i, answer in enumerate(answers):
            callback = (f"check_{idiom['id']}_{i}"
                        if answer == correct
                        else f"wrong_{idiom['id']}")
            keyboard.append([{
                "text": answer,
                "callback_data": callback
            }])

        keyboard.append([{"text": "🔙 Назад", "callback_data": "main_menu"}])

        return text, correct, keyboard

    def _create_choice(self, idiom: Dict[str, Any]
                       ) -> Tuple[str, str, List[List[Dict[str, str]]]]:
        """Создание задания с выбором примера."""
        text = (f"🎯 Выберите правильный пример использования:\n\n"
                f"*{idiom['expression']}*\n\n"
                f"Значение: {idiom['explanation']}")

        correct_example = idiom['example']
        wrong_examples = [
            "I see him every day.",
            "It was very difficult for me.",
            "She started the conversation.",
            "The weather is nice today.",
        ]

        examples = [correct_example] + random.sample(wrong_examples, 2)
        random.shuffle(examples)

        keyboard = []
        for i, example in enumerate(examples):
            callback = (f"check_ex_{idiom['id']}_{i}"
                        if example == correct_example
                        else f"wrong_{idiom['id']}")
            keyboard.append([{
                "text": example,
                "callback_data": callback
            }])

        keyboard.append([{"text": "🔙 Назад", "callback_data": "main_menu"}])

        return text, correct_example, keyboard
