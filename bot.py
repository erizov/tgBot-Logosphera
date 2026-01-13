"""
Основной файл Telegram бота Logosphera.
"""

import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from database import Database
from modules.greeting import GreetingModule
from modules.topic_selector import TopicSelectorModule
from modules.idiom_explainer import IdiomExplainerModule
from modules.practice import PracticeModule
from modules.reflection import ReflectionModule

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Инициализация БД и модулей
db = Database()
greeting_module = GreetingModule()
topic_selector = TopicSelectorModule(db)
idiom_explainer = IdiomExplainerModule()
practice_module = PracticeModule()
reflection_module = ReflectionModule()

# Состояния пользователей
user_states: Dict[int, Dict[str, Any]] = {}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    try:
        user = update.effective_user
        logger.info(f"User {user.id} started the bot")

        # Создание или получение пользователя
        try:
            db_user = db.get_or_create_user(user.id, user.username)
            logger.info(f"User {user.id} created/retrieved from DB")
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            await update.message.reply_text(
                "❌ Ошибка подключения к базе данных. "
                "Пожалуйста, попробуйте позже."
            )
            return

        user_states[user.id] = {
            'state': 'main_menu',
            'current_idiom': None,
        }

        # Приветствие
        greeting_text = greeting_module.get_greeting()
        keyboard = greeting_module.get_menu_keyboard()

        await update.message.reply_text(
            greeting_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        logger.info(f"Start message sent to user {user.id}")
    except Exception as e:
        logger.error(f"Error in start handler: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "❌ Произошла ошибка. Пожалуйста, попробуйте позже."
            )
        except:
            pass


async def button_handler(update: Update,
                         context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data = query.data

    # Инициализация состояния пользователя
    if user_id not in user_states:
        user_states[user_id] = {'state': 'main_menu'}

    # Главное меню
    if data == 'main_menu':
        await show_main_menu(query, context)

    # Выбор темы
    elif data == 'choose_topic':
        await show_topics(query, context)

    # Случайная идиома
    elif data == 'random_idiom':
        await show_random_idiom(query, context)

    # Тема выбрана
    elif data.startswith('topic_'):
        topic = data.replace('topic_', '')
        await show_idioms_by_topic(query, context, topic)

    # Практика
    elif data.startswith('practice_'):
        idiom_id = int(data.replace('practice_', ''))
        await show_practice(query, context, idiom_id)

    # Проверка ответа
    elif data.startswith('check_'):
        await check_answer(query, context, data)

    # Неправильный ответ
    elif data.startswith('wrong_'):
        await show_wrong_answer(query, context)

    # Показать ответ
    elif data.startswith('answer_'):
        idiom_id = int(data.replace('answer_', ''))
        await show_answer(query, context, idiom_id)

    # Достижения
    elif data == 'achievements':
        await show_achievements(query, context)

    # Статистика
    elif data == 'stats':
        await show_stats(query, context)

    # Рефлексия
    elif data.startswith('reflection_'):
        await handle_reflection(query, context, data)

    # Пропустить рефлексию
    elif data == 'skip_reflection':
        await show_main_menu(query, context)


async def show_main_menu(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать главное меню."""
    keyboard = greeting_module.get_menu_keyboard()
    text = "🏠 *Главное меню*\n\nВыберите действие:"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_topics(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать список тем."""
    keyboard = topic_selector.get_topics_keyboard()
    text = "📚 *Выберите тему для изучения:*"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_idioms_by_topic(query, context: ContextTypes.DEFAULT_TYPE,
                                topic: str):
    """Показать идиомы по теме."""
    idioms = topic_selector.get_idioms_for_topic(topic)
    user_id = query.from_user.id

    if not idioms:
        text = "😔 Идиомы по этой теме пока не добавлены."
        keyboard = [[{"text": "🔙 Назад", "callback_data": "choose_topic"}]]
    else:
        # Берем первую идиому из темы
        idiom = idioms[0]
        user_states[user_id]['current_idiom'] = idiom
        text = idiom_explainer.format_idiom_explanation(idiom)
        keyboard = idiom_explainer.get_practice_keyboard(idiom['id'])

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_random_idiom(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать случайную идиому."""
    user_id = query.from_user.id
    db_user = db.get_or_create_user(user_id, query.from_user.username)

    idiom = db.get_random_idiom(db_user['id'])
    if not idiom:
        text = "😔 Все идиомы изучены! Поздравляем!"
        keyboard = [[{"text": "🔙 Главное меню", "callback_data": "main_menu"}]]
    else:
        user_states[user_id]['current_idiom'] = idiom
        text = idiom_explainer.format_idiom_explanation(idiom)
        keyboard = idiom_explainer.get_practice_keyboard(idiom['id'])

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_practice(query, context: ContextTypes.DEFAULT_TYPE,
                        idiom_id: int):
    """Показать практическое задание."""
    user_id = query.from_user.id
    idiom = user_states[user_id].get('current_idiom')

    if not idiom or idiom['id'] != idiom_id:
        # Получаем идиому из БД
        idioms = db.get_idioms_by_topic()
        idiom = next((i for i in idioms if i['id'] == idiom_id), None)
        if not idiom:
            await query.answer("Идиома не найдена", show_alert=True)
            return

    question, answer, keyboard = practice_module.generate_exercise(idiom)
    user_states[user_id]['practice_answer'] = answer

    await query.edit_message_text(
        question,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def check_answer(query, context: ContextTypes.DEFAULT_TYPE, data: str):
    """Проверка ответа пользователя."""
    user_id = query.from_user.id
    parts = data.split('_')
    idiom_id = int(parts[2])

    db_user = db.get_or_create_user(user_id, query.from_user.username)
    db.update_user_progress(db_user['id'], idiom_id, 'completed')

    text = "✅ *Правильно!*\n\n"
    text += "Отлично! Вы правильно ответили на вопрос.\n\n"
    text += "Хотите пройти рефлексию?"

    keyboard = [
        [
            {"text": "💭 Рефлексия", "callback_data": f"reflection_{idiom_id}"},
            {"text": "🔄 Другая идиома", "callback_data": "random_idiom"},
        ],
        [{"text": "🔙 Главное меню", "callback_data": "main_menu"}],
    ]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_wrong_answer(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать сообщение о неправильном ответе."""
    text = "❌ *Неправильно*\n\n"
    text += "Не расстраивайтесь! Попробуйте ещё раз или "
    text += "изучите объяснение идиомы."

    keyboard = [
        [{"text": "🔄 Попробовать снова", "callback_data": "random_idiom"}],
        [{"text": "🔙 Главное меню", "callback_data": "main_menu"}],
    ]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_answer(query, context: ContextTypes.DEFAULT_TYPE,
                      idiom_id: int):
    """Показать правильный ответ."""
    user_id = query.from_user.id
    idiom = user_states[user_id].get('current_idiom')

    if not idiom or idiom['id'] != idiom_id:
        idioms = db.get_idioms_by_topic()
        idiom = next((i for i in idioms if i['id'] == idiom_id), None)

    if idiom:
        text = f"📖 *Правильный ответ:*\n\n*{idiom['expression']}*\n\n"
        text += f"Значение: {idiom['explanation']}\n\n"
        text += f"Пример: {idiom['example']}"

        keyboard = [
            [
                {"text": "💭 Рефлексия", "callback_data": f"reflection_{idiom_id}"},
                {"text": "🔄 Другая идиома", "callback_data": "random_idiom"},
            ],
            [{"text": "🔙 Главное меню", "callback_data": "main_menu"}],
        ]

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )


async def show_achievements(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать достижения пользователя."""
    user_id = query.from_user.id
    db_user = db.get_or_create_user(user_id, query.from_user.username)

    achievements = db.get_user_achievements(db_user['id'])
    progress = db.get_user_progress(db_user['id'])

    text = f"🏆 *Ваши достижения*\n\n"
    text += f"Изучено идиом: *{progress}*\n\n"

    if achievements:
        text += "*Разблокированные достижения:*\n"
        for ach in achievements:
            text += f"{ach['icon']} {ach['name']}\n"
            text += f"   {ach['description']}\n\n"
    else:
        text += "Пока нет достижений. Продолжайте изучать идиомы!"

    keyboard = [[{"text": "🔙 Главное меню", "callback_data": "main_menu"}]]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_stats(query, context: ContextTypes.DEFAULT_TYPE):
    """Показать статистику пользователя."""
    user_id = query.from_user.id
    db_user = db.get_or_create_user(user_id, query.from_user.username)

    progress = db.get_user_progress(db_user['id'])
    reflection_stats = db.get_reflection_stats(db_user['id'])

    text = f"📊 *Ваша статистика*\n\n"
    text += f"Изучено идиом: *{progress}*\n\n"

    if reflection_stats:
        text += reflection_module.format_reflection_stats(reflection_stats)
    else:
        text += "📊 Вы ещё не проходили рефлексию."

    keyboard = [[{"text": "🔙 Главное меню", "callback_data": "main_menu"}]]

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def handle_reflection(query, context: ContextTypes.DEFAULT_TYPE,
                            data: str):
    """Обработка рефлексии."""
    user_id = query.from_user.id
    parts = data.split('_')

    if len(parts) == 2 and parts[1].isdigit():
        # Начало рефлексии
        idiom_id = int(parts[1])
        idiom = user_states[user_id].get('current_idiom')

        if not idiom or idiom['id'] != idiom_id:
            idioms = db.get_idioms_by_topic()
            idiom = next((i for i in idioms if i['id'] == idiom_id), None)

        if idiom:
            ref_type, question = reflection_module.get_reflection_question(idiom)
            user_states[user_id]['reflection_type'] = ref_type
            user_states[user_id]['reflection_idiom_id'] = idiom_id

            keyboard = reflection_module.get_reflection_keyboard()

            await query.edit_message_text(
                question,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
    elif parts[1] == 'text':
        # Запрос текстового ответа
        user_states[user_id]['state'] = 'reflection_text'
        text = "💬 Напишите ваш ответ на вопрос рефлексии:"
        keyboard = [[{"text": "🔙 Отмена", "callback_data": "main_menu"}]]

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    elif parts[1] in ['yes', 'no']:
        # Ответ на рефлексию (да/нет)
        db_user = db.get_or_create_user(user_id, query.from_user.username)
        idiom_id = user_states[user_id].get('reflection_idiom_id')
        ref_type = user_states[user_id].get('reflection_type', 'binary')

        answer = 'Да' if parts[1] == 'yes' else 'Нет'

        if idiom_id:
            db.save_reflection(db_user['id'], idiom_id, ref_type, answer)

        text = "✅ Спасибо за рефлексию!\n\n"
        text += "Ваш ответ сохранён. Это поможет вам лучше "
        text += "запомнить изученный материал."

        keyboard = [
            [{"text": "🔄 Другая идиома", "callback_data": "random_idiom"}],
            [{"text": "🔙 Главное меню", "callback_data": "main_menu"}],
        ]

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    user_id = update.effective_user.id

    if user_id in user_states:
        state = user_states[user_id].get('state')
        if state == 'reflection_text':
            # Сохранение текстового ответа рефлексии
            db_user = db.get_or_create_user(user_id, update.effective_user.username)
            idiom_id = user_states[user_id].get('reflection_idiom_id')
            ref_type = user_states[user_id].get('reflection_type', 'long')

            if idiom_id:
                db.save_reflection(
                    db_user['id'],
                    idiom_id,
                    ref_type,
                    update.message.text
                )

            await update.message.reply_text(
                "✅ Спасибо за развернутый ответ!\n\n"
                "Ваша рефлексия сохранена."
            )
            user_states[user_id]['state'] = 'main_menu'
        else:
            await update.message.reply_text(
                "Используйте кнопки меню для навигации."
            )
    else:
        await update.message.reply_text(
            "Используйте команду /start для начала работы."
        )


def main():
    """Главная функция запуска бота."""
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        logger.error("TELEGRAM_TOKEN не установлен!")
        return

    application = Application.builder().token(token).build()

    # Регистрация обработчиков
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
    )

    # Запуск бота
    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
