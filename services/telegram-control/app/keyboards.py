from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

CATEGORIES = ["security", "ops", "content", "system", "other"]
PRIORITIES = ["low", "medium", "high", "critical"]

CATEGORY_LABELS = {
    "security": "Безопасность",
    "ops": "Операции",
    "content": "Контент",
    "system": "Система",
    "other": "Другое",
}

PRIORITY_LABELS = {
    "low": "Низкий",
    "medium": "Средний",
    "high": "Высокий",
    "critical": "Критический",
}

MODE_LABELS = {
    "realtime": "Сразу",
    "digest": "Дайджест",
}


def control_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="▶️ Старт"),
                KeyboardButton(text="⏸ Пауза"),
                KeyboardButton(text="⚙️ Настройки"),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def settings_keyboard(preference: dict) -> InlineKeyboardMarkup:
    mode = preference["mode"]
    categories = set(preference["categories"])
    priority = preference["min_priority"]
    rows = [
        [
            InlineKeyboardButton(
                text=f"{'🔘' if mode == 'realtime' else '⚪'} {MODE_LABELS['realtime']}",
                callback_data="mode:realtime",
            ),
            InlineKeyboardButton(
                text=f"{'🔘' if mode == 'digest' else '⚪'} {MODE_LABELS['digest']}",
                callback_data="mode:digest",
            ),
        ],
        [
            InlineKeyboardButton(text="▶️ Старт", callback_data="active:true"),
            InlineKeyboardButton(text="⏸ Пауза", callback_data="active:false"),
        ],
    ]
    rows.extend(
        [
            InlineKeyboardButton(
                text=f"{'🔘' if item == priority else '⚪'} {PRIORITY_LABELS[item]}",
                callback_data=f"priority:{item}",
            )
            for item in PRIORITIES[index : index + 2]
        ]
        for index in range(0, len(PRIORITIES), 2)
    )
    rows.extend(
        [
            InlineKeyboardButton(
                text=f"{'✅' if category in categories else '➕'} {CATEGORY_LABELS[category]}",
                callback_data=f"category:{category}",
            )
            for category in CATEGORIES[index : index + 3]
        ]
        for index in range(0, len(CATEGORIES), 3)
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)
