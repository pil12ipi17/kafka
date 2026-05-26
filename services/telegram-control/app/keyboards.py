from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

CATEGORIES = ["security", "ops", "content", "system", "other"]
PRIORITIES = ["low", "medium", "high", "critical"]


def control_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Settings")],
            [KeyboardButton(text="Pause"), KeyboardButton(text="Resume")],
            [KeyboardButton(text="Realtime"), KeyboardButton(text="Digest")],
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
                text=f"{'●' if mode == 'realtime' else '○'} Realtime",
                callback_data="mode:realtime",
            ),
            InlineKeyboardButton(
                text=f"{'●' if mode == 'digest' else '○'} Digest",
                callback_data="mode:digest",
            ),
        ],
        [
            InlineKeyboardButton(
                text="Pause" if preference["active"] else "Resume",
                callback_data=f"active:{str(not preference['active']).lower()}",
            ),
            InlineKeyboardButton(text="Refresh", callback_data="settings:refresh"),
        ],
    ]
    rows.extend(
        [
            InlineKeyboardButton(text=f"{'●' if item == priority else '○'} {item}", callback_data=f"priority:{item}")
            for item in PRIORITIES[index : index + 2]
        ]
        for index in range(0, len(PRIORITIES), 2)
    )
    rows.extend(
        [
            InlineKeyboardButton(
                text=f"{'✓' if category in categories else '+'} {category}",
                callback_data=f"category:{category}",
            )
            for category in CATEGORIES[index : index + 3]
        ]
        for index in range(0, len(CATEGORIES), 3)
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)
