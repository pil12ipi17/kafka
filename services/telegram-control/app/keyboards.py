from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

CATEGORIES = ["security", "ops", "content", "system", "other"]
PRIORITIES = ["low", "medium", "high", "critical"]


def settings_keyboard(preference: dict) -> InlineKeyboardMarkup:
    mode = preference["mode"]
    categories = set(preference["categories"])
    priority = preference["min_priority"]
    rows = [
        [
            InlineKeyboardButton(
                text=f"{'●' if mode == 'realtime' else '○'} realtime",
                callback_data="mode:realtime",
            ),
            InlineKeyboardButton(
                text=f"{'●' if mode == 'digest' else '○'} digest",
                callback_data="mode:digest",
            ),
        ],
        [
            InlineKeyboardButton(
                text="Pause" if preference["active"] else "Resume",
                callback_data=f"active:{str(not preference['active']).lower()}",
            )
        ],
    ]
    rows.extend(
        [
            InlineKeyboardButton(
                text=f"{'●' if item == priority else '○'} {item}",
                callback_data=f"priority:{item}",
            )
        ]
        for item in PRIORITIES
    )
    rows.extend(
        [
            InlineKeyboardButton(
                text=f"{'✓' if category in categories else '+'} {category}",
                callback_data=f"category:{category}",
            )
        ]
        for category in CATEGORIES
    )
    rows.append([InlineKeyboardButton(text="Refresh", callback_data="settings:refresh")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
