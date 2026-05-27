from app.keyboards import CATEGORY_LABELS, MODE_LABELS, PRIORITY_LABELS

STATUS_LABELS = {
    True: "активен",
    False: "на паузе",
}


def settings_text(preference: dict) -> str:
    categories = ", ".join(CATEGORY_LABELS[item] for item in preference["categories"])
    return (
        "⚙️ <b>Настройки уведомлений</b>\n"
        f"🚦 Режим: <b>{MODE_LABELS[preference['mode']]}</b>\n"
        f"🏷 Категории: <b>{categories}</b>\n"
        f"🔥 Мин. важность: <b>{PRIORITY_LABELS[preference['min_priority']]}</b>\n"
        f"📡 Статус: <b>{STATUS_LABELS[preference['active']]}</b>\n\n"
        "Измени фильтры и нажми <b>▶️ Старт</b>, чтобы возобновить поток."
    )


def welcome_text() -> str:
    return (
        "📡 <b>Управление Kafka-уведомлениями</b>\n"
        "Нижняя панель управляет потоком: старт, пауза и настройки фильтров."
    )
