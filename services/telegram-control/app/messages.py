def settings_text(preference: dict) -> str:
    categories = ", ".join(preference["categories"])
    return (
        "<b>Notification settings</b>\n"
        f"Mode: <b>{preference['mode']}</b>\n"
        f"Categories: <b>{categories}</b>\n"
        f"Min priority: <b>{preference['min_priority']}</b>\n"
        f"Status: <b>{'active' if preference['active'] else 'paused'}</b>"
    )


def welcome_text() -> str:
    return (
        "<b>Kafka notification control</b>\n"
        "Use this bot to control which Kafka/Wikimedia notifications reach this chat."
    )
