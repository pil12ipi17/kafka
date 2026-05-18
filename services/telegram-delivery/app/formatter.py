import html
from datetime import datetime, timezone

from app.models import TelegramEvent


def _format_time(value: str | int | None) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, int):
        if value > 10_000_000_000:
            value = value // 1000
        return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return html.escape(value)


def _truncate(value: str | None, limit: int) -> str:
    if not value:
        return "no comment"
    value = " ".join(value.split())
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1]}..."


def format_telegram_message(event: TelegramEvent) -> str:
    source = html.escape(event.source)
    action = html.escape(event.action_type)
    title = html.escape(_truncate(event.entity_title, 140))
    user = html.escape(event.user_name)
    project = html.escape(event.project or event.source)
    summary = html.escape(_truncate(event.summary, 220))
    event_time = _format_time(event.event_time)
    byte_change = f"{event.byte_change:+d} bytes" if event.byte_change is not None else "n/a"

    lines = [
        "📰 <b>Wikimedia update</b>",
        f"<b>Source:</b> {source}",
        f"<b>Action:</b> {action}",
        f"<b>Page:</b> {title}",
        f"<b>User:</b> {user}",
        f"<b>Project:</b> {project}",
        f"<b>Change:</b> {html.escape(byte_change)}",
        f"<b>Time:</b> {event_time}",
        f"<b>Comment:</b> {summary}",
    ]
    if event.source_url:
        lines.append(f'<a href="{html.escape(event.source_url, quote=True)}">Open source</a>')
    return "\n".join(lines)
