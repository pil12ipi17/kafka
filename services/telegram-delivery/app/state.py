import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DeliveryState:
    def __init__(self, state_dir: str) -> None:
        Path(state_dir).mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(Path(state_dir) / "telegram_delivery.sqlite")
        self._connection.execute(
            """
            create table if not exists sent_deliveries (
                event_id text not null,
                chat_id text not null,
                sent_at text not null,
                primary key(event_id, chat_id)
            )
            """
        )
        self._connection.commit()

    def is_sent(self, event_id: str, chat_id: str) -> bool:
        row = self._connection.execute(
            "select 1 from sent_deliveries where event_id = ? and chat_id = ?",
            (event_id, chat_id),
        ).fetchone()
        return row is not None

    def mark_sent(self, event_id: str, chat_id: str) -> None:
        self._connection.execute(
            "insert or ignore into sent_deliveries(event_id, chat_id, sent_at) values(?, ?, ?)",
            (event_id, chat_id, utc_now()),
        )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()
