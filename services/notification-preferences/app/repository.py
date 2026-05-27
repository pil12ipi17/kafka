from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.models import Preference, PreferenceUpsert


class PreferencesRepository:
    def __init__(self, database_url: str = settings.database_url) -> None:
        self._database_url = database_url

    @contextmanager
    def _connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self._database_url, row_factory=dict_row) as connection:
            yield connection

    def init_schema(self) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                create table if not exists user_preferences (
                    chat_id text primary key,
                    categories text[] not null,
                    min_priority text not null,
                    mode text not null,
                    quiet_hours_start text,
                    quiet_hours_end text,
                    timezone text not null,
                    active boolean not null,
                    version integer not null,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )

    def get(self, chat_id: str) -> Preference | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                select chat_id, categories, min_priority, mode, quiet_hours_start,
                       quiet_hours_end, timezone, active, version
                from user_preferences
                where chat_id = %s
                """,
                (chat_id,),
            ).fetchone()
        return self._row_to_preference(row) if row else None

    def upsert(self, chat_id: str, payload: PreferenceUpsert) -> Preference:
        current = self.get(chat_id)
        version = 1 if current is None else current.version + 1
        with self._connection() as connection:
            row = connection.execute(
                """
                insert into user_preferences (
                    chat_id, categories, min_priority, mode, quiet_hours_start,
                    quiet_hours_end, timezone, active, version
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (chat_id) do update set
                    categories = excluded.categories,
                    min_priority = excluded.min_priority,
                    mode = excluded.mode,
                    quiet_hours_start = excluded.quiet_hours_start,
                    quiet_hours_end = excluded.quiet_hours_end,
                    timezone = excluded.timezone,
                    active = excluded.active,
                    version = excluded.version,
                    updated_at = now()
                returning chat_id, categories, min_priority, mode, quiet_hours_start,
                          quiet_hours_end, timezone, active, version
                """,
                (
                    chat_id,
                    [category.value for category in payload.categories],
                    payload.min_priority.value,
                    payload.mode.value,
                    payload.quiet_hours.start,
                    payload.quiet_hours.end,
                    payload.timezone,
                    payload.active,
                    version,
                ),
            ).fetchone()
        return self._row_to_preference(row)

    def update_mode(self, chat_id: str, mode: str) -> Preference | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                update user_preferences
                set mode = %s, version = version + 1, updated_at = now()
                where chat_id = %s
                returning chat_id, categories, min_priority, mode, quiet_hours_start,
                          quiet_hours_end, timezone, active, version
                """,
                (mode, chat_id),
            ).fetchone()
        return self._row_to_preference(row) if row else None

    def update_categories(self, chat_id: str, categories: list[str]) -> Preference | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                update user_preferences
                set categories = %s, version = version + 1, updated_at = now()
                where chat_id = %s
                returning chat_id, categories, min_priority, mode, quiet_hours_start,
                          quiet_hours_end, timezone, active, version
                """,
                (categories, chat_id),
            ).fetchone()
        return self._row_to_preference(row) if row else None

    def _row_to_preference(self, row: dict) -> Preference:
        return Preference(
            chat_id=row["chat_id"],
            categories=row["categories"],
            min_priority=row["min_priority"],
            mode=row["mode"],
            quiet_hours={
                "start": row["quiet_hours_start"],
                "end": row["quiet_hours_end"],
            },
            timezone=row["timezone"],
            active=row["active"],
            version=row["version"],
        )
