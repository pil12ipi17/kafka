from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.models import UserPreference


class PreferencesRepository:
    def __init__(self, database_url: str = settings.database_url) -> None:
        self._database_url = database_url

    @contextmanager
    def _connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self._database_url, row_factory=dict_row) as connection:
            yield connection

    def list_active(self) -> list[UserPreference]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                select chat_id, categories, min_priority, mode, timezone, active, version
                from user_preferences
                where active = true
                """
            ).fetchall()
        return [UserPreference(**row) for row in rows]
