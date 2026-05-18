import json
import logging
import time
from pathlib import Path

from app.models import Recipient

LOGGER = logging.getLogger(__name__)


class RecipientsStore:
    def __init__(self, path: str, reload_interval_seconds: float) -> None:
        self._path = Path(path)
        self._reload_interval_seconds = reload_interval_seconds
        self._last_checked = 0.0
        self._last_mtime = 0.0
        self._recipients: list[Recipient] = []

    def enabled_recipients(self) -> list[Recipient]:
        self._reload_if_needed()
        return [recipient for recipient in self._recipients if recipient.enabled]

    def _reload_if_needed(self) -> None:
        now = time.monotonic()
        if now - self._last_checked < self._reload_interval_seconds:
            return
        self._last_checked = now

        if not self._path.exists():
            LOGGER.warning("recipients_config_missing", extra={"_path": str(self._path)})
            self._recipients = []
            return

        mtime = self._path.stat().st_mtime
        if mtime == self._last_mtime:
            return

        payload = json.loads(self._path.read_text(encoding="utf-8"))
        self._recipients = [Recipient.model_validate(item) for item in payload]
        self._last_mtime = mtime
        LOGGER.info(
            "recipients_config_loaded",
            extra={"_path": str(self._path), "_recipients": len(self._recipients)},
        )
