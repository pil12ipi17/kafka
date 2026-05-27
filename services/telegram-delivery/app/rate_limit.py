import asyncio
import time
from collections import defaultdict


class PerChatRateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self._min_interval_seconds = min_interval_seconds
        self._last_sent: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def wait(self, chat_id: str) -> None:
        async with self._locks[chat_id]:
            elapsed = time.monotonic() - self._last_sent[chat_id]
            delay = self._min_interval_seconds - elapsed
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_sent[chat_id] = time.monotonic()
