import asyncio

import aiohttp

from app.config import settings


class TemporaryTelegramError(Exception):
    def __init__(self, message: str, error_code: int | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code


class PermanentTelegramError(Exception):
    def __init__(self, message: str, error_code: int | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code


class TelegramClient:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def send_message(self, chat_id: str, text: str) -> dict:
        if settings.telegram_dry_run:
            await asyncio.sleep(0)
            return {"ok": True, "dry_run": True, "chat_id": chat_id, "text_preview": text[:120]}
        if not settings.bot_token:
            raise PermanentTelegramError("BOT_TOKEN is empty", error_code=401)

        url = f"{settings.telegram_api_base_url}/bot{settings.bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": settings.telegram_parse_mode,
            "disable_web_page_preview": True,
        }
        try:
            async with self._session.post(url, json=payload) as response:
                body = await response.json(content_type=None)
                if response.status == 200 and body.get("ok") is True:
                    return body
                description = str(body.get("description") or body)
                if response.status == 429 or response.status >= 500:
                    raise TemporaryTelegramError(description, error_code=response.status)
                raise PermanentTelegramError(description, error_code=response.status)
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise TemporaryTelegramError(str(exc)) from exc
