import asyncio
import logging

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.exceptions import TelegramBadRequest

from app.config import settings
from app.health import start_health_server
from app.keyboards import control_keyboard, settings_keyboard
from app.messages import settings_text, welcome_text
from app.preferences_client import PreferencesClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger(__name__)


async def show_settings(message: Message, client: PreferencesClient) -> None:
    preference = await client.get_or_create(str(message.chat.id))
    await message.answer(settings_text(preference), reply_markup=settings_keyboard(preference))


async def update_settings(callback: CallbackQuery, preference: dict) -> None:
    if callback.message is None:
        await callback.answer()
        return
    try:
        await callback.message.edit_text(settings_text(preference), reply_markup=settings_keyboard(preference))
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc):
            raise
    await callback.answer("Updated")


async def main_async() -> None:
    if not settings.bot_token:
        raise RuntimeError("BOT_TOKEN is required for telegram-control-service")

    health_runner = await start_health_server(settings.metrics_port)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = PreferencesClient(settings.preferences_api_url, session)
        bot = Bot(settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dispatcher = Dispatcher()

        @dispatcher.message(Command("start"))
        async def start(message: Message) -> None:
            await client.get_or_create(str(message.chat.id))
            await message.answer(welcome_text(), reply_markup=control_keyboard())
            await show_settings(message, client)

        @dispatcher.message(Command("settings"))
        async def settings_command(message: Message) -> None:
            await show_settings(message, client)

        @dispatcher.message(F.text.casefold() == "settings")
        async def settings_button(message: Message) -> None:
            await show_settings(message, client)

        @dispatcher.message(F.text.casefold() == "pause")
        async def pause_button(message: Message) -> None:
            preference = await client.set_active(str(message.chat.id), False)
            await message.answer(settings_text(preference), reply_markup=settings_keyboard(preference))

        @dispatcher.message(F.text.casefold() == "resume")
        async def resume_button(message: Message) -> None:
            preference = await client.set_active(str(message.chat.id), True)
            await message.answer(settings_text(preference), reply_markup=settings_keyboard(preference))

        @dispatcher.message(F.text.casefold() == "realtime")
        async def realtime_button(message: Message) -> None:
            preference = await client.set_mode(str(message.chat.id), "realtime")
            await message.answer(settings_text(preference), reply_markup=settings_keyboard(preference))

        @dispatcher.message(F.text.casefold() == "digest")
        async def digest_button(message: Message) -> None:
            preference = await client.set_mode(str(message.chat.id), "digest")
            await message.answer(settings_text(preference), reply_markup=settings_keyboard(preference))

        @dispatcher.callback_query(F.data.startswith("mode:"))
        async def mode_callback(callback: CallbackQuery) -> None:
            mode = callback.data.split(":", 1)[1] if callback.data else "realtime"
            preference = await client.set_mode(str(callback.message.chat.id), mode)
            await update_settings(callback, preference)

        @dispatcher.callback_query(F.data.startswith("category:"))
        async def category_callback(callback: CallbackQuery) -> None:
            category = callback.data.split(":", 1)[1] if callback.data else "security"
            preference = await client.toggle_category(str(callback.message.chat.id), category)
            await update_settings(callback, preference)

        @dispatcher.callback_query(F.data.startswith("priority:"))
        async def priority_callback(callback: CallbackQuery) -> None:
            priority = callback.data.split(":", 1)[1] if callback.data else "medium"
            preference = await client.set_priority(str(callback.message.chat.id), priority)
            await update_settings(callback, preference)

        @dispatcher.callback_query(F.data.startswith("active:"))
        async def active_callback(callback: CallbackQuery) -> None:
            active = callback.data.split(":", 1)[1] == "true" if callback.data else True
            preference = await client.set_active(str(callback.message.chat.id), active)
            await update_settings(callback, preference)

        @dispatcher.callback_query(F.data == "settings:refresh")
        async def refresh_callback(callback: CallbackQuery) -> None:
            preference = await client.get_or_create(str(callback.message.chat.id))
            await update_settings(callback, preference)

        LOGGER.info("telegram_control_started")
        try:
            await dispatcher.start_polling(bot)
        finally:
            await bot.session.close()
            await health_runner.cleanup()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
