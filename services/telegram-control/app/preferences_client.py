from typing import Any

import aiohttp


DEFAULT_PREFERENCES = {
    "categories": ["security", "ops"],
    "min_priority": "medium",
    "mode": "realtime",
    "quiet_hours": {"start": None, "end": None},
    "timezone": "Europe/Moscow",
    "active": True,
}


class PreferencesClient:
    def __init__(self, base_url: str, session: aiohttp.ClientSession) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session

    async def get_or_create(self, chat_id: str) -> dict[str, Any]:
        async with self._session.get(f"{self._base_url}/v1/preferences/{chat_id}") as response:
            if response.status == 200:
                return await response.json()
            if response.status != 404:
                response.raise_for_status()

        async with self._session.put(
            f"{self._base_url}/v1/preferences/{chat_id}",
            json=DEFAULT_PREFERENCES,
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def set_mode(self, chat_id: str, mode: str) -> dict[str, Any]:
        await self.get_or_create(chat_id)
        async with self._session.patch(
            f"{self._base_url}/v1/preferences/{chat_id}/mode",
            json={"mode": mode},
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def replace(self, chat_id: str, preference: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "categories": preference["categories"],
            "min_priority": preference["min_priority"],
            "mode": preference["mode"],
            "quiet_hours": preference["quiet_hours"],
            "timezone": preference["timezone"],
            "active": preference["active"],
        }
        async with self._session.put(f"{self._base_url}/v1/preferences/{chat_id}", json=payload) as response:
            response.raise_for_status()
            return await response.json()

    async def set_priority(self, chat_id: str, priority: str) -> dict[str, Any]:
        preference = await self.get_or_create(chat_id)
        preference["min_priority"] = priority
        return await self.replace(chat_id, preference)

    async def set_active(self, chat_id: str, active: bool) -> dict[str, Any]:
        preference = await self.get_or_create(chat_id)
        preference["active"] = active
        return await self.replace(chat_id, preference)

    async def toggle_category(self, chat_id: str, category: str) -> dict[str, Any]:
        preference = await self.get_or_create(chat_id)
        categories = set(preference["categories"])
        if category in categories:
            categories.remove(category)
        else:
            categories.add(category)
        if not categories:
            categories.add(category)

        async with self._session.patch(
            f"{self._base_url}/v1/preferences/{chat_id}/categories",
            json={"categories": sorted(categories)},
        ) as response:
            response.raise_for_status()
            return await response.json()
