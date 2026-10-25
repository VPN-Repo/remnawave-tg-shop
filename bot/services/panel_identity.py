"""Panel 3.x identity: maps the bot's panel reference to the panel's numeric user id.

The bot keys panel users by ``users.panel_user_uuid``. Remnawave 2.x addressed users by
that uuid; 3.x dropped it and addresses users by a numeric ``id``. The reference column
is kept as-is (legacy uuids stay, new users get the id as a string) and this module
resolves a reference to the numeric id, persisting it in ``users.panel_user_id``.
"""
import logging
from typing import Dict, Optional, Protocol

from db.dal import user_dal


def is_numeric_ref(ref: object) -> bool:
    return str(ref).strip().isdigit()


class PanelIdentityStore(Protocol):
    async def get_panel_id(self, ref: str) -> Optional[int]: ...
    async def get_ref_by_panel_id(self, panel_id: int) -> Optional[str]: ...
    async def get_telegram_id(self, ref: str) -> Optional[int]: ...
    async def get_ref_by_telegram_id(self, telegram_id: int) -> Optional[str]: ...
    async def save_panel_id(self, ref: str, panel_id: int) -> None: ...


class DbIdentityStore:
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def get_panel_id(self, ref: str) -> Optional[int]:
        async with self._session_factory() as session:
            user = await user_dal.get_user_by_panel_uuid(session, ref)
            return user.panel_user_id if user else None

    async def get_ref_by_panel_id(self, panel_id: int) -> Optional[str]:
        async with self._session_factory() as session:
            user = await user_dal.get_user_by_panel_id(session, panel_id)
            return user.panel_user_uuid if user else None

    async def get_telegram_id(self, ref: str) -> Optional[int]:
        async with self._session_factory() as session:
            user = await user_dal.get_user_by_panel_uuid(session, ref)
            return user.user_id if user else None

    async def get_ref_by_telegram_id(self, telegram_id: int) -> Optional[str]:
        async with self._session_factory() as session:
            user = await user_dal.get_user_by_id(session, telegram_id)
            return user.panel_user_uuid if user else None

    async def save_panel_id(self, ref: str, panel_id: int) -> None:
        async with self._session_factory() as session:
            user = await user_dal.get_user_by_panel_uuid(session, ref)
            if user and user.panel_user_id != panel_id:
                user.panel_user_id = panel_id
                await session.commit()


class MemoryIdentityStore:
    """Dict-backed store for tests: ``users`` maps telegram_id -> {"ref": ..., "panel_id": ...}."""

    def __init__(self, users: Optional[Dict[int, Dict[str, object]]] = None) -> None:
        self.users: Dict[int, Dict[str, object]] = users or {}

    def _by_ref(self, ref: str):
        return next((u for u in self.users.values() if u.get("ref") == ref), None)

    async def get_panel_id(self, ref: str) -> Optional[int]:
        u = self._by_ref(ref)
        return u.get("panel_id") if u else None  # type: ignore[return-value]

    async def get_ref_by_panel_id(self, panel_id: int) -> Optional[str]:
        u = next((u for u in self.users.values() if u.get("panel_id") == panel_id), None)
        return u.get("ref") if u else None  # type: ignore[return-value]

    async def get_telegram_id(self, ref: str) -> Optional[int]:
        return next((tg for tg, u in self.users.items() if u.get("ref") == ref), None)

    async def get_ref_by_telegram_id(self, telegram_id: int) -> Optional[str]:
        u = self.users.get(telegram_id)
        return u.get("ref") if u else None  # type: ignore[return-value]

    async def save_panel_id(self, ref: str, panel_id: int) -> None:
        u = self._by_ref(ref)
        if u is not None:
            u["panel_id"] = panel_id


_store: Optional[PanelIdentityStore] = None


def configure(store: Optional[PanelIdentityStore]) -> None:
    global _store
    _store = store


def get_store() -> Optional[PanelIdentityStore]:
    if _store is None:
        logging.debug("panel_identity store is not configured; only numeric refs can be resolved")
    return _store
