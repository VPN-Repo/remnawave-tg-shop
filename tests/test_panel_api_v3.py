"""PanelApiService on Remnawave 2.x vs 3.x: the bot keeps its references, the client translates."""
import asyncio
import unittest
from typing import Any, Dict, List, Optional, Tuple

from bot.services import panel_identity
from bot.services.panel_api_service import PanelApiService

UUID = "9bcc75d3-fbef-4f75-bf18-17245f041378"


class FakeSettings:
    PANEL_API_URL = "https://panel.test"
    PANEL_API_KEY = "k"
    PANEL_API_VERSION = "auto"
    LOG_LEVEL = "INFO"
    USER_HWID_DEVICE_LIMIT = None


class FakeClient(PanelApiService):
    """Replaces the HTTP layer: routes is {(method, path): response}; calls are recorded."""

    def __init__(self, routes: Dict[Tuple[str, str], Any], version: str = "3.4.7"):
        super().__init__(FakeSettings())
        self.routes = routes
        self.version = version
        self.calls: List[Tuple[str, str, Optional[dict], Optional[dict]]] = []

    async def _request(self, method: str, endpoint: str, log_full_response: bool = False, **kwargs):
        self.calls.append((method.upper(), endpoint, kwargs.get("json"), kwargs.get("params")))
        if endpoint == "/system/metadata":
            return {"response": {"version": self.version}}
        resp = self.routes.get((method.upper(), endpoint))
        if resp is None:
            return {"error": True, "status_code": 404, "errorCode": "A062"}
        return resp


def run(coro):
    return asyncio.run(coro)


class PanelV3Test(unittest.TestCase):
    def setUp(self):
        self.store = panel_identity.MemoryIdentityStore({
            100: {"ref": UUID, "panel_id": None},  # legacy user: uuid known, id not yet
            200: {"ref": "555", "panel_id": 555},   # user created on 3.x: ref is the id
        })
        panel_identity.configure(self.store)

    def tearDown(self):
        panel_identity.configure(None)

    def test_v2_is_untouched(self):
        c = FakeClient({("GET", f"/users/{UUID}"): {"response": {"uuid": UUID, "username": "tg_100"}},
                        ("GET", "/users/by-telegram-id/100"): {"response": [{"uuid": UUID}]},
                        ("POST", "/hwid/devices/delete"): {"response": {}}}, version="2.7.4")
        self.assertEqual(run(c.get_user_by_uuid(UUID))["uuid"], UUID)
        self.assertEqual(run(c.get_users_by_filter(telegram_id=100))[0]["uuid"], UUID)
        run(c.disconnect_device(UUID, "h1"))
        self.assertIn(("POST", "/hwid/devices/delete", {"userUuid": UUID, "hwid": "h1"}, None), c.calls)
        self.assertFalse(any(p.startswith("/users/stream") for _, p, _, _ in c.calls))

    def test_v3_legacy_uuid_is_resolved_by_username_and_persisted(self):
        c = FakeClient({("GET", "/users/by-username/tg_100"): {"response": {"id": 77, "username": "tg_100"}},
                        ("GET", "/users/77"): {"response": {"id": 77, "username": "tg_100", "status": "ACTIVE"}},
                        ("POST", "/users/77/actions/disable"): {"response": {"status": "DISABLED"}},
                        ("PATCH", "/users"): {"response": {"id": 77}},
                        ("GET", "/hwid/devices/77"): {"response": {"devices": []}},
                        ("POST", "/hwid/devices/delete"): {"response": {}}})
        user = run(c.get_user_by_uuid(UUID))
        self.assertEqual(user["uuid"], UUID, "caller's reference must be preserved")
        self.assertEqual(self.store.users[100]["panel_id"], 77, "resolved id must be persisted")

        self.assertTrue(run(c.update_user_status_on_panel(UUID, enable=False)))
        run(c.update_user_details_on_panel(UUID, {"uuid": UUID, "description": "x"}))
        self.assertIsNotNone(run(c.get_user_devices(UUID)))
        self.assertTrue(run(c.disconnect_device(UUID, "h1")))

        self.assertIn(("PATCH", "/users", {"description": "x", "id": 77}, None), c.calls)
        self.assertIn(("POST", "/hwid/devices/delete", {"userId": 77, "hwid": "h1"}, None), c.calls)
        self.assertFalse(any(UUID in p for _, p, _, _ in c.calls), "uuid must never reach a 3.x panel path")

    def test_v3_lookup_by_telegram_uses_stream_and_returns_known_ref(self):
        c = FakeClient({("GET", "/users/stream"): {"response": {"users": [{"id": 77, "telegramId": 100}], "hasMore": False}}})
        users = run(c.get_users_by_filter(telegram_id=100))
        self.assertEqual(users[0]["uuid"], UUID, "known user keeps its legacy reference")
        self.assertEqual(c.calls[-1][3], {"telegramId": "100", "size": 50})
        self.assertEqual(self.store.users[100]["panel_id"], 77)

    def test_v3_new_user_gets_numeric_reference(self):
        c = FakeClient({("POST", "/users"): {"response": {"id": 901, "username": "tg_300", "telegramId": 300}},
                        ("GET", "/users/901"): {"response": {"id": 901}}})
        created = run(c.create_panel_user(username_on_panel="tg_300", telegram_id=300))
        self.assertEqual(created["response"]["uuid"], "901")
        self.assertEqual(run(c.get_user_by_uuid("901"))["uuid"], "901")
        self.assertIn(("GET", "/users/901", None, None), c.calls)

    def test_v3_happ_encrypt_is_skipped(self):
        c = FakeClient({})
        self.assertIsNone(run(c.encrypt_happ_link("happ://x")))
        self.assertFalse(any(p == "/system/tools/happ/encrypt" for _, p, _, _ in c.calls))

    def test_pinned_version_skips_probe(self):
        c = FakeClient({("GET", f"/users/{UUID}"): {"response": {"uuid": UUID}}}, version="3.4.7")
        c.settings.PANEL_API_VERSION = "v2"
        run(c.get_user_by_uuid(UUID))
        self.assertFalse(any(p == "/system/metadata" for _, p, _, _ in c.calls))


if __name__ == "__main__":
    unittest.main()
