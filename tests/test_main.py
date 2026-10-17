import importlib
import logging
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from helpers import Event, chat, item, page
from tibo_plugin.store import Store


def load_plugin():
    api = types.ModuleType("astrbot.api")
    api.AstrBotConfig, api.logger = dict, logging.getLogger("astrbot-test")
    event = types.ModuleType("astrbot.api.event")
    event.AstrMessageEvent = Event
    class Chain:
        def message(self, text): self.text = text; return self
    event.MessageChain = Chain
    event.filter = types.SimpleNamespace(command=lambda name: lambda fn: fn)
    star = types.ModuleType("astrbot.api.star")
    class Star:
        def __init__(self, context): self.context = context
    star.Context, star.Star = object, Star
    star.StarTools = types.SimpleNamespace()
    star.register = lambda *args: lambda cls: cls
    with patch.dict("sys.modules", {"astrbot":types.ModuleType("astrbot"),"astrbot.api":api,
                                   "astrbot.api.event":event,"astrbot.api.star":star}):
        return importlib.import_module("tibo_plugin.main").TiboNotifyPlugin


Plugin = load_plugin()


class HandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "state.sqlite3")
        self.store.bootstrap(page(mode="bootstrap", high=10))
        self.config = {}
        self.context = types.SimpleNamespace(send_message=AsyncMock(return_value=True))
        self.plugin = Plugin(self.context, self.config)
        self.plugin._store = self.store
        self.plugin.engine = types.SimpleNamespace(subscribe=AsyncMock(return_value=True),
            unsubscribe=lambda target: self.store.unsubscribe(target.key), latest=AsyncMock(return_value=item(1)),
            interval=300, blocked=False, last_error="")
    async def asyncTearDown(self):
        self.store.close(); self.temp.cleanup()
    async def replies(self, handler, event, *args):
        results = [value async for value in handler(event, *args)]
        self.assertFalse(event.llm)
        self.assertTrue(event.stopped)
        self.assertTrue(all(value.stopped for value in results))
        return results

    async def test_group_member_cannot_manage(self):
        result = await self.replies(self.plugin.notify_command, Event(), "on")
        self.assertIn("AstrBot 管理员", result[0])
        self.plugin.engine.subscribe.assert_not_awaited()

    async def test_astrbot_admin_and_scoped_operator_can_manage(self):
        await self.replies(self.plugin.notify_command, Event(admin=True), "on")
        self.config["operator_ids"] = ["onebot-a:bob"]
        await self.replies(self.plugin.notify_command, Event(sender="bob"), "on")
        self.assertEqual(self.plugin.engine.subscribe.await_count, 2)
        await self.replies(self.plugin.notify_command, Event(sender="bob", platform="other"), "on")
        self.assertEqual(self.plugin.engine.subscribe.await_count, 2)

    async def test_private_self_manage_can_be_disabled(self):
        await self.replies(self.plugin.notify_command, Event(group=""), "on")
        self.plugin.engine.subscribe.assert_awaited_once()
        self.config["allow_private_subscriptions"] = False
        await self.replies(self.plugin.notify_command, Event(group=""), "on")
        self.plugin.engine.subscribe.assert_awaited_once()

    async def test_query_only_subscribed_chat_by_default(self):
        event = Event(command="tibo 1")
        result = await self.replies(self.plugin.query_command, event, "1")
        self.assertIn("尚未开放", result[0])
        self.store.subscribe(chat(), page(mode="bootstrap", high=10))
        result = await self.replies(self.plugin.query_command, Event(command="tibo 3"), "3")
        self.assertEqual(len(result), 1)
        self.assertIn("最新第 3 条", result[0])
        self.assertTrue(result[0].startswith("最新第 3 条\n【Codex重置预告：否】"))
        self.assertLess(result[0].index("原文\n"), result[0].index("中文翻译\n"))
        self.assertLessEqual(len(result[0]), 3500)
        self.assertEqual(self.plugin.engine.latest.call_args.args[1], 3)

    async def test_long_query_preserves_requested_order_within_limit(self):
        self.plugin.engine.latest.return_value = item(1, text="Original" * 5000, translation="中文" * 25000)
        result = await self.replies(self.plugin.query_command, Event(admin=True, command="tibo 100"), "100")
        self.assertTrue(result[0].startswith("最新第 100 条\n【Codex重置预告：否】"))
        self.assertLess(result[0].index("原文\n"), result[0].index("中文翻译\n"))
        self.assertEqual(len(result[0]), 3500)
        self.assertEqual(result[0].count("内容已截断，完整内容见链接"), 2)
        self.assertTrue(result[0].endswith("https://x.com/tibo_maker/status/1"))

    async def test_query_true_header_order(self):
        self.plugin.engine.latest.return_value = item(1, is_reset_announcement=True, model="MODEL_MUST_NOT_APPEAR")
        result = await self.replies(self.plugin.query_command, Event(admin=True, command="tibo 1"), "1")
        self.assertTrue(result[0].startswith("最新第 1 条\n【Codex重置预告：是】\n\n原文\nOriginal\n\n中文翻译\n中文内容"))
        self.assertNotIn("AI 判断", result[0])
        self.assertNotIn("翻译模型", result[0])
        self.assertNotIn("MODEL_MUST_NOT_APPEAR", result[0])
        self.assertTrue(result[0].endswith("https://x.com/tibo_maker/status/1"))

    async def test_query_allowlist_is_not_operator_permission(self):
        self.config["query_access"] = "operators"
        self.config["query_user_ids"] = ["onebot-a:alice"]
        await self.replies(self.plugin.query_command, Event(command="tibo 1"), "1")
        self.plugin.engine.latest.assert_awaited_once()
        await self.replies(self.plugin.notify_command, Event(), "on")
        self.plugin.engine.subscribe.assert_not_awaited()

    async def test_bad_arguments_and_surplus_rejected(self):
        for value in ("0", "101", "-1", "3.0", "一", "9"*1000):
            await self.replies(self.plugin.query_command, Event(admin=True, command="tibo " + value), value)
        await self.replies(self.plugin.query_command, Event(admin=True, command="tibo 1 extra"), "1")
        self.plugin.engine.latest.assert_not_awaited()
        await self.replies(self.plugin.notify_command, Event(admin=True, command="tibo-notify on extra"), "on")
        self.plugin.engine.subscribe.assert_not_awaited()

    async def test_account_switch_rechecks_query_access(self):
        self.store.subscribe(chat(), page(mode="bootstrap", high=10))
        async def changed_feed(target, n):
            self.store.bootstrap(page(mode="bootstrap", high=20, feed="feed-b"))
            return item(20, translation="New account content")
        self.plugin.engine.latest.side_effect = changed_feed
        result = await self.replies(self.plugin.query_command, Event(command="tibo 1"), "1")
        self.assertIn("订阅来源已变化", result[0])
        self.assertNotIn("New account content", result[0])

    async def test_actual_umo_preserved_for_send(self):
        target = chat(sender="bob")
        result = await self.plugin._send({"umo":target.umo}, "test content")
        self.assertTrue(result)
        self.assertEqual(self.context.send_message.call_args.args[0], target.umo)
        self.assertEqual(self.context.send_message.call_args.args[1].text, "test content")
