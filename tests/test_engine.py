import asyncio
import logging
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from helpers import FakeClient, chat, page
from tibo_plugin.engine import Engine
from tibo_plugin.models import FeedError
from tibo_plugin.store import Store


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "state.sqlite3")
        self.client = FakeClient()
        self.config = {"feed_url": "fake-source", "poll_interval_seconds": 300}
        self.sender = AsyncMock(return_value=True)
        self.engine = Engine(self.store, self.config, self.sender, logging.getLogger("test"), lambda source: self.client)
        self.sleep_patch = patch("tibo_plugin.engine.asyncio.sleep", new=AsyncMock())
        self.sleep_patch.start()

    async def asyncTearDown(self):
        self.sleep_patch.stop()
        await self.engine.stop()
        self.store.close()
        self.temp.cleanup()

    async def test_bootstrap_never_broadcasts_history(self):
        await self.engine.tick()
        self.assertEqual(self.store.state()["high_water"], 10)
        self.sender.assert_not_called()
        self.assertEqual(self.client.calls, [("bootstrap",), ("incremental", "cursor-10")])

    async def test_shared_feed_poll_two_groups_and_member_dedup(self):
        await self.engine.subscribe(chat())
        await self.engine.subscribe(chat(sender="bob"))
        await self.engine.subscribe(chat(group="200"))
        self.client.pages = [page([11])]
        await self.engine.tick()
        self.assertEqual(self.sender.await_count, 2)
        self.assertEqual(len([c for c in self.client.calls if c[0] == "incremental"]), 1)
        self.assertEqual(len([c for c in self.client.calls if c[0] == "bootstrap"]), 2)

    async def test_first_on_fresh_cursor_does_not_move_global_receive(self):
        await self.engine.tick()
        self.client.head = 13
        await self.engine.subscribe(chat())
        self.assertEqual(self.store.state()["high_water"], 10)
        self.assertEqual(self.store.subscription(chat().key)["boundary"], 13)
        self.client.pages = [page([11, 12, 13, 14])]
        self.engine._last_poll = 0
        await self.engine.tick()
        self.assertEqual(self.sender.await_count, 1)
        self.assertEqual(self.sender.call_args.args[0]["item_id"], "14")

    async def test_parallel_on_is_idempotent(self):
        results = await asyncio.gather(self.engine.subscribe(chat()), self.engine.subscribe(chat(sender="bob")))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.client.calls, [("bootstrap",)])

    async def test_uncertain_send_pauses_only_failed_chat(self):
        first, other = chat(), chat(group="200")
        await self.engine.subscribe(first); await self.engine.subscribe(other)
        async def send(row, text):
            if row["chat_key"] == first.key: raise TimeoutError("secret must not log")
            return True
        self.engine.sender = send
        self.client.pages = [page([11, 12])]
        await self.engine.tick()
        self.assertEqual(self.store.counts(first.key), {"pending": 1, "uncertain": 1})
        self.assertEqual(self.store.counts(other.key), {"sent": 2})
        await self.engine.drain()
        self.assertEqual(self.store.counts(first.key)["uncertain"], 1)

    async def test_missing_platform_can_retry_without_duplicate(self):
        await self.engine.subscribe(chat())
        self.sender.return_value = False
        self.client.pages = [page([11])]
        await self.engine.tick()
        self.assertEqual(self.store.counts(chat().key), {"pending": 1})
        self.sender.return_value = True
        self.store.db.execute("UPDATE deliveries SET next_attempt=0"); self.store.db.commit()
        await self.engine.drain()
        self.assertEqual(self.store.counts(chat().key), {"sent": 1})

    async def test_off_during_send_cancels_all_not_started(self):
        await self.engine.subscribe(chat())
        async def send(row, text):
            self.engine.unsubscribe(chat())
            return True
        self.engine.sender = send
        self.client.pages = [page([11, 12])]
        await self.engine.tick()
        self.assertEqual(self.store.counts(chat().key), {"cancelled": 1, "sent": 1})

    async def test_cancel_during_send_quarantines_and_restart_will_not_repeat(self):
        await self.engine.subscribe(chat())
        self.store.ingest(page([11]))
        started = asyncio.Event()
        async def send(row, text):
            started.set()
            await asyncio.Future()
        self.engine.sender = send
        task = asyncio.create_task(self.engine.drain())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(self.store.counts(chat().key), {"uncertain": 1})

    async def test_latest_nth_exactly_one_and_cooldown(self):
        result = await self.engine.latest(chat(), 3)
        self.assertEqual(result.id, "98")
        self.assertIn(("latest", 3), self.client.calls)
        with self.assertRaises(FeedError) as error: await self.engine.latest(chat(sender="bob"), 1)
        self.assertEqual(error.exception.code, "query_cooldown")
        with self.assertRaises(FeedError): await self.engine.latest(chat(group="200"), 101)

    async def test_config_interval_change_applies_without_new_task(self):
        await self.engine.tick()
        self.engine._last_poll = time.monotonic() - 45
        await self.engine.tick()
        self.assertEqual(len(self.client.calls), 2)
        self.config["poll_interval_seconds"] = 30
        await self.engine.tick()
        self.assertEqual(len(self.client.calls), 3)
        self.config["poll_interval_seconds"] = 1
        self.assertEqual(self.engine.interval, 30)

    async def test_blocked_page_waits_at_least_server_delay(self):
        self.config["poll_interval_seconds"] = 30
        self.client.pages = [page(high=10, blocked=True)]
        await self.engine.tick()
        self.assertTrue(self.engine.blocked)
        self.assertGreater(self.engine._retry_at - time.monotonic(), 59)
        self.engine._last_poll = 0
        await self.engine.tick()
        self.assertEqual(len(self.client.calls), 2)

    async def test_auth_rejection_stops_sends_until_config_change(self):
        await self.engine.subscribe(chat())
        self.store.ingest(page([11]))
        self.client.error = FeedError("unauthorized")
        await self.engine.tick()
        self.sender.assert_not_called()
        self.assertTrue(self.engine._auth_failed)
        count = len(self.client.calls)
        await self.engine.tick()
        self.assertEqual(len(self.client.calls), count)
        replacement = FakeClient(); replacement.head = 11
        self.engine.client_factory = lambda source: replacement
        self.config["feed_url"] = "replacement-token"
        await self.engine.tick()
        self.assertTrue(self.client.closed)
        self.assertEqual(self.sender.await_count, 1)

    async def test_http429_query_respects_shared_backoff(self):
        self.client.error = FeedError("rate_limited", 600)
        with self.assertRaises(FeedError): await self.engine.latest(chat(), 1)
        count = len(self.client.calls)
        await self.engine.tick()
        self.assertEqual(len(self.client.calls), count)
        self.assertGreater(self.engine._retry_at - time.monotonic(), 599)

    async def test_pagination_and_checkpoint_before_fanout(self):
        await self.engine.subscribe(chat())
        self.client.pages = [page([11], more=True), page([12])]
        async def send(row, text):
            self.assertEqual(self.store.state()["high_water"], 12)
            return True
        self.engine.sender = send
        await self.engine.tick()
        self.assertEqual(self.store.counts(chat().key), {"sent": 2})

    async def test_off_on_during_send_false_never_requeues_pre_boundary(self):
        await self.engine.subscribe(chat())
        self.store.ingest(page([11, 12]))
        self.client.head = 12
        async def send(row, text):
            self.engine.unsubscribe(chat())
            await self.engine.subscribe(chat())
            return False
        self.engine.sender = send
        await self.engine.drain()
        self.assertEqual(self.store.subscription(chat().key)["boundary"], 12)
        self.assertEqual(self.store.pending(), [])
        self.assertEqual(self.store.counts(chat().key), {"cancelled": 2})

    async def test_pending_delivery_runs_between_polls_and_during_transient_failure(self):
        await self.engine.subscribe(chat())
        self.store.ingest(page([11]))
        self.engine._last_poll = time.monotonic()
        await self.engine.tick()
        self.assertEqual(self.sender.await_count, 1)
        self.store.ingest(page([12]))
        self.client.error = FeedError("network_error")
        self.engine._last_poll = 0
        await self.engine.tick()
        self.assertEqual(self.sender.await_count, 2)
        self.assertEqual(self.store.counts(chat().key), {"sent": 2})

    async def test_later_off_cancels_in_progress_first_on(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        original = self.client.bootstrap
        async def slow_bootstrap():
            entered.set()
            await release.wait()
            return await original()
        self.client.bootstrap = slow_bootstrap
        pending = asyncio.create_task(self.engine.subscribe(chat()))
        await entered.wait()
        self.engine.unsubscribe(chat())
        release.set()
        with self.assertRaises(FeedError) as error:
            await pending
        self.assertEqual(error.exception.code, "subscription_cancelled")
        self.assertIsNone(self.store.subscription(chat().key))
        self.assertEqual(self.engine._pending_ons, {})
        self.assertTrue(await self.engine.subscribe(chat()))

    async def test_config_source_change_during_request_is_rejected(self):
        original = self.client.bootstrap
        async def changed_source():
            self.config["feed_url"] = "new-source"
            return await original()
        self.client.bootstrap = changed_source
        with self.assertRaises(FeedError) as error:
            await self.engine.subscribe(chat())
        self.assertEqual(error.exception.code, "feed_changed")
        self.assertIsNone(self.store.state())
        self.assertFalse(self.engine._verified)

    async def test_environment_credential_route_has_precedence(self):
        with patch.dict("os.environ", {"TIBO_ASTRBOT_FEED_URL": "env-source"}):
            await self.engine.tick()
            self.assertEqual(self.engine._source, "env-source")
        self.assertNotIn("env-source", repr(dict(self.store.state())))

    async def test_start_twice_stop_cleans_lifecycle(self):
        # Restore real sleep to allow the actual background task to idle.
        self.sleep_patch.stop()
        self.engine.start(); original = self.engine._task
        self.engine.start()
        self.assertIs(self.engine._task, original)
        await asyncio.sleep(0.01)
        await self.engine.stop()
        self.assertIsNone(self.engine._task)
        self.assertTrue(self.client.closed)
        self.sleep_patch.start()
