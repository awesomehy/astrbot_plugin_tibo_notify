import sqlite3
import tempfile
import unittest
from pathlib import Path
from helpers import chat, page
from tibo_plugin.models import FeedError
from tibo_plugin.store import Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state.sqlite3"
        self.store = Store(self.path)
        self.store.bootstrap(page(mode="bootstrap", high=10))
        self.chat = chat()

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def subscribe(self, target=None, high=10):
        return self.store.subscribe(target or self.chat, page(mode="bootstrap", high=high))

    def test_baseline_no_history_and_fresh_subscription_boundary(self):
        self.subscribe(high=12)
        self.store.ingest(page([11, 12, 13]))
        self.assertEqual([r["item_id"] for r in self.store.pending()], ["13"])
        self.assertEqual(self.store.state()["high_water"], 13)

    def test_repeated_on_keeps_boundary_and_deduplicates_group(self):
        self.assertTrue(self.subscribe())
        self.assertFalse(self.subscribe(chat(sender="bob"), high=20))
        self.assertEqual(self.store.subscription(self.chat.key)["boundary"], 10)
        self.store.ingest(page([11]))
        rows = self.store.pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["umo"], chat(sender="bob").umo)

    def test_ingestion_atomic_rollback_on_checkpoint_failure(self):
        self.subscribe()
        self.store.db.execute("CREATE TEMP TRIGGER fail_checkpoint BEFORE UPDATE ON state BEGIN SELECT RAISE(ABORT,'test'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.store.ingest(page([11, 12]))
        self.assertEqual(self.store.state()["high_water"], 10)
        self.assertEqual(self.store.pending(), [])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM items").fetchone()[0], 0)

    def test_ingestion_atomic_rollback_mid_item(self):
        self.subscribe()
        original = self.store._cache
        def fail(feed, item):
            original(feed, item)
            if item.sequence == 12: raise RuntimeError("injected")
        self.store._cache = fail
        with self.assertRaises(RuntimeError): self.store.ingest(page([11, 12]))
        self.assertEqual(self.store.state()["high_water"], 10)
        self.assertEqual(self.store.pending(), [])

    def test_restart_pending_survives_and_no_duplicate(self):
        self.subscribe()
        self.store.ingest(page([11]))
        self.store.close(); self.store = Store(self.path)
        self.store.bootstrap(page(mode="bootstrap", high=20))
        self.assertEqual(self.store.state()["high_water"], 11)
        self.store.ingest(page([11], high=11))
        self.assertEqual(len(self.store.pending()), 1)
        row = self.store.pending()[0]
        self.assertTrue(self.store.claim(row)); self.store.finish(row, "sent")
        self.store.ingest(page([11], high=11))
        self.assertEqual(self.store.pending(), [])

    def test_restart_inflight_quarantines_and_blocks_chat(self):
        self.subscribe(); self.store.ingest(page([11, 12]))
        self.store.claim(self.store.pending()[0])
        self.store.close(); self.store = Store(self.path)
        self.assertEqual(self.store.counts(self.chat.key), {"pending": 1, "uncertain": 1})
        self.assertTrue(self.store.subscription(self.chat.key)["blocked"])
        self.assertEqual(self.store.pending(), [])

    def test_off_cancels_unsent_and_resume_is_fresh(self):
        self.subscribe(); self.store.ingest(page([11, 12]))
        row = self.store.pending()[0]
        self.store.unsubscribe(self.chat.key)
        self.assertFalse(self.store.claim(row))
        self.assertEqual(self.store.pending(), [])
        self.subscribe(high=14)
        self.store.ingest(page([13, 14, 15]))
        self.assertEqual([r["item_id"] for r in self.store.pending()], ["15"])

    def test_chat_failure_isolation_and_maximum(self):
        other = chat(group="200")
        self.subscribe(); self.subscribe(other)
        self.store.ingest(page([11]))
        row = next(r for r in self.store.pending() if r["chat_key"] == self.chat.key)
        self.store.claim(row); self.store.finish(row, "uncertain")
        self.assertEqual([r["chat_key"] for r in self.store.pending()], [other.key])
        with self.assertRaises(FeedError):
            self.store.subscribe(chat(group="300"), page(mode="bootstrap", high=11), maximum=2)

    def test_changed_account_disables_old_chats(self):
        self.subscribe(); self.store.ingest(page([11]))
        self.assertTrue(self.store.bootstrap(page(mode="bootstrap", high=99, feed="feed-b")))
        self.assertEqual(self.store.pending(), [])
        self.assertIsNone(self.store.subscription(self.chat.key))
        self.assertEqual(self.store.state()["feed_id"], "feed-b")

    def test_regression_and_foreign_feed_refused(self):
        for payload in [page(high=9), page(high=11, feed="feed-b")]:
            with self.assertRaises(FeedError): self.store.ingest(payload)
        self.assertEqual(self.store.state()["high_water"], 10)

    def test_malformed_checkpoint_jump_is_rejected(self):
        for value in (page([11], high=99), page(high=99)):
            with self.assertRaises(FeedError): self.store.ingest(value)
        self.assertEqual(self.store.state()["high_water"], 10)

    def test_pruning_keeps_unresolved(self):
        self.subscribe(); self.store.ingest(page([11, 12, 13]))
        self.store.prune(keep=1)
        self.assertEqual(len(self.store.pending()), 3)
