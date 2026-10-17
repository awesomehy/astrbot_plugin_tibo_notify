"""SQLite receive checkpoint and per-chat outbox committed together.

No network await occurs in a transaction. A crashed in-flight send is ambiguous
and must never be automatically retried.
"""
import sqlite3
import time
from pathlib import Path

from .models import Chat, FeedError, FeedItem, FeedPage


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS state (
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),
          feed_id TEXT NOT NULL, cursor TEXT NOT NULL, high_water INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS items (
          feed_id TEXT NOT NULL, item_id TEXT NOT NULL, sequence INTEGER NOT NULL,
          payload TEXT NOT NULL, PRIMARY KEY(feed_id,item_id), UNIQUE(feed_id,sequence)
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
          feed_id TEXT NOT NULL, chat_key TEXT NOT NULL, umo TEXT NOT NULL,
          platform_id TEXT NOT NULL, bot_id TEXT NOT NULL, kind TEXT NOT NULL,
          target_id TEXT NOT NULL, boundary INTEGER NOT NULL,
          active INTEGER NOT NULL DEFAULT 1, blocked INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(feed_id,chat_key)
        );
        CREATE TABLE IF NOT EXISTS deliveries (
          feed_id TEXT NOT NULL, chat_key TEXT NOT NULL, item_id TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('pending','inflight','sent','uncertain','cancelled')),
          attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
          updated_at REAL NOT NULL,
          PRIMARY KEY(feed_id,chat_key,item_id),
          FOREIGN KEY(feed_id,item_id) REFERENCES items(feed_id,item_id),
          FOREIGN KEY(feed_id,chat_key) REFERENCES subscriptions(feed_id,chat_key)
        );
        CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(status,next_attempt);
        """)
        with self.db:
            self.db.execute("""UPDATE subscriptions SET blocked=1 WHERE EXISTS (
                SELECT 1 FROM deliveries d WHERE d.feed_id=subscriptions.feed_id
                AND d.chat_key=subscriptions.chat_key AND d.status='inflight'
                AND EXISTS(SELECT 1 FROM items i WHERE i.feed_id=d.feed_id
                AND i.item_id=d.item_id AND i.sequence>subscriptions.boundary))""")
            self.db.execute("UPDATE deliveries SET status='uncertain' WHERE status='inflight'")

    def close(self):
        self.db.close()

    def state(self):
        row = self.db.execute("SELECT * FROM state WHERE singleton=1").fetchone()
        return dict(row) if row else None

    def _cache(self, feed_id: str, item: FeedItem):
        existing = self.db.execute(
            "SELECT sequence FROM items WHERE feed_id=? AND item_id=?", (feed_id, item.id)
        ).fetchone()
        if existing and existing["sequence"] != item.sequence:
            raise FeedError("invalid_response")
        self.db.execute(
            "INSERT INTO items VALUES(?,?,?,?) ON CONFLICT(feed_id,item_id) DO UPDATE SET payload=excluded.payload",
            (feed_id, item.id, item.sequence, item.dumps()),
        )

    def bootstrap(self, page: FeedPage) -> bool:
        """Return True on account/feed change. Never reset an existing cursor."""
        current = self.state()
        changed = bool(current and current["feed_id"] != page.feed_id)
        with self.db:
            if changed:
                self.db.execute("UPDATE subscriptions SET active=0")
                self.db.execute("UPDATE deliveries SET status='cancelled' WHERE status='pending'")
            if not current or changed:
                self.db.execute("INSERT OR REPLACE INTO state VALUES(1,?,?,?)",
                                (page.feed_id, page.cursor, page.high_water))
            elif page.high_water < current["high_water"]:
                raise FeedError("feed_rewound")
            for item in page.items:
                self._cache(page.feed_id, item)
        return changed

    def subscription(self, key: str):
        row = self.db.execute("""SELECT s.* FROM subscriptions s JOIN state x
            ON s.feed_id=x.feed_id WHERE s.chat_key=?""", (key,)).fetchone()
        return dict(row) if row else None

    def subscribe(self, chat: Chat, page: FeedPage, maximum: int = 200) -> bool:
        current = self.state()
        if not current or current["feed_id"] != page.feed_id:
            raise FeedError("feed_changed")
        old = self.subscription(chat.key)
        if old and old["active"]:
            # Member-specific UMO must not create another subscription or move boundary.
            with self.db:
                self.db.execute("UPDATE subscriptions SET umo=? WHERE feed_id=? AND chat_key=?",
                                (chat.umo, page.feed_id, chat.key))
            return False
        count = self.db.execute("SELECT COUNT(*) FROM subscriptions WHERE active=1").fetchone()[0]
        if count >= maximum:
            raise FeedError("subscription_limit")
        with self.db:
            self.db.execute("""INSERT INTO subscriptions VALUES(?,?,?,?,?,?,?,?,1,0)
                ON CONFLICT(feed_id,chat_key) DO UPDATE SET umo=excluded.umo,
                boundary=excluded.boundary,active=1,blocked=0""",
                (page.feed_id, chat.key, chat.umo, chat.platform_id, chat.bot_id,
                 chat.kind, chat.target_id, page.high_water))
        return True

    def refresh_route(self, chat: Chat):
        with self.db:
            self.db.execute("UPDATE subscriptions SET umo=? WHERE feed_id=(SELECT feed_id FROM state) AND chat_key=?",
                            (chat.umo, chat.key))

    def unsubscribe(self, key: str) -> bool:
        old = self.subscription(key)
        with self.db:
            self.db.execute("UPDATE subscriptions SET active=0 WHERE chat_key=?", (key,))
            self.db.execute("UPDATE deliveries SET status='cancelled' WHERE chat_key=? AND status='pending'", (key,))
        return bool(old and old["active"])

    def ingest(self, page: FeedPage):
        current = self.state()
        if not current or page.feed_id != current["feed_id"]:
            raise FeedError("feed_changed")
        if page.high_water < current["high_water"]:
            raise FeedError("feed_rewound")
        expected_high = page.items[-1].sequence if page.items else current["high_water"]
        if page.high_water != expected_high:
            raise FeedError("invalid_response")
        if page.has_more and page.cursor == current["cursor"]:
            raise FeedError("invalid_response")
        now = time.time()
        with self.db:
            for item in page.items:
                self._cache(page.feed_id, item)
                # A replayed/overlapping page cannot create new outbox rows.
                if item.sequence <= current["high_water"]:
                    continue
                self.db.execute("""INSERT OR IGNORE INTO deliveries
                    (feed_id,chat_key,item_id,status,updated_at)
                    SELECT feed_id,chat_key,?,'pending',? FROM subscriptions
                    WHERE feed_id=? AND active=1 AND boundary<?""",
                    (item.id, now, page.feed_id, item.sequence))
            self.db.execute("UPDATE state SET cursor=?,high_water=? WHERE singleton=1",
                            (page.cursor, page.high_water))

    def pending(self, maximum=20):
        return [dict(row) for row in self.db.execute("""SELECT d.*,i.payload,i.sequence,s.umo,
            s.platform_id,s.bot_id,s.kind,s.target_id FROM deliveries d
            JOIN items i USING(feed_id,item_id) JOIN subscriptions s USING(feed_id,chat_key)
            JOIN state x ON x.feed_id=d.feed_id
            WHERE d.status='pending' AND d.next_attempt<=? AND s.active=1 AND s.blocked=0 AND i.sequence>s.boundary
            ORDER BY i.sequence,d.chat_key LIMIT ?""", (time.time(), maximum))]

    def claim(self, row: dict) -> bool:
        with self.db:
            result = self.db.execute("""UPDATE deliveries SET status='inflight',attempts=attempts+1,updated_at=?
                WHERE feed_id=? AND chat_key=? AND item_id=? AND status='pending'
                AND EXISTS(SELECT 1 FROM subscriptions s WHERE s.feed_id=deliveries.feed_id
                AND s.chat_key=deliveries.chat_key AND s.active=1 AND s.blocked=0
                AND EXISTS(SELECT 1 FROM items i WHERE i.feed_id=deliveries.feed_id
                AND i.item_id=deliveries.item_id AND i.sequence>s.boundary))""",
                (time.time(), row["feed_id"], row["chat_key"], row["item_id"]))
        return result.rowcount == 1

    def finish(self, row: dict, outcome: str):
        if outcome not in ("sent", "pending", "uncertain"):
            raise ValueError("invalid outcome")
        delay = min(300, 5 * 2 ** min(row["attempts"], 6)) if outcome == "pending" else 0
        with self.db:
            active = self.subscription(row["chat_key"])
            stale = not active or not active["active"] or row["sequence"] <= active["boundary"]
            if outcome == "pending" and stale:
                outcome = "cancelled"
            self.db.execute("""UPDATE deliveries SET status=?,next_attempt=?,updated_at=?
                WHERE feed_id=? AND chat_key=? AND item_id=? AND status='inflight'""",
                (outcome, time.time() + delay, time.time(), row["feed_id"], row["chat_key"], row["item_id"]))
            if outcome == "uncertain" and not stale:
                self.db.execute("UPDATE subscriptions SET blocked=1 WHERE feed_id=? AND chat_key=?",
                                (row["feed_id"], row["chat_key"]))

    def counts(self, key: str):
        return {row["status"]: row["n"] for row in self.db.execute("""SELECT status,COUNT(*) n FROM deliveries
            WHERE feed_id=(SELECT feed_id FROM state) AND chat_key=? GROUP BY status""", (key,))}

    def prune(self, keep=1000):
        """Keep newest cache plus every unresolved delivery; checkpoints preserve dedup."""
        with self.db:
            self.db.execute("""DELETE FROM deliveries WHERE status IN ('sent','cancelled')
                AND (feed_id,item_id) IN (SELECT feed_id,item_id FROM items
                ORDER BY sequence DESC LIMIT -1 OFFSET ?)""", (keep,))
            self.db.execute("""DELETE FROM items WHERE rowid IN (SELECT i.rowid FROM items i
                WHERE NOT EXISTS(SELECT 1 FROM deliveries d WHERE d.feed_id=i.feed_id AND d.item_id=i.item_id)
                ORDER BY i.sequence DESC LIMIT -1 OFFSET ?)""", (keep,))
