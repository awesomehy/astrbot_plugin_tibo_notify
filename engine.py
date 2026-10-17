"""One feed receiver per instance, durable outbox, and bounded fanout."""
import asyncio
import contextlib
import os
import time
from collections import OrderedDict
from typing import Awaitable, Callable

from .client import FeedClient
from .models import Chat, FeedError, FeedItem, bounded_int
from .store import Store


class Engine:
    def __init__(self, store: Store, config: dict,
                 sender: Callable[[dict, str], Awaitable[bool]], logger,
                 client_factory=FeedClient):
        self.store, self.config, self.sender, self.logger = store, config, sender, logger
        self.client_factory = client_factory
        self.client = None
        self._source = None
        self._verified = False
        self._task = None
        self._operation_lock = asyncio.Lock()
        self._query_lock = asyncio.Lock()
        self._cooldowns = OrderedDict()
        self._pending_ons = {}
        self._last_query = 0.0
        self.last_error = ""
        self.blocked = False
        self.last_success = None
        self._last_poll = 0.0
        self._retry_at = 0.0
        self._failures = 0
        self._auth_failed = False

    @property
    def interval(self):
        return bounded_int(self.config, "poll_interval_seconds", 300, 30, 86400)

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="tibo-feed-receiver")

    async def stop(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        async with self._operation_lock:
            if self.client:
                await self.client.close()
                self.client = None
            self._verified = False

    def _source_value(self):
        # AstrBot 4.23 does not mask secret schema fields; permit a fixed-name
        # administrator-managed environment variable with no secret in WebUI.
        return (os.environ.get("TIBO_ASTRBOT_FEED_URL", "").strip()
                or str(self.config.get("feed_url", "") or "").strip())

    async def _sync_client(self):
        value = self._source_value()
        if self._source != value:
            if self.client:
                await self.client.close()
            self.client = None
            self._source = value
            self._verified = False
            self._auth_failed = False
            self._retry_at = 0
            self._failures = 0
            self._last_poll = 0
            self.blocked = False
        if not value:
            raise FeedError("not_configured")
        if self.client is None:
            self.client = self.client_factory(value)
        if self._auth_failed:
            raise FeedError("unauthorized")

    def _assert_source_current(self):
        if self._source != self._source_value():
            self._verified = False
            raise FeedError("feed_changed")

    async def _verify(self):
        await self._sync_client()
        if not self._verified:
            page = await self.client.bootstrap()
            self._assert_source_current()
            changed = self.store.bootstrap(page)
            self._verified = True
            if changed:
                self.logger.warning("Tibo feed identity changed; subscriptions disabled, enable again in each chat.")
            return page
        return None

    def _record_failure(self, error: FeedError):
        # Log only locally defined codes. Never log exceptions/URLs/tokens/responses.
        if self.last_error != error.code:
            self.logger.warning("Tibo feed status: %s", error.code)
        self.last_error = error.code
        self._failures += 1
        self._auth_failed = error.code == "unauthorized"
        delay = max(error.retry_after, min(3600, self.interval * 2 ** min(self._failures - 1, 5)))
        self._retry_at = time.monotonic() + delay

    async def _run(self):
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # SQLite/adapter/programming exceptions must not kill the receiver or reveal secrets.
                self._record_failure(FeedError("local_error"))
            await asyncio.sleep(1)

    async def tick(self):
        await self._poll()
        # Delivery has its own cadence. A transient feed outage must not strand
        # already durable pending messages, but revoked/invalid feeds pause it.
        if self.last_error in ("", "network_error", "service_unavailable", "rate_limited"):
            await self.drain()
        self.store.prune()

    async def _poll(self):
        async with self._operation_lock:
            try:
                await self._sync_client()
                now = time.monotonic()
                if now < self._retry_at:
                    return
                if self._last_poll and now - self._last_poll < self.interval:
                    return
                self._last_poll = now
                await self._verify()
                # Bounded pagination: remaining backlog continues on the next cycle.
                for _ in range(5):
                    state = self.store.state()
                    page = await self.client.incremental(state["cursor"])
                    self._assert_source_current()
                    self.store.ingest(page)
                    self.blocked = page.blocked
                    if not page.has_more:
                        break
                self.last_success = time.time()
                self.last_error = ""
                self._failures = 0
                self._retry_at = time.monotonic() + page.retry_after if page.blocked else 0
            except FeedError as error:
                self._record_failure(error)
                return

    async def drain(self):
        for row in self.store.pending(20):
            # A configuration change never leaks queued data into a different feed.
            if (not self._verified or self._source != self._source_value()
                    or self._auth_failed):
                return
            if not self.store.claim(row):
                continue
            try:
                result = await asyncio.wait_for(self.sender(row, FeedItem.loads(row["payload"]).render()), timeout=20)
            except asyncio.CancelledError:
                self.store.finish(row, "uncertain")
                raise
            except Exception:
                self.store.finish(row, "uncertain")
                self.logger.warning("Tibo delivery outcome uncertain; chat paused until off/on.")
            else:
                # False means no matching platform, definitely not sent; True is NOT a receipt.
                self.store.finish(row, "sent" if result is True else "pending" if result is False else "uncertain")
            await asyncio.sleep(0.5)

    async def _guarded_call(self, callback, *args):
        try:
            return await callback(*args)
        except FeedError as error:
            if error.code in {"unauthorized", "rate_limited", "network_error", "service_unavailable", "invalid_response"}:
                self._record_failure(error)
            raise

    async def subscribe(self, chat: Chat):
        ticket = {"cancelled": False}
        tickets = self._pending_ons.setdefault(chat.key, [])
        tickets.append(ticket)
        try:
            return await self._guarded_call(self._subscribe, chat, ticket)
        finally:
            tickets[:] = [pending for pending in tickets if pending is not ticket]
            if not tickets:
                self._pending_ons.pop(chat.key, None)

    async def _subscribe(self, chat: Chat, ticket):
        async with self._operation_lock:
            await self._sync_client()
            if time.monotonic() < self._retry_at:
                raise FeedError("retry_pending")
            initial = await self._verify()
            if ticket["cancelled"]:
                raise FeedError("subscription_cancelled")
            old = self.store.subscription(chat.key)
            if old and old["active"]:
                self.store.refresh_route(chat)
                return False
            if time.monotonic() < self._retry_at:
                raise FeedError("retry_pending")
            # First on uses a fresh server boundary, not a stale polling checkpoint.
            page = initial or await self.client.bootstrap()
            self._assert_source_current()
            if ticket["cancelled"]:
                raise FeedError("subscription_cancelled")
            self.store.bootstrap(page)
            return self.store.subscribe(chat, page,
                                        bounded_int(self.config, "max_subscriptions", 200, 1, 2000))

    def unsubscribe(self, chat: Chat):
        for ticket in self._pending_ons.get(chat.key, ()):
            ticket["cancelled"] = True
        return self.store.unsubscribe(chat.key)

    async def latest(self, chat: Chat, n: int):
        return await self._guarded_call(self._latest, chat, n)

    async def _latest(self, chat: Chat, n: int):
        maximum = bounded_int(self.config, "max_query_index", 100, 1, 100)
        if not 1 <= n <= maximum:
            raise FeedError("invalid_index")
        async with self._query_lock:
            now = time.monotonic()
            cooldown = bounded_int(self.config, "query_cooldown_seconds", 5, 1, 3600)
            # Shared chat cooldown prevents group members collectively hammering the feed.
            if now - self._cooldowns.get(chat.key, -1e10) < cooldown or now - self._last_query < 1:
                raise FeedError("query_cooldown")
            self._cooldowns[chat.key] = now
            self._cooldowns.move_to_end(chat.key)
            while len(self._cooldowns) > 5000:
                self._cooldowns.popitem(last=False)
            self._last_query = now
            async with self._operation_lock:
                await self._sync_client()
                if time.monotonic() < self._retry_at:
                    raise FeedError("retry_pending")
                await self._verify()
                page = await self.client.latest(n)
                self._assert_source_current()
                if page.feed_id != self.store.state()["feed_id"]:
                    raise FeedError("feed_changed")
                return page.items[0] if page.items else None
