"""Read-only HTTP client. Credential-safe errors and redirects disabled."""
import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json

import aiohttp

from .models import FeedError, FeedPage, parse_feed_url


class FeedClient:
    def __init__(self, feed_url: str):
        self.endpoint, self._token = parse_feed_url(feed_url)
        self._session = None

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    @staticmethod
    def retry_after(value: str) -> float:
        try:
            return max(1, min(86400, float(value)))
        except (ValueError, TypeError):
            try:
                moment = parsedate_to_datetime(value)
                return max(1, min(86400, (moment - datetime.now(timezone.utc)).total_seconds()))
            except (TypeError, ValueError, OverflowError):
                return 60

    async def get(self, mode: str, **params) -> FeedPage:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20, connect=5),
                trust_env=False,
            )
        try:
            async with self._session.get(
                self.endpoint, params={"mode": mode, **params},
                headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"},
                allow_redirects=False,
            ) as response:
                if response.status in (401, 403):
                    raise FeedError("unauthorized")
                if response.status == 429:
                    raise FeedError("rate_limited", self.retry_after(response.headers.get("Retry-After", "60")))
                if response.status != 200:
                    raise FeedError("service_unavailable" if response.status >= 500 else "request_rejected")
                # Bound memory even for a chunked response (read(n) is not a full body read).
                raw = bytearray()
                async for chunk in response.content.iter_chunked(16384):
                    raw.extend(chunk)
                    if len(raw) > 5_000_000:
                        raise FeedError("invalid_response")
                try:
                    payload = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    raise FeedError("invalid_response") from None
                return FeedPage.parse(payload, mode)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            raise FeedError("network_error") from None

    async def bootstrap(self):
        return await self.get("bootstrap")

    async def incremental(self, cursor: str):
        return await self.get("incremental", cursor=cursor, limit=10)

    async def latest(self, n: int):
        if not 1 <= n <= 100:
            raise FeedError("invalid_index")
        return await self.get("latest", n=n)
