import unittest
import json
from unittest.mock import Mock
from helpers import page
from tibo_plugin.client import FeedClient
from tibo_plugin.models import FeedError, FEED_ENDPOINT


class Content:
    def __init__(self, chunks): self.chunks = chunks
    async def iter_chunked(self, size):
        for chunk in self.chunks: yield chunk


class Response:
    def __init__(self, status=200, chunks=None, headers=None):
        self.status = status
        self.content = Content(chunks or [b'{"version":1,"mode":"bootstrap","feedId":"feed-a","items":[],"nextCursor":"opaque","highWaterSequence":"0","hasMore":false,"blocked":null}'])
        self.headers = headers or {}
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass


class ClientTests(unittest.IsolatedAsyncioTestCase):
    def client(self, response):
        client = FeedClient(FEED_ENDPOINT + "#token=" + "a" * 43)
        client._session = Mock()
        client._session.get.return_value = response
        return client

    async def test_request_no_query_token_no_redirects(self):
        client = self.client(Response())
        result = await client.bootstrap()
        self.assertEqual(result.high_water, 0)
        args, kwargs = client._session.get.call_args
        self.assertEqual(args, (FEED_ENDPOINT,))
        self.assertEqual(kwargs["params"], {"mode": "bootstrap"})
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer " + "a" * 43)
        self.assertFalse(kwargs["allow_redirects"])

    async def test_error_body_never_exposed(self):
        for status, code in [(401,"unauthorized"),(403,"unauthorized"),(429,"rate_limited"),(503,"service_unavailable"),(302,"request_rejected")]:
            client = self.client(Response(status, [b'sensitive credentials'], {"Retry-After":"120"}))
            with self.subTest(status=status), self.assertRaises(FeedError) as error: await client.bootstrap()
            self.assertEqual(str(error.exception), code)
            if status == 429: self.assertEqual(error.exception.retry_after, 120)

    async def test_large_body_and_malformed_json_rejected(self):
        for chunks in ([b"x" * 5_000_001], [b"not-json"]):
            with self.assertRaises(FeedError) as error: await self.client(Response(chunks=chunks)).bootstrap()
            self.assertEqual(error.exception.code, "invalid_response")

    async def test_latest_bounds_and_request(self):
        client = self.client(Response())
        with self.assertRaises(FeedError): await client.latest(0)
        client._session.get.assert_not_called()

    async def test_valid_maximum_escaped_ten_item_page_fits(self):
        items = [dict(id=str(n), sequence=str(n), text="\x00" * 28000,
                      translation="\x00" * 49152, isResetAnnouncement=False,
                      publishedAt="2026-10-17T10:00:00.000Z",
                      url=f"https://x.com/example/status/{n}", model="test") for n in range(1, 11)]
        payload = dict(version=1, mode="incremental", feedId="feed-a", items=items,
                       highWaterSequence="10", nextCursor="cursor-10", hasMore=False, blocked=None)
        raw = json.dumps(payload).encode()
        self.assertLess(len(raw), 5_000_000)
        client = self.client(Response(chunks=[raw]))
        result = await client.incremental("cursor-0")
        self.assertEqual(len(result.items), 10)
        self.assertEqual(client._session.get.call_args.kwargs["params"]["limit"], 10)

    def test_retry_after_clamped(self):
        self.assertEqual(FeedClient.retry_after("9999999"), 86400)
        self.assertEqual(FeedClient.retry_after("not-a-date"), 60)
