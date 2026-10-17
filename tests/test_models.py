import json
import unittest
from helpers import ROOT, Event, chat, item
from tibo_plugin.models import Chat, FeedError, FeedPage, FEED_ENDPOINT, parse_feed_url, bounded_int


class ModelTests(unittest.TestCase):
    def envelope(self):
        return dict(version=1, mode="incremental", feedId="opaque-feed", items=[dict(id="1", sequence="1",
                    text="original", translation="译文", isResetAnnouncement=False,
                    publishedAt="2026-10-17T10:00:00Z", url="https://x.com/example/status/1", model="test")],
                    nextCursor="opaque-cursor", highWaterSequence="1", hasMore=False, blocked=None)

    def test_contract(self):
        value = FeedPage.parse(self.envelope(), "incremental")
        self.assertEqual(value.items[0].sequence, 1)
        self.assertFalse(value.items[0].is_reset_announcement)

    def test_invalid_pages(self):
        variants = []
        for key, value in (("version", 2), ("highWaterSequence", 1), ("highWaterSequence", "0"),
                           ("hasMore", 1), ("nextCursor", None), ("feedId", "")):
            payload = self.envelope(); payload[key] = value; variants.append(payload)
        payload = self.envelope(); payload["items"][0]["isResetAnnouncement"] = "false"; variants.append(payload)
        payload = self.envelope(); payload["items"][0]["translation"] = ""; variants.append(payload)
        payload = self.envelope(); payload["items"] *= 2; variants.append(payload)
        for payload in variants:
            with self.subTest(payload=payload), self.assertRaises(FeedError):
                FeedPage.parse(payload, "incremental")

    def test_blocked_contract(self):
        payload = self.envelope()
        payload["blocked"] = dict(reason="item_not_ready", retryAfterSeconds=60)
        parsed = FeedPage.parse(payload, "incremental")
        self.assertTrue(parsed.blocked)
        self.assertEqual(parsed.retry_after, 60)
        payload["hasMore"] = True
        with self.assertRaises(FeedError): FeedPage.parse(payload, "incremental")

    def test_secret_fragment_removed(self):
        token = "tibo_astrbot_" + "a" * 43
        endpoint, parsed = parse_feed_url(FEED_ENDPOINT + "#token=" + token)
        self.assertEqual(endpoint, FEED_ENDPOINT)
        self.assertEqual(parsed, token)
        self.assertNotIn(token, endpoint)

    def test_unsafe_urls_rejected(self):
        urls = [FEED_ENDPOINT + "?token=" + "a" * 43, FEED_ENDPOINT.replace("https:", "http:") + "#token=" + "a" * 43,
                FEED_ENDPOINT + "#token=secret&token=second", FEED_ENDPOINT + "#token=" + "a" * 43 + "&other=x",
                FEED_ENDPOINT.replace("tibo-monitor.hhyan.me", "example.com") + "#token=" + "a" * 43]
        for url in urls:
            with self.subTest(url=url), self.assertRaises(FeedError) as error: parse_feed_url(url)
            self.assertEqual(str(error.exception), "invalid_config")

    def test_group_members_same_identity_actual_route_preserved(self):
        alice, bob = chat(sender="alice"), chat(sender="bob")
        self.assertEqual(alice.key, bob.key)
        self.assertNotEqual(alice.umo, bob.umo)
        self.assertNotEqual(alice.key, chat(group="200").key)
        self.assertNotEqual(alice.key, chat(bot="bot-b").key)
        self.assertNotEqual(alice.key, chat(platform="onebot-b").key)

    def test_private_identity_and_missing_bot(self):
        self.assertNotEqual(chat(sender="alice", group="").key, chat(sender="bob", group="").key)
        with self.assertRaises(FeedError): Chat.from_event(Event(bot=""))

    def test_render_bounded_plain_translation(self):
        text = item(1, translation="中" * 6000).render()
        self.assertLessEqual(len(text), 3500)
        self.assertTrue(text.startswith("【重置预告：否】"))
        self.assertIn("原文\nOriginal", text)
        self.assertIn("内容已截断，完整内容见链接", text)
        self.assertIn("https://x.com/", text)
        self.assertEqual(item(1), type(item(1)).loads(item(1).dumps()))

    def test_render_judgment_first_original_translation_dates_and_model(self):
        rendered = item(1, is_reset_announcement=True, text="Full original text", translation="完整中文翻译",
                        published_at="2026-10-17T18:42:03.000Z", model="saved-model").render()
        self.assertTrue(rendered.startswith("【重置预告：是】"))
        self.assertIn("中文翻译\n完整中文翻译", rendered)
        self.assertIn("原文\nFull original text", rendered)
        self.assertIn("发布时间（UTC）：2026-10-17 18:42:03", rendered)
        self.assertIn("北京时间（UTC+8）：2026-10-18 02:42:03", rendered)
        self.assertIn("AI 判断仅供参考（网站已保存结果，插件未重新分析）", rendered)
        self.assertIn("翻译模型：saved-model", rendered)
        self.assertTrue(rendered.endswith("https://x.com/tibo_maker/status/1"))

    def test_render_both_long_fields_truncate_and_preserve_footer(self):
        rendered = item(1, text="English" * 5000, translation="中文翻译" * 10000).render()
        self.assertEqual(len(rendered), 3500)
        self.assertEqual(rendered.count("内容已截断，完整内容见链接"), 2)
        self.assertTrue(rendered.startswith("【重置预告：否】"))
        self.assertIn("发布时间（UTC）", rendered)
        self.assertIn("北京时间（UTC+8）", rendered)
        self.assertIn("翻译模型：test-model", rendered)
        self.assertTrue(rendered.endswith("https://x.com/tibo_maker/status/1"))

    def test_render_does_not_invent_missing_model_or_timezone(self):
        rendered = item(1, model="", published_at="unknown").render()
        self.assertIn("翻译模型：未提供", rendered)
        self.assertIn("发布时间（时区未确认）：unknown", rendered)
        self.assertNotIn("北京时间", rendered)

    def test_config_schema_and_bounds(self):
        schema = json.loads((ROOT / "_conf_schema.json").read_text())
        self.assertTrue(schema["feed_url"]["secret"])
        self.assertEqual(schema["poll_interval_seconds"]["default"], 300)
        self.assertEqual(schema["poll_interval_seconds"]["slider"]["max"], 86400)
        self.assertEqual(bounded_int({"x": 1}, "x", 300, 30, 86400), 30)
        self.assertEqual(bounded_int({"x": "bad"}, "x", 300, 30, 86400), 300)
