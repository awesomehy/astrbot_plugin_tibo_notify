"""Validated feed contract and chat identity; independent of AstrBot."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

FEED_ENDPOINT = "https://tibo-monitor.hhyan.me/api/integrations/astrbot/feed"
MAX_SEQUENCE = 2**63 - 1


class FeedError(Exception):
    """A safe diagnostic: never include response content, URL or credentials."""

    def __init__(self, code: str, retry_after: float = 0):
        super().__init__(code)
        self.code = code
        self.retry_after = retry_after


def sequence(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]{0,18}", value):
        raise FeedError("invalid_response")
    result = int(value)
    if result > MAX_SEQUENCE:
        raise FeedError("invalid_response")
    return result


def parse_feed_url(value: str) -> tuple[str, str]:
    """Only the official HTTPS endpoint; fragments never cross the network."""
    try:
        parts = urlsplit(value.strip())
        endpoint = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        fragments = parse_qs(parts.fragment, strict_parsing=True)
        if endpoint != FEED_ENDPOINT or parts.query or set(fragments) != {"token"}:
            raise ValueError
        tokens = fragments["token"]
        if len(tokens) != 1 or not re.fullmatch(r"[A-Za-z0-9_-]{16,1024}", tokens[0]):
            raise ValueError
        return endpoint, tokens[0]
    except (ValueError, TypeError, AttributeError):
        raise FeedError("invalid_config") from None


def _text(value: Any, maximum: int, required: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (required and not value):
        raise FeedError("invalid_response")
    return value


@dataclass(frozen=True)
class FeedItem:
    id: str
    sequence: int
    text: str
    translation: str
    is_reset_announcement: bool
    published_at: str
    url: str
    model: str

    @classmethod
    def parse(cls, data: dict) -> "FeedItem":
        try:
            if type(data["isResetAnnouncement"]) is not bool:
                raise FeedError("invalid_response")
            link = _text(data["url"], 2048, True)
            parsed = urlsplit(link)
            if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
                raise FeedError("invalid_response")
            return cls(
                _text(data["id"], 256, True), sequence(data["sequence"]),
                _text(data["text"], 50000), _text(data["translation"], 50000, True),
                data["isResetAnnouncement"], _text(data["publishedAt"], 80, True),
                link, _text(data["model"], 256),
            )
        except (KeyError, TypeError, ValueError):
            raise FeedError("invalid_response") from None

    def dumps(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def loads(cls, value: str) -> "FeedItem":
        return cls(**json.loads(value))

    def render(self, maximum: int = 3500) -> str:
        label = "是" if self.is_reset_announcement else "否"
        header = f"【重置预告：{label}】\n\n中文翻译\n"
        middle = "\n\n原文\n"
        try:
            published = datetime.fromisoformat(self.published_at.replace("Z", "+00:00"))
            if published.tzinfo is None:
                raise ValueError("missing timezone")
            utc = published.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            beijing = published.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
            dates = f"发布时间（UTC）：{utc}\n北京时间（UTC+8）：{beijing}"
        except (ValueError, OverflowError):
            dates = f"发布时间（时区未确认）：{self.published_at}"
        footer = (f"\n\n{dates}\nAI 判断仅供参考（网站已保存结果，插件未重新分析）"
                  f"\n翻译模型：{self.model or '未提供'}\n原文链接：{self.url}")
        marker = "…（内容已截断，完整内容见链接）"
        budget = maximum - len(header) - len(middle) - len(footer)
        if budget < len(marker) * 2:
            raise ValueError("message budget too small")
        translated = self.translation
        original = self.text or "（无正文）"
        translated_budget = budget // 2
        original_budget = budget - translated_budget
        if len(translated) < translated_budget:
            original_budget += translated_budget - len(translated)
            translated_budget = len(translated)
        if len(original) < original_budget:
            translated_budget += original_budget - len(original)
            original_budget = len(original)

        def bounded(content: str, allowance: int) -> str:
            if len(content) <= allowance:
                return content
            return content[:allowance - len(marker)] + marker

        return (header + bounded(translated, translated_budget) + middle
                + bounded(original, original_budget) + footer)


@dataclass(frozen=True)
class FeedPage:
    mode: str
    feed_id: str
    items: tuple[FeedItem, ...]
    cursor: str
    high_water: int
    has_more: bool
    blocked: bool = False
    retry_after: float = 0

    @classmethod
    def parse(cls, data: Any, mode: str) -> "FeedPage":
        try:
            if not isinstance(data, dict) or type(data["version"]) is not int or data["version"] != 1 or data["mode"] != mode:
                raise FeedError("invalid_response")
            if type(data["hasMore"]) is not bool or not isinstance(data["items"], list):
                raise FeedError("invalid_response")
            if len(data["items"]) > 50:
                raise FeedError("invalid_response")
            cursor = data["nextCursor"]
            if cursor is None and mode in ("latest", "history"):
                cursor = ""
            cursor = _text(cursor, 4096, mode in ("bootstrap", "incremental"))
            items = tuple(FeedItem.parse(i) for i in data["items"])
            high = sequence(data["highWaterSequence"])
            if any(item.sequence > high for item in items):
                raise FeedError("invalid_response")
            if mode == "incremental":
                seqs = [i.sequence for i in items]
                if seqs != sorted(set(seqs)) or (data["hasMore"] and not items):
                    raise FeedError("invalid_response")
            if len({i.id for i in items}) != len(items):
                raise FeedError("invalid_response")
            if mode == "latest" and len(items) > 1:
                raise FeedError("invalid_response")
            blocked = data.get("blocked")
            if blocked is not None and (
                not isinstance(blocked, dict) or blocked.get("reason") != "item_not_ready"
                or type(blocked.get("retryAfterSeconds")) not in (int, float)
                or not 1 <= blocked["retryAfterSeconds"] <= 86400 or data["hasMore"]
            ):
                raise FeedError("invalid_response")
            return cls(mode, _text(data["feedId"], 256, True), items, cursor, high,
                       data["hasMore"], blocked is not None,
                       float(blocked["retryAfterSeconds"]) if blocked else 0)
        except (KeyError, TypeError, ValueError):
            raise FeedError("invalid_response") from None


@dataclass(frozen=True)
class Chat:
    key: str
    umo: str
    platform_id: str
    bot_id: str
    kind: str
    target_id: str
    sender_id: str

    @classmethod
    def from_event(cls, event) -> "Chat":
        platform = str(event.get_platform_id() or "")
        bot = str(event.get_self_id() or "")
        sender = str(event.get_sender_id() or "")
        private = event.is_private_chat()
        target = sender if private else str(event.get_group_id() or "")
        umo = str(event.unified_msg_origin or "")
        if not all((platform, bot, sender, target, umo)):
            raise FeedError("unsupported_chat")
        kind = "private" if private else "group"
        identity = json.dumps([platform, bot, kind, target], separators=(",", ":"))
        key = hashlib.sha256(identity.encode()).hexdigest()
        return cls(key, umo, platform, bot, kind, target, sender)


def bounded_int(config: dict, key: str, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(high, int(config.get(key, default))))
    except (ValueError, TypeError):
        return default
