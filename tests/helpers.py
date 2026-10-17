import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if "tibo_plugin" not in sys.modules:
    spec = importlib.util.spec_from_file_location("tibo_plugin", ROOT / "__init__.py", submodule_search_locations=[str(ROOT)])
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

from tibo_plugin.models import Chat, FeedItem, FeedPage


def item(number, **kwargs):
    data = dict(id=str(number), sequence=number, text="Original", translation="中文内容", is_reset_announcement=False,
                published_at="2026-10-17T10:00:00.000Z", url=f"https://x.com/tibo_maker/status/{number}", model="test-model")
    data.update(kwargs)
    return FeedItem(**data)


def page(numbers=(), mode="incremental", high=None, feed="feed-a", more=False, blocked=False):
    values = tuple(item(n) for n in numbers)
    if high is None:
        high = max(numbers, default=0)
    return FeedPage(mode, feed, values, f"cursor-{high}", high, more, blocked, 60 if blocked else 0)


class PlainResult(str):
    def stop_event(self):
        self.stopped = True
        return self


class Event:
    def __init__(self, sender="alice", group="100", bot="bot-a", platform="onebot-a", admin=False, command="tibo-notify on"):
        self.sender, self.group, self.bot, self.platform = sender, group, bot, platform
        self.admin, self.command = admin, command
        self.unified_msg_origin = f"{platform}:{'GroupMessage' if group else 'FriendMessage'}:{sender}_{group}" if group else f"{platform}:FriendMessage:{sender}"
        self.stopped = False
        self.llm = True
    def get_platform_id(self): return self.platform
    def get_self_id(self): return self.bot
    def get_sender_id(self): return self.sender
    def get_group_id(self): return self.group
    def is_private_chat(self): return not self.group
    def is_admin(self): return self.admin
    def get_message_str(self): return self.command
    def should_call_llm(self, value): self.llm = value
    def stop_event(self): self.stopped = True
    def plain_result(self, text): return PlainResult(text)


def chat(**kwargs):
    return Chat.from_event(Event(**kwargs))


class FakeClient:
    def __init__(self, source="unused"):
        self.head = 10
        self.feed = "feed-a"
        self.calls = []
        self.pages = []
        self.closed = False
        self.error = None
    async def close(self): self.closed = True
    async def bootstrap(self):
        self.calls.append(("bootstrap",))
        if self.error: raise self.error
        return page(mode="bootstrap", high=self.head, feed=self.feed)
    async def incremental(self, cursor):
        self.calls.append(("incremental", cursor))
        if self.error: raise self.error
        return self.pages.pop(0) if self.pages else page(high=self.head, feed=self.feed)
    async def latest(self, n):
        self.calls.append(("latest", n))
        if self.error: raise self.error
        return page([101 - n], mode="latest", high=100, feed=self.feed)
