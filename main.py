"""AstrBot 4.23+ entry point. Commands never invoke an LLM."""
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools, register

from .engine import Engine
from .models import Chat, FeedError, bounded_int
from .store import Store


ERRORS = {
    "not_configured": "请管理员先在插件设置中填写个人只读订阅链接。不要把链接发到聊天里。",
    "invalid_config": "订阅链接格式不正确，请管理员在插件设置中重新粘贴网站生成的完整链接。",
    "unauthorized": "订阅凭据已失效，请管理员在网站生成新链接并更新插件设置。",
    "rate_limited": "网站暂时限流，请稍后再试。",
    "retry_pending": "正在等待网站的重试间隔，请稍后再试。",
    "query_cooldown": "查询太频繁，请稍等几秒再试。",
    "invalid_index": "请输入 1 到设置上限之间的整数，例如 /tibo 3 只查最新第 3 条。",
    "unsupported_chat": "当前平台没有提供完整的机器人/会话标识，暂不能安全订阅。",
    "subscription_cancelled": "本次开启已被后来的 off 指令取消。",
    "subscription_limit": "订阅会话数量已达到插件设置上限。",
    "feed_changed": "订阅来源已变化，请管理员检查设置，再在当前聊天重新开启。",
    "feed_rewound": "网站订阅游标回退，已暂停接收。请管理员检查网站，不要直接删除本地状态。",
    "invalid_response": "网站返回的数据不符合订阅协议，已暂停本轮接收。",
    "request_rejected": "网站拒绝了订阅请求，请管理员检查版本和凭据。",
    "network_error": "暂时连接不上网站，请稍后再试。",
    "service_unavailable": "网站订阅服务暂不可用，请稍后再试。",
    "local_error": "本地状态暂不可用，请管理员检查磁盘和插件状态。",
}


def _list(config, name):
    value = config.get(name, [])
    return {str(item) for item in value} if isinstance(value, list) else set()


@register("astrbot_plugin_tibo_notify", "awesomehy", "Tibo 中文动态订阅与查询", "1.0.0")
class TiboNotifyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._store = None
        self.engine = None

    async def initialize(self):
        if self.engine is not None:
            return
        self._store = Store(StarTools.get_data_dir("astrbot_plugin_tibo_notify") / "state.sqlite3")
        self.engine = Engine(self._store, self.config, self._send, logger)
        self.engine.start()

    async def terminate(self):
        if self.engine is not None:
            await self.engine.stop()
            self.engine = None
        if self._store is not None:
            self._store.close()
            self._store = None

    async def _send(self, row, text):
        # Use the exact observed UMO, never synthesize a routing string from the key.
        return await self.context.send_message(row["umo"], MessageChain().message(text))

    def _operator(self, event, chat):
        return event.is_admin() or f"{chat.platform_id}:{chat.sender_id}" in _list(self.config, "operator_ids")

    def _can_manage(self, event, chat):
        if self._operator(event, chat):
            return True
        return chat.kind == "private" and bool(self.config.get("allow_private_subscriptions", True))

    def _can_query(self, event, chat):
        if self._operator(event, chat):
            return True
        if f"{chat.platform_id}:{chat.sender_id}" in _list(self.config, "query_user_ids"):
            return True
        access = self.config.get("query_access", "subscribed_chats")
        if access == "everyone":
            return True
        subscription = self._store.subscription(chat.key)
        return access == "subscribed_chats" and bool(subscription and subscription["active"])

    @staticmethod
    def _claim_command(event):
        event.should_call_llm(False)
        event.stop_event()

    @staticmethod
    def _reply(event, text):
        # 4.23.0 replaces event._result when a handler yields a result.
        # Carry STOP on the yielded result as well as suppressing default LLM.
        return event.plain_result(text).stop_event()

    @staticmethod
    def _too_many_args(event):
        # AstrBot's command parser can ignore surplus positional arguments.
        return len(event.get_message_str().strip().split()) > 2

    @filter.command("tibo-notify")
    async def notify_command(self, event: AstrMessageEvent, action: str = "status"):
        """/tibo-notify on|off|status：管理当前聊天订阅。"""
        self._claim_command(event)
        try:
            if self.engine is None:
                raise FeedError("local_error")
            if self._too_many_args(event) or action not in ("on", "off", "status"):
                yield self._reply(event, "用法：/tibo-notify on、/tibo-notify off、/tibo-notify status")
                return
            chat = Chat.from_event(event)
            if action != "status" and not self._can_manage(event, chat):
                yield self._reply(event, "群订阅只能由 AstrBot 管理员或插件设置中的指定操作员管理。")
                return
            if action == "on":
                changed = await self.engine.subscribe(chat)
                subscription = self._store.subscription(chat.key)
                if subscription and subscription["blocked"]:
                    message = "当前聊天已订阅，但有发送结果不确定，已暂停。核对聊天记录后先 off 再 on，可从现在恢复；旧消息不会自动重发。"
                elif changed:
                    message = "已订阅当前聊天，从现在起接收网站新完成处理的 Tibo 动态，不补发历史。"
                else:
                    message = "当前聊天已经订阅，起点保持不变。"
                yield self._reply(event, message)
            elif action == "off":
                self.engine.unsubscribe(chat)
                yield self._reply(event, "已关闭当前聊天的订阅，并取消尚未发送的消息；已提交平台的消息可能仍会到达。")
            else:
                if not self._can_query(event, chat) and not self._can_manage(event, chat):
                    yield self._reply(event, "当前聊天尚未开放 Tibo 查询。请由管理员开启订阅。")
                    return
                subscription = self._store.subscription(chat.key)
                enabled = bool(subscription and subscription["active"])
                counts = self._store.counts(chat.key)
                lines = [f"当前聊天：{'已订阅' if enabled else '未订阅'}", f"轮询间隔：{self.engine.interval} 秒",
                         f"等待发送：{counts.get('pending', 0)} 条", f"结果不确定：{counts.get('uncertain', 0)} 条"]
                if subscription and subscription["blocked"]:
                    lines.append("发送已暂停：请核对聊天记录后 off → on，从当前起点恢复。")
                if self.engine.blocked:
                    lines.append("网站正在重处理较早的动态，增量游标暂缓，稍后自动检查。")
                if self.engine.last_error:
                    lines.append(ERRORS.get(self.engine.last_error, ERRORS["local_error"]))
                lines.append("平台返回成功不代表微信送达回执；请以实际聊天记录为准。")
                yield self._reply(event, "\n".join(lines))
        except FeedError as error:
            yield self._reply(event, ERRORS.get(error.code, ERRORS["local_error"]))
        except Exception:
            logger.warning("Tibo command failed with a local error.")
            yield self._reply(event, ERRORS["local_error"])

    @filter.command("tibo")
    async def query_command(self, event: AstrMessageEvent, index: str = "1"):
        """/tibo N：查询网站最新第 N 条已完成动态，只返回一条。"""
        self._claim_command(event)
        try:
            if self.engine is None:
                raise FeedError("local_error")
            if self._too_many_args(event) or not index.isascii() or not index.isdecimal() or len(index) > 3:
                raise FeedError("invalid_index")
            n = int(index)
            if not 1 <= n <= bounded_int(self.config, "max_query_index", 100, 1, 100):
                raise FeedError("invalid_index")
            chat = Chat.from_event(event)
            if not self._can_query(event, chat):
                yield self._reply(event, "当前聊天尚未开放 Tibo 查询。请由管理员开启订阅，或配置查询白名单。")
                return
            item = await self.engine.latest(chat, n)
            # A changed account can invalidate the old chat subscription during
            # feed verification. Never inherit its query access into the new feed.
            if not self._can_query(event, chat):
                yield self._reply(event, "订阅来源已变化，请管理员在当前聊天重新开启订阅后再查询。")
                return
            if item is None:
                yield self._reply(event, f"目前没有最新第 {n} 条已完成处理的动态。")
            else:
                position = f"\n最新第 {n} 条"
                rendered = item.render(maximum=3500 - len(position))
                heading, separator, rest = rendered.partition("\n")
                yield self._reply(event, heading + position + separator + rest)
        except FeedError as error:
            yield self._reply(event, ERRORS.get(error.code, ERRORS["local_error"]))
        except Exception:
            logger.warning("Tibo query failed with a local error.")
            yield self._reply(event, ERRORS["local_error"])
