from __future__ import annotations

from plugin.astrbot_compat import AstrMessageEvent


class MiscMixin:

    async def nerd_ping(self, event: AstrMessageEvent):
        """健康检查命令，返回当前 scope/session 与运行状态。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        self._debug_scope(mapping=ctx["mapping"], raw_message=ctx["raw_message"])

        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.news_search",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return

        yield self._make_event_result(
            event,
            f"Plugin online. scope={ctx['scope_key']}, session={ctx['session_key']}, group={ctx['mapping'].is_group_chat}",
        )

    async def nerd_summarize(self, event: AstrMessageEvent, text: str = ""):
        """调用当前聊天模型做一句话 nerd 摘要。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.news_search",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return
        target_text = text.strip()
        if not target_text:
            args = self._extract_command_args(event)
            target_text = " ".join(args).strip()
        if not target_text:
            yield self._make_event_result(event, "用法：/nerd_summarize <text>")
            return
        summary = await self._llm_summarize(ctx["unified_msg_origin"], target_text)
        yield self._make_event_result(event, summary)
