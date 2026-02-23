from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from adapters.scope_mapping import build_scope_mapping, stringify_unified_origin
from guards.answer_guard import enforce_answer_guard, format_structured_response
from isct_core import build_source_debug
from plugin.astrbot_compat import AstrMessageEvent, Plain, astr_logger
from runtime.sqlite_runtime import RuntimeDecision


class CommonMixin:
    @staticmethod
    def _default_academic_year(now: datetime | None = None) -> int:
        current = now or datetime.now(timezone.utc)
        return current.year if current.month >= 4 else current.year - 1

    @staticmethod
    def _dedupe_sources(sources: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for item in sources:
            raw = str(item or '').strip()
            if not raw:
                continue
            note = ''
            url_part = raw
            if ' (' in raw and raw.endswith(')'):
                idx = raw.rfind(' (')
                if idx > 0:
                    url_part = raw[:idx].strip()
                    note = raw[idx:].strip()
            try:
                parts = urlsplit(url_part)
                canonical = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ''))
            except Exception:
                canonical = url_part
            key = f"{canonical}{note}"
            if key in seen:
                continue
            seen.add(key)
            out.append(f"{canonical}{note}" if note else canonical)
        return out

    async def _guarded_response(
        self,
        scope_key: str,
        *,
        sources: list[str],
        answer_lines: list[str],
        inference_lines: list[str],
    ) -> str:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        payload = format_structured_response(
            sources=sources,
            answer_lines=answer_lines,
            inference_lines=inference_lines,
        )
        return enforce_answer_guard(payload, allowed_domains)

    async def _with_source_debug(
        self,
        *,
        scope_key: str,
        category: str,
        sources: list[str],
        inference_lines: list[str],
    ) -> tuple[list[str], list[str]]:
        input_sources = self._dedupe_sources(sources)
        resolved_sources, debug_line = await build_source_debug(
            self.runtime,
            self.global_config,
            scope_key=scope_key,
            category=category,
            sources=input_sources,
        )
        resolved_sources = self._dedupe_sources(resolved_sources)
        if self.global_config.source_debug_enabled:
            return resolved_sources, [*inference_lines, debug_line]
        return resolved_sources, inference_lines

    async def _calendar_common(self, event: AstrMessageEvent, mode: str):
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        text = await self._calendar_common_text(ctx, mode=mode)
        yield self._make_event_result(event, text)

    async def _calendar_common_text(self, ctx: dict[str, Any], mode: str) -> str:
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.calendar",
        )
        if not decision.allowed:
            return decision.message or "请求被策略拒绝。"
        picked, all_events = await self.query_ops.calendar_window(
            ctx["scope_key"],
            mode=mode,
            year=self._default_academic_year(),
        )
        if mode == "this-week":
            answer_lines = [
                f"本周候选: {it.get('start_date')}~{it.get('end_date')} {it.get('title')}"
                for it in picked[:8]
            ] or ["本周未检索到关键日程。"]
            inference = ["以 calendar seeds 结构化解析结果生成，优先时间窗口过滤。"]
        else:
            answer_lines = (
                [
                    f"下一关键节点候选: {picked[0].get('start_date')}~{picked[0].get('end_date')} {picked[0].get('title')}"
                ]
                if picked
                else ["未检索到下一关键节点。"]
            )
            inference = ["“下一次”按 start_date 排序。若页面日期缺失，排序可能不准确。"]
        sources, inference = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="calendar",
            sources=[f"{item.get('source_url')} (官方来源)" for item in all_events[:8] if item.get("source_url")],
            inference_lines=inference,
        )
        return await self._guarded_response(
            ctx["scope_key"],
            sources=sources,
            answer_lines=answer_lines,
            inference_lines=inference,
        )

    async def _admin_feature_toggle(self, event: AstrMessageEvent, *, enabled: bool):
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 admin_feature_*。")
            return
        args = self._extract_command_args(event)
        if len(args) < 2:
            yield self._make_event_result(event, "用法：/admin_feature_enable|disable <scope> <feature>")
            return
        target_scope, feature = args[0], args[1]
        cfg = await self.runtime.get_scope_config(target_scope)
        enabled_features = [str(x) for x in cfg.get("enabledFeatures", [])]
        if enabled and feature not in enabled_features:
            enabled_features.append(feature)
        if not enabled:
            enabled_features = [f for f in enabled_features if f != feature]
        await self.runtime.set_scope_config(target_scope, {"enabledFeatures": enabled_features})
        await self.runtime.log_admin_action(
            actor_user_key=ctx["user_key"],
            scope_key=ctx["scope_key"],
            action="admin_feature_enable" if enabled else "admin_feature_disable",
            target=target_scope,
            detail={"feature": feature, "enabled": enabled},
        )
        yield self._make_event_result(event, f"feature updated: {target_scope} {feature} enabled={enabled}")

    async def _push_exam_diff(self, diff: list[dict[str, Any]]) -> None:
        if not diff:
            return
        target_scopes = await self.runtime.list_push_target_scopes()
        if not target_scopes:
            return
        lines = ["[push.exam_update] 检测到考试安排变更："]
        mention_user_keys: set[str] = set()
        for item in diff[:20]:
            code = str(item.get("course_code") or "unknown")
            change = str(item.get("change"))
            if change == "updated":
                fields = ",".join(item.get("fields") or [])
                old = item.get("old") or {}
                new = item.get("new") or {}
                lines.append(
                    f"- {code}: {fields} changed ({old.get('date')}/{old.get('period')}/{old.get('room')} -> {new.get('date')}/{new.get('period')}/{new.get('room')})"
                )
            else:
                lines.append(f"- {code}: {change}")
            for user_key in await self.runtime.list_watchers_for_course(code):
                mention_user_keys.add(user_key)
        mentions = " ".join(self._user_key_to_mention(k) for k in sorted(mention_user_keys) if self._user_key_to_mention(k))
        if mentions:
            lines.append(f"watch 命中: {mentions}")
        message_chain = self._build_message_chain("\n".join(lines))
        for scope in target_scopes:
            target_umo = await self._resolve_target_unified_msg_origin(scope)
            if not target_umo:
                continue
            try:
                await self.context.send_message(target_umo, message_chain)
            except Exception as exc:
                astr_logger.warning("push diff failed scope=%s err=%s", scope, exc)

    @staticmethod
    def _user_key_to_mention(user_key: str) -> str:
        if user_key.startswith("discord:user:"):
            return f"<@{user_key.split(':')[-1]}>"
        return ""

    @staticmethod
    def _parse_literal_value(raw: str) -> Any:
        txt = raw.strip()
        if not txt:
            return ""
        for parser in [json.loads]:
            try:
                return parser(txt)
            except Exception:
                pass
        lowered = txt.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in txt:
                return float(txt)
            return int(txt)
        except ValueError:
            return txt

    @staticmethod
    def _safe_int(raw: Any, *, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
        value = default
        try:
            parsed = raw if isinstance(raw, int) else int(str(raw).strip())
            value = parsed
        except Exception:
            value = default
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    async def _render_exam_latest(self, scope_key: str) -> str:
        latest = await self.runtime.get_latest_exam_version()
        if latest is None:
            return await self._guarded_response(
                scope_key,
                sources=[],
                answer_lines=["当前没有已同步的 exam PDF 版本。"],
                inference_lines=["请先执行 /exam_sync。"],
            )
        records = await self.runtime.get_exam_records(str(latest["version_id"]))
        created_at = datetime.fromtimestamp(int(latest["created_at"]), tz=timezone.utc).isoformat()
        return await self._guarded_response(
            scope_key,
            sources=[f"{latest['pdf_url']} (latest exam pdf)"],
            answer_lines=[
                f"version_id: {latest['version_id']}",
                f"latest_pdf: {latest['pdf_url']}",
                f"synced_at: {created_at}",
                f"record_count: {len(records)}",
                f"parse_error: {latest.get('parse_error') or 'none'}",
            ],
            inference_lines=["若 PDF 更新但记录未变化，diff_count 会保持 0。"],
        )

    async def _preflight(self, scope_key: str, user_key: str, role_ids: list[str], feature_id: str) -> RuntimeDecision:
        is_admin = await self.runtime.is_admin(scope_key=scope_key, user_key=user_key, role_ids=role_ids)
        if not await self.runtime.is_feature_enabled(
            scope_key=scope_key,
            feature_id=feature_id,
            user_key=user_key,
            role_ids=role_ids,
        ):
            return RuntimeDecision(
                allowed=False,
                reason="feature_disabled",
                message="该 scope 未开放此功能。",
            )
        ban = await self.runtime.check_active_ban(scope_key=scope_key, user_key=user_key, is_admin=is_admin)
        if not ban.allowed:
            return ban
        return await self.runtime.check_and_record_rate_limit(
            scope_key=scope_key,
            user_key=user_key,
            feature_id=feature_id,
            is_admin=is_admin,
        )

    async def _remember_origin(self, scope_key: str, unified_msg_origin: Any) -> None:
        self.sync_manager.ensure_started()
        if unified_msg_origin is None:
            return
        await self.runtime.set_push_target(scope_key=scope_key, unified_msg_origin=unified_msg_origin)

    async def _resolve_target_unified_msg_origin(self, scope_key: str) -> Any | None:
        return await self.runtime.get_push_target(scope_key=scope_key)

    async def _llm_summarize(self, unified_msg_origin: Any, text: str) -> str:
        provider_id = await self._call_context_method("get_current_chat_provider_id", unified_msg_origin)
        prompt = (
            "你是 Science Tokyo Plugin。"
            "请用 1 句话总结，不要编造事实。若信息不足直接说信息不足。\n"
            f"输入：{text}"
        )
        output = await self._call_context_method(
            "llm_generate",
            prompt=prompt,
            provider_id=provider_id,
            unified_msg_origin=unified_msg_origin,
        )
        return self._normalize_llm_output(output)

    async def _call_context_method(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        method = getattr(self.context, method_name, None)
        if method is None:
            raise RuntimeError(f"context.{method_name} is not available")
        kwargs_variants = [
            kwargs,
            {k: v for k, v in kwargs.items() if k != "unified_msg_origin"},
            {k: v for k, v in kwargs.items() if k != "provider_id"},
            {},
        ]
        for kw in kwargs_variants:
            try:
                result = method(*args, **kw)
                if inspect.isawaitable(result):
                    return await result
                return result
            except TypeError:
                continue
        raise RuntimeError(f"Failed to call context.{method_name}")

    @staticmethod
    def _normalize_llm_output(output: Any) -> str:
        if output is None:
            return "LLM 返回为空。"
        if isinstance(output, str):
            return output
        if isinstance(output, dict):
            for key in ["text", "content", "answer", "output"]:
                if key in output:
                    return str(output[key])
        return str(output)

    def _extract_event_context(self, event: AstrMessageEvent) -> dict[str, Any]:
        message_obj = getattr(event, "message_obj", None) or getattr(event, "message", None)
        raw_message = self._get_raw_message(message_obj)
        group_id = getattr(message_obj, "group_id", None)
        user_id = self._extract_user_id(event, message_obj, raw_message)
        role_ids = self._extract_role_ids(message_obj, raw_message)
        mapping = build_scope_mapping(
            group_id=group_id,
            user_id=user_id,
            raw_message=raw_message,
            default_platform="discord",
        )
        user_key = f"{mapping.platform}:user:{mapping.user_id or 'unknown'}"
        unified_msg_origin = getattr(event, "unified_msg_origin", None)
        session_key = stringify_unified_origin(unified_msg_origin)
        return {
            "mapping": mapping,
            "raw_message": raw_message,
            "scope_key": mapping.scope_key,
            "user_key": user_key,
            "role_ids": role_ids,
            "unified_msg_origin": unified_msg_origin,
            "session_key": session_key,
        }

    @staticmethod
    def _get_raw_message(message_obj: Any) -> dict[str, Any]:
        raw = getattr(message_obj, "raw_message", None)
        if isinstance(raw, dict):
            return raw
        if raw is None:
            return {}
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    @staticmethod
    def _extract_user_id(event: AstrMessageEvent, message_obj: Any, raw: dict[str, Any]) -> str | None:
        candidates = [
            getattr(message_obj, "user_id", None),
            getattr(getattr(message_obj, "sender", None), "user_id", None),
            getattr(getattr(event, "sender", None), "user_id", None),
            raw.get("user_id"),
            raw.get("userId"),
            raw.get("author_id"),
            raw.get("authorId"),
        ]
        for value in candidates:
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    @staticmethod
    def _extract_role_ids(message_obj: Any, raw: dict[str, Any]) -> list[str]:
        candidates = [
            getattr(message_obj, "role_ids", None),
            raw.get("role_ids"),
            raw.get("roles"),
            raw.get("member_roles"),
        ]
        for value in candidates:
            if isinstance(value, list):
                return [str(item) for item in value]
        return []

    @staticmethod
    def _extract_command_args(event: AstrMessageEvent) -> list[str]:
        text_candidates = [
            getattr(event, "message_str", None),
            getattr(event, "plain_text", None),
            getattr(event, "text", None),
            getattr(getattr(event, "message_obj", None), "message_str", None),
            getattr(getattr(event, "message_obj", None), "text", None),
        ]
        content = ""
        for candidate in text_candidates:
            if candidate is not None and str(candidate).strip():
                content = str(candidate).strip()
                break
        if not content:
            return []
        parts = content.split()
        return parts[1:] if len(parts) > 1 else []

    @staticmethod
    def _build_message_chain(text: str) -> Any:
        try:
            from astrbot.api.message_components import MessageChain  # type: ignore

            chain = MessageChain()
            if hasattr(chain, "append"):
                chain.append(Plain(text))
                return chain
            if hasattr(chain, "message"):
                return chain.message(Plain(text))
        except Exception:
            pass
        return [Plain(text)]

    def _make_event_result(self, event: AstrMessageEvent, text: str) -> Any:
        if hasattr(event, "chain_result"):
            try:
                return event.chain_result(self._build_message_chain(text))
            except Exception:
                pass
        return event.plain_result(text)

    @staticmethod
    def _debug_scope(mapping: Any, raw_message: dict[str, Any]) -> None:
        astr_logger.debug(
            "scope_mapping platform=%s group=%s guild/server=%s group_id=%s scope=%s raw_keys=%s",
            mapping.platform,
            mapping.is_group_chat,
            mapping.guild_or_server_id,
            mapping.group_id,
            mapping.scope_key,
            sorted(list(raw_message.keys())),
        )
