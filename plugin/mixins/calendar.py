from __future__ import annotations

from datetime import datetime, timezone

from plugin.astrbot_compat import AstrMessageEvent, filter


class CalendarMixin:

    async def calendar_this_week(self, event: AstrMessageEvent):
        """学事日历：本周关键节点/无课日/考试周候选。"""
        async for result in self._calendar_common(event, mode="this-week"):
            yield result

    async def calendar_next(self, event: AstrMessageEvent):
        """学事日历：下一次关键节点候选。"""
        async for result in self._calendar_common(event, mode="next"):
            yield result

    @filter.llm_tool("isct_calendar_this_week")
    async def llm_tool_calendar_this_week(self, event: AstrMessageEvent, hint: str = "this-week"):
        """查询本周学事关键节点候选。

        Args:
            hint(string): 固定填 this-week。
        """
        _ = hint
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        return await self._calendar_common_text(ctx, mode="this-week")

    @filter.llm_tool("isct_calendar_next")
    async def llm_tool_calendar_next(self, event: AstrMessageEvent, hint: str = "next"):
        """查询下一次学事关键节点候选。

        Args:
            hint(string): 固定填 next。
        """
        _ = hint
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        return await self._calendar_common_text(ctx, mode="next")

    @filter.llm_tool("isct_calendar_get_academic_schedule")
    async def llm_tool_calendar_get_academic_schedule(self, event: AstrMessageEvent, year: str = ""):
        """获取学事日历结构化数据（学期/假期/考试相关事件）。

        Args:
            year(string): 学年（可选），如 2025。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.calendar",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        target_year = self._safe_int(year, default=self._default_academic_year(), min_value=2000, max_value=2100)
        payload = await self.query_ops.academic_schedule(ctx["scope_key"], year=target_year)
        term_ranges = [item for item in payload.get("term_ranges", []) if isinstance(item, dict)]
        events = [item for item in payload.get("events", []) if isinstance(item, dict)]
        term_lines = [
            f"T{idx}. {it.get('term')} {it.get('start_date')}~{it.get('end_date')}"
            for idx, it in enumerate(term_ranges[:4], start=1)
        ]
        event_lines = [
            f"{idx}. {it.get('start_date')}~{it.get('end_date')} [{it.get('kind')}] {it.get('title')}"
            for idx, it in enumerate(events[:12], start=1)
        ]
        lines = [*term_lines, *event_lines]
        if not lines:
            lines = ["未解析到学事日历事件或学期区间。"]
        source_rows = [
            *[f"{it.get('source_url')} (official schedule)" for it in term_ranges if it.get("source_url")],
            *[f"{it.get('source_url')} (official schedule)" for it in events if it.get("source_url")],
        ]
        sources, inference_lines = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="calendar",
            sources=source_rows[:16],
            inference_lines=[
                "已同时输出 term_ranges 与 events；默认学年按 4 月切换。",
                "建议后续对单日判定走 isct_calendar_is_no_class_day，减少 LLM 推理成本。",
            ],
        )
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=inference_lines,
            )
        )

    @filter.llm_tool("isct_calendar_is_no_class_day")
    async def llm_tool_calendar_is_no_class_day(self, event: AstrMessageEvent, date: str):
        """判断某一天是否为无课日/停课日。

        Args:
            date(string): 日期，格式 YYYY-MM-DD。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.calendar",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        result = await self.calendar_service.is_no_class_day(ctx["scope_key"], date_str=date)
        answer_lines = [
            f"date: {result.get('date')}",
            f"is_no_class_day: {result.get('is_no_class_day')}",
            f"reason: {result.get('reason')}",
            f"next_class_day: {result.get('next_class_day')}",
        ]
        year_hint = self._default_academic_year()
        if isinstance(result.get("date"), str) and len(str(result.get("date"))) >= 4:
            try:
                year_hint = int(str(result.get("date"))[:4])
            except Exception:
                year_hint = self._default_academic_year()
        sources, inference_lines = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="calendar",
            sources=[
                "https://www.titech.ac.jp/english/student/students/life/schedules (official academic schedule)",
                f"https://www.titech.ac.jp/english/student/students/life/schedules/{year_hint} (official academic schedule year page)",
            ],
            inference_lines=["判定为规则匹配，不依赖 LLM。若日期格式错误会直接返回 invalid_date_format。"],
        )
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=answer_lines,
                inference_lines=inference_lines,
            )
        )
