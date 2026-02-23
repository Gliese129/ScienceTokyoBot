from __future__ import annotations

from datetime import datetime, timezone

from plugin.astrbot_compat import AstrMessageEvent, SessionController, filter, session_waiter, SESSION_WAITER_AVAILABLE


class ExamMixin:

    async def exam_latest(self, event: AstrMessageEvent):
        """查询最新一次已同步的考试 PDF 版本与记录统计。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        self._debug_scope(mapping=ctx["mapping"], raw_message=ctx["raw_message"])

        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return

        yield self._make_event_result(event, await self._render_exam_latest(ctx["scope_key"]))

    async def exam_sync(self, event: AstrMessageEvent):
        """管理员触发：抓取并同步最新考试 PDF，生成版本与 diff。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 exam_sync。")
            return
        result = await self.exam_service.sync_latest(ctx["scope_key"])
        if not result.ok:
            text = await self._guarded_response(
                ctx["scope_key"],
                sources=[result.pdf_url] if result.pdf_url else [],
                answer_lines=[f"exam_sync: failed ({result.parse_error or 'unknown'})"],
                inference_lines=[result.message],
            )
            yield self._make_event_result(event, text)
            return

        response = await self._guarded_response(
            ctx["scope_key"],
            sources=[f"{result.pdf_url} (exam pdf)"] if result.pdf_url else [],
            answer_lines=[
                f"exam_sync: ok",
                f"version_id: {result.version_id}",
                f"changed: {result.changed}",
                f"diff_count: {len(result.diff)}",
                f"parse_error: {result.parse_error or 'none'}",
            ],
            inference_lines=["若 diff_count > 0，说明相较上一版考试安排出现了字段变更。"],
        )
        yield self._make_event_result(event, response)
        if result.changed and result.diff:
            await self._push_exam_diff(result.diff)

    async def exam_course(self, event: AstrMessageEvent, query: str = ""):
        """按课程号或关键词查询考试安排记录。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return
        keyword = query.strip()
        if not keyword:
            args = self._extract_command_args(event)
            keyword = " ".join(args).strip()
        if not keyword:
            yield self._make_event_result(event, "用法：/exam_course <course_code|keyword>")
            return
        records = await self.runtime.query_exam_by_course(keyword)
        latest = await self.runtime.get_latest_exam_version()
        sources = [f"{latest['pdf_url']} (latest exam pdf)"] if latest else []
        if not records:
            text = await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=[f"未找到课程 {keyword} 的考试记录。"],
                inference_lines=["请先执行 /exam_sync，或检查课程号拼写。"],
            )
            yield self._make_event_result(event, text)
            return
        answer_lines = []
        for rec in records[:8]:
            answer_lines.append(
                f"{rec.get('date')} {rec.get('period')}限 {rec.get('course_code')} {rec.get('course_title')} room={rec.get('room') or '官网未标注'} type={rec.get('type')}"
            )
        text = await self._guarded_response(
            ctx["scope_key"],
            sources=sources,
            answer_lines=answer_lines,
            inference_lines=[f"仅展示前 {min(8, len(records))} 条，共 {len(records)} 条。"],
        )
        yield self._make_event_result(event, text)

    async def exam_day(self, event: AstrMessageEvent, date: str = ""):
        """按日期（YYYY-MM-DD）查询当天考试列表。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return
        target_date = date.strip()
        if not target_date:
            args = self._extract_command_args(event)
            target_date = args[0].strip() if args else ""
        if not target_date:
            yield self._make_event_result(event, "用法：/exam_day <YYYY-MM-DD>")
            return
        records = await self.runtime.query_exam_by_day(target_date)
        latest = await self.runtime.get_latest_exam_version()
        sources = [f"{latest['pdf_url']} (latest exam pdf)"] if latest else []
        answer_lines = []
        if not records:
            answer_lines.append(f"{target_date} 没有匹配到考试记录。")
        else:
            for rec in records[:20]:
                answer_lines.append(
                    f"{rec.get('period')}限 {rec.get('course_code')} {rec.get('course_title')} room={rec.get('room') or '官网未标注'} type={rec.get('type')}"
                )
        text = await self._guarded_response(
            ctx["scope_key"],
            sources=sources,
            answer_lines=answer_lines,
            inference_lines=[f"记录数：{len(records)}。"],
        )
        yield self._make_event_result(event, text)

    async def exam_watch(self, event: AstrMessageEvent, course_code: str = ""):
        """订阅课程考试变更（watch list）。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])

        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam_watch",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return

        code = course_code.strip()
        if not code:
            args = self._extract_command_args(event)
            code = args[0].strip() if args else ""
        if not code:
            yield self._make_event_result(event, "用法：/exam_watch <course_code>")
            return
        changed, reason = await self.runtime.add_watch_course(ctx["user_key"], code)
        if not changed:
            yield self._make_event_result(event, f"watch 未变化：{reason}")
            return
        courses = ", ".join(await self.runtime.list_watch_courses(ctx["user_key"]))
        yield self._make_event_result(event, f"watch 已更新。当前 watch_courses: {courses}")

    @filter.llm_tool("isct_exam_latest")
    async def llm_tool_exam_latest(self, event: AstrMessageEvent, request: str):
        """查询 Science Tokyo 最新考试 PDF 链接与解析状态。

        Args:
            request(string): 固定填 "latest"。
        """
        _ = request
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        return (await self._render_exam_latest(ctx["scope_key"]))

    @filter.llm_tool("isct_exam_watch")
    async def llm_tool_exam_watch(self, event: AstrMessageEvent, course_code: str):
        """订阅某门课程的考试变更推送（watch list）。

        Args:
            course_code(string): 课程号，例如 CS101。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam_watch",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        changed, reason = await self.runtime.add_watch_course(ctx["user_key"], course_code)
        if not changed:
            return (f"watch 未变化：{reason}")
            return
        courses = ", ".join(await self.runtime.list_watch_courses(ctx["user_key"]))
        return (f"watch 已更新。当前 watch_courses: {courses}")

    @filter.llm_tool("isct_exam_sync")
    async def llm_tool_exam_sync(self, event: AstrMessageEvent, mode: str = "latest"):
        """管理员触发考试 PDF 同步，返回版本与 diff 概览。

        Args:
            mode(string): 固定填 latest。
        """
        _ = mode
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            return ("仅管理员可执行 exam_sync。")
            return
        result = await self.exam_service.sync_latest(ctx["scope_key"])
        if not result.ok:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[result.pdf_url] if result.pdf_url else [],
                    answer_lines=[f"exam_sync: failed ({result.parse_error or 'unknown'})"],
                    inference_lines=[result.message],
                )
            )
            return
        resp = await self._guarded_response(
            ctx["scope_key"],
            sources=[f"{result.pdf_url} (exam pdf)"] if result.pdf_url else [],
            answer_lines=[
                "exam_sync: ok",
                f"version_id: {result.version_id}",
                f"changed: {result.changed}",
                f"diff_count: {len(result.diff)}",
                f"parse_error: {result.parse_error or 'none'}",
            ],
            inference_lines=["若 diff_count > 0，说明相较上一版考试安排出现了字段变更。"],
        )
        if result.changed and result.diff:
            await self._push_exam_diff(result.diff)
        return resp

    @filter.llm_tool("isct_exam_course")
    async def llm_tool_exam_course(self, event: AstrMessageEvent, query: str):
        """按课程号或关键词查询考试记录。

        Args:
            query(string): 课程号或关键词，例如 CS101。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        records = await self.runtime.query_exam_by_course(query)
        latest = await self.runtime.get_latest_exam_version()
        sources = [f"{latest['pdf_url']} (latest exam pdf)"] if latest else []
        if not records:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=sources,
                    answer_lines=[f"未找到课程 {query} 的考试记录。"],
                    inference_lines=["请先执行 /exam_sync，或检查课程号拼写。"],
                )
            )
            return
        lines = [
            f"{rec.get('date')} {rec.get('period')}限 {rec.get('course_code')} {rec.get('course_title')} room={rec.get('room') or '官网未标注'} type={rec.get('type')}"
            for rec in records[:8]
        ]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=[f"仅展示前 {min(8, len(records))} 条，共 {len(records)} 条。"],
            )
        )

    @filter.llm_tool("isct_exam_day")
    async def llm_tool_exam_day(self, event: AstrMessageEvent, date: str):
        """按日期查询当天考试记录。

        Args:
            date(string): 日期，格式 YYYY-MM-DD。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        records = await self.runtime.query_exam_by_day(date)
        latest = await self.runtime.get_latest_exam_version()
        sources = [f"{latest['pdf_url']} (latest exam pdf)"] if latest else []
        lines = (
            [
                f"{rec.get('period')}限 {rec.get('course_code')} {rec.get('course_title')} room={rec.get('room') or '官网未标注'} type={rec.get('type')}"
                for rec in records[:20]
            ]
            if records
            else [f"{date} 没有匹配到考试记录。"]
        )
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=[f"记录数：{len(records)}。"],
            )
        )

    @filter.llm_tool("isct_exam_list_pdfs")
    async def llm_tool_exam_list_pdfs(self, event: AstrMessageEvent, limit: str = "5"):
        """列出考试相关页面解析出的 PDF 链接。

        Args:
            limit(string): 返回 PDF 数量。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        max_rows = self._safe_int(limit, default=5, min_value=1, max_value=20)
        pdfs = await self.query_ops.list_exam_pdfs(ctx["scope_key"], limit=max_rows)
        lines = [f"{idx}. {url}" for idx, url in enumerate(pdfs, start=1)] or ["未找到考试 PDF 链接。"]
        sources, inference_lines = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="exam",
            sources=[f"{url} (exam pdf)" for url in pdfs[:5]],
            inference_lines=["建议先调用 isct_exam_sync 或 isct_exam_parse_pdf，避免重复解析。"],
        )
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=inference_lines,
            )
        )

    @filter.llm_tool("isct_exam_parse_pdf")
    async def llm_tool_exam_parse_pdf(self, event: AstrMessageEvent, pdf_url: str = ""):
        """解析指定考试 PDF（为空则使用最新 PDF）。

        Args:
            pdf_url(string): 目标 PDF 链接（可选）。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        target_pdf = pdf_url.strip() or (await self.exam_service.find_latest_pdf(ctx["scope_key"]) or "")
        if not target_pdf:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=["无法确定可解析的 exam PDF。"],
                    inference_lines=["请先调用 isct_exam_list_pdfs。"],
                )
            )
            return
        try:
            records, parse_error = await self.exam_service.parse_pdf_url(target_pdf)
        except Exception as exc:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[f"{target_pdf} (exam pdf)"],
                    answer_lines=[f"PDF 解析失败: {exc}"],
                    inference_lines=["可改用 isct_exam_sync 走版本化解析与 diff。"],
                )
            )
            return
        lines = [
            f"{idx}. {rec.get('date')} {rec.get('period')}限 {rec.get('course_code')} {rec.get('course_title')} room={rec.get('room') or '官网未标注'}"
            for idx, rec in enumerate(records[:12], start=1)
        ] or ["PDF 可访问但未解析出记录。"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{target_pdf} (exam pdf)"],
                answer_lines=lines,
                inference_lines=[f"parse_error={parse_error or 'none'}; 仅展示前 {min(12, len(records))} 条。"],
            )
        )

    @filter.llm_tool("isct_exam_find_exam")
    async def llm_tool_exam_find_exam(self, event: AstrMessageEvent, query: str):
        """按课程号/关键词查询考试记录（优先已缓存版本）。

        Args:
            query(string): 课程号或关键词。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        records = await self.runtime.query_exam_by_course(query)
        if not records:
            latest_pdf = await self.exam_service.find_latest_pdf(ctx["scope_key"])
            if latest_pdf:
                try:
                    parsed, _ = await self.exam_service.parse_pdf_url(latest_pdf)
                except Exception:
                    parsed = []
                needle = query.strip().lower()
                records = [
                    rec
                    for rec in parsed
                    if needle in str(rec.get("course_code", "")).lower()
                    or needle in str(rec.get("course_title", "")).lower()
                ]
        latest = await self.runtime.get_latest_exam_version()
        sources = [f"{latest['pdf_url']} (latest exam pdf)"] if latest else []
        if not sources:
            sources = ["https://www.titech.ac.jp/student/students/life/undergraduate-exam (official exam page)"]
        lines = [
            f"{idx}. {rec.get('date')} {rec.get('period')}限 {rec.get('course_code')} {rec.get('course_title')} room={rec.get('room') or '官网未标注'}"
            for idx, rec in enumerate(records[:10], start=1)
        ] or [f"未找到 {query} 的考试记录。"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=["优先读取已同步 exam_records；为空时会临时解析最新 PDF。"],
            )
        )

    async def exam_watch_wizard(self, event: AstrMessageEvent):
        """会话模式订阅课程考试变更，支持连续输入课程号。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.exam_watch",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return
        if not SESSION_WAITER_AVAILABLE:
            yield self._make_event_result(event, "当前环境未启用 session_waiter，请使用 /exam_watch <course_code>。")
            return
        yield self._make_event_result(event, "进入会话模式：请直接发送课程号；发送“退出”结束。")

        @session_waiter(timeout=120, record_history_chains=False)
        async def _watcher(controller: SessionController, waiter_event: AstrMessageEvent):
            text = str(getattr(waiter_event, "message_str", "") or "").strip()
            if not text:
                await waiter_event.send(waiter_event.plain_result("请输入课程号，或发送“退出”。"))
                controller.keep(timeout=120, reset_timeout=True)
                return
            if text in {"退出", "取消", "cancel", "exit"}:
                await waiter_event.send(waiter_event.plain_result("会话已结束。"))
                controller.stop()
                return

            waiter_ctx = self._extract_event_context(waiter_event)
            await self._remember_origin(
                scope_key=waiter_ctx["scope_key"],
                unified_msg_origin=waiter_ctx["unified_msg_origin"],
            )
            changed, reason = await self.runtime.add_watch_course(waiter_ctx["user_key"], text)
            if changed:
                courses = ", ".join(await self.runtime.list_watch_courses(waiter_ctx["user_key"]))
                await waiter_event.send(waiter_event.plain_result(f"watch 已更新：{courses}"))
            else:
                await waiter_event.send(waiter_event.plain_result(f"watch 未变化：{reason}"))
            controller.keep(timeout=120, reset_timeout=True)

        await _watcher(event)
        if hasattr(event, "stop_event"):
            event.stop_event()
