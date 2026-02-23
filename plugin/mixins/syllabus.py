from __future__ import annotations

from datetime import datetime, timezone

from plugin.astrbot_compat import AstrMessageEvent, filter


class SyllabusMixin:

    async def course(self, event: AstrMessageEvent, query: str = ""):
        """课程检索：返回课程卡片（缺失字段标注官网未标注）。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return
        keyword = query.strip()
        if not keyword:
            args = self._extract_command_args(event)
            keyword = " ".join(args).strip()
        if not keyword:
            yield self._make_event_result(event, "用法：/course <keyword|course_code>")
            return
        year = datetime.now(timezone.utc).year
        items = await self.syllabus_service.search_courses(
            ctx["scope_key"],
            year=year,
            q_name=keyword,
            limit=3,
            offset=0,
        )
        if not items:
            yield self._make_event_result(
                event,
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=[f"未检索到课程：{keyword}"],
                    inference_lines=["可尝试更精确的课程号，或在 config 里补充 course seeds。"],
                ),
            )
            return
        top = items[0]
        detail = await self.syllabus_service.get_course_detail(ctx["scope_key"], jwc_or_url=top.detail_ref)
        answer_lines = [
            f"名称/课程号: {detail.get('title', top.title)} / {detail.get('code', top.code)}",
            f"学分: {detail.get('credits', '官网未标注')}",
            f"学期/季度: {detail.get('term', '官网未标注')}",
            f"授课语言: {detail.get('language', '官网未标注')}",
            f"考核方式: {detail.get('grading', '官网未标注')}",
            "先修: 官网未标注",
            f"教师: {detail.get('instructors', top.instructors)}",
            f"链接: {detail.get('url', top.detail_ref)}",
            "更新时间: 官网未标注",
        ]
        prefs = await self.runtime.get_user_prefs(ctx["user_key"])
        watch_courses = [str(x) for x in prefs.get("watch_courses", [])]
        inference = [
            f"基于关键词匹配返回 top1；候选数={len(items)}。",
            f"你的 watch_courses: {', '.join(watch_courses) if watch_courses else '无'}。",
        ]
        yield self._make_event_result(
            event,
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{item.detail_ref} (官方来源)" for item in items[:3]],
                answer_lines=answer_lines,
                inference_lines=inference,
            ),
        )

    async def course_compare(self, event: AstrMessageEvent, course_a: str = "", course_b: str = ""):
        """课程对比：对两个关键词的候选课程做并列比较。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course_compare",
        )
        if not decision.allowed:
            yield self._make_event_result(event, decision.message or "请求被策略拒绝。")
            return
        left_kw = course_a.strip()
        right_kw = course_b.strip()
        if not (left_kw and right_kw):
            args = self._extract_command_args(event)
            if len(args) >= 2:
                left_kw, right_kw = args[0], args[1]
        if not (left_kw and right_kw):
            yield self._make_event_result(event, "用法：/course_compare <A> <B>")
            return
        year = datetime.now(timezone.utc).year
        left = await self.syllabus_service.search_courses(
            ctx["scope_key"], year=year, q_name=left_kw, limit=1, offset=0
        )
        right = await self.syllabus_service.search_courses(
            ctx["scope_key"], year=year, q_name=right_kw, limit=1, offset=0
        )
        sources = [f"{x.detail_ref} (官方来源)" for x in (left + right)]
        if not left or not right:
            yield self._make_event_result(
                event,
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=sources,
                    answer_lines=["至少一个课程关键词未匹配到结果。"],
                    inference_lines=["请换用更精确的课程号或缩写。"],
                ),
            )
            return
        l, r = left[0], right[0]
        answer_lines = [
            f"A: {l.title} | {l.detail_ref}",
            f"B: {r.title} | {r.detail_ref}",
            "A 摘要: 详情可通过 isct_syllabus_get_course_detail 拉取",
            "B 摘要: 详情可通过 isct_syllabus_get_course_detail 拉取",
        ]
        inference = [
            "适配度解释：当前仅基于标题/摘要关键词匹配，未接入结构化 syllabus 字段抽取。",
            "若需要精确对比学分/先修/考核，请在下一步接入 syllabus 详情页解析。",
        ]
        yield self._make_event_result(
            event,
            await self._guarded_response(ctx["scope_key"], sources=sources, answer_lines=answer_lines, inference_lines=inference),
        )

    @filter.llm_tool("isct_course_search")
    async def llm_tool_course_search(self, event: AstrMessageEvent, query: str):
        """课程检索工具，返回课程卡片样式结果（官方 syllabus）。

        Args:
            query(string): 课程号或关键词。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        items = await self.syllabus_service.search_courses(
            ctx["scope_key"],
            year=datetime.now(timezone.utc).year,
            q_name=query,
            limit=3,
            offset=0,
        )
        if not items:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=[f"未检索到课程：{query}"],
                    inference_lines=["可尝试更精确课程号，或在 config 补充 course seeds。"],
                )
            )
            return
        top = items[0]
        detail = await self.syllabus_service.get_course_detail(ctx["scope_key"], jwc_or_url=top.detail_ref)
        prefs = await self.runtime.get_user_prefs(ctx["user_key"])
        watch_courses = [str(x) for x in prefs.get("watch_courses", [])]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{item.detail_ref} (官方来源)" for item in items[:3]],
                answer_lines=[
                    f"名称/课程号: {detail.get('title', top.title)} / {detail.get('code', top.code)}",
                    f"学分: {detail.get('credits', '官网未标注')}",
                    f"学期/季度: {detail.get('term', '官网未标注')}",
                    f"授课语言: {detail.get('language', '官网未标注')}",
                    f"考核方式: {detail.get('grading', '官网未标注')}",
                    "先修: 官网未标注",
                    f"教师: {detail.get('instructors', top.instructors)}",
                    f"链接: {detail.get('url', top.detail_ref)}",
                    "更新时间: 官网未标注",
                ],
                inference_lines=[
                    f"基于关键词匹配返回 top1；候选数={len(items)}。",
                    f"watch_courses: {', '.join(watch_courses) if watch_courses else '无'}。",
                ],
            )
        )

    @filter.llm_tool("isct_course_compare")
    async def llm_tool_course_compare(self, event: AstrMessageEvent, course_a: str, course_b: str):
        """对比两门课程候选结果。

        Args:
            course_a(string): 课程A关键词。
            course_b(string): 课程B关键词。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course_compare",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        year = datetime.now(timezone.utc).year
        left = await self.syllabus_service.search_courses(
            ctx["scope_key"], year=year, q_name=course_a, limit=1, offset=0
        )
        right = await self.syllabus_service.search_courses(
            ctx["scope_key"], year=year, q_name=course_b, limit=1, offset=0
        )
        sources = [f"{x.detail_ref} (官方来源)" for x in (left + right)]
        if not left or not right:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=sources,
                    answer_lines=["至少一个课程关键词未匹配到结果。"],
                    inference_lines=["请换用更精确的课程号或缩写。"],
                )
            )
            return
        l, r = left[0], right[0]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=[
                    f"A: {l.title} | {l.detail_ref}",
                    f"B: {r.title} | {r.detail_ref}",
                    "A 摘要: 详情可通过 isct_syllabus_get_course_detail 拉取",
                    "B 摘要: 详情可通过 isct_syllabus_get_course_detail 拉取",
                ],
                inference_lines=[
                    "适配度解释：当前仅基于标题/摘要关键词匹配。",
                    "若要精确对比学分/先修/考核，请接入 syllabus 详情抽取。",
                ],
            )
        )

    @filter.llm_tool("isct_syllabus_list_groups")
    async def llm_tool_syllabus_list_groups(self, event: AstrMessageEvent, limit: str = "12"):
        """列出 syllabus 常见 group（先缩小学系范围后再检索课程）。

        Args:
            limit(string): 返回条数，建议 8-20。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        max_rows = self._safe_int(limit, default=12, min_value=1, max_value=30)
        groups = await self.syllabus_service.list_groups(ctx["scope_key"], limit=max_rows)
        lines = [f"{idx}. {g.group_id} | {g.name_en} | {g.name_ja}" for idx, g in enumerate(groups, start=1)]
        if not lines:
            lines = ["未解析到 syllabus groups。"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=["https://syllabus.s.isct.ac.jp/ (official syllabus root)"],
                answer_lines=lines,
                inference_lines=["建议先选 group，再调用 isct_syllabus_search_courses 缩小检索范围。"],
            )
        )

    @filter.llm_tool("isct_syllabus_search_courses")
    async def llm_tool_syllabus_search_courses(
        self,
        event: AstrMessageEvent,
        q_name: str = "",
        teacher: str = "",
        code: str = "",
        group: str = "",
        year: str = "",
        limit: str = "10",
        offset: str = "0",
    ):
        """按关键词/教师/课程号/group 检索 Science Tokyo 课程。

        Args:
            q_name(string): 课程名关键词。
            teacher(string): 教师名关键词。
            code(string): 课程号关键词。
            group(string): 学院/开课单位 group_id。
            year(string): 学年，如 2025；留空默认当前年。
            limit(string): 返回条数。
            offset(string): 偏移量。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        target_year = self._safe_int(year, default=datetime.now(timezone.utc).year, min_value=2000, max_value=2100)
        max_rows = self._safe_int(limit, default=10, min_value=1, max_value=30)
        start = self._safe_int(offset, default=0, min_value=0, max_value=1000)
        items = await self.query_ops.search_courses(
            ctx["scope_key"],
            year=target_year,
            q_name=q_name,
            teacher=teacher,
            code=code,
            group=group,
            limit=max_rows,
            offset=start,
        )
        lines = [
            f"{idx}. {it.title} | code={it.code} | teacher={it.instructors} | detail={it.detail_ref}"
            for idx, it in enumerate(items, start=1)
        ]
        if not lines:
            lines = ["未匹配到课程。"]
        sources, inference_lines = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="course",
            sources=[f"{it.detail_ref} (official syllabus)" for it in items[:5]],
            inference_lines=[
                "当前为 list 阶段，详细字段请用 isct_syllabus_get_course_detail。",
                "建议优先设置 group 以减少噪声和 token。",
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

    @filter.llm_tool("isct_syllabus_get_course_detail")
    async def llm_tool_syllabus_get_course_detail(self, event: AstrMessageEvent, jwc_or_url: str, fields: str = ""):
        """拉取单门课程详情（字段可裁剪）。

        Args:
            jwc_or_url(string): 课程 jwc 或详情页完整 URL。
            fields(string): 逗号分隔字段，如 grading,credits,language。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.course",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        fields_list = [f.strip() for f in fields.split(",") if f.strip()]
        detail = await self.syllabus_service.get_course_detail(
            ctx["scope_key"],
            jwc_or_url=jwc_or_url,
            fields=fields_list,
        )
        if not detail:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=["课程详情拉取失败或链接不在允许域名内。"],
                    inference_lines=["请传入 syllabus.s.isct.ac.jp 的 jwc 或课程链接。"],
                )
            )
            return
        lines = [f"{k}: {v}" for k, v in detail.items()]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{detail.get('url', '')} (official syllabus)"],
                answer_lines=lines,
                inference_lines=["字段为页面抽取结果，缺失项会标注官网未标注。"],
            )
        )
