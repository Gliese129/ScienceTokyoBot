from __future__ import annotations

from plugin.astrbot_compat import AstrMessageEvent, filter


class NewsMixin:

    async def news(self, event: AstrMessageEvent, query: str = ""):
        """新闻检索：返回 3-5 条相关条目与一句话摘要。"""
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
        keyword = query.strip()
        if not keyword:
            args = self._extract_command_args(event)
            keyword = " ".join(args).strip()
        if not keyword:
            yield self._make_event_result(event, "用法：/news <keyword>")
            return
        items = await self.news_service.list_current_students_news(
            ctx["scope_key"], keyword=keyword, lang="en", limit=5
        )
        sources = [f"{item.url} ({item.source_note})" for item in items]
        if not sources:
            sources = ["https://students.isct.ac.jp/en/news (official news list)"]
        answer_lines = [
            f"{idx}. {item.title} ({item.date}) — {item.snippet or '无摘要'}"
            for idx, item in enumerate(items, start=1)
        ]
        if not answer_lines:
            answer_lines = [f"未检索到相关新闻：{keyword}"]
        inference = ["摘要来自列表页与详情页文本片段匹配，不保证语义摘要完整。"]
        yield self._make_event_result(
            event,
            await self._guarded_response(ctx["scope_key"], sources=sources, answer_lines=answer_lines, inference_lines=inference),
        )

    @filter.llm_tool("isct_news_search")
    async def llm_tool_news_search(self, event: AstrMessageEvent, query: str):
        """检索校园新闻并返回摘要条目。

        Args:
            query(string): 新闻关键词。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.news_search",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        items = await self.news_service.list_current_students_news(ctx["scope_key"], keyword=query, lang="en", limit=5)
        sources = [f"{item.url} ({item.source_note})" for item in items]
        if not sources:
            sources = ["https://students.isct.ac.jp/en/news (official news list)"]
        lines = [
            f"{idx}. {item.title} ({item.date}) — {item.snippet or '无摘要'}"
            for idx, item in enumerate(items, start=1)
        ]
        if not lines:
            lines = [f"未检索到相关新闻：{query}"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=["摘要来自列表页与详情页文本片段匹配，不保证语义摘要完整。"],
            )
        )

    @filter.llm_tool("isct_news_list_current_students")
    async def llm_tool_news_list_current_students(
        self, event: AstrMessageEvent, keyword: str = "", tab: str = "", lang: str = "en", limit: str = "8"
    ):
        """列出在学生新闻（支持 tab 过滤，如奖学金 tab=112）。

        Args:
            keyword(string): 关键词过滤。
            tab(string): 分类 tab id（可选），例如 112。
            lang(string): en 或 ja。
            limit(string): 条目数量。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.news_search",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        tab_int = None
        if str(tab).strip():
            tab_int = self._safe_int(tab, default=0, min_value=0, max_value=9999)
        max_rows = self._safe_int(limit, default=8, min_value=1, max_value=20)
        items = await self.query_ops.list_news(
            ctx["scope_key"],
            keyword=keyword,
            tab=tab_int,
            lang=lang,
            limit=max_rows,
        )
        lines = [
            f"{idx}. {it.title} ({it.date}) tags={','.join(it.tags)} url={it.url}"
            for idx, it in enumerate(items, start=1)
        ] or ["未检索到匹配新闻。"]
        sources, inference_lines = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="news",
            sources=[f"{it.url} ({it.source_note})" for it in items[:8]],
            inference_lines=["建议 list->detail：先列条目，再用 isct_news_get_item 拉详情。"],
        )
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=sources,
                answer_lines=lines,
                inference_lines=inference_lines,
            )
        )

    @filter.llm_tool("isct_news_get_item")
    async def llm_tool_news_get_item(self, event: AstrMessageEvent, url: str):
        """获取单条新闻详情并抽取关键点。

        Args:
            url(string): 新闻详情 URL。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.news_search",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        payload = await self.news_service.get_news_item(ctx["scope_key"], url=url)
        if not payload:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=["新闻详情拉取失败或链接不在允许域名内。"],
                    inference_lines=["请传入 students.isct.ac.jp / titech.ac.jp 下的链接。"],
                )
            )
            return
        lines = [
            f"title: {payload.get('title')}",
            f"date: {payload.get('date')}",
            f"summary: {payload.get('body_summary')}",
            f"key_points: {' | '.join(payload.get('key_points', [])[:5]) or '官网未标注'}",
        ]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{payload.get('url')} (official news detail)"],
                answer_lines=lines,
                inference_lines=["key_points 由规则抽取，不等同于完整人工解读。"],
            )
        )

    @filter.llm_tool("isct_news_list_legacy")
    async def llm_tool_news_list_legacy(self, event: AstrMessageEvent, keyword: str = "", limit: str = "5"):
        """列出旧站（原东工大/原医科齿科）历史公告入口。

        Args:
            keyword(string): 关键词过滤。
            limit(string): 返回条数。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.news_search",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        max_rows = self._safe_int(limit, default=5, min_value=1, max_value=20)
        items = await self.news_service.list_legacy_announcements(ctx["scope_key"], keyword=keyword, limit=max_rows)
        lines = [f"{idx}. {it.title} | {it.url}" for idx, it in enumerate(items, start=1)] or ["未找到历史公告入口。"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{it.url} ({it.source_note})" for it in items[:6]]
                or ["https://www.isct.ac.jp/en/news/it87dcs7t5y2 (official legacy notice)"],
                answer_lines=lines,
                inference_lines=["该工具用于兼容合并前历史通知，不保证每条都有完整正文。"],
            )
        )

    @filter.llm_tool("isct_abroad_list_programs")
    async def llm_tool_abroad_list_programs(self, event: AstrMessageEvent, lang: str = "en", limit: str = "8"):
        """列出留学/交换项目入口。

        Args:
            lang(string): en 或 ja。
            limit(string): 返回条目数。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.abroad",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        max_rows = self._safe_int(limit, default=8, min_value=1, max_value=20)
        items = await self.campus_service.list_abroad_programs(ctx["scope_key"], lang=lang, limit=max_rows)
        lines = [f"{idx}. {it['program_name']} | {it['url']}" for idx, it in enumerate(items, start=1)] or ["未检索到项目。"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{it['url']} (official abroad page)" for it in items[:6]]
                or ["https://students.isct.ac.jp/en/016/global/abroad (official abroad page)"],
                answer_lines=lines,
                inference_lines=["更具体申请条件需进入详情页再解析。"],
            )
        )

    @filter.llm_tool("isct_scholarship_overview")
    async def llm_tool_scholarship_overview(self, event: AstrMessageEvent, lang: str = "en"):
        """奖学金总览入口导航。

        Args:
            lang(string): en 或 ja。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.scholarship",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        payload = await self.campus_service.get_scholarship_overview(ctx["scope_key"], lang=lang)
        if not payload:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=["未获取到奖学金总览。"],
                    inference_lines=["请检查 allowedDomains 与 scholarship seeds 配置。"],
                )
            )
            return
        lines = [
            f"categories: {', '.join(payload.get('categories', [])[:10]) or '官网未标注'}",
            f"links: {' | '.join(payload.get('links', [])[:10]) or '官网未标注'}",
        ]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{payload.get('source_url')} (official scholarships overview)"],
                answer_lines=lines,
                inference_lines=["可进一步结合 isct_news_list_current_students(tab=112) 做最新公告筛选。"],
            )
        )

    @filter.llm_tool("isct_scholarship_announcements")
    async def llm_tool_scholarship_announcements(self, event: AstrMessageEvent, lang: str = "en", limit: str = "8"):
        """列出奖学金最新公告（tab=112）。

        Args:
            lang(string): en 或 ja。
            limit(string): 返回条数。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.scholarship",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        max_rows = self._safe_int(limit, default=8, min_value=1, max_value=20)
        items = await self.campus_service.list_scholarship_announcements(
            ctx["scope_key"], lang=lang, limit=max_rows
        )
        lines = [f"{idx}. {it.title} ({it.date}) | {it.url}" for idx, it in enumerate(items, start=1)] or ["暂无条目。"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{it.url} ({it.source_note})" for it in items[:8]]
                or ["https://students.isct.ac.jp/en/news?tab=112 (official scholarship tab)"],
                answer_lines=lines,
                inference_lines=["来源为在学生新闻 tab=112（tuition and scholarships）。"],
            )
        )

    @filter.llm_tool("isct_clubs_overview")
    async def llm_tool_clubs_overview(self, event: AstrMessageEvent, lang: str = "en", limit: str = "10"):
        """课外活动/社团入口总览。

        Args:
            lang(string): en 或 ja。
            limit(string): 返回条数。
        """
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        decision = await self._preflight(
            scope_key=ctx["scope_key"],
            user_key=ctx["user_key"],
            role_ids=ctx["role_ids"],
            feature_id="feature.clubs",
        )
        if not decision.allowed:
            return (decision.message or "请求被策略拒绝。")
            return
        max_rows = self._safe_int(limit, default=10, min_value=1, max_value=20)
        payload = await self.campus_service.get_clubs_overview(ctx["scope_key"], lang=lang, limit=max_rows)
        if not payload:
            return (
                await self._guarded_response(
                    ctx["scope_key"],
                    sources=[],
                    answer_lines=["未检索到社团/课外活动入口。"],
                    inference_lines=["可在 scope config 中补充 clubs seeds。"],
                )
            )
            return
        sections = payload.get("sections", [])
        links = payload.get("links", [])
        lines = [f"{idx}. {sections[idx-1]} | {links[idx-1]}" for idx in range(1, min(len(sections), len(links)) + 1)]
        if not lines:
            lines = [f"source: {payload.get('source_url')}"]
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[f"{payload.get('source_url')} (official extracurricular page)"] + [f"{u} (official link)" for u in links[:6]],
                answer_lines=lines,
                inference_lines=["若启用非官方社团站，请在输出中明确标注非官方来源。"],
            )
        )
