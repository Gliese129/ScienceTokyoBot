from __future__ import annotations

import inspect
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
# Avoid stale module cache collisions across plugin reloads in same interpreter.
for _mod_name in list(sys.modules.keys()):
    if _mod_name in {"guards", "adapters", "runtime", "services", "isct_core"}:
        del sys.modules[_mod_name]
        continue
    if (
        _mod_name.startswith("guards.")
        or _mod_name.startswith("adapters.")
        or _mod_name.startswith("runtime.")
        or _mod_name.startswith("services.")
        or _mod_name.startswith("isct_core.")
    ):
        del sys.modules[_mod_name]

from adapters.scope_mapping import build_scope_mapping, stringify_unified_origin
from guards.answer_guard import enforce_answer_guard, format_structured_response
from isct_core import QueryOps, SyncManager, build_source_debug, load_global_config
from runtime.sqlite_runtime import KVRuntime, RuntimeDecision
from services.calendar import CalendarService
from services.campus import CampusInfoService
from services.discovery import DiscoveryService
from services.exam import ExamService
from services.news import NewsService
from services.syllabus import SyllabusService

try:
    from astrbot.api.star import Context, Star, register
except ModuleNotFoundError:
    class Context:  # type: ignore[no-redef]
        pass

    class Star:  # type: ignore[no-redef]
        def __init__(self, context: Context) -> None:
            self.context = context

    def register(*_args: Any, **_kwargs: Any):  # type: ignore[no-redef]
        def _inner(cls: type) -> type:
            return cls

        return _inner

    AstrMessageEvent = Any  # type: ignore

    class _DummyFilter:
        @staticmethod
        def command(_name: str):
            def _inner(func):
                return func

            return _inner

        @staticmethod
        def llm_tool(_name: str | None = None, **_kwargs: Any):
            def _inner(func):
                return func

            return _inner

    filter = _DummyFilter()  # type: ignore[assignment]

    class Plain:  # type: ignore[no-redef]
        def __init__(self, text: str) -> None:
            self.text = text
else:
    try:
        from astrbot.api.event import AstrMessageEvent, filter
    except Exception:
        AstrMessageEvent = Any  # type: ignore

        class _DummyFilter:
            @staticmethod
            def command(_name: str):
                def _inner(func):
                    return func

                return _inner

            @staticmethod
            def llm_tool(_name: str | None = None, **_kwargs: Any):
                def _inner(func):
                    return func

                return _inner

        filter = _DummyFilter()  # type: ignore[assignment]

    try:
        from astrbot.api.message_components import Plain
    except Exception:
        class Plain:  # type: ignore[no-redef]
            def __init__(self, text: str) -> None:
                self.text = text

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path
except Exception:
    def get_astrbot_data_path() -> Path:  # type: ignore[no-redef]
        return Path("data")

try:
    from astrbot.core.utils.session_waiter import SessionController, session_waiter
    SESSION_WAITER_AVAILABLE = True
except Exception:
    SESSION_WAITER_AVAILABLE = False
    SessionController = Any  # type: ignore[assignment]

    def session_waiter(*_args: Any, **_kwargs: Any):  # type: ignore[no-redef]
        def _inner(func: Any) -> Any:
            return func

        return _inner


try:
    from astrbot.api import logger as astr_logger
except Exception:
    astr_logger = logging.getLogger("astrbot_plugin_isct_bot")


@register("astrbot_plugin_isct_bot", "Gliese", "ScienceTokyo Plugin", "0.7.0")
class ScienceTokyoNerdBotPlugin(Star):
    PLUGIN_NAME = "astrbot_plugin_isct_bot"

    def __init__(self, context: Context, config: dict[str, Any] | None = None) -> None:
        super().__init__(context)
        self.page_config = config or {}
        plugin_runtime_name = str(getattr(self, "name", self.PLUGIN_NAME))
        self.plugin_data_dir = Path(get_astrbot_data_path()) / "plugin_data" / plugin_runtime_name
        self.plugin_data_dir.mkdir(parents=True, exist_ok=True)
        self.runtime = KVRuntime(
            plugin_name=self.PLUGIN_NAME,
            db_path=self.plugin_data_dir / "runtime.sqlite3",
            page_config=self.page_config,
        )
        self.exam_service = ExamService(self.runtime)
        self.discovery_service = DiscoveryService(self.runtime)
        self.syllabus_service = SyllabusService(self.runtime)
        self.calendar_service = CalendarService(self.runtime)
        self.news_service = NewsService(self.runtime)
        self.campus_service = CampusInfoService(self.runtime)
        self.global_config = load_global_config(Path(__file__).parent / "config" / "plugin_global.json")
        self.runtime.set_fallback_sources(self.global_config.raw.get("fallbackSources", {}))
        self.query_ops = QueryOps(
            exam_service=self.exam_service,
            syllabus_service=self.syllabus_service,
            calendar_service=self.calendar_service,
            news_service=self.news_service,
            campus_service=self.campus_service,
        )
        self.sync_manager = SyncManager(
            runtime=self.runtime,
            global_config=self.global_config,
            fetcher=self.exam_service.fetcher,
            calendar_service=self.calendar_service,
            exam_service=self.exam_service,
            syllabus_service=self.syllabus_service,
            logger=astr_logger,
        )
        self.sync_manager.ensure_started()
        astr_logger.info(
            "ScienceTokyo Plugin initialized with SQLite runtime, db=%s, config_keys=%s, source_debug=%s",
            str(self.plugin_data_dir / "runtime.sqlite3"),
            sorted(list(self.page_config.keys())),
            self.global_config.source_debug_enabled,
        )

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

    async def calendar_this_week(self, event: AstrMessageEvent):
        """学事日历：本周关键节点/无课日/考试周候选。"""
        async for result in self._calendar_common(event, mode="this-week"):
            yield result

    async def calendar_next(self, event: AstrMessageEvent):
        """学事日历：下一次关键节点候选。"""
        async for result in self._calendar_common(event, mode="next"):
            yield result

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

    @filter.command("isct_admin_config_show")
    async def admin_config_show(self, event: AstrMessageEvent, scope: str = ""):
        """管理员命令：查看目标 scope 的生效配置。"""
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 admin_config_show。")
            return
        target_scope = scope.strip() or ctx["scope_key"]
        if not scope.strip():
            args = self._extract_command_args(event)
            target_scope = args[0] if args else target_scope
        cfg = await self.runtime.get_effective_config(target_scope)
        await self.runtime.log_admin_action(
            actor_user_key=ctx["user_key"],
            scope_key=ctx["scope_key"],
            action="admin_config_show",
            target=target_scope,
            detail={},
        )
        yield self._make_event_result(event, json.dumps(cfg, ensure_ascii=False, indent=2)[:3800])

    @filter.command("isct_admin_config_set")
    async def admin_config_set(self, event: AstrMessageEvent, scope: str = "", path: str = "", value: str = ""):
        """管理员命令：按路径写入 scope 配置（支持字面量解析）。"""
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 admin_config_set。")
            return
        target_scope = scope.strip()
        target_path = path.strip()
        raw_value = value.strip()
        if not (target_scope and target_path and raw_value):
            args = self._extract_command_args(event)
            if len(args) >= 3:
                target_scope, target_path = args[0], args[1]
                raw_value = " ".join(args[2:])
        if not (target_scope and target_path and raw_value):
            yield self._make_event_result(event, "用法：/admin_config_set <scope> <path> <value>")
            return
        value = self._parse_literal_value(raw_value)
        await self.runtime.set_scope_path(target_scope, target_path, value)
        await self.runtime.log_admin_action(
            actor_user_key=ctx["user_key"],
            scope_key=ctx["scope_key"],
            action="admin_config_set",
            target=target_scope,
            detail={"path": target_path, "value": value},
        )
        yield self._make_event_result(event, f"config updated: {target_scope} {target_path}={value}")

    @filter.command("isct_admin_feature_enable")
    async def admin_feature_enable(self, event: AstrMessageEvent):
        """管理员命令：为目标 scope 启用某 feature。"""
        async for result in self._admin_feature_toggle(event, enabled=True):
            yield result

    @filter.command("isct_admin_feature_disable")
    async def admin_feature_disable(self, event: AstrMessageEvent):
        """管理员命令：为目标 scope 禁用某 feature。"""
        async for result in self._admin_feature_toggle(event, enabled=False):
            yield result

    @filter.command("isct_admin_audit")
    async def admin_audit(self, event: AstrMessageEvent):
        """管理员命令：查看最近审计日志。"""
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 admin_audit。")
            return
        logs = await self.runtime.list_admin_audit(limit=20)
        lines = [
            f"#{row['id']} {row['action']} by={row['actor_user_key']} scope={row['scope_key']} target={row.get('target')} at={row['created_at']}"
            for row in logs
        ]
        yield self._make_event_result(event, "\\n".join(lines) if lines else "暂无审计日志。")

    @filter.command("isct_admin_source_debug")
    async def admin_source_debug(self, event: AstrMessageEvent, category: str = ""):
        """管理员命令：输出指定 category 的 source 配置与回退来源。"""
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 admin_source_debug。")
            return
        target = category.strip()
        if not target:
            args = self._extract_command_args(event)
            target = args[0] if args else ""
        if not target:
            yield self._make_event_result(
                event,
                "用法：/isct_admin_source_debug <exam|course|calendar|news|scholarship|abroad|clubs|news_legacy>",
            )
            return
        cfg = await self.runtime.get_effective_config(ctx["scope_key"])
        source_cfg = cfg.get("sources", {})
        allowed_domains = [str(x) for x in source_cfg.get("allowedDomains", [])]
        seeds = [str(x) for x in source_cfg.get("seeds", {}).get(target, [])]
        fallbacks = self.global_config.fallback_sources(target)
        text = (
            f"category={target}\n"
            f"allowed_domains({len(allowed_domains)}):\n- " + "\n- ".join(allowed_domains[:20]) + "\n\n"
            f"seeds({len(seeds)}):\n- " + ("\n- ".join(seeds[:20]) if seeds else "无") + "\n\n"
            f"fallbacks({len(fallbacks)}):\n- " + ("\n- ".join(fallbacks[:20]) if fallbacks else "无")
        )
        yield self._make_event_result(event, text)

    @filter.command("isct_admin_sync_status")
    async def admin_sync_status(self, event: AstrMessageEvent):
        """管理员命令：查看定时同步任务状态。"""
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 admin_sync_status。")
            return
        report = await self.sync_manager.build_status_report()
        lines = []
        for job in report.get("jobs", []):
            lines.append(
                f"{job.get('job_name')}: run={job.get('last_run_at')} ok={job.get('last_success_at')} changed={job.get('changed')} "
                f"records={job.get('last_record_count')} sha={job.get('last_sha256') or ''} err={job.get('last_error') or ''}"
            )
        years = report.get("current_cached_years", [])
        lines.append(f"current_cached_years={years}")
        yield self._make_event_result(event, "\n".join(lines) if lines else "暂无同步状态。")

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

    @filter.command("isct_help")
    async def command_isct_help(self, event: AstrMessageEvent):
        """管理员帮助命令：输出插件工具与管理员指令清单。"""
        ctx = self._extract_event_context(event)
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 isct_help。")
            return
        help_text = (
            "ScienceTokyo Plugin Help\n"
            "LLM tools:\n"
            "- isct_exam_sync(mode=latest)\n"
            "- isct_exam_latest(request=latest)\n"
            "- isct_exam_list_pdfs(limit)\n"
            "- isct_exam_parse_pdf(pdf_url)\n"
            "- isct_exam_find_exam(query)\n"
            "- isct_exam_course(query)\n"
            "- isct_exam_day(date)\n"
            "- isct_exam_watch(course_code)\n"
            "- isct_course_search(query)\n"
            "- isct_course_compare(course_a, course_b)\n"
            "- isct_syllabus_list_groups(limit)\n"
            "- isct_syllabus_search_courses(q_name, teacher, code, group, year, limit, offset)\n"
            "- isct_syllabus_get_course_detail(jwc_or_url, fields)\n"
            "- isct_calendar_this_week(hint=this-week)\n"
            "- isct_calendar_next(hint=next)\n"
            "- isct_calendar_get_academic_schedule(year)\n"
            "- isct_calendar_is_no_class_day(date)\n"
            "- isct_news_search(query)\n\n"
            "- isct_news_list_current_students(keyword, tab, lang, limit)\n"
            "- isct_news_get_item(url)\n"
            "- isct_news_list_legacy(keyword, limit)\n"
            "- isct_abroad_list_programs(lang, limit)\n"
            "- isct_scholarship_overview(lang)\n"
            "- isct_scholarship_announcements(lang, limit)\n"
            "- isct_clubs_overview(lang, limit)\n\n"
            "Admin commands (only commands):\n"
            "- /isct_admin_config_show [scope]\n"
            "- /isct_admin_config_set <scope> <path> <value>\n"
            "- /isct_admin_feature_enable <scope> <feature>\n"
            "- /isct_admin_feature_disable <scope> <feature>\n"
            "- /isct_admin_audit\n"
            "- /isct_admin_source_debug <category>\n"
            "- /isct_admin_sync_status\n"
            "- /isct_admin_push_test [scope]\n"
            "- /isct_admin_mod_strike <user_key> <severe|light> <reason>\n"
            "- /isct_admin_exam_cache_set <pdf_url>\n"
            "- /isct_help\n"
        )
        yield self._make_event_result(event, help_text)

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
        target_year = self._safe_int(year, default=datetime.now(timezone.utc).year, min_value=2000, max_value=2100)
        payload = await self.query_ops.academic_schedule(ctx["scope_key"], year=target_year)
        events = [item for item in payload.get("events", []) if isinstance(item, dict)]
        lines = [
            f"{idx}. {it.get('start_date')}~{it.get('end_date')} [{it.get('kind')}] {it.get('title')}"
            for idx, it in enumerate(events[:12], start=1)
        ]
        if not lines:
            lines = ["未解析到学事日历事件。"]
        sources, inference_lines = await self._with_source_debug(
            scope_key=ctx["scope_key"],
            category="calendar",
            sources=[f"{it.get('source_url')} (official schedule)" for it in events[:8] if it.get("source_url")],
            inference_lines=["建议后续对单日判定走 isct_calendar_is_no_class_day，减少 LLM 推理成本。"],
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
        return (
            await self._guarded_response(
                ctx["scope_key"],
                sources=[
                    "https://www.titech.ac.jp/english/student/students/life/schedules/2025 (official academic schedule)"
                ],
                answer_lines=answer_lines,
                inference_lines=["判定为规则匹配，不依赖 LLM。若日期格式错误会直接返回 invalid_date_format。"],
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

    @filter.command("isct_admin_push_test")
    async def push_test(self, event: AstrMessageEvent, target_scope: str = ""):
        """管理员命令：向目标 scope 发送主动推送测试消息。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])

        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可触发 push_test。")
            return

        scope = target_scope.strip()
        if not scope:
            args = self._extract_command_args(event)
            scope = args[0] if args else ctx["scope_key"]
        target_scope = scope
        target_umo = await self._resolve_target_unified_msg_origin(target_scope)
        if not target_umo:
            yield self._make_event_result(event, "目标 scope 尚未记录 unified_msg_origin。")
            return

        message_chain = self._build_message_chain(
            f"[push_test] {datetime.now(timezone.utc).isoformat()} scope={target_scope}"
        )
        await self.context.send_message(target_umo, message_chain)
        yield self._make_event_result(event, f"push_test 已投递到 {target_scope}")

    @filter.command("isct_admin_mod_strike")
    async def mod_strike(self, event: AstrMessageEvent):
        """管理员命令：手动记录违规并触发惩罚升级。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可执行 mod_strike。")
            return

        args = self._extract_command_args(event)
        if len(args) < 3:
            yield self._make_event_result(event, "用法：/mod_strike <user_key> <severe|light> <reason>")
            return
        target_user_key = args[0]
        severe = args[1].lower() == "severe"
        reason = " ".join(args[2:])
        decision = await self.runtime.add_violation_score(
            scope_key=ctx["scope_key"],
            user_key=target_user_key,
            severe=severe,
            reason=reason,
        )
        yield self._make_event_result(event, decision.message or f"strike applied: {decision.reason}")

    @filter.command("isct_admin_exam_cache_set")
    async def exam_cache_set(self, event: AstrMessageEvent, pdf_url: str = ""):
        """管理员命令：手动写入一个 exam 版本（调试用途）。"""
        ctx = self._extract_event_context(event)
        await self._remember_origin(scope_key=ctx["scope_key"], unified_msg_origin=ctx["unified_msg_origin"])
        if not await self.runtime.is_admin(ctx["scope_key"], ctx["user_key"], ctx["role_ids"]):
            yield self._make_event_result(event, "仅管理员可写入 exam 缓存。")
            return
        target_pdf_url = pdf_url.strip()
        if not target_pdf_url:
            args = self._extract_command_args(event)
            target_pdf_url = args[0].strip() if args else ""
        if not target_pdf_url:
            yield self._make_event_result(event, "用法：/exam_cache_set <pdf_url>")
            return
        fake_hash = f"manual_{int(datetime.now(timezone.utc).timestamp())}"
        save_result = await self.runtime.save_exam_version(
            pdf_url=target_pdf_url,
            pdf_hash=fake_hash,
            records=[],
            parse_error="manual_set",
        )
        yield self._make_event_result(
            event,
            f"exam version 写入成功：version_id={save_result.get('version_id')} url={target_pdf_url}",
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
        resolved_sources, debug_line = await build_source_debug(
            self.runtime,
            self.global_config,
            scope_key=scope_key,
            category=category,
            sources=sources,
        )
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
        picked, all_events = await self.query_ops.calendar_window(ctx["scope_key"], mode=mode)
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


__all__ = ["ScienceTokyoNerdBotPlugin"]
