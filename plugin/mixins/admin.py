from __future__ import annotations

import json
from datetime import datetime, timezone

from plugin.astrbot_compat import AstrMessageEvent, filter


class AdminMixin:

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
