from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
# Avoid stale module cache collisions across plugin reloads in same interpreter.
for _mod_name in list(sys.modules.keys()):
    if _mod_name in {"guards", "adapters", "runtime", "services", "isct_core", "plugin"}:
        del sys.modules[_mod_name]
        continue
    if (
        _mod_name.startswith("guards.")
        or _mod_name.startswith("adapters.")
        or _mod_name.startswith("runtime.")
        or _mod_name.startswith("services.")
        or _mod_name.startswith("isct_core.")
        or _mod_name.startswith("plugin.")
    ):
        del sys.modules[_mod_name]

from isct_core import QueryOps, SyncManager, load_global_config
from plugin.astrbot_compat import Context, Star, astr_logger, get_astrbot_data_path, register
from plugin.mixins import AdminMixin, CalendarMixin, CommonMixin, ExamMixin, MiscMixin, NewsMixin, SyllabusMixin
from runtime.sqlite_runtime import KVRuntime
from services.calendar import CalendarService
from services.campus import CampusInfoService
from services.discovery import DiscoveryService
from services.exam import ExamService
from services.news import NewsService
from services.syllabus import SyllabusService


@register("astrbot_plugin_isct_bot", "Gliese", "ScienceTokyo Plugin", "0.7.0")
class ScienceTokyoNerdBotPlugin(
    AdminMixin,
    ExamMixin,
    CalendarMixin,
    NewsMixin,
    SyllabusMixin,
    MiscMixin,
    CommonMixin,
    Star,
):
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
        self.parser_provider_id = str(self.page_config.get("parser_provider_id", "")).strip()
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
            context=self.context,
            parser_provider_id=self.parser_provider_id,
        )
        self.sync_manager.ensure_started()
        astr_logger.info(
            "ScienceTokyo Plugin initialized with SQLite runtime, db=%s, config_keys=%s, source_debug=%s, parser_provider_id=%s",
            str(self.plugin_data_dir / "runtime.sqlite3"),
            sorted(list(self.page_config.keys())),
            self.global_config.source_debug_enabled,
            self.parser_provider_id or "<empty>",
        )


__all__ = ["ScienceTokyoNerdBotPlugin"]
