from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from services.calendar import CalendarService
from services.campus import CampusInfoService
from services.exam import ExamService
from services.news import NewsService, NewsItem
from services.syllabus import SyllabusService, SyllabusCourse


class QueryOps:
    def __init__(
        self,
        *,
        exam_service: ExamService,
        syllabus_service: SyllabusService,
        calendar_service: CalendarService,
        news_service: NewsService,
        campus_service: CampusInfoService,
    ) -> None:
        self.exam_service = exam_service
        self.syllabus_service = syllabus_service
        self.calendar_service = calendar_service
        self.news_service = news_service
        self.campus_service = campus_service

    async def list_exam_pdfs(self, scope_key: str, *, limit: int) -> list[str]:
        return await self.exam_service.list_exam_pdfs(scope_key, limit=max(1, limit))

    async def search_courses(
        self,
        scope_key: str,
        *,
        q_name: str = "",
        teacher: str = "",
        code: str = "",
        group: str = "",
        year: int | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[SyllabusCourse]:
        target_year = int(year or datetime.now(timezone.utc).year)
        return await self.syllabus_service.search_courses(
            scope_key,
            year=target_year,
            q_name=q_name,
            teacher=teacher,
            code=code,
            group=group,
            limit=max(1, limit),
            offset=max(0, offset),
        )

    async def list_news(
        self,
        scope_key: str,
        *,
        keyword: str = "",
        tab: int | None = None,
        lang: str = "en",
        limit: int = 8,
    ) -> list[NewsItem]:
        return await self.news_service.list_current_students_news(
            scope_key,
            keyword=keyword,
            tab=tab,
            lang=lang,
            limit=max(1, limit),
        )

    async def academic_schedule(self, scope_key: str, *, year: int | None = None) -> dict[str, Any]:
        return await self.calendar_service.get_academic_schedule(
            scope_key,
            year=year or datetime.now(timezone.utc).year,
        )

    async def calendar_window(
        self,
        scope_key: str,
        *,
        mode: str,
        year: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        payload = await self.academic_schedule(scope_key, year=year)
        events = [item for item in payload.get("events", []) if isinstance(item, dict)]
        today = datetime.now(timezone.utc).date()
        if mode == "this-week":
            end_day = today + timedelta(days=7)
            picked = []
            for item in events:
                try:
                    start = datetime.strptime(str(item.get("start_date")), "%Y-%m-%d").date()
                except Exception:
                    continue
                if today <= start <= end_day:
                    picked.append(item)
            return picked, events
        future = []
        for item in events:
            try:
                start = datetime.strptime(str(item.get("start_date")), "%Y-%m-%d").date()
            except Exception:
                continue
            if start >= today:
                future.append(item)
        future.sort(key=lambda x: str(x.get("start_date")))
        return (future[:1] if future else []), events
