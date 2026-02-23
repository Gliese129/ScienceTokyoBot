from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

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
        now = datetime.now(timezone.utc)
        default_ay = now.year if now.month >= 4 else now.year - 1
        return await self.calendar_service.get_academic_schedule(
            scope_key,
            year=year or default_ay,
        )

    async def calendar_window(
        self,
        scope_key: str,
        *,
        mode: str,
        year: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        _ = year
        today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
        if mode == "this-week":
            week_start = today - timedelta(days=today.weekday())
            week_end = week_start + timedelta(days=6)
            events = await self.calendar_service.list_events_overlap(
                scope_key,
                start_date=week_start,
                end_date=week_end,
            )
            return events, events
        next_event = await self.calendar_service.find_next_event(
            scope_key,
            from_date=today,
            include_class_range=False,
        )
        if not next_event:
            return [], []
        return [next_event], [next_event]
