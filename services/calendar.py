from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

from runtime.sqlite_runtime import KVRuntime
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed, html_to_text

_FULL_DATE_RE = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})")
_MD_DATE_RE = re.compile(r"\b(\d{1,2})[./-](\d{1,2})\b")
_MONTH_RANGE_RE = re.compile(
    r"\b(?P<sm>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+(?P<sd>\d{1,2})\s*[-–]\s*"
    r"(?:(?P<em>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+)?"
    r"(?P<ed>\d{1,2})(?:,\s*(?P<ey>20\d{2}))?\b",
    re.IGNORECASE,
)
_MONTH_DAY_RE = re.compile(
    r"\b(?P<m>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(?P<d>\d{1,2})(?:,\s*(?P<y>20\d{2}))?\b",
    re.IGNORECASE,
)
_TERM_RE = re.compile(r"\b([1-4])Q\b", re.IGNORECASE)
_YEAR_HINT_RE = re.compile(r"(20\d{2})")


@dataclass(frozen=True)
class CalendarEvent:
    title: str
    start_date: str
    end_date: str
    kind: str
    is_no_class: bool
    source_url: str


class CalendarService:
    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime
        self.fetcher = Fetcher(runtime)

    async def get_academic_schedule(self, scope_key: str, *, year: int | None = None) -> dict:
        target_year = int(year or datetime.utcnow().year)
        indexed_events = await self.runtime.list_calendar_events(target_year)
        if indexed_events:
            term_ranges = await self.runtime.list_term_ranges(target_year)
            return {"year": target_year, "events": indexed_events, "term_ranges": term_ranges, "source": "indexed"}
        cache_key = f"calendar.schedule::{scope_key}::{target_year}"
        cached = await self.runtime.get_search_cache(cache_key, max_age_sec=6 * 60 * 60)
        if cached:
            return {"year": target_year, "events": cached, "source": "cache"}

        config = await self.runtime.get_effective_config(scope_key)
        sources = config.get("sources", {})
        allowed_domains = [str(x) for x in sources.get("allowedDomains", [])]
        seeds = [str(x) for x in sources.get("seeds", {}).get("calendar", [])]

        events: list[CalendarEvent] = []
        for seed in seeds[:6]:
            if not host_allowed(seed, allowed_domains):
                continue
            try:
                page = await self.fetcher.fetch_text(seed)
            except Exception:
                continue
            events.extend(self.parse_calendar_html(page.text, seed, target_year))

        # De-dup
        unique: dict[tuple[str, str, str], CalendarEvent] = {}
        for event in events:
            unique[(event.title, event.start_date, event.end_date)] = event
        normalized = [asdict(item) for item in unique.values()]
        normalized.sort(key=lambda x: (x.get("start_date", ""), x.get("title", "")))
        await self.runtime.put_search_cache(cache_key, normalized)
        return {"year": target_year, "events": normalized, "source": "live"}

    def parse_calendar_html(self, html_text: str, source_url: str, year: int) -> list[CalendarEvent]:
        text = html_to_text(html_text)
        return self._extract_events(text, source_url, year)

    def extract_schedule_pdfs(self, html_text: str, source_url: str) -> dict[str, str]:
        links = extract_links(html_text, source_url)
        monthly = ""
        dow = ""
        for link in links:
            if not link.url.lower().endswith(".pdf"):
                continue
            text = (link.text or "").lower()
            if "monthly class schedule" in text:
                monthly = link.url
            if "day of the week" in text:
                dow = link.url
        return {"monthly_pdf_url": monthly, "dow_pdf_url": dow}

    def extract_term_ranges(self, html_text: str, source_url: str, year: int) -> list[dict]:
        text = html_to_text(html_text)
        rows: list[dict] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line in lines:
            term_m = _TERM_RE.search(line)
            if not term_m:
                continue
            dates = self._extract_dates(line, year)
            if not dates:
                continue
            rows.append(
                {
                    "term": f"{term_m.group(1)}Q",
                    "start_date": min(dates).strftime("%Y-%m-%d"),
                    "end_date": max(dates).strftime("%Y-%m-%d"),
                    "source_url": source_url,
                }
            )
        dedup = {}
        for row in rows:
            dedup[row["term"]] = row
        return list(dedup.values())

    async def is_no_class_day(self, scope_key: str, *, date_str: str) -> dict[str, str | bool]:
        target = self._parse_date_safe(date_str)
        if target is None:
            return {
                "date": date_str,
                "is_no_class_day": False,
                "reason": "invalid_date_format",
                "next_class_day": "",
            }
        target_key = target.strftime("%Y-%m-%d")
        indexed_event_ids = await self.runtime.get_no_class_event_ids_by_date(target_key)
        if indexed_event_ids:
            titles = await self.runtime.get_calendar_event_titles_by_ids(indexed_event_ids)
            next_class_day = target
            for _ in range(60):
                next_class_day = next_class_day + timedelta(days=1)
                next_key = next_class_day.strftime("%Y-%m-%d")
                if not await self.runtime.get_no_class_event_ids_by_date(next_key):
                    break
            return {
                "date": target_key,
                "is_no_class_day": True,
                "reason": " / ".join(titles[:3]) if titles else "indexed no-class day",
                "next_class_day": next_class_day.strftime("%Y-%m-%d"),
            }
        schedule = await self.get_academic_schedule(scope_key, year=target.year)
        events = schedule.get("events", [])
        matched = []
        for item in events:
            if not item.get("is_no_class"):
                continue
            start = self._parse_date_safe(str(item.get("start_date")))
            end = self._parse_date_safe(str(item.get("end_date")))
            if start and end and start <= target <= end:
                matched.append(str(item.get("title")))
        is_no_class = bool(matched)
        next_class_day = target
        if is_no_class:
            for _ in range(45):
                next_class_day = next_class_day + timedelta(days=1)
                if not self._is_date_in_no_class(next_class_day, events):
                    break
        return {
            "date": target.strftime("%Y-%m-%d"),
            "is_no_class_day": is_no_class,
            "reason": " / ".join(matched[:3]) if matched else "not in no-class events",
            "next_class_day": next_class_day.strftime("%Y-%m-%d"),
        }

    def _extract_events(self, text: str, source_url: str, year: int) -> list[CalendarEvent]:
        out: list[CalendarEvent] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line in lines:
            if len(line) < 8:
                continue
            lower = line.lower()
            if not any(
                key in lower
                for key in [
                    "holiday",
                    "vacation",
                    "break",
                    "no class",
                    "exam",
                    "休",
                    "試験",
                    "授業",
                    "補講",
                ]
            ):
                continue
            ranges = self._extract_date_ranges(line, year)
            if not ranges:
                continue
            if "exam" in lower or "試験" in lower:
                kind = "exam"
            elif any(k in lower for k in ["holiday", "vacation", "break", "休", "no class"]):
                kind = "holiday"
            else:
                kind = "calendar"
            is_no_class = kind == "holiday" or "休講" in line or "no classes" in lower or "no class" in lower
            for start, end in ranges:
                out.append(
                    CalendarEvent(
                        title=line[:200],
                        start_date=start.strftime("%Y-%m-%d"),
                        end_date=end.strftime("%Y-%m-%d"),
                        kind=kind,
                        is_no_class=is_no_class,
                        source_url=source_url,
                    )
                )
        return out

    @staticmethod
    def _extract_date_ranges(text: str, fallback_year: int) -> list[tuple[date, date]]:
        ranges: list[tuple[date, date]] = []
        year_hint = CalendarService._extract_year_hint(text, fallback_year)
        for match in _FULL_DATE_RE.finditer(text):
            y = int(match.group(1))
            m = int(match.group(2))
            d = int(match.group(3))
            try:
                full = date(y, m, d)
                ranges.append((full, full))
            except ValueError:
                continue
        for match in _MONTH_RANGE_RE.finditer(text):
            start_month = CalendarService._month_to_number(match.group("sm"))
            start_day = int(match.group("sd"))
            end_month_raw = match.group("em")
            end_day = int(match.group("ed"))
            end_year_raw = match.group("ey")
            end_month = CalendarService._month_to_number(end_month_raw) if end_month_raw else start_month
            if start_month <= 0 or end_month <= 0:
                continue
            end_year = int(end_year_raw) if end_year_raw else year_hint
            start_year = end_year
            if start_month > end_month:
                start_year = end_year - 1
            try:
                start = date(start_year, start_month, start_day)
                end = date(end_year, end_month, end_day)
                if start <= end:
                    ranges.append((start, end))
                else:
                    ranges.append((end, start))
            except ValueError:
                continue
        for match in _MONTH_DAY_RE.finditer(text):
            month = CalendarService._month_to_number(match.group("m"))
            day = int(match.group("d"))
            year_raw = match.group("y")
            if month <= 0:
                continue
            year = int(year_raw) if year_raw else year_hint
            try:
                single = date(year, month, day)
                ranges.append((single, single))
            except ValueError:
                continue
        for match in _MD_DATE_RE.finditer(text):
            m = int(match.group(1))
            d = int(match.group(2))
            try:
                single = date(year_hint, m, d)
                ranges.append((single, single))
            except ValueError:
                continue
        if not ranges:
            return []
        # De-dup while preserving order.
        seen: set[tuple[str, str]] = set()
        out: list[tuple[date, date]] = []
        for start, end in ranges:
            key = (start.isoformat(), end.isoformat())
            if key in seen:
                continue
            seen.add(key)
            out.append((start, end))
        return out

    @staticmethod
    def _extract_dates(text: str, fallback_year: int) -> list[date]:
        ranges = CalendarService._extract_date_ranges(text, fallback_year)
        out: list[date] = []
        for start, end in ranges:
            out.append(start)
            if end != start:
                out.append(end)
        return out

    @staticmethod
    def _parse_date_safe(text: str) -> date | None:
        raw = (text or "").strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        return None

    def _is_date_in_no_class(self, target: date, events: list[dict]) -> bool:
        for item in events:
            if not item.get("is_no_class"):
                continue
            start = self._parse_date_safe(str(item.get("start_date")))
            end = self._parse_date_safe(str(item.get("end_date")))
            if start and end and start <= target <= end:
                return True
        return False

    @staticmethod
    def _month_to_number(name: str | None) -> int:
        if not name:
            return 0
        key = name.strip().lower()[:3]
        mapping = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        return mapping.get(key, 0)

    @staticmethod
    def _extract_year_hint(text: str, fallback_year: int) -> int:
        years = [int(m.group(1)) for m in _YEAR_HINT_RE.finditer(text or "")]
        if years:
            return max(years)
        return fallback_year
