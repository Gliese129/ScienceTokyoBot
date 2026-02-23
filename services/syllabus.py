from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from urllib.parse import urlencode, urlparse

from runtime.sqlite_runtime import KVRuntime
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed, html_to_text, text_snippet

_COURSE_CODE_RE = re.compile(r"\b[A-Z]{2,5}\d{3,4}[A-Z]?\b")
_YEAR_RE = re.compile(r"\b(20\d{2})\b")


@dataclass(frozen=True)
class SyllabusGroup:
    group_id: str
    name_en: str
    name_ja: str
    notes: str


@dataclass(frozen=True)
class SyllabusCourse:
    title: str
    code: str
    instructors: str
    year: int
    term: str
    campus: str
    credits: str
    detail_ref: str


class SyllabusService:
    BASE_URL = "https://syllabus.s.isct.ac.jp"
    DEFAULT_GROUPS = ["100", "200", "300", "400", "500", "600"]

    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime
        self.fetcher = Fetcher(runtime)

    async def list_groups(self, scope_key: str, *, limit: int = 30) -> list[SyllabusGroup]:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        if not host_allowed(self.BASE_URL, allowed_domains):
            return []
        page = await self.fetcher.fetch_text(self.BASE_URL)
        links = extract_links(page.text, self.BASE_URL)
        groups: list[SyllabusGroup] = []
        seen: set[str] = set()
        for link in links:
            if "/search" not in link.url:
                continue
            parsed = urlparse(link.url)
            if "group=" not in parsed.query:
                continue
            group_id = ""
            for token in parsed.query.split("&"):
                if token.startswith("group="):
                    group_id = token.split("=", 1)[1]
                    break
            if not group_id or group_id in seen:
                continue
            seen.add(group_id)
            name = (link.text or f"group {group_id}").strip()
            groups.append(
                SyllabusGroup(
                    group_id=group_id,
                    name_en=name[:80],
                    name_ja=name[:80],
                    notes="from syllabus top page",
                )
            )
            if len(groups) >= max(1, limit):
                break
        if groups:
            return groups
        # Fallback to common science tokyo groups.
        return [
            SyllabusGroup("100", "Science", "理学院", "fallback"),
            SyllabusGroup("200", "Engineering", "工学院", "fallback"),
            SyllabusGroup("300", "Materials", "物質理工学院", "fallback"),
            SyllabusGroup("400", "Computing", "情報理工学院", "fallback"),
            SyllabusGroup("500", "Life Science", "生命理工学院", "fallback"),
            SyllabusGroup("600", "Environment", "環境・社会理工学院", "fallback"),
        ][: max(1, limit)]

    async def search_courses(
        self,
        scope_key: str,
        *,
        year: int,
        q_name: str = "",
        teacher: str = "",
        code: str = "",
        group: str = "",
        limit: int = 10,
        offset: int = 0,
    ) -> list[SyllabusCourse]:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        if not host_allowed(self.BASE_URL, allowed_domains):
            return []

        available_years = await self.list_available_years(scope_key)
        target_year = int(year)
        if available_years and target_year not in available_years:
            target_year = max(available_years)

        groups = [group.strip()] if group.strip() else list(self.DEFAULT_GROUPS)
        params = [
            ("hl", "en"),
            ("year", str(target_year)),
            ("name", q_name),
            ("teacher", teacher),
            ("code", code),
        ]
        for g in groups:
            params.append(("group", g))
        search_url = f"{self.BASE_URL}/search?{urlencode(params, doseq=True)}"

        cache_key = f"syllabus.search::{scope_key}::{search_url}"
        cached = await self.runtime.get_search_cache(cache_key, max_age_sec=10 * 60)
        if cached:
            items = [SyllabusCourse(**item) for item in cached]
            return items[offset : offset + max(1, limit)]

        page = await self.fetcher.fetch_text(search_url)
        links = extract_links(page.text, search_url)
        out: list[SyllabusCourse] = []
        seen: set[str] = set()
        for link in links:
            url_l = link.url.lower()
            if "/courses/" not in url_l:
                continue
            if "/search" in url_l:
                continue
            if link.url in seen:
                continue
            seen.add(link.url)
            raw_title = (link.text or "").strip()
            parsed_code = self._extract_code(raw_title) or code.upper().strip()
            title = raw_title or parsed_code or "unknown course"
            out.append(
                SyllabusCourse(
                    title=title[:160],
                    code=parsed_code or "官网未标注",
                    instructors=teacher.strip() or "官网未标注",
                    year=target_year,
                    term="官网未标注",
                    campus="官网未标注",
                    credits="官网未标注",
                    detail_ref=link.url,
                )
            )
        await self.runtime.put_search_cache(cache_key, [asdict(x) for x in out])
        return out[offset : offset + max(1, limit)]

    async def list_available_years(self, scope_key: str, *, force_refresh: bool = False) -> list[int]:
        cached = await self.runtime.get_syllabus_available_years()
        if force_refresh:
            cached = []
        if cached:
            return cached
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        search_url = f"{self.BASE_URL}/search?hl=en"
        if not host_allowed(search_url, allowed_domains):
            return []
        try:
            page = await self.fetcher.fetch_text(search_url)
        except Exception:
            return []
        years: set[int] = set()
        for matched in _YEAR_RE.findall(page.text or ""):
            year = int(matched)
            if 2000 <= year <= 2100:
                years.add(year)
        out = sorted(years)
        if out:
            await self.runtime.set_syllabus_available_years(out)
        return out

    async def get_course_detail(
        self,
        scope_key: str,
        *,
        jwc_or_url: str,
        fields: list[str] | None = None,
    ) -> dict[str, str]:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]

        target = jwc_or_url.strip()
        if not target:
            return {}
        if target.startswith("http://") or target.startswith("https://"):
            url = target
        else:
            url = f"{self.BASE_URL}/courses?jwc={target}"
        if not host_allowed(url, allowed_domains):
            return {}

        page = await self.fetcher.fetch_text(url)
        text = html_to_text(page.text)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        title = lines[0] if lines else "官网未标注"
        code = self._extract_code(text) or "官网未标注"
        detail = {
            "title": title[:180],
            "code": code,
            "instructors": self._extract_after_label(lines, ["Instructor", "Teacher", "担当教員"]),
            "year": self._extract_after_label(lines, ["Academic Year", "Year", "年度"]),
            "term": self._extract_after_label(lines, ["Quarter", "Term", "学期"]),
            "credits": self._extract_after_label(lines, ["Credit", "Credits", "単位"]),
            "language": self._extract_after_label(lines, ["Language", "授業言語"]),
            "schedule": self._extract_after_label(lines, ["Schedule", "Time", "曜日", "時限"]),
            "objectives": self._extract_after_label(lines, ["Objectives", "到達目標"]),
            "outline": self._extract_after_label(lines, ["Outline", "内容", "授業の概要"]),
            "grading": self._extract_after_label(lines, ["Grading", "Evaluation", "成績評価"]),
            "textbooks": self._extract_after_label(lines, ["Textbook", "教材"]),
            "notes": text_snippet(text, "note", window=180) or "官网未标注",
            "url": url,
        }
        normalized = {k: (v if v else "官网未标注") for k, v in detail.items()}
        if fields:
            field_set = {item.strip() for item in fields if item.strip()}
            field_set.update({"title", "code", "url"})
            normalized = {k: v for k, v in normalized.items() if k in field_set}
        return normalized

    @staticmethod
    def _extract_code(text: str) -> str:
        m = _COURSE_CODE_RE.search((text or "").upper())
        if not m:
            return ""
        return m.group(0)

    @staticmethod
    def _extract_after_label(lines: list[str], labels: list[str]) -> str:
        lowered_labels = [label.lower() for label in labels]
        for line in lines:
            low = line.lower()
            for label in lowered_labels:
                if label in low:
                    if ":" in line:
                        value = line.split(":", 1)[1].strip()
                    elif "：" in line:
                        value = line.split("：", 1)[1].strip()
                    else:
                        value = line.replace(label, "", 1).strip()
                    if value:
                        return value[:300]
        return ""
