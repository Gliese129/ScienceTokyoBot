from __future__ import annotations

from runtime.sqlite_runtime import KVRuntime
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed, html_to_text
from services.news import NewsService, NewsItem


class CampusInfoService:
    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime
        self.fetcher = Fetcher(runtime)
        self.news_service = NewsService(runtime)

    async def list_abroad_programs(self, scope_key: str, *, lang: str = "en", limit: int = 10) -> list[dict]:
        path = "https://students.isct.ac.jp/en/016/global/abroad"
        if lang.lower() == "ja":
            path = "https://students.isct.ac.jp/ja/016/global/abroad"
        page = await self._safe_fetch_allowed(scope_key, path)
        if not page:
            return []
        links = extract_links(page, path)
        out: list[dict] = []
        seen: set[str] = set()
        for link in links:
            if link.url in seen:
                continue
            title = (link.text or "").strip()
            if len(title) < 3:
                continue
            low = f"{title} {link.url}".lower()
            if not any(k in low for k in ["abroad", "exchange", "program", "taste", "ayseas", "留学"]):
                continue
            seen.add(link.url)
            out.append(
                {
                    "program_name": title[:180],
                    "duration_hint": "官网未标注",
                    "target": "Science Tokyo students",
                    "short_desc": "from current-students abroad page",
                    "url": link.url,
                }
            )
            if len(out) >= max(1, limit):
                break
        return out

    async def get_scholarship_overview(self, scope_key: str, *, lang: str = "en") -> dict:
        overview_url = "https://students.isct.ac.jp/en/012/tuition-and-scholarship/specific-scholarships"
        if lang.lower() == "ja":
            overview_url = "https://students.isct.ac.jp/ja/012/tuition-and-scholarship/specific-scholarships"
        page = await self._safe_fetch_allowed(scope_key, overview_url)
        if not page:
            return {}
        links = extract_links(page, overview_url)
        categories = []
        out_links = []
        for link in links[:30]:
            text = (link.text or "").strip()
            if not text:
                continue
            low = f"{text} {link.url}".lower()
            if "scholar" in low or "奨学" in low or "jasso" in low or "mext" in low:
                categories.append(text[:120])
                out_links.append(link.url)
        if not out_links:
            out_links.append(overview_url)
        return {
            "categories": list(dict.fromkeys(categories))[:10],
            "links": list(dict.fromkeys(out_links))[:20],
            "source_url": overview_url,
        }

    async def list_scholarship_announcements(
        self, scope_key: str, *, lang: str = "en", limit: int = 10
    ) -> list[NewsItem]:
        return await self.news_service.list_current_students_news(
            scope_key,
            keyword="",
            tab=112,
            lang=lang,
            limit=limit,
        )

    async def get_clubs_overview(self, scope_key: str, *, lang: str = "en", limit: int = 20) -> dict:
        clubs_url = (
            "https://students.isct.ac.jp/en/012/student-life-and-support/extracurricular-activities/university-festivals"
        )
        if lang.lower() == "ja":
            clubs_url = (
                "https://students.isct.ac.jp/ja/012/student-life-and-support/extracurricular-activities/university-festivals"
            )
        page = await self._safe_fetch_allowed(scope_key, clubs_url)
        if not page:
            return {}
        links = extract_links(page, clubs_url)
        sections = []
        mapped_links = []
        for link in links:
            text = (link.text or "").strip()
            if not text:
                continue
            low = f"{text} {link.url}".lower()
            if any(k in low for k in ["club", "circle", "festival", "extracurricular", "課外", "新歓"]):
                sections.append(text[:120])
                mapped_links.append(link.url)
            if len(sections) >= max(1, limit):
                break
        if not mapped_links:
            mapped_links = [clubs_url]
        return {
            "sections": list(dict.fromkeys(sections))[:limit],
            "links": list(dict.fromkeys(mapped_links))[:limit],
            "source_url": clubs_url,
        }

    async def _safe_fetch_allowed(self, scope_key: str, url: str) -> str:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        if not host_allowed(url, allowed_domains):
            return ""
        try:
            page = await self.fetcher.fetch_text(url)
        except Exception:
            return ""
        return page.text

    async def extract_page_summary(self, scope_key: str, url: str, *, keyword: str = "") -> str:
        page = await self._safe_fetch_allowed(scope_key, url)
        if not page:
            return ""
        text = html_to_text(page)
        if not text:
            return ""
        if keyword:
            idx = text.lower().find(keyword.lower())
            if idx >= 0:
                start = max(0, idx - 140)
                end = min(len(text), idx + len(keyword) + 140)
                return text[start:end]
        return text[:280]
