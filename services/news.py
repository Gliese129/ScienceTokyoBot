from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from urllib.parse import urlencode, urlparse

from runtime.sqlite_runtime import KVRuntime
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed, html_to_text, text_snippet

_DATE_RE = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})")
_DATE_JP_RE = re.compile(r"(20\d{2})年(\d{1,2})月(\d{1,2})日")
_EN_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class NewsItem:
    title: str
    date: str
    url: str
    tags: list[str]
    snippet: str
    source_note: str


class NewsService:
    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime
        self.fetcher = Fetcher(runtime)

    async def list_current_students_news(
        self,
        scope_key: str,
        *,
        keyword: str = "",
        tab: int | None = None,
        lang: str = "en",
        limit: int = 10,
    ) -> list[NewsItem]:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        lang_tag = "ja" if lang.lower() == "ja" else "en"
        base_url = f"https://students.isct.ac.jp/{lang_tag}/news"
        home_url = f"https://students.isct.ac.jp/{lang_tag}"
        if not host_allowed(home_url, allowed_domains):
            return []

        params = {"tab": str(tab)} if tab is not None else {}
        list_url = f"{base_url}?{urlencode(params)}" if params else base_url
        cache_key = f"news.list::{scope_key}::{lang_tag}::{list_url}::{keyword.lower()}::{limit}"
        cached = await self.runtime.get_search_cache(cache_key, max_age_sec=10 * 60)
        if cached:
            return [NewsItem(**item) for item in cached]

        links = []
        tried: list[str] = []
        # 1) try /<lang>/news first (can be tab-filtered)
        try:
            page = await self.fetcher.fetch_text(list_url)
            links = self._extract_news_links(page.text, list_url)
            tried.append(list_url)
        except Exception:
            links = []
        # 2) fallback to /<lang> home if news page is sparse or client-rendered
        if len(links) < 2:
            try:
                home_page = await self.fetcher.fetch_text(home_url)
                fallback_links = self._extract_news_links(home_page.text, home_url)
                if len(fallback_links) > len(links):
                    links = fallback_links
                tried.append(home_url)
            except Exception:
                pass
        # 3) try configured seeds for this category
        cfg_seeds = [str(x) for x in config.get("sources", {}).get("seeds", {}).get("news", [])]
        for seed in cfg_seeds:
            if not host_allowed(seed, allowed_domains):
                continue
            if f"/{lang_tag}" not in seed:
                continue
            if seed in tried:
                continue
            if len(links) >= 2:
                break
            try:
                seed_page = await self.fetcher.fetch_text(seed)
            except Exception:
                continue
            seed_links = self._extract_news_links(seed_page.text, seed)
            if len(seed_links) > len(links):
                links = seed_links
        needle = keyword.strip().lower()
        out: list[NewsItem] = []
        seen: set[str] = set()

        for link in links:
            if link.url in seen:
                continue
            if "/news/" not in link.url:
                continue
            if not host_allowed(link.url, allowed_domains):
                continue
            title = (link.text or "").strip()
            if not title:
                continue
            if needle and needle not in f"{title} {link.url}".lower():
                continue
            seen.add(link.url)

            snippet = ""
            date_str = ""
            try:
                detail = await self.fetcher.fetch_text(link.url)
                detail_text = html_to_text(detail.text)
                snippet = text_snippet(detail_text, keyword or title, window=120)
                date_str = self._extract_date(detail_text)
            except Exception:
                snippet = title[:180]
                date_str = ""

            source_note = "非官方来源" if "welcome.titech.app" in link.url else "官方来源"
            out.append(
                NewsItem(
                    title=title[:180],
                    date=date_str or "官网未标注",
                    url=link.url,
                    tags=self._guess_tags(title, link.url),
                    snippet=(snippet or title)[:240],
                    source_note=source_note,
                )
            )
            if len(out) >= max(1, limit):
                break
        await self.runtime.put_search_cache(cache_key, [asdict(item) for item in out])
        return out

    async def get_news_item(self, scope_key: str, *, url: str) -> dict:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        if not host_allowed(url, allowed_domains):
            return {}
        page = await self.fetcher.fetch_text(url)
        text = html_to_text(page.text)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        title = lines[0] if lines else "官网未标注"
        key_points = []
        for line in lines:
            low = line.lower()
            if any(k in low for k in ["deadline", "締切", "対象", "eligibility", "date", "期間"]):
                key_points.append(line[:200])
            if len(key_points) >= 5:
                break
        links = [link.url for link in extract_links(page.text, url)[:12]]
        return {
            "title": title[:200],
            "date": self._extract_date(text) or "官网未标注",
            "body_summary": text_snippet(text, "deadline", window=220) or text[:260],
            "key_points": key_points,
            "links": links,
            "url": url,
        }

    async def list_legacy_announcements(
        self,
        scope_key: str,
        *,
        keyword: str = "",
        limit: int = 5,
    ) -> list[NewsItem]:
        config = await self.runtime.get_effective_config(scope_key)
        allowed_domains = [str(x) for x in config.get("sources", {}).get("allowedDomains", [])]
        # Official transition notice page.
        pivot = "https://www.isct.ac.jp/en/news/it87dcs7t5y2"
        if not host_allowed(pivot, allowed_domains):
            return []
        try:
            page = await self.fetcher.fetch_text(pivot)
        except Exception:
            return []
        links = extract_links(page.text, pivot)
        needle = keyword.strip().lower()
        out: list[NewsItem] = []
        seen: set[str] = set()
        for link in links:
            host = (urlparse(link.url).hostname or "").lower()
            if "titech.ac.jp" not in host and "tmd.ac.jp" not in host:
                continue
            if link.url in seen:
                continue
            title = (link.text or link.url).strip()
            if needle and needle not in f"{title} {link.url}".lower():
                continue
            seen.add(link.url)
            out.append(
                NewsItem(
                    title=title[:180],
                    date="官网未标注",
                    url=link.url,
                    tags=["legacy"],
                    snippet="legacy current-students source",
                    source_note="官方历史来源",
                )
            )
            if len(out) >= max(1, limit):
                break
        return out

    @staticmethod
    def _extract_date(text: str) -> str:
        m = _DATE_RE.search(text or "")
        if m:
            yyyy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        m_jp = _DATE_JP_RE.search(text or "")
        if m_jp:
            yyyy, mm, dd = int(m_jp.group(1)), int(m_jp.group(2)), int(m_jp.group(3))
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        m2 = _EN_DATE_RE.search(text or "")
        if not m2:
            return ""
        month_name, day, year = m2.group(1), int(m2.group(2)), int(m2.group(3))
        try:
            dt = datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return ""

    @staticmethod
    def _guess_tags(title: str, url: str) -> list[str]:
        blob = f"{title} {url}".lower()
        tags = []
        if "scholar" in blob or "奨学" in blob:
            tags.append("scholarship")
        if "abroad" in blob or "留学" in blob:
            tags.append("abroad")
        if "exam" in blob or "試験" in blob:
            tags.append("exam")
        if "tuition" in blob or "学費" in blob:
            tags.append("tuition")
        return tags or ["general"]

    @staticmethod
    def _extract_news_links(html_text: str, base_url: str) -> list:
        links = extract_links(html_text, base_url)
        out = []
        seen = set()
        for link in links:
            url = link.url
            if "/news/" not in url:
                continue
            if url in seen:
                continue
            seen.add(url)
            out.append(link)
        return out
