from __future__ import annotations

from dataclasses import asdict, dataclass

from runtime.sqlite_runtime import KVRuntime
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed, text_snippet


@dataclass(frozen=True)
class SearchItem:
    title: str
    url: str
    snippet: str
    source_note: str


class DiscoveryService:
    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime
        self.fetcher = Fetcher(runtime)

    async def search(
        self,
        *,
        scope_key: str,
        category: str,
        keyword: str,
        top_n: int = 5,
    ) -> list[SearchItem]:
        key = f"{scope_key}|{category}|{keyword.strip().lower()}"
        config = await self.runtime.get_effective_config(scope_key)
        ttl = int(config.get("cache", {}).get("ttlDiscoverySec", 30 * 60) or 30 * 60)
        cached = await self.runtime.get_search_cache(key, max_age_sec=max(60, ttl))
        if cached:
            return [SearchItem(**item) for item in cached[:top_n]]

        sources = config.get("sources", {})
        allowed_domains = [str(x) for x in sources.get("allowedDomains", [])]
        seeds = [str(x) for x in sources.get("seeds", {}).get(category, [])]

        items: list[SearchItem] = []
        needle = keyword.strip().lower()
        for seed in seeds[:5]:
            if not host_allowed(seed, allowed_domains):
                continue
            try:
                seed_page = await self.fetcher.fetch_text(seed)
            except Exception:
                continue
            links = extract_links(seed_page.text, seed)
            for link in links:
                if not host_allowed(link.url, allowed_domains):
                    continue
                title = link.text.strip() or link.url.rsplit("/", 1)[-1]
                # first stage: title/url filter
                title_blob = f"{title} {link.url}".lower()
                if needle and needle not in title_blob:
                    continue
                try:
                    detail = await self.fetcher.fetch_text(link.url)
                except Exception:
                    continue
                snippet = text_snippet(detail.text, keyword)
                note = "非官方来源" if "welcome.titech.app" in link.url else "官方来源"
                items.append(
                    SearchItem(
                        title=title[:120],
                        url=link.url,
                        snippet=snippet[:220],
                        source_note=note,
                    )
                )
                if len(items) >= top_n:
                    break
            if len(items) >= top_n:
                break

        await self.runtime.put_search_cache(key, [asdict(item) for item in items])
        return items[:top_n]
