from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


@dataclass(frozen=True)
class LinkItem:
    url: str
    text: str


class _AnchorParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self._inside_a = False
        self._current_href = ""
        self._current_text: list[str] = []
        self.items: list[LinkItem] = []

    def handle_starttag(self, tag: str, attrs):
        if tag.lower() != "a":
            return
        href = ""
        for key, value in attrs:
            if key.lower() == "href" and value:
                href = str(value).strip()
                break
        if not href:
            return
        self._inside_a = True
        self._current_href = urljoin(self.base_url, href)
        self._current_text = []

    def handle_data(self, data: str):
        if self._inside_a:
            self._current_text.append(data)

    def handle_endtag(self, tag: str):
        if tag.lower() != "a" or not self._inside_a:
            return
        text = " ".join("".join(self._current_text).split())
        self.items.append(LinkItem(url=self._current_href, text=text))
        self._inside_a = False
        self._current_href = ""
        self._current_text = []


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs):
        lowered = tag.lower()
        if lowered in {"script", "style"}:
            self._skip_depth += 1
            return
        if lowered in {"br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str):
        lowered = tag.lower()
        if lowered in {"script", "style"} and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if lowered in {"p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_data(self, data: str):
        if self._skip_depth > 0:
            return
        if data:
            self.parts.append(data)


def extract_links(html_text: str, base_url: str) -> list[LinkItem]:
    parser = _AnchorParser(base_url)
    parser.feed(html_text or "")
    parser.close()
    # de-dup while preserving order
    out: list[LinkItem] = []
    seen: set[tuple[str, str]] = set()
    for item in parser.items:
        key = (item.url, item.text)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def html_to_text(html_text: str) -> str:
    parser = _TextParser()
    parser.feed(html_text or "")
    parser.close()
    text = "".join(parser.parts)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def host_allowed(url: str, allowed_domains: list[str]) -> bool:
    host = (urlparse(url).hostname or "").lower()
    for domain in allowed_domains:
        domain = domain.lower()
        if host == domain or host.endswith(f".{domain}"):
            return True
    return False


def text_snippet(text: str, keyword: str, window: int = 90) -> str:
    raw = re.sub(r"\s+", " ", text or "").strip()
    if not raw:
        return ""
    needle = keyword.strip().lower()
    if not needle:
        return raw[: window * 2]
    idx = raw.lower().find(needle)
    if idx < 0:
        return raw[: window * 2]
    start = max(0, idx - window)
    end = min(len(raw), idx + len(needle) + window)
    return raw[start:end]
