from __future__ import annotations

import hashlib
import urllib.request
from dataclasses import dataclass

from runtime.sqlite_runtime import KVRuntime


@dataclass(frozen=True)
class FetchTextResult:
    url: str
    text: str
    content_hash: str
    etag: str | None
    last_modified: str | None
    from_cache: bool


@dataclass(frozen=True)
class FetchBinaryResult:
    url: str
    payload: bytes
    content_hash: str
    etag: str | None
    last_modified: str | None


class Fetcher:
    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime

    async def fetch_text(self, url: str, max_age_sec: int = 24 * 60 * 60, timeout_sec: int = 12) -> FetchTextResult:
        cached = await self.runtime.get_cache(url, max_age_sec=max_age_sec)
        if cached:
            return FetchTextResult(
                url=url,
                text=str(cached.get("extracted_text") or ""),
                content_hash=str(cached.get("content_hash") or ""),
                etag=cached.get("etag"),
                last_modified=cached.get("last_modified"),
                from_cache=True,
            )

        req = urllib.request.Request(url, headers={"User-Agent": "ScienceTokyoBot/0.4"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            payload = resp.read()
            content_type = str(resp.headers.get("Content-Type") or "")
            charset = "utf-8"
            if "charset=" in content_type:
                charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except Exception:
                text = payload.decode("utf-8", errors="replace")
            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")

        content_hash = hashlib.sha256(payload).hexdigest()
        await self.runtime.put_cache(
            url=url,
            content_hash=content_hash,
            extracted_text=text,
            etag=etag,
            last_modified=last_modified,
        )
        return FetchTextResult(
            url=url,
            text=text,
            content_hash=content_hash,
            etag=etag,
            last_modified=last_modified,
            from_cache=False,
        )

    async def fetch_bytes(self, url: str, timeout_sec: int = 15) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "ScienceTokyoBot/0.4"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            return resp.read()

    async def fetch_binary(self, url: str, timeout_sec: int = 15) -> FetchBinaryResult:
        req = urllib.request.Request(url, headers={"User-Agent": "ScienceTokyoBot/0.4"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            payload = resp.read()
            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")
        content_hash = hashlib.sha256(payload).hexdigest()
        return FetchBinaryResult(
            url=url,
            payload=payload,
            content_hash=content_hash,
            etag=etag,
            last_modified=last_modified,
        )
