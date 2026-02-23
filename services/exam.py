from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from runtime.sqlite_runtime import KVRuntime
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed

_DATE_RE = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})")
_JP_MD_RE = re.compile(r"(\d{1,2})月(\d{1,2})日")
_CODE_RE = re.compile(r"\b[A-Z]{2,5}\d{3,4}[A-Z]?\b")
_PERIOD_JP_RE = re.compile(r"([1-6])\s*限")
_PERIOD_NUM_RE = re.compile(r"\b([1-6])\b")
_AY_Q_RE = re.compile(
    r"(?:(?:AY|Academic\s*Year)\s*[-: ]?\s*)?(20\d{2}).{0,40}?([1-4])Q",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ExamSyncResult:
    ok: bool
    pdf_url: str | None
    pdf_hash: str | None
    version_id: str | None
    changed: bool
    diff: list[dict]
    parse_error: str | None
    message: str


class ExamService:
    def __init__(self, runtime: KVRuntime) -> None:
        self.runtime = runtime
        self.fetcher = Fetcher(runtime)

    async def find_latest_pdf(self, scope_key: str) -> str | None:
        candidates = await self.list_exam_pdfs(scope_key, limit=30)
        if not candidates:
            return None
        return candidates[-1]

    async def list_exam_pdfs(self, scope_key: str, limit: int = 10) -> list[str]:
        config = await self.runtime.get_effective_config(scope_key)
        sources = config.get("sources", {})
        allowed_domains = [str(x) for x in sources.get("allowedDomains", [])]
        seeds = [str(x) for x in sources.get("seeds", {}).get("exam", [])]
        candidates: list[str] = []
        for seed in seeds[:8]:
            if not host_allowed(seed, allowed_domains):
                continue
            try:
                page = await self.fetcher.fetch_text(seed)
            except Exception:
                continue
            for link in extract_links(page.text, seed):
                url_lower = link.url.lower()
                text_lower = link.text.lower()
                if not host_allowed(link.url, allowed_domains):
                    continue
                if not url_lower.endswith(".pdf"):
                    continue
                if any(k in text_lower for k in ["試験", "exam", "期末", "定期"]):
                    candidates.append(link.url)
                elif any(k in url_lower for k in ["exam", "shiken", "test", "final"]):
                    candidates.append(link.url)
        uniq = sorted(list(dict.fromkeys(candidates)))
        return uniq[-max(1, limit) :]

    async def sync_latest(self, scope_key: str) -> ExamSyncResult:
        pdf_url = await self.find_latest_pdf(scope_key)
        if not pdf_url:
            return ExamSyncResult(
                ok=False,
                pdf_url=None,
                pdf_hash=None,
                version_id=None,
                changed=False,
                diff=[],
                parse_error="no_pdf_found",
                message="未在 exam seeds 中找到 PDF 链接。",
            )
        try:
            pdf_bytes = await self.fetcher.fetch_bytes(pdf_url)
        except Exception as exc:
            return ExamSyncResult(
                ok=False,
                pdf_url=pdf_url,
                pdf_hash=None,
                version_id=None,
                changed=False,
                diff=[],
                parse_error=f"download_failed:{exc}",
                message="PDF 下载失败。",
            )

        pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()
        fetched_at = datetime.now(timezone.utc)
        text, parse_error = self._extract_pdf_text(pdf_bytes)
        records = self._parse_records_from_text(text, pdf_url, fetched_at=fetched_at)

        save_result = await self.runtime.save_exam_version(
            pdf_url=pdf_url,
            pdf_hash=pdf_hash,
            records=records,
            parse_error=parse_error,
        )
        return ExamSyncResult(
            ok=True,
            pdf_url=pdf_url,
            pdf_hash=pdf_hash,
            version_id=str(save_result.get("version_id")),
            changed=bool(save_result.get("changed")),
            diff=list(save_result.get("diff") or []),
            parse_error=save_result.get("parse_error"),
            message="exam 同步完成。",
        )

    async def parse_pdf_url(self, pdf_url: str) -> tuple[list[dict], str | None]:
        payload = await self.fetcher.fetch_bytes(pdf_url)
        return self.parse_pdf_payload(pdf_url=pdf_url, payload=payload, fetched_at=datetime.now(timezone.utc))

    def parse_pdf_payload(
        self,
        *,
        pdf_url: str,
        payload: bytes,
        fetched_at: datetime,
    ) -> tuple[list[dict], str | None]:
        text, parse_error = self._extract_pdf_text(payload)
        records = self._parse_records_from_text(text, pdf_url, fetched_at=fetched_at)
        return records, parse_error

    def _extract_pdf_text(self, payload: bytes) -> tuple[str, str | None]:
        text_parts: list[str] = []
        # 1) pdfplumber
        try:
            import pdfplumber  # type: ignore

            with pdfplumber.open(io.BytesIO(payload)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    if page_text:
                        text_parts.append(page_text)
            text = "\n".join(text_parts).strip()
            if text:
                return text, None
        except Exception:
            pass

        # 2) pypdf
        try:
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(io.BytesIO(payload))
            for page in reader.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    text_parts.append(page_text)
            text = "\n".join(text_parts).strip()
            if text:
                return text, None
        except Exception:
            pass

        # 3) fallback bytes decode
        fallback = payload.decode("utf-8", errors="ignore")
        if fallback.strip():
            return fallback, "raw_decode_fallback"
        return "", "extract_failed"

    def _parse_records_from_text(self, text: str, pdf_url: str, *, fetched_at: datetime) -> list[dict]:
        records: list[dict] = []
        if not text:
            return records
        ay_q = self._extract_ay_q(text)
        fallback_year = fetched_at.year
        for raw_line in text.splitlines():
            line = " ".join(raw_line.split())
            if len(line) < 6:
                continue
            date, date_conf = self._extract_date(line, ay_q=ay_q, fallback_year=fallback_year)
            code = self._extract_code(line)
            if not date or not code:
                continue
            period = self._extract_period(line)
            rec_type = self._extract_type(line)
            room = self._extract_room(line)
            records.append(
                {
                    "date": date,
                    "type": rec_type,
                    "period": period,
                    "course_code": code,
                    "course_title": self._extract_title(line, code),
                    "instructors": "",
                    "room": room,
                    "source_pdf_url": pdf_url,
                    "row_text_raw": line,
                    "confidence": min(0.95, max(0.2, date_conf + (0.15 if room else 0.05))),
                }
            )
        # de-dup by date+code+period
        seen: set[tuple[str, str, str]] = set()
        out: list[dict] = []
        for rec in records:
            key = (str(rec.get("date")), str(rec.get("course_code")), str(rec.get("period")))
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def _extract_date(
        self,
        line: str,
        *,
        ay_q: tuple[int, int] | None,
        fallback_year: int,
    ) -> tuple[str | None, float]:
        m = _DATE_RE.search(line)
        if not m:
            m2 = _JP_MD_RE.search(line)
            if not m2:
                return None, 0.0
            month, day = int(m2.group(1)), int(m2.group(2))
            year = fallback_year
            confidence = 0.4
            if ay_q:
                ay, _q = ay_q
                year = ay + 1 if month <= 3 else ay
                confidence = 0.72
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d"), confidence
            except ValueError:
                return None, 0.0
        yyyy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(yyyy, mm, dd).strftime("%Y-%m-%d"), 0.9
        except ValueError:
            return None, 0.0

    def _extract_code(self, line: str) -> str | None:
        m = _CODE_RE.search(line.upper())
        if not m:
            return None
        return m.group(0)

    def _extract_period(self, line: str) -> str:
        m = _PERIOD_JP_RE.search(line)
        if m:
            return m.group(1)
        m2 = _PERIOD_NUM_RE.search(line)
        if m2:
            return m2.group(1)
        return "unknown"

    @staticmethod
    def _extract_type(line: str) -> str:
        lower = line.lower()
        if any(k in lower for k in ["追試", "makeup", "追再"]):
            return "makeup"
        if any(k in lower for k in ["補講", "other", "注意"]):
            return "other"
        return "exam"

    @staticmethod
    def _extract_room(line: str) -> str:
        # naive extraction: keep last token if contains room-like marks
        tokens = line.split()
        for token in reversed(tokens):
            if any(k in token for k in ["教室", "Room", "H", "W", "M"]) and len(token) <= 20:
                return token
        return ""

    @staticmethod
    def _extract_title(line: str, code: str) -> str:
        idx = line.upper().find(code)
        if idx < 0:
            return ""
        after = line[idx + len(code) :].strip()
        return after[:80]

    @staticmethod
    def _extract_ay_q(text: str) -> tuple[int, int] | None:
        m = _AY_Q_RE.search(text or "")
        if not m:
            return None
        try:
            ay = int(m.group(1))
            quarter = int(m.group(2))
            return ay, quarter
        except Exception:
            return None
