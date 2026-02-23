from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from isct_core.config_loader import GlobalConfig
from runtime.sqlite_runtime import KVRuntime
from services.calendar import CalendarService
from services.exam import ExamService
from services.fetcher import Fetcher
from services.html_utils import extract_links, host_allowed, html_to_text
from services.syllabus import SyllabusService

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None  # type: ignore

try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore


class SyncManager:
    def __init__(
        self,
        *,
        runtime: KVRuntime,
        global_config: GlobalConfig,
        fetcher: Fetcher,
        calendar_service: CalendarService,
        exam_service: ExamService,
        syllabus_service: SyllabusService,
        logger: Any,
        context: Any | None = None,
        parser_provider_id: str = "",
        scope_key: str = "discord:guild:sync",
    ) -> None:
        self.runtime = runtime
        self.global_config = global_config
        self.fetcher = fetcher
        self.calendar_service = calendar_service
        self.exam_service = exam_service
        self.syllabus_service = syllabus_service
        self.logger = logger
        self.context = context
        self.parser_provider_id = (parser_provider_id or "").strip()
        self.scope_key = scope_key
        self.tz = ZoneInfo("Asia/Tokyo")
        self._started = False
        self._tasks: list[asyncio.Task] = []
        self._confidence_threshold = 0.75

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        loops = [
            ("sync_syllabus_years", 24 * 60 * 60, self._job_syllabus_years),
            ("sync_calendar_html", 24 * 60 * 60, self._job_calendar_html),
            ("sync_monthly_class_pdf", 24 * 60 * 60, self._job_monthly_class_pdf),
            ("sync_dow_class_pdf", 7 * 24 * 60 * 60, self._job_dow_class_pdf),
            ("sync_exam_pdf", 24 * 60 * 60, self._job_exam_pdf),
        ]
        for name, interval_sec, runner in loops:
            task = asyncio.create_task(self._run_periodic(name, interval_sec, runner))
            self._tasks.append(task)

    def ensure_started(self) -> None:
        if self._started:
            return
        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            return
        self.start()

    async def _run_periodic(
        self,
        job_name: str,
        interval_sec: int,
        runner: Callable[[], Awaitable[dict[str, Any]]],
    ) -> None:
        while True:
            await self._run_job(job_name, runner)
            await asyncio.sleep(max(60, int(interval_sec)))

    async def _run_job(self, job_name: str, runner: Callable[[], Awaitable[dict[str, Any]]]) -> None:
        now_tokyo = datetime.now(self.tz)
        start_ms = int(time.time() * 1000)
        self.logger.info("JOB_START name=%s at=%s tz=Asia/Tokyo", job_name, now_tokyo.isoformat())
        last_error = None
        last_source_url = None
        last_sha = None
        last_record_count: int | None = None
        changed = False
        success = False
        old_status = await self.runtime.get_sync_job_status(job_name)
        try:
            payload = await runner()
            last_source_url = payload.get("last_source_url")
            last_sha = payload.get("last_sha256")
            last_record_count = payload.get("last_record_count")
            changed = bool(payload.get("changed"))
            success = True
        except Exception as exc:  # pragma: no cover
            last_error = str(exc)
            self.logger.exception("PARSE name=%s ok=false records=0 parse_error=%s", job_name, last_error)
        end_ms = int(time.time() * 1000)
        self.logger.info(
            "JOB_END name=%s ms=%s changed=%s",
            job_name,
            max(1, end_ms - start_ms),
            str(changed).lower(),
        )
        await self.runtime.put_sync_job_status(
            job_name=job_name,
            last_run_at=int(time.time()),
            last_success_at=int(time.time()) if success else (old_status or {}).get("last_success_at"),
            last_error=last_error,
            last_source_url=last_source_url,
            last_sha256=last_sha,
            last_record_count=last_record_count,
            changed=changed if success else None,
        )

    async def llm_parse_if_changed(self, source_key: str, source_url: str, payload: str | bytes) -> dict[str, Any]:
        payload_bytes: bytes
        if isinstance(payload, str):
            payload_bytes = payload.encode("utf-8", errors="ignore")
        else:
            payload_bytes = payload
        sha256 = hashlib.sha256(payload_bytes).hexdigest()
        cached = await self.runtime.get_parsed_cache(source_key, source_url)
        if cached and str(cached.get("sha256") or "") == sha256:
            return {
                "changed": False,
                "sha256": sha256,
                "parser": "cache",
                "parsed_json": cached.get("parsed_json"),
                "records_count": int(cached.get("record_count") or 0),
                "parse_error": cached.get("parse_error"),
                "provider_id": cached.get("provider_id"),
            }

        det = await self._run_deterministic_parser(source_key, source_url, payload)
        det_valid = self._validate_schema(source_key, det["parsed_json"]) if det["ok"] else False
        if det["ok"] and det_valid and float(det["confidence"]) >= self._confidence_threshold:
            await self.runtime.upsert_parsed_cache(
                source_key=source_key,
                source_url=source_url,
                sha256=sha256,
                parsed_json=det["parsed_json"],
                parsed_at=int(time.time()),
                provider_id="deterministic",
                parse_error=det["parse_error"],
                record_count=int(det["records_count"]),
            )
            return {
                "changed": True,
                "sha256": sha256,
                "parser": "deterministic",
                "parsed_json": det["parsed_json"],
                "records_count": int(det["records_count"]),
                "parse_error": det["parse_error"],
                "provider_id": "deterministic",
            }

        llm_error = ""
        llm_json: Any = None
        parser_used = "deterministic"
        provider_id = "deterministic"
        if self.context is not None and self.parser_provider_id:
            parser_used = "llm"
            provider_id = self.parser_provider_id
            try:
                llm_prompt = self._build_llm_prompt(source_key, source_url, payload)
                llm_resp = await self.context.llm_generate(
                    chat_provider_id=self.parser_provider_id,
                    prompt=llm_prompt,
                )
                llm_json = self._parse_llm_json_output(llm_resp)
                if not self._validate_schema(source_key, llm_json):
                    raise ValueError("schema_validation_failed")
            except Exception as exc:
                llm_error = str(exc)
                llm_json = None

        final_json = llm_json if llm_json is not None else det["parsed_json"]
        if not self._validate_schema(source_key, final_json):
            final_json = det["parsed_json"] if self._validate_schema(source_key, det["parsed_json"]) else None
            parser_used = "deterministic"
            provider_id = "deterministic"
        final_count = self._records_count(source_key, final_json)
        if parser_used == "llm" and llm_json is not None:
            final_error = ""
        else:
            final_error = llm_error or det["parse_error"]
        await self.runtime.upsert_parsed_cache(
            source_key=source_key,
            source_url=source_url,
            sha256=sha256,
            parsed_json=final_json if final_json is not None else {},
            parsed_at=int(time.time()),
            provider_id=provider_id,
            parse_error=final_error,
            record_count=final_count,
        )
        return {
            "changed": True,
            "sha256": sha256,
            "parser": parser_used,
            "parsed_json": final_json if final_json is not None else {},
            "records_count": final_count,
            "parse_error": final_error,
            "provider_id": provider_id,
        }

    async def _run_deterministic_parser(self, source_key: str, source_url: str, payload: str | bytes) -> dict[str, Any]:
        now_year = datetime.now(self.tz).year
        if source_key == "calendar_html":
            html_text = payload if isinstance(payload, str) else payload.decode("utf-8", errors="ignore")
            year = self._extract_year_from_calendar_url(source_url) or now_year
            events = [item.__dict__ for item in self.calendar_service.parse_calendar_html(html_text, source_url, year)]
            term_ranges = self.calendar_service.extract_term_ranges(html_text, source_url, year)
            parsed = {"events": events, "term_ranges": term_ranges}
            confidence = 0.85 if events else 0.35
            parse_error = "" if events else "no_calendar_events"
            return {
                "ok": bool(events),
                "confidence": confidence,
                "parsed_json": parsed,
                "records_count": len(events),
                "parse_error": parse_error,
            }
        if source_key == "monthly_pdf":
            payload_bytes = payload if isinstance(payload, bytes) else payload.encode("utf-8", errors="ignore")
            text, parse_error = self._extract_pdf_text(payload_bytes)
            parsed = self._parse_monthly_meta(text, source_url)
            confidence = 0.85 if int(parsed.get("line_count") or 0) > 0 else 0.3
            return {
                "ok": int(parsed.get("line_count") or 0) > 0,
                "confidence": confidence,
                "parsed_json": parsed,
                "records_count": int(parsed.get("line_count") or 0),
                "parse_error": parse_error or "",
            }
        if source_key == "dow_pdf":
            payload_bytes = payload if isinstance(payload, bytes) else payload.encode("utf-8", errors="ignore")
            text, parse_error = self._extract_pdf_text(payload_bytes)
            rows = self._parse_dow_rows(text)
            confidence = 0.8 if len(rows) >= 3 else 0.4
            return {
                "ok": bool(rows),
                "confidence": confidence,
                "parsed_json": rows,
                "records_count": len(rows),
                "parse_error": parse_error or ("" if rows else "no_dow_rows"),
            }
        if source_key == "exam_pdf":
            payload_bytes = payload if isinstance(payload, bytes) else payload.encode("utf-8", errors="ignore")
            records, parse_error = self.exam_service.parse_pdf_payload(
                pdf_url=source_url,
                payload=payload_bytes,
                fetched_at=datetime.now(timezone.utc),
            )
            avg_conf = 0.0
            if records:
                avg_conf = sum(float(rec.get("confidence") or 0.0) for rec in records) / len(records)
            return {
                "ok": bool(records),
                "confidence": avg_conf,
                "parsed_json": records,
                "records_count": len(records),
                "parse_error": parse_error or ("" if records else "no_exam_records"),
            }
        return {
            "ok": False,
            "confidence": 0.0,
            "parsed_json": {},
            "records_count": 0,
            "parse_error": f"unsupported_source_key:{source_key}",
        }

    def _build_llm_prompt(self, source_key: str, source_url: str, payload: str | bytes) -> str:
        if isinstance(payload, bytes):
            raw_text, _ = self._extract_pdf_text(payload)
        else:
            raw_text = html_to_text(payload)
        excerpt = raw_text[:8000]
        schema = self._schema_hint(source_key)
        return (
            "You are a strict parser.\n"
            "Return ONLY valid JSON. No markdown, no code fences, no extra commentary.\n"
            "Dates must be YYYY-MM-DD. title length <= 200.\n"
            f"source_key={source_key}\n"
            f"source_url={source_url}\n"
            f"JSON_SCHEMA_HINT={schema}\n"
            "INPUT_TEXT_BEGIN\n"
            f"{excerpt}\n"
            "INPUT_TEXT_END\n"
            "OUTPUT_JSON:"
        )

    @staticmethod
    def _parse_llm_json_output(output: Any) -> Any:
        text = ""
        if isinstance(output, str):
            text = output.strip()
        elif isinstance(output, dict):
            for key in ["text", "content", "answer", "output"]:
                if key in output:
                    text = str(output[key]).strip()
                    break
        if not text:
            raise ValueError("empty_llm_output")
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
        return json.loads(text)

    @staticmethod
    def _is_yyyy_mm_dd(value: str) -> bool:
        if not isinstance(value, str):
            return False
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value))

    def _validate_schema(self, source_key: str, parsed_json: Any) -> bool:
        if source_key == "calendar_html":
            if not isinstance(parsed_json, dict):
                return False
            events = parsed_json.get("events")
            term_ranges = parsed_json.get("term_ranges")
            if not isinstance(events, list) or not isinstance(term_ranges, list):
                return False
            for item in events:
                if not isinstance(item, dict):
                    return False
                required = ["title", "start_date", "end_date", "kind", "is_no_class", "source_url"]
                if any(key not in item for key in required):
                    return False
                if len(str(item.get("title") or "")) > 200:
                    return False
                if not self._is_yyyy_mm_dd(str(item.get("start_date") or "")):
                    return False
                if not self._is_yyyy_mm_dd(str(item.get("end_date") or "")):
                    return False
            return True
        if source_key == "monthly_pdf":
            if not isinstance(parsed_json, dict):
                return False
            return "source_url" in parsed_json and "line_count" in parsed_json
        if source_key == "dow_pdf":
            if not isinstance(parsed_json, list):
                return False
            for item in parsed_json:
                if not isinstance(item, dict):
                    return False
                if "row_no" not in item or "dates" not in item:
                    return False
            return True
        if source_key == "exam_pdf":
            if not isinstance(parsed_json, list):
                return False
            required = [
                "date",
                "type",
                "period",
                "course_code",
                "course_title",
                "instructors",
                "room",
                "source_pdf_url",
                "row_text_raw",
                "confidence",
            ]
            for item in parsed_json:
                if not isinstance(item, dict):
                    return False
                if any(key not in item for key in required):
                    return False
                if len(str(item.get("course_title") or "")) > 200:
                    return False
                if not self._is_yyyy_mm_dd(str(item.get("date") or "")):
                    return False
            return True
        return False

    @staticmethod
    def _schema_hint(source_key: str) -> str:
        hints = {
            "calendar_html": '{"events":[{"title":"","start_date":"YYYY-MM-DD","end_date":"YYYY-MM-DD","kind":"","is_no_class":true,"source_url":""}],"term_ranges":[{"term":"1Q","start_date":"YYYY-MM-DD","end_date":"YYYY-MM-DD","source_url":""}]}',
            "monthly_pdf": '{"source_url":"","line_count":1,"updated_at_str":"YYYY-MM-DD","q1_range":"YYYY-MM-DD~YYYY-MM-DD"}',
            "dow_pdf": '[{"row_no":"1","week_label":"第1回","dates":["4/9","4/10"],"row_text_raw":""}]',
            "exam_pdf": '[{"date":"YYYY-MM-DD","type":"exam","period":"1","course_code":"AB123","course_title":"","instructors":"","room":"","source_pdf_url":"","row_text_raw":"","confidence":0.8}]',
        }
        return hints.get(source_key, "{}")

    @staticmethod
    def _records_count(source_key: str, parsed_json: Any) -> int:
        if source_key == "calendar_html" and isinstance(parsed_json, dict):
            events = parsed_json.get("events")
            return len(events) if isinstance(events, list) else 0
        if source_key == "monthly_pdf" and isinstance(parsed_json, dict):
            return int(parsed_json.get("line_count") or 0)
        if isinstance(parsed_json, list):
            return len(parsed_json)
        return 0

    async def _job_syllabus_years(self) -> dict[str, Any]:
        url = "https://syllabus.s.isct.ac.jp/search?hl=en"
        cfg = await self.runtime.get_effective_config(self.scope_key)
        allowed = [str(x) for x in cfg.get("sources", {}).get("allowedDomains", [])]
        is_allowed = host_allowed(url, allowed)
        self.logger.info("SOURCE url=%s allowed=%s reason=%s", url, str(is_allowed).lower(), "" if is_allowed else "domain_not_allowed")
        if not is_allowed:
            self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", url)
            self.logger.info("PARSE name=syllabus_years ok=false records=0 parse_error=domain_not_allowed")
            self.logger.info("DB name=syllabus_years upsert_versions=0 upsert_records=0 upsert_indexes=0")
            return {"last_source_url": url, "last_sha256": "", "last_record_count": 0, "changed": False}
        page = await self.fetcher.fetch_text(url, max_age_sec=0)
        old_state = await self.runtime.get_source_state(url)
        changed = (old_state or {}).get("sha256") != page.content_hash
        self.logger.info(
            "FETCH url=%s ok=true bytes=%s etag=%s last_modified=%s sha256=%s changed=%s",
            url,
            len(page.text.encode("utf-8", errors="ignore")),
            page.etag or "",
            page.last_modified or "",
            page.content_hash,
            str(changed).lower(),
        )
        if changed:
            years = await self.syllabus_service.list_available_years(self.scope_key, force_refresh=True)
            parse_error = ""
            upsert_indexes = 1
        else:
            years = await self.runtime.get_syllabus_available_years()
            parse_error = "skipped_unchanged"
            upsert_indexes = 0
        await self.runtime.upsert_source_state(
            source_url=url,
            etag=page.etag,
            last_modified=page.last_modified,
            sha256=page.content_hash,
            fetched_at=int(time.time()),
            parse_error=None if parse_error == "" else parse_error,
            extra={"kind": "syllabus_years"},
        )
        self.logger.info(
            "PARSE name=syllabus_years ok=true parser=deterministic records=%s parse_error=%s",
            len(years),
            parse_error,
        )
        self.logger.info("DB name=syllabus_years upsert_versions=0 upsert_records=0 upsert_indexes=%s", upsert_indexes)
        return {
            "last_source_url": url,
            "last_sha256": page.content_hash,
            "last_record_count": len(years),
            "changed": changed,
        }

    async def _job_calendar_html(self) -> dict[str, Any]:
        root_url = "https://www.titech.ac.jp/english/student/students/life/schedules"
        cfg = await self.runtime.get_effective_config(self.scope_key)
        allowed = [str(x) for x in cfg.get("sources", {}).get("allowedDomains", [])]
        root_allowed = host_allowed(root_url, allowed)
        self.logger.info(
            "SOURCE url=%s allowed=%s reason=%s",
            root_url,
            str(root_allowed).lower(),
            "" if root_allowed else "domain_not_allowed",
        )
        if not root_allowed:
            self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", root_url)
            self.logger.info("PARSE name=calendar_html ok=false records=0 parse_error=domain_not_allowed")
            self.logger.info("DB name=calendar_html upsert_versions=0 upsert_records=0 upsert_indexes=0")
            return {"last_source_url": root_url, "last_sha256": "", "last_record_count": 0, "changed": False}

        root_page = await self.fetcher.fetch_text(root_url, max_age_sec=0)
        root_old = await self.runtime.get_source_state(root_url)
        root_changed = (root_old or {}).get("sha256") != root_page.content_hash
        self.logger.info(
            "FETCH url=%s ok=true bytes=%s etag=%s last_modified=%s sha256=%s changed=%s",
            root_url,
            len(root_page.text.encode("utf-8", errors="ignore")),
            root_page.etag or "",
            root_page.last_modified or "",
            root_page.content_hash,
            str(root_changed).lower(),
        )
        await self.runtime.upsert_source_state(
            source_url=root_url,
            etag=root_page.etag,
            last_modified=root_page.last_modified,
            sha256=root_page.content_hash,
            fetched_at=int(time.time()),
            parse_error=None,
            extra={"kind": "calendar_root"},
        )

        year_urls = self._extract_calendar_year_urls(root_page.text, root_url)
        if not year_urls:
            now_year = datetime.now(self.tz).year
            year_urls = [
                f"{root_url}/{max(now_year - 1, 2000)}",
                f"{root_url}/{now_year}",
            ]

        total_records = 0
        total_indexes = 0
        changed_any = False
        last_url = root_url
        last_sha = root_page.content_hash
        touched_years: list[int] = []

        for url in year_urls[:3]:
            year = self._extract_year_from_calendar_url(url)
            if year <= 0:
                continue
            touched_years.append(year)
            is_allowed = host_allowed(url, allowed)
            self.logger.info("SOURCE url=%s allowed=%s reason=%s", url, str(is_allowed).lower(), "" if is_allowed else "domain_not_allowed")
            if not is_allowed:
                self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", url)
                continue
            try:
                page = await self.fetcher.fetch_text(url, max_age_sec=0)
            except Exception as exc:
                self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", url)
                self.logger.info("PARSE name=calendar_html ok=false records=0 parse_error=%s", str(exc))
                continue
            old_state = await self.runtime.get_source_state(url)
            changed = (old_state or {}).get("sha256") != page.content_hash
            self.logger.info(
                "FETCH url=%s ok=true bytes=%s etag=%s last_modified=%s sha256=%s changed=%s",
                url,
                len(page.text.encode("utf-8", errors="ignore")),
                page.etag or "",
                page.last_modified or "",
                page.content_hash,
                str(changed).lower(),
            )
            parse = await self.llm_parse_if_changed("calendar_html", url, page.text)
            changed_any = changed_any or bool(parse.get("changed"))
            parsed_json = parse.get("parsed_json")
            events = []
            term_ranges = []
            if isinstance(parsed_json, dict):
                events = [dict(x) for x in (parsed_json.get("events") or []) if isinstance(x, dict)]
                term_ranges = [dict(x) for x in (parsed_json.get("term_ranges") or []) if isinstance(x, dict)]
            parse_error = str(parse.get("parse_error") or "")
            parser_used = str(parse.get("parser") or "deterministic")
            records_count = int(parse.get("records_count") or len(events))
            self.logger.info(
                "PARSE name=calendar_html ok=%s parser=%s records=%s parse_error=%s",
                "true" if records_count > 0 else "false",
                parser_used,
                records_count,
                parse_error,
            )
            upsert_records = await self.runtime.replace_calendar_events(year, events) if bool(parse.get("changed")) else 0
            upsert_indexes = await self.runtime.replace_term_ranges(year, term_ranges) if bool(parse.get("changed")) else 0
            await self.runtime.upsert_source_state(
                source_url=url,
                etag=page.etag,
                last_modified=page.last_modified,
                sha256=page.content_hash,
                fetched_at=int(time.time()),
                parse_error=None if parse_error in {"", "skipped_unchanged"} else parse_error,
                extra={"year": year, "kind": "calendar_html"},
            )
            total_records += upsert_records
            total_indexes += upsert_indexes
            last_url = url
            last_sha = page.content_hash

        index_map: dict[str, list[str]] = {}
        for year in touched_years:
            for event in await self.runtime.list_calendar_events(year):
                if not event.get("is_no_class"):
                    continue
                try:
                    start = datetime.strptime(str(event.get("start_date")), "%Y-%m-%d").date()
                    end = datetime.strptime(str(event.get("end_date")), "%Y-%m-%d").date()
                except Exception:
                    continue
                cursor = start
                while cursor <= end:
                    key = cursor.strftime("%Y-%m-%d")
                    index_map.setdefault(key, []).append(str(event.get("event_id")))
                    cursor = cursor + timedelta(days=1)
        idx_count = await self.runtime.replace_calendar_no_class_index(index_map)
        total_indexes += idx_count
        self.logger.info(
            "DB name=calendar_html upsert_versions=0 upsert_records=%s upsert_indexes=%s",
            total_records,
            total_indexes,
        )
        return {
            "last_source_url": last_url,
            "last_sha256": last_sha,
            "last_record_count": total_records,
            "changed": changed_any or root_changed,
        }

    async def _job_monthly_class_pdf(self) -> dict[str, Any]:
        return await self._sync_calendar_pdf(kind="monthly")

    async def _job_dow_class_pdf(self) -> dict[str, Any]:
        return await self._sync_calendar_pdf(kind="dow")

    async def _sync_calendar_pdf(self, *, kind: str) -> dict[str, Any]:
        schedule_url = "https://www.titech.ac.jp/english/student/students/life/schedules"
        cfg = await self.runtime.get_effective_config(self.scope_key)
        allowed = [str(x) for x in cfg.get("sources", {}).get("allowedDomains", [])]
        is_allowed = host_allowed(schedule_url, allowed)
        self.logger.info(
            "SOURCE url=%s allowed=%s reason=%s",
            schedule_url,
            str(is_allowed).lower(),
            "" if is_allowed else "domain_not_allowed",
        )
        if not is_allowed:
            self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", schedule_url)
            self.logger.info("PARSE name=%s_pdf ok=false records=0 parse_error=domain_not_allowed", kind)
            self.logger.info("DB name=%s_pdf upsert_versions=0 upsert_records=0 upsert_indexes=0", kind)
            return {"last_source_url": schedule_url, "last_sha256": "", "last_record_count": 0, "changed": False}
        page = await self.fetcher.fetch_text(schedule_url, max_age_sec=0)
        self.logger.info(
            "FETCH url=%s ok=true bytes=%s etag=%s last_modified=%s sha256=%s changed=true",
            schedule_url,
            len(page.text.encode("utf-8", errors="ignore")),
            page.etag or "",
            page.last_modified or "",
            page.content_hash,
        )
        links = self.calendar_service.extract_schedule_pdfs(page.text, schedule_url)
        pdf_url = links["monthly_pdf_url"] if kind == "monthly" else links["dow_pdf_url"]
        if not pdf_url:
            self.logger.info("PARSE name=%s_pdf ok=false records=0 parse_error=no_pdf_link", kind)
            self.logger.info("DB name=%s_pdf upsert_versions=0 upsert_records=0 upsert_indexes=0", kind)
            return {"last_source_url": schedule_url, "last_sha256": "", "last_record_count": 0, "changed": False}
        is_pdf_allowed = host_allowed(pdf_url, allowed)
        self.logger.info(
            "SOURCE url=%s allowed=%s reason=%s",
            pdf_url,
            str(is_pdf_allowed).lower(),
            "" if is_pdf_allowed else "domain_not_allowed",
        )
        if not is_pdf_allowed:
            self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", pdf_url)
            self.logger.info("PARSE name=%s_pdf ok=false records=0 parse_error=domain_not_allowed", kind)
            self.logger.info("DB name=%s_pdf upsert_versions=0 upsert_records=0 upsert_indexes=0", kind)
            return {"last_source_url": pdf_url, "last_sha256": "", "last_record_count": 0, "changed": False}
        binary = await self.fetcher.fetch_binary(pdf_url)
        old_state = await self.runtime.get_source_state(pdf_url)
        changed = (old_state or {}).get("sha256") != binary.content_hash
        self.logger.info(
            "FETCH url=%s ok=true bytes=%s etag=%s last_modified=%s sha256=%s changed=%s",
            pdf_url,
            len(binary.payload),
            binary.etag or "",
            binary.last_modified or "",
            binary.content_hash,
            str(changed).lower(),
        )
        parse = await self.llm_parse_if_changed(f"{kind}_pdf", pdf_url, binary.payload)
        parse_records = int(parse.get("records_count") or 0)
        parse_error = str(parse.get("parse_error") or "")
        parser_used = str(parse.get("parser") or "deterministic")
        upsert_indexes = 0
        if bool(parse.get("changed")):
            parsed_json = parse.get("parsed_json")
            if kind == "monthly" and isinstance(parsed_json, dict):
                await self.runtime.put_schedule_meta("monthly_class_schedule", parsed_json)
                upsert_indexes = 1
            if kind == "dow" and isinstance(parsed_json, list):
                upsert_indexes = await self.runtime.replace_dow_schedule_rows(pdf_url, parsed_json)
        self.logger.info(
            "PARSE name=%s_pdf ok=%s parser=%s records=%s parse_error=%s",
            kind,
            "true" if parse_records > 0 else "false",
            parser_used,
            parse_records,
            parse_error or "",
        )
        await self.runtime.upsert_source_state(
            source_url=pdf_url,
            etag=binary.etag,
            last_modified=binary.last_modified,
            sha256=binary.content_hash,
            fetched_at=int(time.time()),
            parse_error=None if parse_error in {"", "skipped_unchanged"} else parse_error,
            extra={"kind": f"{kind}_class_pdf", "parser": parser_used},
        )
        self.logger.info("DB name=%s_pdf upsert_versions=0 upsert_records=0 upsert_indexes=%s", kind, upsert_indexes)
        return {
            "last_source_url": pdf_url,
            "last_sha256": binary.content_hash,
            "last_record_count": parse_records,
            "changed": changed,
        }

    async def _job_exam_pdf(self) -> dict[str, Any]:
        cfg = await self.runtime.get_effective_config(self.scope_key)
        allowed = [str(x) for x in cfg.get("sources", {}).get("allowedDomains", [])]
        pdf_url = await self.exam_service.find_latest_pdf(self.scope_key)
        if not pdf_url:
            self.logger.info("SOURCE url= allowed=false reason=no_exam_pdf_found")
            self.logger.info("PARSE name=exam_pdf ok=false records=0 parse_error=no_pdf")
            self.logger.info("DB name=exam_pdf upsert_versions=0 upsert_records=0 upsert_indexes=0")
            return {"last_source_url": "", "last_sha256": "", "last_record_count": 0, "changed": False}
        is_allowed = host_allowed(pdf_url, allowed)
        self.logger.info("SOURCE url=%s allowed=%s reason=%s", pdf_url, str(is_allowed).lower(), "" if is_allowed else "domain_not_allowed")
        if not is_allowed:
            self.logger.info("FETCH url=%s ok=false bytes=0 etag= last_modified= sha256= changed=false", pdf_url)
            self.logger.info("PARSE name=exam_pdf ok=false records=0 parse_error=domain_not_allowed")
            self.logger.info("DB name=exam_pdf upsert_versions=0 upsert_records=0 upsert_indexes=0")
            return {"last_source_url": pdf_url, "last_sha256": "", "last_record_count": 0, "changed": False}
        binary = await self.fetcher.fetch_binary(pdf_url)
        old_state = await self.runtime.get_source_state(pdf_url)
        changed = (old_state or {}).get("sha256") != binary.content_hash
        self.logger.info(
            "FETCH url=%s ok=true bytes=%s etag=%s last_modified=%s sha256=%s changed=%s",
            pdf_url,
            len(binary.payload),
            binary.etag or "",
            binary.last_modified or "",
            binary.content_hash,
            str(changed).lower(),
        )
        latest = await self.runtime.get_latest_exam_version()
        parse = await self.llm_parse_if_changed("exam_pdf", pdf_url, binary.payload)
        records = parse.get("parsed_json")
        if not isinstance(records, list):
            records = []
        parse_error = str(parse.get("parse_error") or "")
        parser_used = str(parse.get("parser") or "deterministic")
        self.logger.info(
            "PARSE name=exam_pdf ok=%s parser=%s records=%s parse_error=%s",
            "true" if len(records) > 0 else "false",
            parser_used,
            len(records),
            parse_error or "",
        )
        if bool(parse.get("changed")) or not latest:
            save_result = await self.runtime.save_exam_version(
                pdf_url=pdf_url,
                pdf_hash=binary.content_hash,
                records=records,
                parse_error=parse_error,
            )
        else:
            save_result = {"changed": False, "version_id": latest.get("version_id")}
        await self.runtime.upsert_source_state(
            source_url=pdf_url,
            etag=binary.etag,
            last_modified=binary.last_modified,
            sha256=binary.content_hash,
            fetched_at=int(time.time()),
            parse_error=None if parse_error in {"", "skipped_unchanged"} else parse_error,
            extra={"kind": "exam_pdf", "version_id": save_result.get("version_id"), "parser": parser_used},
        )
        self.logger.info(
            "DB name=exam_pdf upsert_versions=%s upsert_records=%s upsert_indexes=2",
            1 if save_result.get("changed") else 0,
            len(records) if save_result.get("changed") else 0,
        )
        return {
            "last_source_url": pdf_url,
            "last_sha256": binary.content_hash,
            "last_record_count": len(records),
            "changed": bool(save_result.get("changed")),
        }

    async def build_status_report(self) -> dict[str, Any]:
        statuses = await self.runtime.list_sync_job_status()
        years = await self.runtime.get_syllabus_available_years()
        return {
            "jobs": statuses,
            "current_cached_years": years,
        }

    @staticmethod
    def _extract_calendar_year_urls(html_text: str, base_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for link in extract_links(html_text, base_url):
            m = re.search(r"/schedules/(20\d{2})\b", link.url)
            if not m:
                continue
            if link.url in seen:
                continue
            seen.add(link.url)
            urls.append(link.url)
        urls.sort(key=SyncManager._extract_year_from_calendar_url, reverse=True)
        return urls

    @staticmethod
    def _extract_year_from_calendar_url(url: str) -> int:
        m = re.search(r"/schedules/(20\d{2})\b", url)
        if not m:
            return 0
        try:
            return int(m.group(1))
        except Exception:
            return 0

    def _parse_monthly_meta(self, text: str, pdf_url: str) -> dict[str, Any]:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        updated_at_str = ""
        for line in lines:
            m = re.search(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})", line)
            if not m:
                continue
            if any(k in line.lower() for k in ["update", "updated", "更新"]):
                updated_at_str = f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                break
        labels = {
            "1Q": "q1_range",
            "2Q": "q2_range",
            "3Q": "q3_range",
            "4Q": "q4_range",
            "Spring Semester": "spring_range",
            "Fall Semester": "fall_range",
        }
        meta: dict[str, Any] = {
            "source_url": pdf_url,
            "line_count": len(lines),
            "updated_at_str": updated_at_str,
            "head": lines[:20],
            "updated_at": int(time.time()),
        }
        for line in lines:
            for label, key in labels.items():
                if label.lower() not in line.lower():
                    continue
                ranges = self.calendar_service._extract_date_ranges(line, datetime.now(self.tz).year)  # noqa: SLF001
                if not ranges:
                    continue
                start = min(item[0] for item in ranges).strftime("%Y-%m-%d")
                end = max(item[1] for item in ranges).strftime("%Y-%m-%d")
                meta[key] = f"{start}~{end}"
        return meta

    @staticmethod
    def _extract_pdf_text(payload: bytes) -> tuple[str, str | None]:
        text_parts: list[str] = []
        if pdfplumber is not None:
            try:
                with pdfplumber.open(io.BytesIO(payload)) as pdf:  # type: ignore[attr-defined]
                    for page in pdf.pages:
                        page_text = page.extract_text() or ""
                        if page_text:
                            text_parts.append(page_text)
                text = "\n".join(text_parts).strip()
                if text:
                    return text, None
            except Exception:
                pass
        if PdfReader is not None:
            try:
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
        fallback = payload.decode("utf-8", errors="ignore")
        if fallback.strip():
            return fallback, "raw_decode_fallback"
        return "", "extract_failed"

    @staticmethod
    def _parse_dow_rows(text: str) -> list[dict[str, Any]]:
        row_re = re.compile(r"(?:第\s*(\d+)\s*回|No\.?\s*(\d+))", re.IGNORECASE)
        date_re = re.compile(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2}|\d{1,2}/\d{1,2}|\d{1,2}月\d{1,2}日|〃)")
        rows: list[dict[str, Any]] = []
        prev_dates: list[str] = []
        for line in text.splitlines():
            norm = " ".join(line.split())
            if len(norm) < 4:
                continue
            m = row_re.search(norm)
            if not m:
                continue
            row_no = m.group(1) or m.group(2) or ""
            raw_dates = date_re.findall(norm)
            dates: list[str] = []
            for idx, token in enumerate(raw_dates):
                if token == "〃":
                    if idx < len(prev_dates):
                        dates.append(prev_dates[idx])
                    continue
                dates.append(token)
            if dates:
                prev_dates = list(dates)
            rows.append(
                {
                    "row_no": row_no,
                    "week_label": f"第{row_no}回" if row_no else "",
                    "dates": dates[:10],
                    "row_text_raw": norm,
                }
            )
        uniq = {}
        for row in rows:
            uniq[str(row["row_no"])] = row
        return list(uniq.values())
