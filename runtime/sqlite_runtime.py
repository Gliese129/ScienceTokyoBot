from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


DEFAULT_ALLOWED_DOMAINS = [
    "isct.ac.jp",
    "students.isct.ac.jp",
    "syllabus.s.isct.ac.jp",
    "titech.ac.jp",
    "tmd.ac.jp",
    "titech.info",
    "welcome.titech.app",
]

DEFAULT_SCOPE_CONFIG = {
    "enabledFeatures": [
        "feature.course",
        "feature.course_compare",
        "feature.exam",
        "feature.exam_watch",
        "feature.calendar",
        "feature.clubs",
        "feature.news_search",
        "feature.scholarship",
        "feature.abroad",
        "feature.admin",
        "feature.prefs",
    ],
    "rateLimit": {
        "perUser": [
            {"limit": 10, "windowSec": 10 * 60},
            {"limit": 60, "windowSec": 24 * 60 * 60},
        ],
        "perScope": [
            {"limit": 100, "windowSec": 10 * 60},
        ],
    },
    "moderation": {
        "decayHalfLifeDays": 7,
        "warningIncrement": 0.25,
        "banDurationsSec": {
            "1": 10 * 60,
            "2": 24 * 60 * 60,
            "3": None,
        },
    },
    "sources": {
        "allowedDomains": DEFAULT_ALLOWED_DOMAINS,
        "seeds": {
            "exam": [
                "https://www.titech.ac.jp/english/student/students/life/undergraduate-exam",
            ],
            "course": [
                "https://syllabus.s.isct.ac.jp/search",
            ],
            "calendar": [
                "https://www.titech.ac.jp/english/student/students/life/schedules",
            ],
            "news": [
                "https://students.isct.ac.jp/en",
                "https://students.isct.ac.jp/ja",
            ],
            "scholarship": [
                "https://students.isct.ac.jp/en",
                "https://students.isct.ac.jp/ja",
                "https://students.isct.ac.jp/en/012/tuition-and-scholarship/specific-scholarships",
                "https://students.isct.ac.jp/en/012/tuition-and-scholarship/scholarships",
                "https://students.isct.ac.jp/ja/012/tuition-and-scholarship/specific-scholarships",
            ],
            "abroad": [
                "https://students.isct.ac.jp/en/016/global/abroad",
            ],
            "clubs": [
                "https://students.isct.ac.jp/en/012/student-life-and-support/extracurricular-activities/university-festivals",
            ],
            "news_legacy": [
                "https://www.isct.ac.jp/en/news/it87dcs7t5y2",
            ],
        },
    },
    "admins": {
        "userKeys": [],
        "roleIds": [],
    },
}


@dataclass(frozen=True)
class RuntimeDecision:
    allowed: bool
    reason: str | None = None
    message: str | None = None
    cooldown_minutes: int | None = None


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = deepcopy(value)
    return out


def _parent_scope(scope_key: str) -> str | None:
    parts = scope_key.split(":")
    if len(parts) == 4 and parts[0] == "discord" and parts[1] == "channel":
        return f"discord:guild:{parts[2]}"
    return None


def _now_ts() -> int:
    return int(time.time())


class KVRuntime:
    def __init__(self, plugin_name: str, db_path: str | Path, page_config: Mapping[str, Any] | None = None) -> None:
        self.plugin_name = plugin_name
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._push_target_memory: dict[str, Any] = {}
        self._fallback_sources: dict[str, list[str]] = {}
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        self.base_config = deepcopy(DEFAULT_SCOPE_CONFIG)
        self.apply_page_config(page_config or {})

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                    url TEXT PRIMARY KEY,
                    content_hash TEXT,
                    extracted_text TEXT,
                    etag TEXT,
                    last_modified TEXT,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_cache (
                    query TEXT PRIMARY KEY,
                    result_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS exam_versions (
                    version_id TEXT PRIMARY KEY,
                    pdf_url TEXT NOT NULL,
                    pdf_hash TEXT NOT NULL,
                    parse_error TEXT,
                    created_at INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS exam_records (
                    version_id TEXT NOT NULL,
                    date TEXT,
                    type TEXT,
                    period TEXT,
                    course_code TEXT,
                    course_title TEXT,
                    instructors TEXT,
                    room TEXT,
                    source_pdf_url TEXT,
                    row_text_raw TEXT,
                    confidence REAL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_items (
                    scope_key TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    url TEXT,
                    first_seen_at INTEGER NOT NULL,
                    PRIMARY KEY (scope_key, fingerprint)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_user_key TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT,
                    detail_json TEXT,
                    created_at INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_state (
                    source_url TEXT PRIMARY KEY,
                    etag TEXT,
                    last_modified TEXT,
                    sha256 TEXT,
                    fetched_at INTEGER,
                    parse_error TEXT,
                    extra_json TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS calendar_events (
                    year INTEGER NOT NULL,
                    event_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    kind TEXT,
                    is_no_class INTEGER NOT NULL,
                    source_url TEXT,
                    PRIMARY KEY (year, event_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS calendar_no_class_index (
                    date TEXT PRIMARY KEY,
                    event_ids_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS term_ranges (
                    year INTEGER NOT NULL,
                    term TEXT NOT NULL,
                    start_date TEXT,
                    end_date TEXT,
                    source_url TEXT,
                    PRIMARY KEY (year, term)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dow_schedule_rows (
                    source_url TEXT NOT NULL,
                    row_no TEXT NOT NULL,
                    week_label TEXT,
                    dates_json TEXT,
                    row_text_raw TEXT,
                    PRIMARY KEY (source_url, row_no)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schedule_meta (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_job_status (
                    job_name TEXT PRIMARY KEY,
                    last_run_at INTEGER,
                    last_success_at INTEGER,
                    last_error TEXT,
                    last_source_url TEXT,
                    last_sha256 TEXT,
                    last_record_count INTEGER,
                    changed INTEGER
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsed_cache (
                    source_key TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    parsed_json TEXT NOT NULL,
                    parsed_at INTEGER NOT NULL,
                    provider_id TEXT,
                    parse_error TEXT,
                    record_count INTEGER NOT NULL,
                    PRIMARY KEY (source_key, source_url)
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_exam_records_version_date ON exam_records(version_id, date)")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_exam_records_version_course ON exam_records(version_id, course_code)"
            )
            self._conn.commit()

    def apply_page_config(self, page_config: Mapping[str, Any] | None) -> None:
        overlay = self._page_config_overlay(page_config or {})
        self.base_config = _deep_merge(DEFAULT_SCOPE_CONFIG, overlay)

    async def get_effective_config(self, scope_key: str) -> dict[str, Any]:
        base = deepcopy(self.base_config)
        global_cfg = await self._kv_get_json("scope_config::global:default", default={})
        if global_cfg:
            base = _deep_merge(base, global_cfg)
        parent = _parent_scope(scope_key)
        if parent:
            parent_cfg = await self._kv_get_json(f"scope_config::{parent}", default={})
            if parent_cfg:
                base = _deep_merge(base, parent_cfg)
        scope_cfg = await self._kv_get_json(f"scope_config::{scope_key}", default={})
        if scope_cfg:
            base = _deep_merge(base, scope_cfg)
        base = self._merge_fallback_sources(base)
        return base

    def set_fallback_sources(self, fallback_sources: Mapping[str, Any] | None) -> None:
        normalized: dict[str, list[str]] = {}
        if isinstance(fallback_sources, Mapping):
            for key, value in fallback_sources.items():
                if isinstance(value, list):
                    normalized[str(key)] = [str(item) for item in value if str(item).strip()]
        self._fallback_sources = normalized

    async def is_admin(self, scope_key: str, user_key: str, role_ids: list[str]) -> bool:
        config = await self.get_effective_config(scope_key)
        admins = config.get("admins", {})
        if user_key in admins.get("userKeys", []):
            return True
        if set(role_ids) & set(admins.get("roleIds", [])):
            return True
        return False

    async def is_feature_enabled(self, scope_key: str, feature_id: str, user_key: str, role_ids: list[str]) -> bool:
        if await self.is_admin(scope_key, user_key, role_ids):
            return True
        config = await self.get_effective_config(scope_key)
        return feature_id in set(config.get("enabledFeatures", []))

    async def check_active_ban(self, scope_key: str, user_key: str, is_admin: bool) -> RuntimeDecision:
        if is_admin:
            return RuntimeDecision(allowed=True)

        now = _now_ts()
        candidates = [
            await self._kv_get_json(f"ban::scope::{scope_key}::{user_key}", default=None),
            await self._kv_get_json(f"ban::global::{user_key}", default=None),
        ]
        for ban in candidates:
            if not isinstance(ban, dict):
                continue
            expires_at = ban.get("expires_at")
            if expires_at is not None and int(expires_at) <= now:
                continue
            reason = str(ban.get("reason") or "policy")
            if expires_at is None:
                return RuntimeDecision(
                    allowed=False,
                    reason="banned",
                    message=f"当前为永久封禁状态。原因类别：{reason}。",
                    cooldown_minutes=None,
                )
            cooldown = max(1, math.ceil((int(expires_at) - now) / 60))
            return RuntimeDecision(
                allowed=False,
                reason="banned",
                message=f"当前为封禁状态。原因类别：{reason}，剩余约 {cooldown} 分钟。",
                cooldown_minutes=cooldown,
            )
        return RuntimeDecision(allowed=True)

    async def check_and_record_rate_limit(
        self,
        scope_key: str,
        user_key: str,
        feature_id: str,
        is_admin: bool,
    ) -> RuntimeDecision:
        if is_admin:
            return RuntimeDecision(allowed=True)

        config = await self.get_effective_config(scope_key)
        now = _now_ts()

        for rule in config.get("rateLimit", {}).get("perUser", []):
            limit = int(rule["limit"])
            window = int(rule["windowSec"])
            key = f"ratelimit::user::{scope_key}::{user_key}::{window}"
            timestamps = await self._kv_get_json(key, default=[])
            timestamps = [int(ts) for ts in timestamps if int(ts) >= now - window]
            if len(timestamps) >= limit:
                cooldown = max(1, math.ceil((min(timestamps) + window - now) / 60))
                await self._kv_put_json(key, timestamps)
                return RuntimeDecision(
                    allowed=False,
                    reason="rate_limited",
                    cooldown_minutes=cooldown,
                    message=f"请求频率超过阈值，请在 {cooldown} 分钟后重试。",
                )
            timestamps.append(now)
            await self._kv_put_json(key, timestamps)

        for rule in config.get("rateLimit", {}).get("perScope", []):
            limit = int(rule["limit"])
            window = int(rule["windowSec"])
            key = f"ratelimit::scope::{scope_key}::{window}"
            timestamps = await self._kv_get_json(key, default=[])
            timestamps = [int(ts) for ts in timestamps if int(ts) >= now - window]
            if len(timestamps) >= limit:
                cooldown = max(1, math.ceil((min(timestamps) + window - now) / 60))
                await self._kv_put_json(key, timestamps)
                return RuntimeDecision(
                    allowed=False,
                    reason="rate_limited",
                    cooldown_minutes=cooldown,
                    message=f"当前会话请求总量过高，请在 {cooldown} 分钟后重试。",
                )
            timestamps.append(now)
            await self._kv_put_json(key, timestamps)

        return RuntimeDecision(allowed=True)

    async def add_watch_course(self, user_key: str, course_code: str) -> tuple[bool, str]:
        code = course_code.strip().upper()
        if not code:
            return False, "empty_code"
        prefs = await self._kv_get_json(f"prefs::{user_key}", default={})
        watch = list(dict.fromkeys(prefs.get("watch_courses", [])))
        if code in watch:
            return False, "already_exists"
        watch.append(code)
        prefs["watch_courses"] = watch
        await self._kv_put_json(f"prefs::{user_key}", prefs)
        return True, "ok"

    async def list_watch_courses(self, user_key: str) -> list[str]:
        prefs = await self._kv_get_json(f"prefs::{user_key}", default={})
        return [str(x) for x in prefs.get("watch_courses", [])]

    async def get_user_prefs(self, user_key: str) -> dict[str, Any]:
        value = await self._kv_get_json(f"prefs::{user_key}", default={})
        return value if isinstance(value, dict) else {}

    async def add_violation_score(self, scope_key: str, user_key: str, severe: bool, reason: str) -> RuntimeDecision:
        config = await self.get_effective_config(scope_key)
        moderation = config.get("moderation", {})
        default_moderation = DEFAULT_SCOPE_CONFIG["moderation"]
        key = f"ban_score::{scope_key}::{user_key}"
        old = await self._kv_get_json(key, default={"score": 0.0, "updated_at": _now_ts()})
        now = _now_ts()
        elapsed_days = max(0.0, (now - int(old.get("updated_at", now))) / 86400)
        half_life_days = float(moderation.get("decayHalfLifeDays", default_moderation["decayHalfLifeDays"]))
        decayed = float(old.get("score", 0.0)) * (0.5 ** (elapsed_days / max(half_life_days, 0.1)))
        warning_increment = float(moderation.get("warningIncrement", default_moderation["warningIncrement"]))
        new_score = decayed + (1.0 if severe else warning_increment)
        await self._kv_put_json(key, {"score": new_score, "updated_at": now})
        if not severe:
            return RuntimeDecision(
                allowed=False,
                reason="warning",
                message=f"已记录警告（{reason}）。",
            )

        level = min(3, max(1, math.ceil(new_score)))
        ban_durations = moderation.get("banDurationsSec", default_moderation["banDurationsSec"])
        duration = ban_durations.get(str(level))
        expires_at = None if duration is None else now + int(duration)
        await self._kv_put_json(
            f"ban::scope::{scope_key}::{user_key}",
            {"level": level, "reason": reason, "expires_at": expires_at},
        )
        if expires_at is None:
            return RuntimeDecision(allowed=False, reason="banned", message="当前为永久封禁状态。")
        cooldown = max(1, math.ceil((expires_at - now) / 60))
        return RuntimeDecision(
            allowed=False,
            reason="banned",
            cooldown_minutes=cooldown,
            message=f"当前为封禁状态。原因类别：{reason}，剩余约 {cooldown} 分钟。",
        )

    async def set_push_target(self, scope_key: str, unified_msg_origin: Any) -> None:
        self._push_target_memory[scope_key] = unified_msg_origin
        await self._kv_put_json(
            f"push_target::{scope_key}",
            {
                "unified_msg_origin": self._json_safe(unified_msg_origin),
                "updated_at": self.iso_now(),
            },
        )

    async def get_push_target(self, scope_key: str) -> Any | None:
        if scope_key in self._push_target_memory:
            return self._push_target_memory[scope_key]
        value = await self._kv_get_json(f"push_target::{scope_key}", default={})
        if isinstance(value, dict) and value.get("unified_msg_origin") is not None:
            return value["unified_msg_origin"]
        return None

    async def list_push_target_scopes(self) -> list[str]:
        with self._lock:
            cur = self._conn.execute("SELECT key FROM kv_store WHERE key LIKE 'push_target::%'")
            rows = cur.fetchall()
        out: list[str] = []
        for (key,) in rows:
            if isinstance(key, str) and key.startswith("push_target::"):
                out.append(key.split("push_target::", 1)[1])
        return sorted(list(dict.fromkeys(out)))

    async def set_scope_config(self, scope_key: str, config_patch: dict[str, Any]) -> None:
        current = await self._kv_get_json(f"scope_config::{scope_key}", default={})
        merged = _deep_merge(current if isinstance(current, dict) else {}, config_patch)
        await self._kv_put_json(f"scope_config::{scope_key}", merged)

    async def get_scope_config(self, scope_key: str) -> dict[str, Any]:
        value = await self._kv_get_json(f"scope_config::{scope_key}", default={})
        return value if isinstance(value, dict) else {}

    async def set_scope_path(self, scope_key: str, path: str, value: Any) -> None:
        if not path.strip():
            raise ValueError("empty path")
        current = await self.get_scope_config(scope_key)
        parts = [p for p in path.split(".") if p]
        cursor: Any = current
        parent: Any = None
        parent_key: Any = None
        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            next_part = parts[i + 1] if not is_last else None
            if part.isdigit():
                idx = int(part)
                if not isinstance(cursor, list):
                    new_list: list[Any] = []
                    if isinstance(parent, dict):
                        parent[parent_key] = new_list
                    elif isinstance(parent, list) and isinstance(parent_key, int):
                        while len(parent) <= parent_key:
                            parent.append({})
                        parent[parent_key] = new_list
                    else:
                        current = new_list
                    cursor = new_list
                while len(cursor) <= idx:
                    cursor.append({} if (next_part and not next_part.isdigit()) else [])
                if is_last:
                    cursor[idx] = value
                else:
                    parent, parent_key, cursor = cursor, idx, cursor[idx]
                continue

            # dict key
            if not isinstance(cursor, dict):
                new_dict: dict[str, Any] = {}
                if isinstance(parent, list) and isinstance(parent_key, int):
                    while len(parent) <= parent_key:
                        parent.append({})
                    parent[parent_key] = new_dict
                elif isinstance(parent, dict):
                    parent[parent_key] = new_dict
                else:
                    current = new_dict
                cursor = new_dict
            if is_last:
                cursor[part] = value
            else:
                if part not in cursor or not isinstance(cursor[part], (dict, list)):
                    cursor[part] = [] if (next_part and next_part.isdigit()) else {}
                parent, parent_key, cursor = cursor, part, cursor[part]
        await self._kv_put_json(f"scope_config::{scope_key}", current)

    async def log_admin_action(
        self,
        *,
        actor_user_key: str,
        scope_key: str,
        action: str,
        target: str | None,
        detail: dict[str, Any] | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO audit_log (actor_user_key, scope_key, action, target, detail_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    actor_user_key,
                    scope_key,
                    action,
                    target,
                    json.dumps(detail or {}, ensure_ascii=False),
                    _now_ts(),
                ),
            )
            self._conn.commit()

    async def list_admin_audit(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 200))
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT id, actor_user_key, scope_key, action, target, detail_json, created_at
                FROM audit_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": int(row[0]),
                    "actor_user_key": str(row[1]),
                    "scope_key": str(row[2]),
                    "action": str(row[3]),
                    "target": str(row[4]) if row[4] is not None else None,
                    "detail": json.loads(row[5] or "{}"),
                    "created_at": int(row[6]),
                }
            )
        return out

    async def get_cache(self, url: str, max_age_sec: int = 24 * 60 * 60) -> dict[str, Any] | None:
        now = _now_ts()
        with self._lock:
            cur = self._conn.execute(
                "SELECT content_hash, extracted_text, etag, last_modified, updated_at FROM cache WHERE url = ?",
                (url,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        updated_at = int(row[4])
        if updated_at + max_age_sec < now:
            return None
        return {
            "content_hash": row[0],
            "extracted_text": row[1],
            "etag": row[2],
            "last_modified": row[3],
            "updated_at": updated_at,
        }

    async def put_cache(
        self,
        *,
        url: str,
        content_hash: str,
        extracted_text: str,
        etag: str | None,
        last_modified: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO cache (url, content_hash, extracted_text, etag, last_modified, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    extracted_text = excluded.extracted_text,
                    etag = excluded.etag,
                    last_modified = excluded.last_modified,
                    updated_at = excluded.updated_at
                """,
                (url, content_hash, extracted_text, etag, last_modified, _now_ts()),
            )
            self._conn.commit()

    async def get_search_cache(self, query: str, max_age_sec: int = 10 * 60) -> list[dict[str, Any]] | None:
        now = _now_ts()
        with self._lock:
            cur = self._conn.execute("SELECT result_json, updated_at FROM search_cache WHERE query = ?", (query,))
            row = cur.fetchone()
        if row is None:
            return None
        if int(row[1]) + max_age_sec < now:
            return None
        try:
            value = json.loads(row[0])
            if isinstance(value, list):
                return value
        except Exception:
            return None
        return None

    async def put_search_cache(self, query: str, items: list[dict[str, Any]]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO search_cache (query, result_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(query) DO UPDATE SET
                    result_json = excluded.result_json,
                    updated_at = excluded.updated_at
                """,
                (query, json.dumps(items, ensure_ascii=False), _now_ts()),
            )
            self._conn.commit()

    async def save_exam_version(
        self,
        *,
        pdf_url: str,
        pdf_hash: str,
        records: list[dict[str, Any]],
        parse_error: str | None = None,
    ) -> dict[str, Any]:
        latest = await self.get_latest_exam_version()
        if latest and latest.get("pdf_hash") == pdf_hash:
            return {
                "changed": False,
                "version_id": latest["version_id"],
                "previous_version_id": latest.get("previous_version_id"),
                "diff": [],
                "parse_error": latest.get("parse_error"),
            }
        previous_version_id = latest["version_id"] if latest else None
        version_id = f"v_{_now_ts()}_{uuid.uuid4().hex[:8]}"
        created_at = _now_ts()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO exam_versions (version_id, pdf_url, pdf_hash, parse_error, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (version_id, pdf_url, pdf_hash, parse_error, created_at),
            )
            for rec in records:
                self._conn.execute(
                    """
                    INSERT INTO exam_records (
                        version_id, date, type, period, course_code, course_title,
                        instructors, room, source_pdf_url, row_text_raw, confidence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        version_id,
                        rec.get("date"),
                        rec.get("type"),
                        rec.get("period"),
                        rec.get("course_code"),
                        rec.get("course_title"),
                        rec.get("instructors"),
                        rec.get("room"),
                        rec.get("source_pdf_url", pdf_url),
                        rec.get("row_text_raw"),
                        float(rec.get("confidence", 0.0)),
                    ),
                )
            self._conn.commit()
        diff = await self.diff_exam_versions(version_id=version_id, previous_version_id=previous_version_id)
        return {
            "changed": True,
            "version_id": version_id,
            "previous_version_id": previous_version_id,
            "diff": diff,
            "parse_error": parse_error,
        }

    async def get_latest_exam_version(self) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT version_id, pdf_url, pdf_hash, parse_error, created_at
                FROM exam_versions
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {
            "version_id": str(row[0]),
            "pdf_url": str(row[1]),
            "pdf_hash": str(row[2]),
            "parse_error": row[3],
            "created_at": int(row[4]),
        }

    async def get_exam_records(self, version_id: str | None = None) -> list[dict[str, Any]]:
        if version_id is None:
            latest = await self.get_latest_exam_version()
            if not latest:
                return []
            version_id = str(latest["version_id"])
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT date, type, period, course_code, course_title, instructors, room, source_pdf_url, row_text_raw, confidence
                FROM exam_records
                WHERE version_id = ?
                ORDER BY date, period, course_code
                """,
                (version_id,),
            )
            rows = cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "date": row[0],
                    "type": row[1],
                    "period": row[2],
                    "course_code": row[3],
                    "course_title": row[4],
                    "instructors": row[5],
                    "room": row[6],
                    "source_pdf_url": row[7],
                    "row_text_raw": row[8],
                    "confidence": float(row[9] or 0.0),
                }
            )
        return out

    async def query_exam_by_course(self, keyword: str) -> list[dict[str, Any]]:
        needle = keyword.strip().lower()
        if not needle:
            return []
        latest = await self.get_latest_exam_version()
        if not latest:
            return []
        version_id = str(latest["version_id"])
        like = f"%{needle}%"
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT date, type, period, course_code, course_title, instructors, room, source_pdf_url, row_text_raw, confidence
                FROM exam_records
                WHERE version_id = ?
                  AND (LOWER(COALESCE(course_code,'')) LIKE ? OR LOWER(COALESCE(course_title,'')) LIKE ?)
                ORDER BY date, period, course_code
                """,
                (version_id, like, like),
            )
            rows = cur.fetchall()
        return [
            {
                "date": row[0],
                "type": row[1],
                "period": row[2],
                "course_code": row[3],
                "course_title": row[4],
                "instructors": row[5],
                "room": row[6],
                "source_pdf_url": row[7],
                "row_text_raw": row[8],
                "confidence": float(row[9] or 0.0),
            }
            for row in rows
        ]

    async def query_exam_by_day(self, date_str: str) -> list[dict[str, Any]]:
        target = date_str.strip()
        if not target:
            return []
        latest = await self.get_latest_exam_version()
        if not latest:
            return []
        version_id = str(latest["version_id"])
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT date, type, period, course_code, course_title, instructors, room, source_pdf_url, row_text_raw, confidence
                FROM exam_records
                WHERE version_id = ?
                  AND date = ?
                ORDER BY period, course_code
                """,
                (version_id, target),
            )
            rows = cur.fetchall()
        return [
            {
                "date": row[0],
                "type": row[1],
                "period": row[2],
                "course_code": row[3],
                "course_title": row[4],
                "instructors": row[5],
                "room": row[6],
                "source_pdf_url": row[7],
                "row_text_raw": row[8],
                "confidence": float(row[9] or 0.0),
            }
            for row in rows
        ]

    async def get_source_state(self, source_url: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT source_url, etag, last_modified, sha256, fetched_at, parse_error, extra_json
                FROM source_state WHERE source_url = ?
                """,
                (source_url,),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            extra = json.loads(row[6]) if row[6] else {}
        except Exception:
            extra = {}
        return {
            "source_url": row[0],
            "etag": row[1],
            "last_modified": row[2],
            "sha256": row[3],
            "fetched_at": int(row[4] or 0),
            "parse_error": row[5],
            "extra": extra,
        }

    async def upsert_source_state(
        self,
        *,
        source_url: str,
        etag: str | None,
        last_modified: str | None,
        sha256: str | None,
        fetched_at: int,
        parse_error: str | None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO source_state (source_url, etag, last_modified, sha256, fetched_at, parse_error, extra_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_url) DO UPDATE SET
                    etag = excluded.etag,
                    last_modified = excluded.last_modified,
                    sha256 = excluded.sha256,
                    fetched_at = excluded.fetched_at,
                    parse_error = excluded.parse_error,
                    extra_json = excluded.extra_json
                """,
                (
                    source_url,
                    etag,
                    last_modified,
                    sha256,
                    int(fetched_at),
                    parse_error,
                    json.dumps(extra or {}, ensure_ascii=False),
                ),
            )
            self._conn.commit()

    async def replace_calendar_events(self, year: int, events: list[dict[str, Any]]) -> int:
        year_int = int(year)
        with self._lock:
            self._conn.execute("DELETE FROM calendar_events WHERE year = ?", (year_int,))
            count = 0
            for idx, event in enumerate(events):
                event_id = str(event.get("event_id") or f"{year_int}_{idx}")
                self._conn.execute(
                    """
                    INSERT INTO calendar_events (year, event_id, title, start_date, end_date, kind, is_no_class, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        year_int,
                        event_id,
                        str(event.get("title") or ""),
                        str(event.get("start_date") or ""),
                        str(event.get("end_date") or ""),
                        str(event.get("kind") or ""),
                        1 if bool(event.get("is_no_class")) else 0,
                        str(event.get("source_url") or ""),
                    ),
                )
                count += 1
            self._conn.commit()
        return count

    async def list_calendar_events(self, year: int) -> list[dict[str, Any]]:
        year_int = int(year)
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT event_id, title, start_date, end_date, kind, is_no_class, source_url
                FROM calendar_events
                WHERE year = ?
                ORDER BY start_date, title
                """,
                (year_int,),
            )
            rows = cur.fetchall()
        return [
            {
                "event_id": row[0],
                "title": row[1],
                "start_date": row[2],
                "end_date": row[3],
                "kind": row[4],
                "is_no_class": bool(row[5]),
                "source_url": row[6],
            }
            for row in rows
        ]

    async def replace_calendar_no_class_index(self, mapping: dict[str, list[str]]) -> int:
        with self._lock:
            self._conn.execute("DELETE FROM calendar_no_class_index")
            count = 0
            for date_key, event_ids in mapping.items():
                self._conn.execute(
                    """
                    INSERT INTO calendar_no_class_index (date, event_ids_json)
                    VALUES (?, ?)
                    """,
                    (str(date_key), json.dumps(event_ids, ensure_ascii=False)),
                )
                count += 1
            self._conn.commit()
        return count

    async def get_no_class_event_ids_by_date(self, date_key: str) -> list[str]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT event_ids_json FROM calendar_no_class_index WHERE date = ?",
                (date_key,),
            )
            row = cur.fetchone()
        if not row:
            return []
        try:
            payload = json.loads(row[0] or "[]")
        except Exception:
            payload = []
        if not isinstance(payload, list):
            return []
        return [str(x) for x in payload]

    async def get_calendar_event_titles_by_ids(self, event_ids: list[str]) -> list[str]:
        if not event_ids:
            return []
        placeholders = ",".join(["?"] * len(event_ids))
        with self._lock:
            cur = self._conn.execute(
                f"SELECT title FROM calendar_events WHERE event_id IN ({placeholders})",
                tuple(event_ids),
            )
            rows = cur.fetchall()
        return [str(row[0]) for row in rows if row and row[0]]

    async def replace_term_ranges(self, year: int, rows: list[dict[str, Any]]) -> int:
        year_int = int(year)
        with self._lock:
            self._conn.execute("DELETE FROM term_ranges WHERE year = ?", (year_int,))
            count = 0
            for row in rows:
                self._conn.execute(
                    """
                    INSERT INTO term_ranges (year, term, start_date, end_date, source_url)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        year_int,
                        str(row.get("term") or ""),
                        str(row.get("start_date") or ""),
                        str(row.get("end_date") or ""),
                        str(row.get("source_url") or ""),
                    ),
                )
                count += 1
            self._conn.commit()
        return count

    async def list_term_ranges(self, year: int) -> list[dict[str, Any]]:
        year_int = int(year)
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT term, start_date, end_date, source_url
                FROM term_ranges
                WHERE year = ?
                ORDER BY term
                """,
                (year_int,),
            )
            rows = cur.fetchall()
        return [
            {
                "term": row[0],
                "start_date": row[1],
                "end_date": row[2],
                "source_url": row[3],
            }
            for row in rows
        ]

    async def replace_dow_schedule_rows(self, source_url: str, rows: list[dict[str, Any]]) -> int:
        src = str(source_url)
        with self._lock:
            self._conn.execute("DELETE FROM dow_schedule_rows WHERE source_url = ?", (src,))
            count = 0
            for row in rows:
                self._conn.execute(
                    """
                    INSERT INTO dow_schedule_rows (source_url, row_no, week_label, dates_json, row_text_raw)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        src,
                        str(row.get("row_no") or ""),
                        str(row.get("week_label") or ""),
                        json.dumps(row.get("dates") or [], ensure_ascii=False),
                        str(row.get("row_text_raw") or ""),
                    ),
                )
                count += 1
            self._conn.commit()
        return count

    async def count_dow_schedule_rows(self, source_url: str) -> int:
        src = str(source_url)
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(1) FROM dow_schedule_rows WHERE source_url = ?", (src,))
            row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    async def put_schedule_meta(self, key: str, value: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO schedule_meta (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (key, json.dumps(value, ensure_ascii=False), _now_ts()),
            )
            self._conn.commit()

    async def get_schedule_meta(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute("SELECT value_json FROM schedule_meta WHERE key = ?", (key,))
            row = cur.fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row[0] or "{}")
        except Exception:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    async def put_sync_job_status(
        self,
        *,
        job_name: str,
        last_run_at: int,
        last_success_at: int | None,
        last_error: str | None,
        last_source_url: str | None,
        last_sha256: str | None,
        last_record_count: int | None,
        changed: bool | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sync_job_status (
                    job_name, last_run_at, last_success_at, last_error, last_source_url, last_sha256, last_record_count, changed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_name) DO UPDATE SET
                    last_run_at = excluded.last_run_at,
                    last_success_at = excluded.last_success_at,
                    last_error = excluded.last_error,
                    last_source_url = excluded.last_source_url,
                    last_sha256 = excluded.last_sha256,
                    last_record_count = excluded.last_record_count,
                    changed = excluded.changed
                """,
                (
                    job_name,
                    int(last_run_at),
                    int(last_success_at) if last_success_at is not None else None,
                    last_error,
                    last_source_url,
                    last_sha256,
                    int(last_record_count) if last_record_count is not None else None,
                    None if changed is None else (1 if changed else 0),
                ),
            )
            self._conn.commit()

    async def list_sync_job_status(self) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT job_name, last_run_at, last_success_at, last_error, last_source_url, last_sha256, last_record_count, changed
                FROM sync_job_status
                ORDER BY job_name
                """
            )
            rows = cur.fetchall()
        return [
            {
                "job_name": row[0],
                "last_run_at": int(row[1] or 0),
                "last_success_at": int(row[2] or 0) if row[2] is not None else None,
                "last_error": row[3],
                "last_source_url": row[4],
                "last_sha256": row[5],
                "last_record_count": int(row[6] or 0) if row[6] is not None else None,
                "changed": None if row[7] is None else bool(row[7]),
            }
            for row in rows
        ]

    async def get_sync_job_status(self, job_name: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT job_name, last_run_at, last_success_at, last_error, last_source_url, last_sha256, last_record_count, changed
                FROM sync_job_status
                WHERE job_name = ?
                """,
                (job_name,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "job_name": row[0],
            "last_run_at": int(row[1] or 0),
            "last_success_at": int(row[2] or 0) if row[2] is not None else None,
            "last_error": row[3],
            "last_source_url": row[4],
            "last_sha256": row[5],
            "last_record_count": int(row[6] or 0) if row[6] is not None else None,
            "changed": None if row[7] is None else bool(row[7]),
        }

    async def get_parsed_cache(self, source_key: str, source_url: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT sha256, parsed_json, parsed_at, provider_id, parse_error, record_count
                FROM parsed_cache
                WHERE source_key = ? AND source_url = ?
                """,
                (source_key, source_url),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            parsed_json = json.loads(row[1] or "null")
        except Exception:
            parsed_json = None
        return {
            "sha256": str(row[0] or ""),
            "parsed_json": parsed_json,
            "parsed_at": int(row[2] or 0),
            "provider_id": row[3],
            "parse_error": row[4],
            "record_count": int(row[5] or 0),
        }

    async def upsert_parsed_cache(
        self,
        *,
        source_key: str,
        source_url: str,
        sha256: str,
        parsed_json: Any,
        parsed_at: int,
        provider_id: str | None,
        parse_error: str | None,
        record_count: int,
    ) -> None:
        payload = json.dumps(parsed_json, ensure_ascii=False, default=self._json_default)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO parsed_cache (
                    source_key, source_url, sha256, parsed_json, parsed_at, provider_id, parse_error, record_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key, source_url) DO UPDATE SET
                    sha256 = excluded.sha256,
                    parsed_json = excluded.parsed_json,
                    parsed_at = excluded.parsed_at,
                    provider_id = excluded.provider_id,
                    parse_error = excluded.parse_error,
                    record_count = excluded.record_count
                """,
                (
                    source_key,
                    source_url,
                    sha256,
                    payload,
                    int(parsed_at),
                    provider_id,
                    parse_error,
                    int(record_count),
                ),
            )
            self._conn.commit()

    async def set_syllabus_available_years(self, years: list[int]) -> None:
        normalized = sorted(list({int(y) for y in years if int(y) > 2000}))
        await self._kv_put_json("syllabus::available_years", normalized)

    async def get_syllabus_available_years(self) -> list[int]:
        value = await self._kv_get_json("syllabus::available_years", default=[])
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            try:
                out.append(int(item))
            except Exception:
                continue
        return sorted(list(dict.fromkeys(out)))

    async def diff_exam_versions(self, *, version_id: str, previous_version_id: str | None) -> list[dict[str, Any]]:
        if not previous_version_id:
            return []
        old_records = await self.get_exam_records(previous_version_id)
        new_records = await self.get_exam_records(version_id)
        old_map = {str(rec.get("course_code") or f"__{idx}"): rec for idx, rec in enumerate(old_records)}
        new_map = {str(rec.get("course_code") or f"__{idx}"): rec for idx, rec in enumerate(new_records)}
        keys = sorted(set(old_map.keys()) | set(new_map.keys()))
        out: list[dict[str, Any]] = []
        fields = ["date", "period", "room", "type"]
        for key in keys:
            old_rec = old_map.get(key)
            new_rec = new_map.get(key)
            if old_rec is None and new_rec is not None:
                out.append({"course_code": key, "change": "added", "old": None, "new": new_rec})
                continue
            if old_rec is not None and new_rec is None:
                out.append({"course_code": key, "change": "removed", "old": old_rec, "new": None})
                continue
            assert old_rec is not None and new_rec is not None
            changed_fields = [f for f in fields if str(old_rec.get(f) or "") != str(new_rec.get(f) or "")]
            if changed_fields:
                out.append(
                    {
                        "course_code": key,
                        "change": "updated",
                        "fields": changed_fields,
                        "old": old_rec,
                        "new": new_rec,
                    }
                )
        return out

    async def list_watchers_for_course(self, course_code: str) -> list[str]:
        code = course_code.strip().upper()
        if not code:
            return []
        with self._lock:
            cur = self._conn.execute("SELECT key, value_json FROM kv_store WHERE key LIKE 'prefs::%'")
            rows = cur.fetchall()
        users: list[str] = []
        for key, value_json in rows:
            try:
                payload = json.loads(value_json)
            except Exception:
                continue
            watch_courses = [str(x).upper() for x in payload.get("watch_courses", [])]
            if code in watch_courses and isinstance(key, str) and key.startswith("prefs::"):
                users.append(key.split("prefs::", 1)[1])
        return sorted(list(dict.fromkeys(users)))

    async def _kv_put_json(self, key: str, value: Any) -> None:
        payload = json.dumps(value, ensure_ascii=False, default=self._json_default)
        updated_at = self.iso_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO kv_store (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (key, payload, updated_at),
            )
            self._conn.commit()

    async def _kv_get_json(self, key: str, default: Any) -> Any:
        with self._lock:
            cur = self._conn.execute("SELECT value_json FROM kv_store WHERE key = ?", (key,))
            row = cur.fetchone()
        if row is None:
            return deepcopy(default)
        try:
            return json.loads(row[0])
        except Exception:
            return deepcopy(default)

    def _merge_fallback_sources(self, config: dict[str, Any]) -> dict[str, Any]:
        out = deepcopy(config)
        sources = out.setdefault("sources", {})
        seeds = sources.setdefault("seeds", {})
        if not isinstance(seeds, dict):
            seeds = {}
            sources["seeds"] = seeds
        for category, urls in self._fallback_sources.items():
            if category not in seeds or not isinstance(seeds.get(category), list) or not seeds.get(category):
                seeds[category] = [str(url) for url in urls if str(url).strip()]
        return out

    @staticmethod
    def _page_config_overlay(page_config: Mapping[str, Any]) -> dict[str, Any]:
        enabled_features = KVRuntime._to_str_list(page_config.get("enabled_features"))
        allowed_domains = KVRuntime._to_str_list(page_config.get("allowed_domains"))
        admin_user_keys = KVRuntime._to_str_list(page_config.get("admin_user_keys"))
        admin_role_ids = KVRuntime._to_str_list(page_config.get("admin_role_ids"))
        exam_seeds = KVRuntime._to_str_list(page_config.get("exam_seeds"))
        course_seeds = KVRuntime._to_str_list(page_config.get("course_seeds"))
        calendar_seeds = KVRuntime._to_str_list(page_config.get("calendar_seeds"))
        news_seeds = KVRuntime._to_str_list(page_config.get("news_seeds"))
        scholarship_seeds = KVRuntime._to_str_list(page_config.get("scholarship_seeds"))
        abroad_seeds = KVRuntime._to_str_list(page_config.get("abroad_seeds"))
        clubs_seeds = KVRuntime._to_str_list(page_config.get("clubs_seeds"))
        news_legacy_seeds = KVRuntime._to_str_list(page_config.get("news_legacy_seeds"))

        per_user_10m = KVRuntime._to_int(page_config.get("per_user_limit_10min"), 10)
        per_user_day = KVRuntime._to_int(page_config.get("per_user_limit_day"), 60)
        per_scope_10m = KVRuntime._to_int(page_config.get("per_scope_limit_10min"), 100)
        warning_increment = KVRuntime._to_float(page_config.get("warning_increment"), 0.25)
        decay_half_life_days = KVRuntime._to_float(page_config.get("decay_half_life_days"), 7.0)

        level1_min = KVRuntime._to_int(page_config.get("ban_level_1_minutes"), 10)
        level2_hour = KVRuntime._to_int(page_config.get("ban_level_2_hours"), 24)
        level3_perm = KVRuntime._to_bool(page_config.get("ban_level_3_permanent"), True)

        return {
            "enabledFeatures": enabled_features or deepcopy(DEFAULT_SCOPE_CONFIG["enabledFeatures"]),
            "rateLimit": {
                "perUser": [
                    {"limit": per_user_10m, "windowSec": 10 * 60},
                    {"limit": per_user_day, "windowSec": 24 * 60 * 60},
                ],
                "perScope": [
                    {"limit": per_scope_10m, "windowSec": 10 * 60},
                ],
            },
            "moderation": {
                "decayHalfLifeDays": decay_half_life_days,
                "warningIncrement": warning_increment,
                "banDurationsSec": {
                    "1": level1_min * 60,
                    "2": level2_hour * 60 * 60,
                    "3": None if level3_perm else 7 * 24 * 60 * 60,
                },
            },
            "sources": {
                "allowedDomains": allowed_domains or deepcopy(DEFAULT_ALLOWED_DOMAINS),
                "seeds": {
                    "exam": exam_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["exam"]),
                    "course": course_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["course"]),
                    "calendar": calendar_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["calendar"]),
                    "news": news_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["news"]),
                    "scholarship": scholarship_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["scholarship"]),
                    "abroad": abroad_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["abroad"]),
                    "clubs": clubs_seeds or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["clubs"]),
                    "news_legacy": news_legacy_seeds
                    or deepcopy(DEFAULT_SCOPE_CONFIG["sources"]["seeds"]["news_legacy"]),
                },
            },
            "admins": {
                "userKeys": admin_user_keys,
                "roleIds": admin_role_ids,
            },
        }

    @staticmethod
    def _to_str_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return [str(value).strip()] if str(value).strip() else []

    @staticmethod
    def _to_int(value: Any, fallback: int) -> int:
        try:
            return int(value)
        except Exception:
            return fallback

    @staticmethod
    def _to_float(value: Any, fallback: float) -> float:
        try:
            return float(value)
        except Exception:
            return fallback

    @staticmethod
    def _to_bool(value: Any, fallback: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True
            if lowered in {"false", "0", "no", "n"}:
                return False
        if value is None:
            return fallback
        return bool(value)

    @staticmethod
    def iso_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _json_default(value: Any) -> Any:
        if hasattr(value, "model_dump") and callable(value.model_dump):
            return value.model_dump()
        if hasattr(value, "to_dict") and callable(value.to_dict):
            return value.to_dict()
        if hasattr(value, "__dict__"):
            return dict(value.__dict__)
        return str(value)

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [KVRuntime._json_safe(x) for x in value]
        if isinstance(value, dict):
            return {str(k): KVRuntime._json_safe(v) for k, v in value.items()}
        return KVRuntime._json_default(value)
