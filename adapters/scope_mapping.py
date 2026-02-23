from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScopeMapping:
    platform: str
    is_group_chat: bool
    guild_or_server_id: str | None
    group_id: str | None
    user_id: str | None
    scope_key: str


def detect_platform(raw_message: dict[str, Any] | None, default: str = "discord") -> str:
    raw_message = raw_message or {}
    values = [
        raw_message.get("platform"),
        raw_message.get("adapter"),
        raw_message.get("platform_name"),
    ]
    lowered = " ".join(str(v).lower() for v in values if v is not None)
    if "wechat" in lowered or "wx" in lowered:
        return "wechat"
    return default


def extract_guild_or_server_id(raw_message: dict[str, Any] | None) -> str | None:
    raw_message = raw_message or {}
    candidates = [
        "guild_id",
        "guildId",
        "server_id",
        "serverId",
        "guild",
        "server",
    ]
    for key in candidates:
        value = raw_message.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def stringify_unified_origin(unified_msg_origin: Any) -> str:
    if isinstance(unified_msg_origin, (str, int, float, bool)):
        return str(unified_msg_origin)
    if unified_msg_origin is None:
        return "none"
    try:
        return json.dumps(unified_msg_origin, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return repr(unified_msg_origin)


def build_scope_mapping(
    *,
    group_id: Any,
    user_id: Any,
    raw_message: dict[str, Any] | None,
    default_platform: str = "discord",
) -> ScopeMapping:
    platform = detect_platform(raw_message, default=default_platform)
    guild_or_server_id = extract_guild_or_server_id(raw_message)
    group_id_s = str(group_id).strip() if group_id is not None and str(group_id).strip() else None
    user_id_s = str(user_id).strip() if user_id is not None and str(user_id).strip() else None
    is_group = group_id_s is not None

    if platform == "wechat":
        if is_group:
            scope_key = f"wechat:group:{group_id_s}"
        else:
            scope_key = f"wechat:dm:{user_id_s or 'unknown'}"
    else:
        if is_group:
            scope_key = f"discord:channel:{guild_or_server_id or 'unknown'}:{group_id_s}"
        else:
            scope_key = f"discord:dm:{user_id_s or 'unknown'}"

    return ScopeMapping(
        platform=platform,
        is_group_chat=is_group,
        guild_or_server_id=guild_or_server_id,
        group_id=group_id_s,
        user_id=user_id_s,
        scope_key=scope_key,
    )

