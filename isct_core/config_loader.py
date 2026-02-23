from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GlobalConfig:
    raw: dict[str, Any]

    @property
    def default_limit(self) -> int:
        value = self.raw.get("toolDefaults", {}).get("defaultLimit", 8)
        try:
            return max(1, int(value))
        except Exception:
            return 8

    @property
    def max_list_limit(self) -> int:
        value = self.raw.get("toolDefaults", {}).get("maxListLimit", 20)
        try:
            return max(1, int(value))
        except Exception:
            return 20

    @property
    def source_debug_enabled(self) -> bool:
        value = self.raw.get("toolDefaults", {}).get("sourceDebugEnabled", True)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    def fallback_sources(self, category: str) -> list[str]:
        items = self.raw.get("fallbackSources", {}).get(category, [])
        if not isinstance(items, list):
            return []
        return [str(x) for x in items if str(x).strip()]


def load_global_config(path: str | Path) -> GlobalConfig:
    target = Path(path)
    if not target.exists():
        return GlobalConfig(raw={})
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return GlobalConfig(raw=payload)
