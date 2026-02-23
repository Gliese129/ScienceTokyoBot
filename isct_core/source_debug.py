from __future__ import annotations

from typing import Any

from isct_core.config_loader import GlobalConfig
from runtime.sqlite_runtime import KVRuntime


async def build_source_debug(
    runtime: KVRuntime,
    global_config: GlobalConfig,
    *,
    scope_key: str,
    category: str,
    sources: list[str] | None,
) -> tuple[list[str], str]:
    effective = await runtime.get_effective_config(scope_key)
    source_cfg = effective.get("sources", {})
    allowed_domains = [str(x) for x in source_cfg.get("allowedDomains", [])]
    seeds = [str(x) for x in source_cfg.get("seeds", {}).get(category, [])]

    resolved = [str(x) for x in (sources or []) if str(x).strip()]
    if not resolved:
        resolved = [f"{seed} (seed)" for seed in seeds[:5]]
    if not resolved:
        resolved = [f"{url} (fallback)" for url in global_config.fallback_sources(category)]

    debug = (
        f"source_debug: category={category}, "
        f"allowed_domains={len(allowed_domains)}, "
        f"seeds={len(seeds)}, "
        f"resolved_sources={len(resolved)}"
    )
    return resolved, debug
