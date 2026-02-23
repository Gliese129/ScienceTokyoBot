# ScienceTokyoBot Caching and Sync Strategy (AstrBot)

## Scope

This plugin uses a three-layer cache model to reduce token cost and improve deterministic responses.

- L0 parsed cache: hash-gated structured parse results stored in SQLite.
- L1 index cache: lightweight list/index structures (news lists, link indexes).
- L2 query cache: short-TTL cache for search/detail acceleration.

## Cache Matrix

| Module | Strategy | Notes |
| --- | --- | --- |
| calendar | L0 | Store `term_ranges` + `events`; query should not require LLM |
| exam | L0 (+optional L1) | Store parsed exam records by PDF hash/version |
| campus | L0 | Low-frequency static pages |
| news | L1 + L2 | List/index cached, detail cached by TTL |
| discovery | L2 (optional L1) | Query-oriented, avoid large long-term storage |
| syllabus | groups: L0/L1, detail: lazy L0, search: L2 | Detail cache is bounded by max records |

## AstrBot Integration Rules

### 1) Plugin config via `_conf_schema.json`

Expose these controls in WebUI:

- `parser_provider_id` (`_special: select_provider`)
- `sync_enable_*`
- `sync_interval_*_sec`
- `cache_ttl_*`
- `cache_max_records_syllabus_detail`

Reference: [AstrBot Plugin Config](https://docs.astrbot.app/dev/star/guides/plugin-config.html)

### 2) LLM parser fallback

Only sync/startup parsing may call LLM fallback, via:

- `context.llm_generate(chat_provider_id=..., prompt=...)`

Query tools should read SQLite results and avoid LLM for deterministic answers.

Reference: [AstrBot AI Guide](https://docs.astrbot.app/dev/star/guides/ai.html)

### 3) Data storage location

Use plugin data path:

- `get_astrbot_data_path() / "plugin_data" / self.name`
- SQLite file example: `data/plugin_data/astrbot_plugin_isct_bot/runtime.sqlite3`

Reference: [AstrBot Storage Guide](https://docs.astrbot.app/dev/star/guides/storage.html)

## Sync Pipeline

1. Fetch source payload.
2. Compute `sha256`.
3. If unchanged and parsed cache exists: return cached parsed JSON.
4. If changed:
   - deterministic parser first
   - if low confidence or parser error: LLM fallback parser
   - strict JSON parse and schema validation
5. Persist parsed result with parse metadata.

Minimum log fields per run:

- `job_start/job_end`
- `source_url`, `allowed`
- `sha256`, `changed`
- `parser` (`cache`/`deterministic`/`llm`), `records_count`, `parse_error`

## Test-time recommended settings

- Enable sync for calendar/exam (and syllabus years).
- Use lightweight parser provider in `parser_provider_id`.
- Keep news/discovery TTL short during debugging.
- Keep syllabus detail cache bounded with `cache_max_records_syllabus_detail`.
