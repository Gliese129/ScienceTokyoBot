## 0.9.1 - Calendar Output Fix + Main Split

- Fixed `isct_calendar_get_academic_schedule` output to include `term_ranges` (1Q-4Q) in answer rendering.
- Fixed calendar source rendering:
  - de-duplicated source URLs (canonicalized, stable order)
  - removed hardcoded `/schedules/2025` in `isct_calendar_is_no_class_day`
- Updated academic-year default for calendar queries to April-based AY rollover.
- Split monolithic `main.py` into mixins:
  - `plugin/mixins/{common,admin,exam,calendar,news,syllabus,misc}.py`
  - root `main.py` now keeps registration + initialization and composes mixins.
- Added strategy documentation:
  - `docs/caching_and_sync.md`
- Updated zip build inputs to include `plugin/`, `docs/`, and `CHANGELOG.md`.

## 0.9.0 - Cache & Sync Strategy Refactor

- Added plugin config `parser_provider_id` with `_special: select_provider` for dedicated parser model selection in AstrBot WebUI.
- Implemented hash-gated parsed cache pipeline in sync manager:
  - deterministic parser first
  - LLM fallback via `context.llm_generate(chat_provider_id=..., prompt=...)`
  - strict JSON parse + schema validation before persist
- Added `parsed_cache` SQLite table and runtime methods:
  - `get_parsed_cache`
  - `upsert_parsed_cache`
  - `prune_parsed_cache_source`
- Added sync/cache configuration model in runtime effective config:
  - `sync.enable.{syllabus,calendar,exam}`
  - `sync.intervalSec.*`
  - `cache.ttlNewsDetailSec`
  - `cache.ttlDiscoverySec`
  - `cache.maxSyllabusDetailRecords`
- Sync loops now respect enable flags and per-job interval values from plugin config.
- Improved service caching policies:
  - `NewsService.get_news_item`: L2 TTL cache by URL
  - `DiscoveryService.search`: TTL driven by `cache.ttlDiscoverySec`
  - `SyllabusService.get_course_detail`: lazy L0 write-through cache with max-record pruning
- Enhanced HTML text extraction for table-heavy pages:
  - table/thead/tbody/tfoot newline separators
  - td/th separators
  - NBSP normalization
- Logging now includes parser mode and record stats in sync parse phase.
