# ScienceTokyo Plugin (AstrBot)

This repository now follows AstrBot-first layout:

- `main.py` defines the `Star` plugin class and all handlers
- plugin display name: `ScienceTokyo Plugin`
- plugin id/name: `astrbot_plugin_isct_bot`
- business logic is organized by package type under project root (`guards/`, `adapters/`, `runtime/`, `services/`)
- query orchestration and debug helpers are split into `isct_core/` (to keep `main.py` thinner)
- global defaults are stored in `config/plugin_global.json` (tool limits / fallback sources / source debug switch)
- no-LLM periodic sync jobs run in background (`isct_core/sync_manager.py`) for syllabus years, calendar html/pdf and exam pdf indexing
- page config from AstrBot WebUI is loaded via `__init__(context, config)` and used as default baseline
- parser tasks can use dedicated `parser_provider_id` (WebUI plugin config) via `context.llm_generate(chat_provider_id=..., prompt=...)`
- runtime state for config/prefs/ban score uses SQLite (`data/plugin_data/{plugin_name}/runtime.sqlite3`)
- push target uses `event.unified_msg_origin` and active delivery uses `self.context.send_message(unified_msg_origin, ...)`
- plugin data directory is resolved by `get_astrbot_data_path() / "plugin_data" / self.name`
- orchestration internals (scope resolve / moderation / rate-limit / push scheduling / cache) are not exposed as LLM tools

## Quick Start

```bash
python -m pytest -q
```

## AstrBot Usage

1. In AstrBot WebUI, configure Discord adapter.
2. Enable `Auto-register plugin commands as Discord slash commands`.
3. Load this plugin (`main.py` exports `ScienceTokyoNerdBotPlugin`, class is defined in this file).
4. Only admin commands are exposed as slash commands:
   - `/isct_help`
   - `/isct_admin_config_show [scope]`
   - `/isct_admin_config_set <scope> <path> <value>`
   - `/isct_admin_feature_enable <scope> <feature>`
   - `/isct_admin_feature_disable <scope> <feature>`
   - `/isct_admin_audit`
   - `/isct_admin_source_debug <category>`
   - `/isct_admin_sync_status`
   - `/isct_admin_push_test [scope_key]`
   - `/isct_admin_mod_strike <user_key> <severe|light> <reason>`
   - `/isct_admin_exam_cache_set <pdf_url>`

## LLM Tool Calling

- This plugin also registers `llm_tool`s:
  - `isct_exam_latest`
  - `isct_exam_watch`
  - `isct_exam_sync`
  - `isct_exam_list_pdfs`
  - `isct_exam_parse_pdf`
  - `isct_exam_find_exam`
  - `isct_exam_course`
  - `isct_exam_day`
  - `isct_course_search`
  - `isct_course_compare`
  - `isct_syllabus_list_groups`
  - `isct_syllabus_search_courses`
  - `isct_syllabus_get_course_detail`
  - `isct_calendar_this_week`
  - `isct_calendar_next`
  - `isct_calendar_get_academic_schedule`
  - `isct_calendar_is_no_class_day`
  - `isct_news_search`
  - `isct_news_list_current_students`
  - `isct_news_get_item`
  - `isct_news_list_legacy`
  - `isct_abroad_list_programs`
  - `isct_scholarship_overview`
  - `isct_scholarship_announcements`
  - `isct_clubs_overview`
- In AstrBot, make sure tool calling is enabled for the chat provider and these tools are turned on (e.g. `/tool ls`, `/tool on <tool_name>`).
- `llm_tool` descriptions come from the function docstring and `Args` section, so do not remove them.
- To debug source issues quickly, run `/isct_admin_source_debug <category>` and check the resolved `allowed_domains`, `seeds`, and `fallbacks`.
- For scheduled parser jobs, set `parser_provider_id` in plugin config to a lightweight model provider id (empty means deterministic-only parsing).

## Build Zip For AstrBot

```bash
python build_astrbot_zip.py
```

Output:
- `/Users/miaozean/Desktop/workspace/astrbot/ScienceTokyoBot/dist/astrbot_plugin_isct_bot.zip`

Note:
- Upload zip basename must use only letters/digits/underscore (no `-` / `.`), otherwise AstrBot may fail to import uploaded plugin module.

The builder also prints:
- `Verify: ...` (ensures zip contains SQLite runtime wiring + AstrBot plugin data path usage)
- `SHA256: ...` (for upload artifact verification)
