# ScienceTokyo Plugin (AstrBot)

基于 AstrBot 的 Science Tokyo 信息服务插件。插件通过 LLM Tool Calling 提供考试、课程、校历、新闻、奖学金、留学与社团信息检索，并内置 SQLite 缓存、定时同步、权限与风控能力。

## 项目定位

- 插件 ID: `astrbot_plugin_isct_bot`
- 显示名: `ScienceTokyo Plugin`
- 入口文件: `main.py`（导出 `ScienceTokyoNerdBotPlugin`）
- 运行方式: 以 AstrBot 插件形式运行（不是独立 Web 服务）

## 核心能力

- 官方来源优先：仅在允许域名与种子页范围内抓取、解析。
- 考试信息：同步最新考试 PDF、版本 diff、按课程/日期查询、watch 订阅推送。
- 课程信息：课程搜索、课程对比、群组列表、课程详情拉取。
- 校历信息：本周关键节点、下一关键节点、学年日历、无课日判定。
- 校园资讯：在学生新闻、历史公告、奖学金、留学项目、社团入口。
- 策略控制：按 scope 配置功能开关、限流、违规分数与封禁策略。
- 数据落地：运行态写入 SQLite，支持缓存与后台定时同步。

## 目录结构

```text
.
├── main.py                       # AstrBot 插件注册与初始化
├── plugin/mixins/                # 按领域拆分的命令与 llm_tool
├── runtime/sqlite_runtime.py     # SQLite 运行时与配置/审计/缓存
├── services/                     # 业务抓取与解析（exam/news/syllabus/calendar/campus）
├── isct_core/                    # query 编排、全局配置、sync 管理
├── config/plugin_global.json     # 全局默认策略与 fallback 来源
├── _conf_schema.json             # AstrBot WebUI 配置 schema
├── build_astrbot_zip.py          # 打包上传 zip
└── docs/caching_and_sync.md      # 缓存与同步策略说明
```

## 环境要求

- Python `>=3.10`
- 可用的 AstrBot 运行环境
- 推荐（可选）安装 PDF 解析库以提高考试 PDF 抽取质量：
  - `pdfplumber`
  - `pypdf`

## 快速开始

### 1) 本地校验

```bash
python -m pytest -q
```

### 2) 构建 AstrBot 上传包

```bash
python build_astrbot_zip.py
```

构建产物：

- `dist/astrbot_plugin_isct_bot.zip`

脚本会同时输出：

- `Verify: ...`（校验 zip 关键内容）
- `SHA256: ...`（产物哈希）

### 3) 在 AstrBot 中接入

1. 在 AstrBot WebUI 上传 `dist/astrbot_plugin_isct_bot.zip`。
2. 启用插件并重载。
3. 若接入 Discord，建议打开「自动注册插件命令为 Discord Slash Command」。
4. 在聊天提供商中开启工具调用，并启用本插件 llm_tool。

## 插件配置（WebUI）

配置项来自 `_conf_schema.json`。以下是常用项：

| 配置项 | 说明 |
| --- | --- |
| `enabled_features` | 逗号分隔功能开关列表（如 `feature.exam,feature.calendar`） |
| `allowed_domains` | 允许抓取域名白名单 |
| `exam_seeds` | 考试页面种子地址 |
| `course_seeds` | 课程/教学大纲种子地址 |
| `calendar_seeds` | 校历种子地址 |
| `news_seeds` | 新闻种子地址 |
| `scholarship_seeds` | 奖学金种子地址 |
| `abroad_seeds` | 留学项目种子地址 |
| `clubs_seeds` | 社团/课外活动种子地址 |
| `news_legacy_seeds` | 历史公告入口种子地址 |
| `admin_user_keys` | 管理员用户键，逗号分隔（示例：`discord:user:1234567890`） |
| `admin_role_ids` | 管理员角色 ID，逗号分隔 |
| `parser_provider_id` | 同步/解析任务专用模型提供商 ID |
| `sync_enable_*` | 各同步任务开关 |
| `sync_interval_*_sec` | 各同步任务周期（秒） |
| `cache_ttl_*` | 查询/详情缓存 TTL |
| `cache_max_records_syllabus_detail` | syllabus 详情缓存最大记录数 |
| `per_user_limit_*` / `per_scope_limit_*` | 限流阈值 |
| `warning_increment` / `decay_half_life_days` / `ban_level_*` | 违规分与封禁策略 |

## 管理命令（Slash Command）

当前明确注册的管理员命令如下：

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

## LLM Tools 清单

### Exam

- `isct_exam_latest`
- `isct_exam_watch`
- `isct_exam_sync`
- `isct_exam_list_pdfs`
- `isct_exam_parse_pdf`
- `isct_exam_find_exam`
- `isct_exam_course`
- `isct_exam_day`

### Syllabus

- `isct_course_search`
- `isct_course_compare`
- `isct_syllabus_list_groups`
- `isct_syllabus_search_courses`
- `isct_syllabus_get_course_detail`

### Calendar

- `isct_calendar_this_week`
- `isct_calendar_next`
- `isct_calendar_get_academic_schedule`
- `isct_calendar_is_no_class_day`

### News / Campus

- `isct_news_search`
- `isct_news_list_current_students`
- `isct_news_get_item`
- `isct_news_list_legacy`
- `isct_abroad_list_programs`
- `isct_scholarship_overview`
- `isct_scholarship_announcements`
- `isct_clubs_overview`

## 数据与缓存

- 运行时数据库：`data/plugin_data/<plugin_name>/runtime.sqlite3`
- 缓存策略：
  - L0：结构化解析缓存（`parsed_cache`）
  - L1：列表/索引缓存
  - L2：短 TTL 查询缓存（`search_cache`）
- 同步任务（可开关）：
  - syllabus 年份
  - calendar HTML/PDF
  - exam PDF

更多说明见：`docs/caching_and_sync.md`

## 常见排查

1. 查不到数据：检查 `allowed_domains` 与对应 `*_seeds` 是否正确。
2. PDF 解析质量不稳定：安装 `pdfplumber`/`pypdf`，并配置 `parser_provider_id` 作为回退解析。
3. 管理命令无权限：核对 `admin_user_keys` / `admin_role_ids`。
4. 工具未被模型调用：确认 AstrBot 侧已启用 tool calling，且工具已开启。

## 安全与隐私

- 文档与示例均使用占位符，不包含任何个人账号、用户名或私有路径信息。
- 请勿将真实管理员 ID、访问令牌、私有链接写入仓库。
- 生产环境建议定期轮换管理员标识并审计 `audit_log`。

## 版本说明

- 版本变更记录见 `CHANGELOG.md`。
- 插件元信息见 `metadata.yaml`。
