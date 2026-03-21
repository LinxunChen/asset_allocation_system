# Satellite Agent V1

Event-driven, technically-confirmed opportunity agent for a core-satellite portfolio.

## What is included

- Python package scaffold for the satellite detection agent
- SQLite storage for events, insights, bars, snapshots, opportunity cards, and alert history
- SQLite storage for events, insights, bars, snapshots, opportunity cards, alert history, structured logs, and run summaries
- Event normalization, rule-based/OpenAI-backed extraction, indicator computation, scoring, entry/exit generation, and Feishu notification
- A single-process service loop and a CLI for database initialization, watchlist seeding, and one-shot execution
- Free-source adapters for SEC filings and Yahoo Finance price bars
- Unit/integration-style tests covering indicators, normalization, scoring, and deduplicated notifications

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
.venv/bin/pip install -e .
satellite-agent init-db
satellite-agent write-default-config
satellite-agent sync-watchlist
SATELLITE_USE_SEC_FILINGS_SOURCE=1 satellite-agent run-once --name morning_scan --note "baseline config"
SATELLITE_USE_SEC_FILINGS_SOURCE=1 satellite-agent run-once --name threshold_test --event-score-threshold 65 --swing-market-score-threshold 58 --swing-priority-threshold 78
satellite-agent sync-yahoo-bars --symbol NVDA --timeframe 1d
satellite-agent report-runs --limit 5
satellite-agent report-runs --limit 5 --json
satellite-agent report-run
satellite-agent report-run --json
satellite-agent report-errors --limit 10
satellite-agent report-errors --limit 10 --json
satellite-agent report-sources
satellite-agent report-sources --json
satellite-agent report-strategy --days 14
satellite-agent report-strategy --days 14 --json
satellite-agent replay-evaluate --replay-path tests/fixtures/events.jsonl --name replay_baseline --note "fixture replay"
satellite-agent replay-evaluate --replay-path tests/fixtures/events.jsonl --name replay_tuned --event-score-threshold 65 --position-market-score-threshold 60
satellite-agent replay-evaluate --replay-path tests/fixtures/events.jsonl --json
satellite-agent annotate-run --run-id run_a --name tuned_thresholds --note "raised market confirmation floor"
satellite-agent compare-runs --run-id run_a --run-id run_b
satellite-agent compare-runs --run-id run_a --run-id run_b --json
satellite-agent batch-replay --spec-path config/satellite_agent/batch_replay.template.json --output-dir ./data/satellite_agent/experiments/batch_runs
satellite-agent batch-replay --spec-path config/satellite_agent/batch_replay.template.json --output-dir ./data/satellite_agent/experiments/batch_runs --json
satellite-agent report-batch --manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_id>_manifest.json
satellite-agent report-batch --manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_id>_manifest.json --markdown-path ./data/reports/latest_batch.md
satellite-agent compare-batches --left-manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_a>_manifest.json --right-manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_b>_manifest.json
satellite-agent compare-batches --left-manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_a>_manifest.json --right-manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_b>_manifest.json --markdown-path ./data/reports/batch_diff.md
satellite-agent list-batches --dir ./data/satellite_agent/experiments/batch_runs --limit 10
satellite-agent promote-batch --manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_id>_manifest.json --output-config-path ./config/satellite_agent/agent.recommended.json
satellite-agent promote-batch --manifest-path ./data/satellite_agent/experiments/batch_runs/<batch_id>_manifest.json --output-config-path ./config/satellite_agent/agent.recommended.json --force
SATELLITE_FEISHU_WEBHOOK=https://example.feishu.cn/webhook satellite-agent send-test-notification --symbol NVDA
satellite-agent demo-flow --workspace-dir ./data/satellite_agent/experiments/demo_flow
satellite-agent daily-run --workspace-dir ./data/satellite_agent/daily_run --config-path ./config/satellite_agent/agent.recommended.json --replay-path tests/fixtures/events.jsonl
satellite-agent run-once --workspace-dir ./data/satellite_agent/run_once --replay-path tests/fixtures/events.jsonl
satellite-agent serve --workspace-dir ./data/satellite_agent/serve
python3 -m unittest discover -s tests -v
```

`<batch_id>`, `<batch_a>`, and `<batch_b>` are placeholders. Run `satellite-agent list-batches --dir ./data/satellite_agent/experiments/batch_runs --limit 10` first, then copy a real manifest path from the output.

## Environment variables

- `SATELLITE_DB_PATH`: SQLite path, default `./data/satellite_agent/agent.db`
- `SATELLITE_CONFIG_PATH`: runtime config path, default `./config/satellite_agent/agent.json`
- `SATELLITE_FEISHU_WEBHOOK`: Feishu webhook URL
- `SATELLITE_OPENAI_API_KEY`: OpenAI API key
- `SATELLITE_OPENAI_MODEL`: OpenAI model, default `gpt-4o-mini`
- `SATELLITE_OPENAI_BASE_URL`: override for compatible API endpoint
- `SATELLITE_DRY_RUN`: `1` to skip outbound webhook calls
- `SATELLITE_POLL_SECONDS`: event polling cadence, default `60`
- `SATELLITE_SEC_USER_AGENT`: SEC required user agent string
- `SATELLITE_USE_SEC_FILINGS_SOURCE`: `1` to pull SEC Atom feeds for the watchlist
- `SATELLITE_USE_GOOGLE_NEWS_SOURCE`: `1` to enable Google News RSS event ingestion
- `SATELLITE_USE_GOOGLE_RESEARCH_SOURCE`: `1` to enable analyst/research headline ingestion
- `SATELLITE_CROSS_SOURCE_DEDUP_HOURS`: duplicate suppression window across sources, default `12`

## 中文说明

- 中文版评分体系说明见 [scoring_guide.zh-CN.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/scoring_guide.zh-CN.md)
- 完整决策链路说明见 [decision_logic.zh-CN.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/decision_logic.zh-CN.md)
- 后续如果评分权重、阈值或优先级逻辑发生变化，这份中文文档会同步维护

## Watchlist 维护

- 观察池现在只推荐维护一份模板：[agent.template.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/agent.template.json)。
- `satellite-agent write-default-config` 会把这份模板写成你的 `config/satellite_agent/agent.json`；`seed-watchlist` 和 `sync-watchlist --use-defaults` 也都会读取同一份模板。
- 也就是说，代码里不再单独维护另一套默认 watchlist 常量，避免“改了模板但程序默认值还是旧的”。
- `watchlist` 现在支持两种写法：
  - 平铺：`stocks` / `etfs`
  - 分组：`stock_groups` / `etf_groups`
- 推荐使用分组写法，后续维护更清楚；程序会自动把各组拍平成实际生效的 watchlist。
- `SEC` 只读取股票池，`Google News` 会读取整个 watchlist（股票 + ETF）。
- 修改 `config/satellite_agent/agent.json` 后，运行阶段会自动检测并同步 watchlist；也可以手动执行 `satellite-agent sync-watchlist` 立即刷新数据库。
- 运行命令会比较当前配置和数据库里的 watchlist，不一致时会自动同步；手动执行 `sync-watchlist` 主要用于想立即刷新或单独验证配置的时候。

## 飞书通知配置

- 现在可以直接在 [agent.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/agent.json) 中维护通知配置：

```json
{
  "notifications": {
    "feishu_webhook": "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx",
    "dry_run": false
  }
}
```

- `feishu_webhook` 为空时，不会真实外发，只会在运行记录里标记 `no_transport_configured`。
- `dry_run: true` 时，会保留提醒判定与落盘，但跳过真正的 webhook 调用。
- `send-test-notification`、`run-once`、`serve` 会统一读取这份通知配置。
- 也仍然支持使用环境变量 `SATELLITE_FEISHU_WEBHOOK`；如果两边都配置，当前以 `config/satellite_agent/agent.json` 为准。

## Strategy Tuning

- `config/satellite_agent/agent.json` can now carry a `strategy` block with global and per-horizon threshold overrides.
- `run-once`, `serve`, and `replay-evaluate` support temporary experiment flags:
  - `--event-score-threshold`
  - `--swing-market-score-threshold`
  - `--position-market-score-threshold`
  - `--swing-priority-threshold`
  - `--position-priority-threshold`
- CLI experiment overrides are captured in the stored config snapshot, so `report-run` and `compare-runs` reflect the actual thresholds used for that run.
- `batch-replay` runs multiple replay experiments from one JSON spec, writes each experiment into an isolated SQLite file, and returns a ranked summary.
- `batch-replay` now prints per-card threshold margins, so you can see how far each card sits above or below the `event`, `market`, and `priority` cutoffs.
- `demo-flow` is the fastest way to run the current MVP end to end: it executes a replay evaluation, a batch experiment, and writes the linked reports into one workspace directory.
- `batch-replay` also writes a manifest JSON into the output directory, and `report-batch` can reload that manifest later without rerunning experiments.
- `batch-replay` also writes a Markdown report next to the manifest by default, so each batch leaves behind a human-readable summary artifact.
- `list-batches` scans a batch output directory and shows the latest manifests, recommended setup, next-step title, and saved report path.
- `list-batches` now also summarizes whether the recent recommendation is stable or drifting, so you can tell if the tuning direction is actually changing.
- `list-batches` now includes a winner snapshot for each batch, including alerts, cards, events, and nearest threshold margins when that metadata is available.
- `promote-batch` exports the recommended experiment from a batch manifest into a usable runtime config file, so replay tuning can flow into a real config artifact.
- `promote-batch --force` now creates an automatic backup of the existing target config before overwrite, and prints a Chinese strategy change summary for auditability.
- `send-test-notification` sends a Feishu test card without waiting for a real event, so webhook and phone delivery can be verified before market open.
- `daily-run` is a safer day-to-day wrapper that runs one configured cycle and writes a Chinese review report plus structured payload into a dedicated workspace directory.
- `daily-run` now includes a Chinese health verdict, so you can quickly distinguish between normal runs, runs that need attention, and blocked runs.
- `daily_run_review.md` now prioritizes Chinese interpretation over raw counters and includes per-card source links, making it easier to read and drill into the original event.
- `batch-replay` now also recommends a preferred experiment, favoring stronger output first and, when output ties, the stricter passing setup.
- The recommendation logic is configurable via `recommendation.weights` in the batch replay spec.
- `batch-replay` also emits a short summary section that explains the most important differences inside the batch.
- `batch-replay` and `compare-batches` now emit a `Next Step` section with heuristic follow-up guidance, such as expanding replay coverage or promoting a new baseline.
- `compare-batches` compares two saved batch manifests and reports changes in alerts, cards, events, and threshold margins for experiments with the same name.
- `compare-batches` now also includes a direct summary of whether the new batch is looser or tighter, whether recommendation changed, and how overall output moved.

### Batch Replay Spec

- Use [batch_replay.template.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/batch_replay.template.json) as the starting point.
- Each experiment accepts `name`, `note`, and an `overrides` object.
- Supported override keys:
  - `event_score_threshold`
  - `swing_market_score_threshold`
  - `position_market_score_threshold`
  - `swing_priority_threshold`
  - `position_priority_threshold`
- Supported recommendation weights:
  - `alerts_sent`
  - `cards_generated`
  - `events_processed`
  - `strictness`
  - `priority_proximity`
  - `failures`
- `batch-replay` isolates each experiment in its own SQLite file to avoid replay state and duplicate-event contamination across runs.

## Notes

- External market/news providers are intentionally kept behind interfaces so the first iteration can run with replayed or manually ingested data.
- Real-time price refresh currently uses Yahoo Finance chart endpoints and caches bars into SQLite on demand.
- News and research ingestion can now run from RSS/Atom feeds, with cross-source fingerprint deduplication before scoring.
- Local runtime config can now control watchlist membership and source toggles without code edits.
- Each `run_once` now writes structured logs and a summarized run record into SQLite for easier debugging and replay analysis.
- CLI reporting commands can show recent runs, single-run details, and aggregated error summaries directly from SQLite.
- Reporting commands also support `--json` for automation, dashboards, or downstream scripts.
- Source health is checked per adapter before fetching, recorded in SQLite, and viewable with `report-sources`.
- `report-strategy` summarizes event-type quality, source stability, and recent alert volume from the accumulated SQLite history.
- `replay-evaluate` runs a replay source through the live pipeline, then emits a combined run summary plus strategy report for quick experiment review.
- `compare-runs` compares multiple experiment runs side by side using run-scoped summaries, event-type results, source health, and alert totals.
- `compare-runs` now includes a compact threshold summary so you can see the key parameter differences without opening each run detail.
- Runs can now carry `name`, `note`, and a config snapshot, making replay experiments easier to compare and annotate over time.
- `annotate-run` lets you rename or append notes to an existing run after the fact without touching the rest of the stored summary.
- `batch-replay` is the preferred way to compare multiple threshold sets against the same replay input because it avoids cross-run state pollution.
- The batch replay summary now includes card-level margin diagnostics such as `event_margin`, `market_margin`, and `priority_margin` for fast threshold debugging.
- Batch manifests are useful for archiving and sharing results because they point to every experiment DB and preserve the ranking snapshot.
- Batch Markdown reports are useful for quick review because they preserve the same ranking, recommendation, summary, and next-step guidance shown in the CLI.
- Batch reports now include a `Winner Snapshot` section so you can read the best setup's key output and margin diagnostics without opening the raw manifest.
- `demo-flow` writes a replay report, replay payload JSON, batch manifest, batch report, and batch index report together, so the full MVP path is reviewable from one folder.
- `demo-flow` now also writes a recommended runtime config file, so the MVP path ends with a concrete config artifact instead of stopping at a report.
- When `promote-batch` overwrites an existing config, it writes a timestamped `.bak.json` file next to the target so you can roll back quickly.
- `promote-batch` now includes a Chinese per-field strategy diff, such as threshold and RSI changes, before you adopt a new recommendation.
- Feishu notifications now use interactive cards with clearer score, entry, stop, and source-link sections, making them much easier to consume on mobile.
- `daily-run` prefers `config/satellite_agent/agent.recommended.json` when present, making it easy to move from replay recommendation into a repeatable operational run.
- Batch comparisons are name-based, so keep experiment names stable across specs if you want clean before/after diffs.
- The batch recommendation is heuristic, not a portfolio decision engine; it is meant to narrow down which parameter set deserves the next round of testing.
- If you want the recommender to prefer fewer but stricter signals, increase `strictness`; if you want it to prefer busier signal output, increase `alerts_sent` and `cards_generated`.
- If `SATELLITE_OPENAI_API_KEY` is absent, the extractor falls back to deterministic rule-based parsing.
