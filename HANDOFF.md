# Handoff

Last updated: 2026-03-15 (Asia/Shanghai)

## Current State

The `satellite_agent` project is operational as a local Python service with:

- event ingestion adapters for SEC and Google News
- market-data-backed scoring
- SQLite persistence
- replay evaluation
- batch replay experiments
- Markdown run reviews
- Chinese-first reporting and notification copy
- live `run-once` / `serve` artifact writing
- alert deduplication and run-level alert budgets
- grouped watchlist config and template-driven defaults

The watchlist/config setup was simplified today:

- daily editing target: [config/agent.json](/Users/linxun/CodeSpace/asset_allocation_system/config/agent.json)
- default template: [config/agent.template.json](/Users/linxun/CodeSpace/asset_allocation_system/config/agent.template.json)
- batch replay template: [config/batch_replay.template.json](/Users/linxun/CodeSpace/asset_allocation_system/config/batch_replay.template.json)
- Python code no longer maintains a second hard-coded default watchlist list

Test status at handoff:

- Command run: `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v`
- Result: `47` tests passed

## Most Recent User-Facing Behavior

The latest real `run-once` validation completed successfully:

- run id: `87d865817512fcf9`
- duration: `54.3s`
- events fetched: `4`
- events processed: `3`
- cards generated: `6`
- alerts sent: `0`
- review output: [data/live_run/latest_live_review.md](/Users/linxun/CodeSpace/asset_allocation_system/data/live_run/latest_live_review.md)
- structured output: [data/live_run/latest_live_payload.json](/Users/linxun/CodeSpace/asset_allocation_system/data/live_run/latest_live_payload.json)

Important behavior confirmed today:

- `agent.json` now preserves readable Chinese instead of `\uXXXX` escapes
- template naming now uses `template`, not `example`
- watchlist was synced from config into SQLite successfully
- Google News candidate volume is far lower than before and no longer explodes into thousands of items
- the review now shows `最终推送卡片` before the long `卡片解读` section
- real networked runs are working when executed outside the sandbox

## Known Issues

The system is usable, but these issues are still open:

1. Some symbols in the watchlist should not be treated as normal SEC-feed symbols.
   - confirmed problematic examples: `ARM`, `BRK.B`, `TCEHY`, `NBIS`
   - current behavior: SEC returns a company-information HTML page instead of a usable feed

2. Some symbols still hit market-data fetch instability.
   - seen in recent runs for symbols such as `BLK`, `MA`, `BRK.B`, `NVDA`
   - failures are non-fatal, but they reduce score quality and can suppress otherwise-valid cards

3. The next real improvement should be per-symbol source routing.
   - ETFs, ADRs, OTC names, non-US symbols, and normal US equities should not all use the same source policy

4. Current thresholds remain conservative.
   - real runs are producing candidate cards
   - many are still suppressed by `quality_cutoff` / `threshold_not_met`

## Immediate Next Tasks

1. Add watchlist-level asset metadata or source-routing rules.
   - Example goal: decide per symbol whether it should use `SEC`, `Google News`, both, or neither.

2. Exclude or downgrade SEC fetching for symbols that are not standard SEC feed targets.
   - First pass should cover `ARM`, `BRK.B`, `TCEHY`, `NBIS`.

3. Improve market-data robustness for symbols that intermittently fail quote/bar fetches.

4. Re-evaluate thresholds after source-routing is added.
   - Right now the pipeline is cleaner, but still conservative enough that recent live runs sent `0` alerts.

5. Keep all config/reporting changes covered by tests.

## Useful Commands

Run tests:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

Write a fresh runtime config from the maintained template:

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-default-config
```

Sync the configured watchlist into SQLite:

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main sync-watchlist
```

Run one real live cycle:

```bash
SATELLITE_USE_SEC_FILINGS_SOURCE=1 SATELLITE_USE_GOOGLE_NEWS_SOURCE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --limit 10
```

Run continuous live monitoring:

```bash
SATELLITE_USE_SEC_FILINGS_SOURCE=1 SATELLITE_USE_GOOGLE_NEWS_SOURCE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main serve --limit 10
```

Open the most recent live review:

```bash
sed -n '1,160p' data/live_run/latest_live_review.md
```

## Environment Notes

- Do not install dependencies globally.
- Use the project-local environment only: `.venv`.
- `rg` is not usable in this environment; prefer `grep`, `find`, and `sed`.
- Sandbox runs may block outbound network access; real source validation may require escalated execution.

## Resume Instruction

When resuming work, start by reading this file and then continue with:

`Add per-symbol asset/source routing so ETFs, ADRs, non-US names, and standard US equities do not all use the same SEC/news policy.`
