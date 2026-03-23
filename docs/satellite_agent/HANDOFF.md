# Satellite Agent Handoff

Last updated: 2026-03-20 (Asia/Shanghai)

## Current State

`satellite_agent` has moved past a single-chain prototype and is now in the first phase of a modular decision-system refactor.

The execution framework is still:

- `预备池 -> 确认池 -> 兑现池 -> 飞书推送`

But the underlying decision layer is no longer only ad-hoc logic inside `service.py`.  
We now have a first version of an internal decision-engine layer:

- `资讯事件理解引擎`
- `股市行情理解引擎`
- `产业题材理解引擎`

These live under:

- [src/satellite_agent/decision_engines/__init__.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/__init__.py)
- [src/satellite_agent/decision_engines/types.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/types.py)
- [src/satellite_agent/decision_engines/event.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/event.py)
- [src/satellite_agent/decision_engines/market.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/market.py)
- [src/satellite_agent/decision_engines/theme.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/theme.py)
- [src/satellite_agent/decision_engines/mappers.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/mappers.py)

The current refactor status is:

- `service.py` now orchestrates the engines instead of directly doing all low-level reasoning itself.
- `DecisionPacket` is now part of the real flow.
- `DecisionPacket -> OpportunityCard` mapping exists.
- `DecisionPacket -> delivery view` mapping now exists and is reused by notifier + live diagnostics.
- `decision_records` are now persisted.
- `decision_outcomes` schema exists and is usable.
- A first outcome backfill command now exists.

## New Decision-Layer Persistence

SQLite now includes:

- `decision_records`
- `decision_outcomes`

Implemented in:

- [src/satellite_agent/store.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/store.py)
- [src/satellite_agent/outcomes.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/outcomes.py)

What is already working:

- Each `DecisionPacket` can be written into `decision_records`
- `decision_diagnostics` are now included in replay/live payloads
- Markdown review now includes a `决策记录` section
- A CLI command can backfill outcomes from stored daily bars
- `card_diagnostics` now carry normalized display fields generated from a shared delivery-view mapper

Backfill command:

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main backfill-decision-outcomes --run-id <run_id>
```

## Most Recent Validation

### Test status

Full suite:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

Result:

- `88/88 OK`

Targeted new tests added:

- [tests/test_outcomes.py](/Users/linxun/CodeSpace/asset_allocation_system/tests/test_outcomes.py)
- updated [tests/test_reporting.py](/Users/linxun/CodeSpace/asset_allocation_system/tests/test_reporting.py)

### Latest real run

Command used:

```bash
SATELLITE_USE_SEC_FILINGS_SOURCE=1 SATELLITE_USE_GOOGLE_NEWS_SOURCE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --limit 10
```

Latest run:

- run id: `e403bf124dc1798b`
- elapsed: `96.6s`
- events processed: `32`
- cards generated: `64`
- alerts sent: `4`
- prewatch light alerts sent: `1`
- run-once review: [data/satellite_agent/run_once/run_once_review.md](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/run_once/run_once_review.md)
- run-once payload: [data/satellite_agent/run_once/run_once_payload.json](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/run_once/run_once_payload.json)
- serve review: [data/satellite_agent/serve/serve_review.md](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/serve_review.md)
- serve payload: [data/satellite_agent/serve/serve_payload.json](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/serve_payload.json)
- serve llm usage report: [data/satellite_agent/serve/llm_usage/report.md](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/llm_usage/report.md)

That payload now contains:

- `card_diagnostics`
- `decision_diagnostics`
- `prewatch_candidates`
- normalized delivery-view fields inside `card_diagnostics`, including:
  - `identity`
  - `event_type_display`
  - `priority_display`
  - `horizon_display`
  - `action_label_effective`
  - `confidence_label_effective`
  - `source_summary`
  - `event_reason_line`
  - `market_reason_line`
  - `theme_reason_line`
  - `valid_until_text`

Payload sanity check completed:

- `decision_diagnostics` count for latest run: `67`

### Latest outcome backfill

Most recently verified backfill command:

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main backfill-decision-outcomes --run-id 82cc976bda5c75c8
```

Result:

- scanned: `6`
- updated: `0`
- skipped: `6`

Important interpretation:

- The outcome pipeline itself is working
- This specific run had insufficient usable local `1d` bars for those decisions, so all were skipped safely
- No crash, no corrupted state

## What Has Been Improved Recently

Recent major milestones already in the codebase:

- Chinese-first review and card copy
- formalized config/template structure:
  - [config/satellite_agent/agent.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/agent.json)
  - [config/satellite_agent/agent.template.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/agent.template.json)
- configuration direction now favors `watchlist-only` maintenance:
  - user maintains `stock_items / etf_items`
  - system hosts theme mapping internally
  - legacy `groups` remain compatible but are no longer the preferred user-facing model
- auto watchlist sync during runtime
- prewatch pool with:
  - structural prewatch
  - event-driven prewatch
  - theme memory support
- confirmation-pool theme linkage
- Feishu notification chain fully working
- LLM budget accounting and usage persistence
- `report-llm-usage / write-llm-usage-report` 已可用，且 `run-once / daily-run / serve` 会自动产出 LLM 用量报告
- Yahoo + Stooq + stale-cache market-data fallback
- source-fetch probe / degraded-fetch resilience
- notifier now reads unified delivery-view fields instead of hand-assembling most display semantics
- live card diagnostics now expose the same delivery-view semantics used by notifier

## Known Limits / Open Work

### 1. `DecisionPacket` is not yet the sole source of truth

Current state:

- `DecisionPacket` exists
- `decision_records` exist
- `OpportunityCard` is still the dominant delivery object
- notifier and live diagnostics now reuse shared delivery-view mapping

What remains:

- reporting still mostly formats raw card-shaped data directly
- `DecisionPacket` is not yet the single input for md/feishu/review generation
- the system is not yet fully `DecisionPacket-first`

### 2. `decision_outcomes` need better bar coverage

Current state:

- outcome backfill works
- but result quality depends on local `1d` bars already being in SQLite

What remains:

- either improve historical bar availability
- or add a safe bar-sync/backfill helper before outcome calculation

### 3. LLM is still limited

Current state:

- LLM budgets, fallback, and usage persistence are in place
- event extraction can already use LLM conditionally

What remains:

- formalize `LLM v1` around:
  - event understanding
  - theme reasoning
  - decision-reason generation
- keep pricing/risk plans rule-based

### 4. Dynamic theme discovery is still partial

Current state:

- theme decisions use static config + recent theme memory + linkage

What remains:

- promote theme understanding from “static group aware” to:
  - `watchlist + sector/theme ETF driven dynamic state`

### 5. Exit pool is still mostly an interface placeholder

Current state:

- exit-related fields now have a place in the market understanding plan
- outcomes/backfill groundwork exists

What remains:

- real `兑现池` decision logic
- later, likely with holdings awareness

## Recommended Next Task

The most natural next step is:

`Push reporting further onto the shared delivery-view / DecisionPacket path, and reduce remaining direct OpportunityCard coupling in markdown generation.`

That should include:

1. reuse the shared delivery-view mapper in `reporting.py`
2. make md review sections rely less on raw card fields and more on normalized delivery fields
3. continue shrinking ad-hoc display logic in `service.py` / `main.py`
4. keep `OpportunityCard` as a downstream transport/view model, not a reasoning model

After that, the next high-value step is:

`Improve outcome usefulness by ensuring sufficient daily bars exist for post-decision backfill.`

## Files Most Recently Changed

- [src/satellite_agent/decision_engines/mappers.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/decision_engines/mappers.py)
- [src/satellite_agent/notifier.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/notifier.py)
- [src/satellite_agent/main.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/main.py)

## Last Completed Work Item

Completed in this thread:

- centralized delivery-view mapping for notifier and live diagnostics
- validated with:
  - targeted tests:
    - `PYTHONPATH=src .venv/bin/python -m unittest tests.test_notifier tests.test_reporting tests.test_outcomes -v`
  - full suite:
    - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v`
  - real live run:
    - `SATELLITE_USE_SEC_FILINGS_SOURCE=1 SATELLITE_USE_GOOGLE_NEWS_SOURCE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --limit 10`

## Useful Commands

Run all tests:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

Run one live cycle:

```bash
SATELLITE_USE_SEC_FILINGS_SOURCE=1 SATELLITE_USE_GOOGLE_NEWS_SOURCE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --limit 10
```

Run continuous monitoring:

```bash
SATELLITE_USE_SEC_FILINGS_SOURCE=1 SATELLITE_USE_GOOGLE_NEWS_SOURCE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main serve --limit 10
```

Backfill outcomes for a run:

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main backfill-decision-outcomes --run-id <run_id>
```

Open latest review:

```bash
sed -n '1,220p' data/satellite_agent/run_once/run_once_review.md
```

Inspect latest payload keys:

```bash
python3 - <<'PY'
import json
from pathlib import Path
obj = json.loads(Path('data/satellite_agent/run_once/run_once_payload.json').read_text())
print(sorted(obj.keys()))
PY
```

## Environment Notes

- Use only the project-local environment: `.venv`
- Do not install dependencies globally
- `rg` is not usable in this environment (`bad CPU type`); prefer `grep`, `find`, `sed`, or short Python one-liners
- Network-requiring validations still depend on runtime environment; sandboxed runs may behave differently

## Resume Instruction

When resuming, read this file first, then continue with:

`Refactor notifier/reporting to consume DecisionPacket-derived diagnostics more directly, and reduce remaining direct OpportunityCard coupling in the decision path.`
