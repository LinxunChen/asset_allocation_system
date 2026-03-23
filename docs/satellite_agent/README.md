# Satellite Agent

这是当前仓库里已经真正落地并持续运行的 agent。

## 你最常看的文档

- 交接文档：[HANDOFF.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)
- 决策链路说明：[decision_logic.zh-CN.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/decision_logic.zh-CN.md)
- 评分体系说明：[scoring_guide.zh-CN.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/scoring_guide.zh-CN.md)

## 代码、配置、数据目录

- 代码：[src/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent)
- 配置：[config/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent)
- 数据：[data/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent)

## 当前配置原则

- 你主要维护 `watchlist.stock_items / watchlist.etf_items`
- 标的展示名称来自 `name`
- 题材归属默认由系统内置题材目录自动维护
- `stock_groups / etf_groups / themes` 仅作为高级兼容能力保留，后续会逐步淡出

## 常用入口

```bash
satellite-agent daily-run --workspace-dir ./data/satellite_agent/daily_run --config-path ./config/satellite_agent/agent.recommended.json --replay-path tests/fixtures/events.jsonl
satellite-agent run-once --workspace-dir ./data/satellite_agent/run_once --replay-path tests/fixtures/events.jsonl
satellite-agent serve --workspace-dir ./data/satellite_agent/serve
```

## LLM 用量查看

查看最近 7 天的终端报告：

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-llm-usage --days 7
```

把 LLM 用量正式落盘到某个 workspace：

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-llm-usage-report --workspace-dir ./data/satellite_agent/serve --days 7
```

自动运行时的默认行为：

- `run-once`：每轮自动刷新 `LLM 用量报告`
- `daily-run`：每轮自动刷新 `LLM 用量报告`
- `serve`：按小时节流刷新 `LLM 用量报告`

## 策略赛马快速开始

先生成一份可编辑的赛马模板：

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-batch-replay-template --path ./config/satellite_agent/batch_replay.local.json
```

然后用这份 spec 跑离线赛马：

```bash
PYTHONPATH=src .venv/bin/python -m satellite_agent.main batch-replay --spec-path ./config/satellite_agent/batch_replay.local.json --output-dir ./data/satellite_agent/experiments/batch_runs
```

常见用途：

- `纯规则`、`规则 + LLM特征抽取`、`纯规则 + 宏观风险覆盖层`、`规则 + LLM特征抽取 + 宏观风险覆盖层` 并排比较
- 如果你想先验证“宏观覆盖层本身有没有帮助”，优先看：
  - `rules_only`
  - `rules_plus_macro_overlay`
- 统一看：
  - 平均真实收益
  - 胜率
  - 最大回撤
  - 盈亏比
  - `T+7 / T+14 / T+30`
- 输出推荐策略，但不会自动覆盖正式配置

## 运行产物位置

- `run-once`：[data/satellite_agent/run_once](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/run_once)
- `serve`：[data/satellite_agent/serve](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve)
- `daily-run`：[data/satellite_agent/daily_run](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/daily_run)
- 历史效果复盘主报告：[data/satellite_agent/serve/historical_effect/review.md](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/historical_effect/review.md)
- LLM 用量报告：[data/satellite_agent/serve/llm_usage/report.md](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/llm_usage/report.md)

## 当前定位

- 卫星仓位 agent 已进入可持续迭代阶段
- 文档、配置、运行产物都已尽量隔离在 `satellite_agent` 前缀下
- 后续如果引入核心仓位 agent，再考虑抽共享底层模块
