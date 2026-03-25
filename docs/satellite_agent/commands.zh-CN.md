# Satellite Agent 常用命令速查

这份文档只整理“日常最常用、最容易忘”的命令。

默认都在项目根目录执行：

```bash
cd /Users/chenxi/CodeSpace/asset_allocation_system
```

当前更稳的调用方式是：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main <command>
```

## 运行类

手动跑一轮：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --workspace-dir ./data/satellite_agent/run_once
```

常驻运行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main serve --workspace-dir ./data/satellite_agent/serve
```

日常运行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main daily-run --workspace-dir ./data/satellite_agent/daily_run
```

如果要用 replay 文件测试一轮：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --workspace-dir ./data/satellite_agent/run_once --replay-path tests/fixtures/events.jsonl
```

## 卡片预览

正式卡：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA
```

观察卡：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --watch
```

观察提醒：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --prewatch-light
```

兑现池卡：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --exit-pool
```

## 历史效果复盘

默认滚动近 30 天复盘：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-performance-review --workspace-dir ./data/satellite_agent/serve
```

自定义窗口复盘：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-performance-review --workspace-dir ./data/satellite_agent/serve --start-date 2026-03-01 --end-date 2026-03-14
```

活的月报：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-performance-review --workspace-dir ./data/satellite_agent/serve --month 2026-03
```

输出位置约定：

- 滚动主报告：`data/satellite_agent/serve/historical_effect/review.md`
- 自定义窗口：`data/satellite_agent/serve/historical_effect/windows/<start>_to_<end>/review.md`
- 活的月报：`data/satellite_agent/serve/historical_effect/monthly/YYYY-MM/review.md`

## LLM 用量

查看最近 7 天用量：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-llm-usage --days 7
```

落盘 LLM 用量报告：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-llm-usage-report --workspace-dir ./data/satellite_agent/serve --days 7
```

## 策略和结果查看

查看历史策略摘要：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-strategy --days 30
```

查看某次运行详情：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-run --run-id <run_id>
```

查看最近运行列表：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-runs --limit 10
```

## 赛马 / 回放

单次 replay 评估：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main replay-evaluate --replay-path tests/fixtures/events.jsonl --days 14
```

生成 batch replay 模板：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-batch-replay-template --path ./config/satellite_agent/batch_replay.local.json
```

运行 batch replay：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main batch-replay --spec-path ./config/satellite_agent/batch_replay.local.json --output-dir ./data/satellite_agent/experiments/batch_runs
```

## 其他常用

查看 watchlist 配置：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-watchlist-config
```

查看题材参考：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-theme-reference
```

生成 demo flow：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main demo-flow --workspace-dir ./data/satellite_agent/experiments/demo_flow
```

## 本地密钥约定

LLM key 不写进 `config/satellite_agent/agent.json`。

放在：

```bash
config/satellite_agent/.env.local
```

变量名：

```bash
SATELLITE_OPENAI_API_KEY
```
