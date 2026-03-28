# 卫星仓位 Agent 使用文档

最后更新：2026-03-28

这份文档只回答“怎么用”。  
如果你想看系统为什么这样设计，请看 [design_principles.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/design_principles.zh-CN.md)。  

当前 `docs/satellite_agent` 的长期主文档只有两份：

- [design_principles.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/design_principles.zh-CN.md)
- [usage_guide.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/usage_guide.zh-CN.md)

如果你想快速定位文档入口或查看短期交接信息，可以再看：

- [README.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/README.md)
- [HANDOFF.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)
- [satellite_agent_flow.html](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/satellite_agent_flow.html)

## 1. 推荐调用方式

默认在项目根目录执行：

```bash
cd /Users/chenxi/CodeSpace/asset_allocation_system
```

当前更稳的调用方式是：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main <command>
```

## 2. 运行命令

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

如果要用 replay 文件做一轮本地验证：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main run-once --workspace-dir ./data/satellite_agent/run_once --replay-path tests/fixtures/events.jsonl
```

## 3. 卡片预览

正式卡：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA
```

观察卡：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --watch
```

候选池轻提醒：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --candidate-optional-light
```

兼容旧参数：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --prewatch-light
```

持仓管理卡：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --exit-pool
```

## 4. 查看运行与策略结果

查看最近运行列表：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-runs --limit 10
```

查看某次运行详情：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-run --run-id <run_id>
```

查看历史策略摘要：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-strategy --days 30
```

## 5. 历史效果复盘

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

注意：

- 历史效果复盘默认是“单笔决策复盘”
- 它不负责做整条动作链的收益归因
- 如果你要排查状态机或通知行为，请看后面的“状态机审计”命令

## 6. 状态机审计 / 链路审计

查看当前仍处于活跃周期的标的：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-active-cycles --limit 20
```

如果要显式查看终态样本：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-active-cycles --status terminal --limit 20
```

把周期审计报告落盘到某个 workspace：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-cycle-audit --workspace-dir ./data/satellite_agent/serve --limit 50
```

查看某次运行涉及标的的周期审计摘要：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-run-cycle-audit --run-id <run_id> --limit 20
```

预演清理某次运行关联到的历史异常决策：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main cleanup-historical-cycle-anomalies --run-id <run_id>
```

真正执行历史异常清理：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main cleanup-historical-cycle-anomalies --run-id <run_id> --apply
```

说明：

- `report-active-cycles` 默认只看真正活跃的 `pending_entry / holding_active`
- `report-run-cycle-audit` 更适合排查“这次 run 有没有继续制造新异常”
- `cleanup-historical-cycle-anomalies` 默认是预演，只有加 `--apply` 才会真的改库

## 7. Replay / 赛马

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

常见用途：

- 并排比较规则版本、宏观覆盖层版本、LLM 特征版本
- 验证候选池、确认池、持仓管理规则的改动是否值得保留
- 回看某次参数调整后，收益、胜率、回撤、盈亏比是否一起变化

## 8. LLM 用量

查看最近 7 天用量：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-llm-usage --days 7
```

落盘 LLM 用量报告：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-llm-usage-report --workspace-dir ./data/satellite_agent/serve --days 7
```

## 9. 其他常用命令

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

## 10. 常看目录

- 代码目录：[src/satellite_agent](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent)
- 配置目录：[config/satellite_agent](/Users/chenxi/CodeSpace/asset_allocation_system/config/satellite_agent)
- 运行数据目录：[data/satellite_agent](/Users/chenxi/CodeSpace/asset_allocation_system/data/satellite_agent)
- `serve` 产物目录：[data/satellite_agent/serve](/Users/chenxi/CodeSpace/asset_allocation_system/data/satellite_agent/serve)
- 历史效果复盘主报告：[data/satellite_agent/serve/historical_effect/review.md](/Users/chenxi/CodeSpace/asset_allocation_system/data/satellite_agent/serve/historical_effect/review.md)
- 周期审计报告：[data/satellite_agent/serve/cycle_audit/cycle_audit.md](/Users/chenxi/CodeSpace/asset_allocation_system/data/satellite_agent/serve/cycle_audit/cycle_audit.md)
- LLM 用量报告：[data/satellite_agent/serve/llm_usage/report.md](/Users/chenxi/CodeSpace/asset_allocation_system/data/satellite_agent/serve/llm_usage/report.md)

## 11. 本地密钥约定

LLM key 不写进 `config/satellite_agent/agent.json`。

放在：

```bash
config/satellite_agent/.env.local
```

变量名：

```bash
SATELLITE_OPENAI_API_KEY
```
