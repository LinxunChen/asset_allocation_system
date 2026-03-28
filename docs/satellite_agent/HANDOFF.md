# 卫星仓位 Agent 交接说明

最后更新：2026-03-28

这份文档只服务短期交接。  
如果这里和代码冲突，以最新代码为准。  
长期原则请看 [design_principles.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/design_principles.zh-CN.md)，命令和使用方式请看 [usage_guide.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/usage_guide.zh-CN.md)。

## 当前代码现状

当前主链路已经不是“候选提醒实验品”，而是一个基本闭环：

- 事件抓取与去重
- 第一池 `candidate_pool`
- 第二池 `confirmation`
- 第三池 `holding_management`
- 运行复盘
- 历史效果复盘
- 状态机审计

其中最关键的新收口，不在卡片样式，而在状态机和审计能力。

## 当前已稳定下来的几条规则

### 1. 活跃周期状态机

每个标的当前只允许处于以下三种状态之一：

- `pending_entry`
- `holding_active`
- `terminal`

核心约束：

- `holding_active` 后不允许再发新的入场类正式卡
- `pending_entry` 下如果判断弱于上一张正式卡，应降级为观察卡，而不是继续发正式卡
- terminal outcome 写入后，后续事件才视为新周期

### 2. 对外展示口径

- 候选池统一叫 `candidate_pool`
- 对外观察次数统一用 `近72h进入候选池 X 次`
- 外部卡片不再展示历史动作箭头链

### 3. 审计能力

当前已经补了周期审计底座，可以排查：

- 某只票当前处于什么状态
- 为什么被 suppress
- 为什么降级为观察卡
- terminal 后是否正确开了新周期
- 某次 run 有没有制造新的状态机异常

## 当前最值得继续推进的方向

优先级建议如下：

1. 用真实样本继续验证状态机边界
2. 校准第三池退出规则
3. 继续压候选池噪声
4. 再考虑更复杂的算法增强

更具体地说，接下来最值钱的是：

- replay 验证 `pending_entry -> 降级观察 -> terminal -> new cycle`
- 验证 `holding_active` 期间是否还会漏出新的正式入场卡
- 校准 `profit_protection_exit / invalidation_exit / window_close_evaluation`

## 关键代码入口

- 主链路：[service.py](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent/service.py)
- 周期审计：[cycle_audit.py](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent/cycle_audit.py)
- CLI 入口：[main.py](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent/main.py)
- 后验结果：[outcomes.py](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent/outcomes.py)
- 报表输出：[reporting.py](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent/reporting.py)
- 存储层：[store.py](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent/store.py)

## 常用入口

常用命令已经集中在 [usage_guide.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/usage_guide.zh-CN.md)。  
如果只是快速排查当前状态，优先看这些：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-active-cycles --limit 20
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-run-cycle-audit --run-id <run_id> --limit 20
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-performance-review --workspace-dir ./data/satellite_agent/serve
```

## 交接提醒

- 这份文档不负责长期设计说明
- 如果这里和最新实现不一致，优先相信代码和测试
- 如果后续阶段目标变了，先改这份文档，再决定要不要把变化沉淀进长期原则文档
