# Asset Allocation System

这个仓库现在先承载 `卫星仓位 agent`，后面还会继续加入 `核心仓位 agent`。为了避免两条线互相污染，文档入口已经按 agent 维度拆开。

## 当前文档入口

- 卫星仓位 agent 总览：[docs/satellite_agent/README.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/README.md)
- 卫星仓位交接文档：[docs/satellite_agent/HANDOFF.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)
- 核心仓位 agent 占位入口：[docs/core_agent/README.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/core_agent/README.md)

## 当前代码与运行目录

- 卫星 agent 代码：[src/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent)
- 卫星 agent 配置：[config/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent)
- 卫星 agent 运行产物：[data/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent)

## 当前配置原则

- 你主要维护卫星 agent 的 `watchlist`
- `theme` 由系统内部题材目录自动托管
- `group` 作为历史兼容结构保留，但会逐步从用户配置里淡出

## 当前状态

- 已正式落地：卫星仓位 agent
- 计划后续加入：核心仓位 agent
- 共享底层引擎目前仍以卫星 agent 为主实现，等核心 agent 开始开发后再抽共享模块

## 建议阅读顺序

1. 先看 [docs/satellite_agent/README.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/README.md)
2. 再看 [docs/satellite_agent/HANDOFF.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)
3. 需要理解策略细节时，再看：
   - [docs/satellite_agent/decision_logic.zh-CN.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/decision_logic.zh-CN.md)
   - [docs/satellite_agent/scoring_guide.zh-CN.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/scoring_guide.zh-CN.md)
