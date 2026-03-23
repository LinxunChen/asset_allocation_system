# Workspace Handoff

当前仓库已经按 agent 维度拆分文档入口，避免后续 `satellite_agent` 和 `core_agent` 混在同一份交接文档里。

## 交接入口

- 卫星仓位 agent：[docs/satellite_agent/HANDOFF.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)
- 核心仓位 agent（待开发）：[docs/core_agent/README.md](/Users/linxun/CodeSpace/asset_allocation_system/docs/core_agent/README.md)

## 当前建议

- 卫星仓位 agent 的持续开发、复盘、运行方式，都以 `docs/satellite_agent/HANDOFF.md` 为准
- 核心仓位 agent 启动开发后，再单独新增 `docs/core_agent/HANDOFF.md`
- 共享底层引擎是否抽离，等核心 agent 真正开始接入后再做，不在当前阶段提前过度设计
