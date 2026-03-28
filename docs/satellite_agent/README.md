# 卫星仓位 Agent

最后更新：2026-03-28

`satellite_agent` 是一个面向股票与 ETF 观察池的机会发现与执行辅助系统。

它的目标不是自动下单，而是把“关注名单里的事件、结构和风险”整理成一条更清晰的决策链，帮助使用者完成三件事：

- 更早发现值得继续跟踪的机会
- 在结构和催化同时到位时给出正式动作建议
- 在已经进场后，把判断切换到持仓管理和退出提醒

## 它解决什么问题

很多资讯系统只能做到“把新闻推过来”，但很难回答这些更接近真实交易的问题：

- 这条消息到底值不值得继续跟踪
- 现在只是先观察，还是已经可以正式出手
- 已经进场后，后面应该继续持有、保护利润，还是退出

`satellite_agent` 的做法不是把所有内容混成一个黑盒分数，而是把决策拆成三池流转和一个后验复盘闭环。

## 主链路

系统的对外主骨架可以概括成：

1. 在关注名单内持续发现事件和扫描行情
2. 做归池判断
3. 进入三池中的某一层
4. 对正式机会做成交后的持仓管理
5. 把整个过程写入复盘与审计

三池分工是：

- 第一池 `candidate_pool`
  候选池。值得继续跟踪，但暂时不适合正式出手。
- 第二池 `confirmation`
  确认池。消息和盘面都支持，适合给出正式机会卡。
- 第三池 `holding_management`
  持仓管理。已经成交后，统一处理止盈、失效、窗口到期等退出逻辑。

如果你想看更直观的系统骨架图，直接打开：

- [satellite_agent_flow.html](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/satellite_agent_flow.html)

## 系统输出什么

当前系统会持续产出这些东西：

- 候选池记录
- 正式机会卡
- 持仓管理 / 退出卡
- 运行复盘
- 历史效果复盘
- 状态机审计

其中要特别说明两点：

- 历史效果复盘默认是“单笔决策复盘”，不是把整条动作链揉成一笔收益故事
- 状态机审计用于排查系统流程是否正确，不直接参与策略绩效评价

## 它不做什么

当前版本不直接负责：

- 自动下单
- 真实账户仓位账本
- 全市场无约束扫描
- 用单一黑盒模型完全替代人工理解

它更像一个“围绕关注名单持续工作的交易助手”，而不是全自动交易系统。

## 当前项目重点

当前代码主线已经收口到：

- 三池闭环
- 活跃周期状态机
- 单笔决策复盘
- 状态机审计

接下来更值得继续投入的方向主要是：

- 用真实样本验证状态机边界
- 校准第三池退出规则
- 降低候选池噪声

## 从哪里继续看

如果你是第一次看这个项目，建议顺序是：

1. 先看这份 [README.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/README.md)
2. 再看 [satellite_agent_flow.html](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/satellite_agent_flow.html)
3. 想理解长期边界时，看 [design_principles.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/design_principles.zh-CN.md)
4. 想实际运行或排查时，看 [usage_guide.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/usage_guide.zh-CN.md)
5. 想接手当前阶段任务时，看 [HANDOFF.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)

## 代码、配置、数据目录

- 代码：[src/satellite_agent](/Users/chenxi/CodeSpace/asset_allocation_system/src/satellite_agent)
- 配置：[config/satellite_agent](/Users/chenxi/CodeSpace/asset_allocation_system/config/satellite_agent)
- 数据：[data/satellite_agent](/Users/chenxi/CodeSpace/asset_allocation_system/data/satellite_agent)
