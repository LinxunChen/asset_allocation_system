# 卫星仓位 Agent 关键设计原则

最后更新：2026-03-28

这份文档不描述“某次实现细节”，而是沉淀当前系统长期应当保持的关键设计原则。

如果后续实现、文案、报表或数据结构与本文档冲突，应优先回到这里判断：

- 这是原则真的该改了
- 还是实现偏离了原则

相关文档：

- 导航页：[README.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/README.md)
- 使用文档：[usage_guide.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/usage_guide.zh-CN.md)
- 交接页：[HANDOFF.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/HANDOFF.md)
- 方案图：[satellite_agent_flow.html](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/satellite_agent_flow.html)

## 1. 系统定位

`satellite_agent` 的系统定位是：

`事件驱动 + 技术确认 + 分层提醒 + 持仓管理辅助`

它不是自动交易系统，也不是把所有判断都揉成一个黑盒评分器。

当前系统更强调三件事：

- 尽早发现值得研究的机会
- 在合适的结构位置给出更明确的动作建议
- 在已经进场后，把系统行为切换到持仓管理而不是重复喊入场

## 2. 三池分工必须清楚

当前对外语义以三池为准：

1. `candidate_pool`
   第一池，负责候选观察。
2. `confirmation`
   第二池，负责正式入场判断。
3. `holding_management`
   第三池，负责持仓管理和退出提示。

要求：

- 新代码、对外文案、报表口径默认使用上述命名
- `prewatch` 只允许作为兼容历史数据的旧别名存在
- 不应再继续把 `prewatch` 当成新的主口径扩散

## 3. 单笔决策复盘，不做整链收益归因

历史效果复盘的基本单位始终是“单笔决策”，不是“整条动作链”。

也就是说，复盘回答的是：

- 这张正式卡后来有没有成交
- 成交后结果如何
- 是止盈、失效退出，还是复盘窗口结算

而不是：

- 把 `观察 -> 试探建仓 -> 确认做多 -> 持仓 -> 退出` 当成一笔合成交易去评价
- 把多次动作拼成一条故事链再做收益归因

这样做的原因：

- 绩效评价口径更稳定
- 更容易回答“这次决策本身好不好”
- 可以避免链路拼接方式变化时把历史收益口径一起带歪

## 4. 状态机审计不等于绩效复盘

系统里允许存在“按 `symbol + cycle` 看链路”的能力，但它的职责必须限定为：

- 状态机审计
- 通知行为排查
- 历史异常定位

它不用于：

- 收益统计
- 策略优劣评分
- 替代单笔复盘

更直接地说：

- 单笔复盘回答“这次判断做得对不对”
- 状态机审计回答“系统流程有没有乱套”

两者可以同时存在，但不能混成同一种报表口径。

## 5. 活跃周期必须围绕“是否已进场”来定义

每个标的在任一时刻只应有一个当前活跃周期，状态限定为：

- `pending_entry`
  已发正式入场卡，但尚未成交，也尚未结束
- `holding_active`
  已成交，当前处于持仓中
- `terminal`
  当前周期已经结束

以下结果会终结当前周期，后续事件应视为新周期：

- `not_entered_price_invalidated`
- `not_entered_window_expired`
- `profit_protection_exit`
- `invalidation_exit`
- `window_close_evaluation`

## 6. 同一活跃周期内，动作不能反复横跳

### `holding_active`

一旦标的进入 `holding_active`：

- 不再发送新的入场类正式卡
- 不再发送新的降级观察卡
- 后续只允许进入第三池持仓管理

`is_breakthrough_event` 可以保留为未来接口，但默认不改变上述行为。

### `pending_entry`

一旦标的处于 `pending_entry`：

- 若新判断弱于上一张正式动作，不再发新的正式卡
- 应改为降级观察卡
- 这类观察卡必须明确提示“前次正式机会未进场，建议撤销此前挂单，等待新结构”

动作强弱排序固定为：

`确认做多 > 试探建仓 > 加入观察`

## 7. 用户卡片只展示当前判断，不讲历史故事

外部卡片的目标是帮助执行，不是完整展示内部推理痕迹。

因此：

- 正式卡只展示当前判断和当前执行计划
- 降级观察卡只展示当前状态调整
- 持仓管理卡只展示当前退出原因或管理建议
- `chain_summary` 仅保留给日志、报表和调试
- 不把 `A -> B -> C` 这样的历史动作链作为用户正文主字段

## 8. 对外观察次数只用近 72 小时口径

候选池相关的对外展示统一使用：

`近72h进入候选池 X 次`

要求：

- 不再对外展示累计 observation count
- 不再用累计扫描次数作为用户感知主口径
- 如果需要更长窗口统计，只留给内部报表或调试

## 9. 正式卡以“可执行”为核心，不追求信息堆叠

正式卡应优先服务执行，因此默认保留：

- 标题
- 顶部概览
- 执行计划
- 四个稳定板块：
  - 事件摘要
  - 市场与结构
  - 题材联动
  - 宏观环境

默认不应回到这些旧做法：

- 在正文大段展示动作链
- 重复堆叠相似解释
- 把内部评分拆解原样暴露给用户

## 10. 历史遗留异常不能污染当前判断

随着状态机逐步收口，历史库里可能存在旧行为残留，例如：

- 已经进入 `holding_active` 后，又留下新的正式入场决策

这类数据可以用于考古，但不应持续污染当前策略判断。

因此：

- 审计时应区分 `current_run` 和 `historical_carryover`
- 当前状态机验证优先看当前 run 是否继续制造新异常
- 历史脏样本可以隔离、标记，必要时可以清理

## 11. 新增设计前，优先判断会不会破坏上述边界

当后续要增加新能力时，优先检查这几个问题：

1. 会不会把单笔复盘重新拉回整链收益归因
2. 会不会让 `holding_active` 又重新放开新的入场类动作
3. 会不会把内部调试信息再次泄漏到用户卡片正文
4. 会不会继续扩散 `prewatch` 这种旧命名
5. 会不会让对外口径重新出现累计观察次数

如果答案是“会”，默认应停下来重新设计，而不是先实现再修。

## 12. 文档维护约定

这份文档用于维护“长期原则”，不是记录每次开发日志。

当前 `docs/satellite_agent` 的长期主文档只有两份：

- [design_principles.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/design_principles.zh-CN.md)
- [usage_guide.zh-CN.md](/Users/chenxi/CodeSpace/asset_allocation_system/docs/satellite_agent/usage_guide.zh-CN.md)

除此之外，可以存在少量辅助文件，例如：

- 导航页
- 短期交接页
- 方案图

适合写进这里的内容：

- 口径边界
- 状态机约束
- 三池职责
- 复盘与审计的职责分工
- 对外展示的稳定规范

不适合写进这里的内容：

- 某次 bug 修复过程
- 某个 run 的临时观察
- 具体开发待办
- 命令速查以外的用户使用说明

如果原则发生变化，应先改本文档，再去改实现和其他说明文档。
