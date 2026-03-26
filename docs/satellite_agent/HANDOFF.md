# Satellite Agent Handoff

Last updated: 2026-03-26 (Asia/Shanghai)

## 以最新代码为准的补充说明

这份 handoff 里关于“兑现池 v1 已落地”的主体判断仍然成立，但从最新代码看，当前开发重心已经继续往“复盘产品化”和“调参保护”推进，不能只把它理解成卡片文案阶段。

相对本文下方旧内容，当前代码里已经额外落地这些变化：

- 历史效果复盘默认加入 `近 30 天（调参）` vs `近 90 天（基准）` 双窗口对比
  - 对应文件：
    - `src/satellite_agent/main.py`
    - `src/satellite_agent/reporting.py`
- 复盘新增 `路径质量 / 浮盈浮亏摘要`
  - 不只看已退出收益，也看已成交样本的：
    - `最大浮盈`
    - `最大回撤`
    - `T+7 / T+14 / T+30` 锁定收益
    - 代表性 `Top3` 样本
  - 并对重复代表样本做去重，避免同一路径重复污染判断
- 复盘新增样本量保护
  - 近 30 天样本不足时，不再直接给“调参数”建议
  - 会明确降级成“先观察，不建议直接调整参数”
- 预备池链路的后验展示继续补强
  - 新增 `观察后表现` 区块
  - 可以看：
    - 观察提醒转化
    - 观察到确认的平均耗时
    - 未经过提醒直接升池的样本
    - 仍在观察中的标的数
- 历史效果复盘与模拟闭环表述进一步统一
  - `未进场` 已改成更准确的 `未成交`
  - `模拟进场` 相关表述统一收敛为 `模拟成交`
- 运行产物增加外网异常提示
  - 若 `sec_edgar` 等来源是典型网络连通性问题，运行复盘和 source health 不再直接暴露原始报错
  - 会改成更面向使用者的提示：先检查 `VPN / 当前网络`

本轮相关改动主要集中在：

- `src/satellite_agent/main.py`
- `src/satellite_agent/reporting.py`
- `src/satellite_agent/notifier.py`
- `src/satellite_agent/outcomes.py`
- `src/satellite_agent/store.py`
- `tests/test_reporting.py`
- `tests/test_outcomes.py`
- `tests/test_notifier.py`

如果你要继续开发，请优先把当前阶段理解为：

- `预备池 -> 确认池 -> 兑现池 -> 后验复盘` 的主闭环已经连上
- 当前更值得投入的是：
  - 复盘输出是否真的支持调参判断
  - 观察链路是否能解释“为什么升池 / 为什么没升池”
  - 真实样本下的新止盈区和兑现池规则是否偏保守或偏激进

## 当前目标

当前卫星仓位 agent 的主线目标已经从“基础跑通”切换到“交易闭环完善”。  
这条线当前最重要的事情是：

- 把 `预备池 -> 确认池 -> 兑现池 -> 后验复盘` 真正串成一套一致口径
- 把卡片内容和 LLM 表达继续打磨成可日常使用的交易助手
- 在不引入组合级仓位账本的前提下，让真实执行和后验回测尽量一致

当前默认目录与入口：

- 代码：[src/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent)
- 配置：[config/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent)
- 数据：[data/satellite_agent](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent)

## 已完成

### 1. 目录、配置、文档已按 satellite_agent 隔离

- 运行、配置、实验、文档都已经收进 `satellite_agent` 命名空间
- 用户配置已经收成 `watchlist-only`
  - 你只维护 `watchlist.stock_items / etf_items`
  - `theme` 由系统内部托管
  - `group` 已从用户配置淡出
- 题材映射已支持导出为只读参考文件：
  - [config/satellite_agent/theme_reference.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/theme_reference.json)

### 2. 推送卡片已经完成第一轮重构

当前卡片已经区分为：

- 预备池观察卡
- 正式操作卡
- 兑现池卡

并且已经做了这些收敛：

- 标题统一为 `名称（代码） | 动作建议 | 事件类型`
- `trend_state` 对外显示为 `结构状态`
- `bias` 对外显示为 `事件倾向`
- `事件 / 市场 / 结构 / 信号评分 / 决策结论 / 执行计划` 已做基本分区
- 预备池轻推已经接入 lite 版 LLM narration
- 预备池重复轻推已做抑制：
  - `4h` 硬冷却
  - `12h` 内内容不变且分数变化小于 `4` 时不重复发

本地预览命令可用：

```bash
cd /Users/linxun/CodeSpace/asset_allocation_system
PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA
PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --watch
PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NBIS --prewatch-light
```

### 3. Qwen narration 已接入并可本地持久化

当前模型配置：

- `model = Qwen/Qwen3.5-35B-A3B`
- `base_url = https://api.siliconflow.cn/v1/chat/completions`

配置边界：

- 非敏感行为配置放在：
  - [config/satellite_agent/agent.json](/Users/linxun/CodeSpace/asset_allocation_system/config/satellite_agent/agent.json)
- API key 放在：
  - `config/satellite_agent/.env.local`
  - 变量名固定为 `SATELLITE_OPENAI_API_KEY`
  - 不写进 `config/satellite_agent/agent.json`
  - 已被 `.gitignore` 忽略

当前默认口径：

- `LLM narration = on`
- `LLM event extraction = off`
- `LLM ranking assist = off`

也就是说，LLM 现在主要只影响：

- 事实摘要
- 影响推理
- 决策理由
- 风险提示

不会改变：

- 打分
- 动作建议
- 价格计划

### 4. LLM 用量报告已落地

已支持：

- 每次调用落库
- 统计调用次数、成功/失败、token、延迟
- 生成中文报告
- 自动写入运行产物

可用命令：

```bash
cd /Users/linxun/CodeSpace/asset_allocation_system
PYTHONPATH=src .venv/bin/python -m satellite_agent.main report-llm-usage --days 7
PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-llm-usage-report --workspace-dir ./data/satellite_agent/serve --days 7
```

产物路径：

- [data/satellite_agent/serve/llm_usage/report.md](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/llm_usage/report.md)
- [data/satellite_agent/serve/llm_usage/report_payload.json](/Users/linxun/CodeSpace/asset_allocation_system/data/satellite_agent/serve/llm_usage/report_payload.json)

运行复盘里也已经嵌入 `LLM 用量摘要`。

### 5. 历史效果复盘体系已成型

当前已经有：

- 运行过程复盘
- 历史效果复盘
- 程序抽检
- AI 复核
- LLM 用量报告

并且：

- `serve` 下历史效果复盘按小时节流刷新
- `run-once / daily-run` 会自动更新相关产物
- 默认滚动主报告路径：
  - `data/satellite_agent/serve/historical_effect/review.md`
- 自定义窗口复盘会写到：
  - `data/satellite_agent/serve/historical_effect/windows/<start>_to_<end>/review.md`
- 活的月报会写到：
  - `data/satellite_agent/serve/historical_effect/monthly/YYYY-MM/review.md`

### 6. 兑现池与止盈区重构 v1 已完成

这是本轮最大的新增功能。

#### 6.1 新的止盈区逻辑

止盈区不再主要靠 `ATR 静态目标`，而是改成：

- `1R = entry_reference - invalidation_level`
- `swing` 理论目标：
  - `2.0R ~ 3.0R`
- `position` 理论目标：
  - `2.5R ~ 4.0R`
- 再用结构阻力修正：
  - `swing -> resistance_20`
  - `position -> resistance_60`
- 再做最低赔率保护：
  - `swing >= 1.5R`
  - `position >= 2.0R`

如果赔率不合格：

- 不强推确认池
- 会降级为观察/预备池处理

对应文件：

- [src/satellite_agent/entry_exit.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/entry_exit.py)

#### 6.2 新的兑现池口径

盈利退出底层已经统一到：

- `exit_reason = exit_pool`

并带子原因：

- `target_hit`
  - 达标止盈
- `weakening_after_tp_zone`
  - 提前锁盈
- `macro_protection`
  - 宏观保护

兑现池第一版规则：

- 仅面向已形成正向运行的多头票
- 达到止盈区中位价：
  - 记 `target_hit`
- 已进入止盈区后，连续两天收盘跌回下沿之下：
  - 记 `weakening_after_tp_zone`
- 宏观风险高且已有至少 `1 ATR` 浮盈：
  - 记 `macro_protection`

同 bar 冲突时：

- `失效退出` 仍优先于盈利退出

对应文件：

- [src/satellite_agent/outcomes.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/outcomes.py)
- [src/satellite_agent/service.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/service.py)
- [src/satellite_agent/notifier.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/notifier.py)
- [src/satellite_agent/reporting.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/reporting.py)
- [src/satellite_agent/store.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/store.py)
- [src/satellite_agent/main.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/main.py)

#### 6.3 兑现池卡与运行链路

当前已经支持：

- 运行时识别兑现池退出
- 生成兑现池卡
- 落决策记录
- 进入运行复盘摘要
- 历史效果复盘展示为：
  - `兑现池退出（含达标止盈 / 提前锁盈 / 宏观保护）`

## 最近验证

本轮我实际补跑并确认通过的核心测试有：

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_outcomes tests.test_scoring_service
PYTHONPATH=src .venv/bin/python -m unittest tests.test_reporting tests.test_notifier
```

结果：

- `tests.test_outcomes + tests.test_scoring_service`
  - `40 tests OK`
- `tests.test_reporting + tests.test_notifier`
  - `48 tests OK`

说明当前这几条主链路是绿的：

- 兑现池退出与后验重算
- 新止盈区间与赔率降级
- 预备池/确认池/兑现池卡片展示
- 历史效果复盘与运行复盘展示

## 下一步建议

下一阶段优先级我建议这样排：

### 1. 继续打磨兑现池展示

当前兑现池功能已接通，但产品层还可以继续优化：

- 兑现池卡文案再收一轮
- `serve_review.md` 里的兑现池摘要做得更醒目
- 让 `达标止盈 / 提前锁盈 / 宏观保护` 三类原因在卡片里更好理解

### 2. 用真实样本审新止盈区是否偏保守/偏激进

新止盈区和兑现池已经能跑，但是否“专业且好用”，还需要用真实历史样本再看：

- 哪些票被过早降级
- 哪些票的目标区仍偏近
- 哪些票的 `position` 目标还不够像卫星仓位

这一步建议优先做“样本审查”，不要先急着再改公式。

### 3. 再推进 LLM 特征抽取

当前 LLM 主要还在表达层。  
后面真正影响策略质量的下一条大线是：

- 让 LLM 作为特征提取器
- 输出结构化事件特征
- 再进入 `event_score`
- 然后用 batch replay 做权重回测

### 4. 宏观风险覆盖层继续回测

当前宏观覆盖层 v1 已接进主链路，但还需要验证：

- 它到底是在帮你避坑
- 还是只是让系统更保守

建议后面把：

- `纯规则`
- `纯规则 + 宏观覆盖层`

先稳定赛马比较。

## 风险与阻塞点

### 1. 兑现池逻辑已落地，但仍是 v1

当前是第一版规则化兑现池，优点是：

- 可回测
- 可解释
- 和真实执行口径一致

但还没有：

- 分批兑现
- 组合级仓位账本
- 更细的持仓管理逻辑

所以这版更适合：

- 单信号级别的盈利管理

还不适合：

- 真实组合仓位调度

### 2. 新止盈区间会改变部分老样本行为

这不是 bug，而是预期内结果。  
新的 `R multiple + 阻力修正 + 最低赔率` 会让一部分以前能进确认池的票，现在降级为观察。

这会带来：

- 某些旧测试样本行为变化
- 某些旧策略直觉被纠正

当前已经把核心回归收绿，但后面仍建议多看真实样本。

### 3. LLM 仍未进入正式打分层

当前 LLM 主要解决的是：

- 卡片可读性

还没有真正进入：

- `event_score`
- `ranking`
- 策略赛马主比较组

所以当前的“策略质量提升”主要还来自：

- 止盈区
- 兑现池
- 宏观覆盖

而不是 LLM 本身。

### 4. 赛马框架已具备，但还不是当前第一优先级

当前 batch replay、模板和中文报告都已具备基础能力。  
但在兑现池和止盈区刚重构完的阶段，我建议先别急着大规模赛马，而是：

- 先审真实样本
- 再做下一轮策略比较

## 快速命令

### 运行 serve

```bash
cd /Users/linxun/CodeSpace/asset_allocation_system
PYTHONPATH=src .venv/bin/python -m satellite_agent.main serve --workspace-dir ./data/satellite_agent/serve
```

### 预览正式卡 / 观察卡 / 预备池轻推

```bash
cd /Users/linxun/CodeSpace/asset_allocation_system
PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA
PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NVDA --watch
PYTHONPATH=src .venv/bin/python -m satellite_agent.main preview-alert-render --symbol NBIS --prewatch-light
```

### 生成 LLM 用量报告

```bash
cd /Users/linxun/CodeSpace/asset_allocation_system
PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-llm-usage-report --workspace-dir ./data/satellite_agent/serve --days 7
```

### 同步 watchlist 与导出题材参考

```bash
cd /Users/linxun/CodeSpace/asset_allocation_system
PYTHONPATH=src .venv/bin/python -m satellite_agent.main sync-watchlist
PYTHONPATH=src .venv/bin/python -m satellite_agent.main write-theme-reference --path ./config/satellite_agent/theme_reference.json
```

## 本次涉及的主要文件

- [src/satellite_agent/entry_exit.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/entry_exit.py)
- [src/satellite_agent/main.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/main.py)
- [src/satellite_agent/models.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/models.py)
- [src/satellite_agent/notifier.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/notifier.py)
- [src/satellite_agent/outcomes.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/outcomes.py)
- [src/satellite_agent/reporting.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/reporting.py)
- [src/satellite_agent/service.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/service.py)
- [src/satellite_agent/store.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/store.py)
- [tests/test_outcomes.py](/Users/linxun/CodeSpace/asset_allocation_system/tests/test_outcomes.py)
- [tests/test_reporting.py](/Users/linxun/CodeSpace/asset_allocation_system/tests/test_reporting.py)
- [tests/test_scoring_service.py](/Users/linxun/CodeSpace/asset_allocation_system/tests/test_scoring_service.py)
