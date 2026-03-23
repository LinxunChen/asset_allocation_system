# 评分体系说明

本文档说明 `satellite_agent` 当前版本的评分体系。目标是用尽量透明、可解释、可调试的方式，把一条资讯事件转换成一张机会卡片。

当前实现不是机器学习训练出来的黑盒模型，而是“规则抽取 + 加权打分”的工程化方案。这样做的好处是：

- 可以解释每一分是怎么来的
- 可以明确知道调高某个阈值会发生什么
- 适合当前 MVP 阶段做 replay、回测和参数实验

相关代码入口：

- 事件抽取：[llm.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/llm.py)
- 评分逻辑：[scoring.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/scoring.py)
- 指标计算：[indicators.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/indicators.py)
- 默认阈值：[config.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/config.py)
- 数据结构：[models.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/models.py)

## 一、总体流程

一条事件进入系统后，会经历这几个阶段：

1. `SourceEvent`
   来自新闻、SEC、研报摘要或 replay 文件的原始事件。
2. `EventInsight`
   系统对事件的结构化理解，包括事件类型、重要性、可信度、新颖度、情绪等。
3. `IndicatorSnapshot`
   当前价格和技术面的快照，包括 RSI、ATR、均线、相对量能、支撑阻力等。
4. `OpportunityCard`
   把事件层和市场层合并后，形成最终机会卡片。

系统的三个核心分数是：

- `event_score`
- `market_score`
- `final_score`

它们分别回答不同问题：

- `event_score`：这条消息本身值不值得重视
- `market_score`：当前价格位置和技术状态能不能做
- `final_score`：综合后这张卡片整体有多强

## 二、事件层分数：event_score

### 1. 输入字段

`event_score` 来自 `EventInsight`，核心字段如下：

- `importance`
- `source_credibility`
- `novelty`
- `sentiment`
- `theme_relevance`

这些字段定义在 [models.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/models.py)。

### 2. 这些字段怎么来

有两条路径：

1. 配置了 OpenAI API
   大模型会按约定返回结构化 JSON。
2. 没配置 OpenAI API
   使用规则提取器 `RuleBasedExtractor`。

规则提取器的当前逻辑在 [llm.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/llm.py)：

- 如果文本像 `earnings/guidance/sec/m&a`，`importance` 会更高
- 如果来源是 `filing/earnings/press_release`，`source_credibility` 会更高
- 如果文本里出现 `new/first/launch/initiate`，`novelty` 会更高
- 如果文本里出现 `ai/cloud/chip/data center`，`theme_relevance` 会更高
- `sentiment` 通过正负关键词做简单归一化，范围在 `-1` 到 `1`

### 3. 计算公式

`event_score` 当前公式为：

```text
event_score =
  0.30 * importance
  + 0.25 * source_credibility
  + 0.20 * novelty
  + 0.15 * theme_relevance
  + 0.10 * sentiment_strength
```

其中：

```text
sentiment_strength = abs(sentiment) * 100
```

### 4. 这意味着什么

这里用的是 `abs(sentiment)`，不是情绪正负本身。也就是说：

- 强烈利多会提高事件强度
- 强烈利空也会提高事件强度

原因是 `event_score` 评估的是“这件事有多强、有多值得看”，不是直接评估“适不适合买入”。

事件方向会影响 `bias`：

- `sentiment >= 0` 时偏向 `long`
- `sentiment < 0` 时偏向 `short`

## 三、市场层分数：market_score

`market_score` 来自 `IndicatorSnapshot`，当前公式由 5 个子分数组成：

```text
market_score =
  0.25 * trend_score
  + 0.15 * volume_score
  + 0.20 * volatility_score
  + 0.20 * proximity_score
  + 0.20 * rsi_score
```

### 1. trend_score

看趋势状态 `trend_state`，当前取值为：

- `bullish`
- `neutral`
- `bearish`

如果当前 `bias = long`：

- `bullish = 88`
- `neutral = 58`
- `bearish = 30`

如果当前 `bias = short`，分值反转：

- `bullish = 30`
- `neutral = 58`
- `bearish = 88`

含义很直接：

- 做多更偏好上升趋势
- 做空更偏好下降趋势

### 2. volume_score

量能分的实现是：

```text
volume_score = clamp(relative_volume * 55, 20, 100)
```

其中 `relative_volume` 在 [indicators.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/indicators.py) 中计算：

- 当前量能相对过去一段时间的平均量能越高
- 量能确认越强

### 3. volatility_score

波动分使用 `ATR%` 与阈值比较：

- 如果 `atr_percent <= atr_percent_ceiling`，分数给 `85`
- 如果超过阈值，开始扣分
- 最低不会低于 `20`

当前默认阈值：

- `swing atr_percent_ceiling = 8`
- `position atr_percent_ceiling = 10`

含义：

- 波动太低，未必有机会
- 波动太大，也不利于风险控制
- 当前策略默认更重视“可操作、可承受”的波动水平

### 4. proximity_score

这个分数评估“当前价格位置是否合适”。

对于 `long`：

- 如果系统判断为 `is_pullback`，给 `85`
- 如果系统判断为 `intraday_breakout`，给 `78`
- 否则看当前价格离 `support_20` 有多远，越接近支撑越高

对于 `short`：

- 主要看当前价格离 `resistance_20` 有多远，越接近阻力越高

含义：

- 同样一条好消息，追高和回踩买点的质量不一样
- 这个分数本质上在做“位置过滤”

### 5. rsi_score

RSI 在当前系统里不是主引擎，而是确认因子。

对于 `long`：

- 如果 `RSI` 在 `[rsi_floor, rsi_ceiling]` 内，给 `90`
- 如果略低于 `floor` 但仍可接受，给 `72`
- 否则给 `38`

对于 `short`：

- 逻辑做镜像处理

当前默认阈值：

- `swing`: `rsi_floor = 45`, `rsi_ceiling = 68`
- `position`: `rsi_floor = 50`, `rsi_ceiling = 65`

这表示：

- `swing` 容忍度略高
- `position` 更保守

## 四、综合分数：final_score

最终分数的当前公式是：

```text
final_score = 0.6 * event_score + 0.4 * market_score
```

这反映了当前系统设计哲学：

- 事件层更重要
- 技术层负责确认和择时

也就是说，目前这个 Agent 不是一个“纯技术扫描器”，而是一个“事件驱动 + 技术确认”的系统。

## 五、优先级：priority

`priority` 不是简单看 `final_score` 一个数，而是分三道门。

### 第一道门：事件门

要求：

```text
event_score >= event_score_threshold
```

默认值：

- `event_score_threshold = 60`

### 第二道门：市场门

要求：

```text
market_score >= horizon.market_score_threshold
```

默认值：

- `swing.market_score_threshold = 55`
- `position.market_score_threshold = 55`

### 第三道门：优先级门

如果前两道门都过了，再看最终分数：

- `final_score >= priority_threshold` 时，标记为 `high`
- 否则如果 `final_score >= 60`，标记为 `normal`
- 否则标记为 `suppressed`

默认值：

- `swing.priority_threshold = 75`
- `position.priority_threshold = 75`

## 六、参数短码怎么读

批次实验输出中经常会看到类似：

```text
E65.0/S58.0-78.0/P60.0-80.0/D12
```

含义如下：

- `E65.0`
  全局事件门槛 `event_score_threshold = 65`
- `S58.0-78.0`
  `swing.market_score_threshold = 58`
  `swing.priority_threshold = 78`
- `P60.0-80.0`
  `position.market_score_threshold = 60`
  `position.priority_threshold = 80`
- `D12`
  跨源去重窗口 `cross_source_dedup_hours = 12`

如果一组参数更高，通常表示：

- 提醒更少
- 过滤更严
- 更偏向“质量优先”

如果一组参数更低，通常表示：

- 提醒更多
- 覆盖更广
- 更偏向“机会不漏掉”

## 七、当前默认阈值

当前默认配置在 [config.py](/Users/linxun/CodeSpace/asset_allocation_system/src/satellite_agent/config.py)：

### 全局

- `event_score_threshold = 60`
- `cross_source_dedup_hours = 12`

### swing

- `ttl_days = 3`
- `market_score_threshold = 55`
- `priority_threshold = 75`
- `rsi_floor = 45`
- `rsi_ceiling = 68`
- `atr_percent_ceiling = 8`

### position

- `ttl_days = 20`
- `market_score_threshold = 55`
- `priority_threshold = 75`
- `rsi_floor = 50`
- `rsi_ceiling = 65`
- `atr_percent_ceiling = 10`

## 八、当前体系的优点与限制

### 优点

- 可解释
- 易调参
- 适合做 replay 和批量实验
- 适合做 CLI 报告和阈值诊断

### 限制

- 权重目前是经验规则，不是统计学习结果
- 某些子分数仍然是离散映射，不够平滑
- 事件抽取质量会受数据源和摘要质量影响
- 市场分目前主要靠常见技术指标，因子仍然偏少

## 九、后续维护约定

这份文档应当随着代码一起维护。后续如果发生以下变化，需要同步更新本文：

- `event_score` 或 `market_score` 权重调整
- 默认阈值调整
- 新增或删除评分因子
- `priority` 判定逻辑变化
- `batch-replay` 参数短码含义变化

如果代码与本文不一致，以代码为准，但应尽快修正文档，避免解释漂移。
