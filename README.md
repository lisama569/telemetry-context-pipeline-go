# telemetry-context-pipeline-go

一套进程内的可观测数据流水线，覆盖 **追踪（trace）、指标（metrics）、日志（logs）**
三类信号的采集、上下文延续、属性治理与导出，重点保证跨异步边界与异常场景下
结论的一致性与可解释性。仅依赖 Go 标准库。

## 包结构

| 包 | 职责 |
| --- | --- |
| `trace` | TraceID/SpanID、确定性采样器、Span 生命周期与跨异步上下文延续 |
| `attributes` | 敏感字段脱敏、单键基数上限、单批键数上限；三入口共用 |
| `metrics` | 并发写入聚合、有界缓冲、失败批次重试与可区分的丢弃原因 |
| `logs` | 自动注入追踪上下文、属性经统一 Processor 过滤后输出 |
| `pipeline` | 用统一 `Config` 组装以上子系统并管理启停生命周期 |

## 一、采样规则

- 采样器是 `trace.DeterministicSampler`，判定只对**链路根节点**做一次：
  取 TraceID 前 8 字节映射到 `[0,1)`，小于采样率 `SampleRate` 即
  `RECORD_AND_SAMPLE`，否则 `DROP`。
- 判定是**纯函数 + 稳定哈希**：同一个 TraceID 在任意入口、任意时刻（采样率不变时）
  结论相同，因此同一条链路的所有 Span 采样结果统一，且可凭 TraceID + SampleRate 复算追溯。
- 边界：`SampleRate <= 0` 全量丢弃，`>= 1` 全量采样，非法值（负/NaN）夹取到 `0`。
- `SetRate` 只影响调整之后**新开的链路**；判定结论（Decision 与 SampleRate）记录在
  随链路传递的 Trace Context 上，已存在的链路即使采样率中途变化也不会翻转。
- `DecisionDrop` 的 Span 不进入导出器；其余结论的 Span 结束时导出，导出错误原样返回，
  不静默吞掉。

## 二、上下文传递规则（跨异步 / 取消 / 超时 / 并发交错）

- 追踪状态是纯值类型 `trace.Context`。`StartSpan` 在入口处把父状态**一次性快照复制**，
  子 Span 沿用父链路的 `TraceID / Decision / SampleRate`，仅更换 SpanID 并回填 ParentSpanID。
- 返回的子 context 通过 `ContextWith` 挂载在**独立的 `context.Background()`** 上，
  只携带追踪状态、不携带父取消信号。因此：
  - 父请求被取消或超时，只影响取消信号，已经复制出的追踪状态不丢失、不可变；
  - 多个异步任务并发交错时各自持有独立副本，不会串上下文；
  - Span 的结束/导出不依赖父 context 存活。
- **业务侧用法**：取消信号用业务自己的 context 传递，追踪传播用 Span 返回的 context，
  二者职责分离（见 `pipeline/pipeline_test.go` 的端到端示例）。
- 日志通过 `trace.FromContext` 提取上下文并写入记录；无追踪上下文时 TraceID 为零值，
  与真实链路可区分。

## 三、属性治理：基数控制与敏感过滤

`attributes.Processor` 被追踪、指标标签、日志三个入口**共享同一个实例**，
同一属性在任何入口的处理结论一致。输入按键名排序后处理，输出确定。

处理优先级与去向（每个输入键都会返回一条 `AttrResult`，不存在静默消失）：

1. **敏感字段**：键名精确命中 `SensitiveKeys`，或大小写不敏感地包含
   `SensitiveSubstrings` 中任一子串 → 键保留、值替换为占位符（默认 `[REDACTED]`），
   原因分别为 `sensitive_exact_match` / `sensitive_substring_match`。
2. **单批键数上限** `MaxKeys`：排序靠后的键丢弃，原因 `max_keys_exceeded`。
3. **单键基数上限** `MaxCardinality`：已见过的旧值始终放行，仅丢弃超限后的**新值**，
   原因 `cardinality_exceeded`。

## 四、指标聚合与导出降级策略

`metrics.Aggregator` 后台单协程消费写入并聚合：Counter 累加、Gauge 取最新值；
序列以 `SeriesID.Key()`（名称 + 排序后标签）稳定归一，不同 map 构造顺序不会拆成多条。

**所有缓冲都有界，`Record` 在任何情况下都不阻塞业务调用**，降级路径与原因如下：

| 场景 | 行为 | DropReason |
| --- | --- | --- |
| 写入瞬时突发、通道已满 | 本次写入立即丢弃并计数 | `channel_full` |
| 时间序列数达 `MaxSeries` | 拒绝新序列，已存在序列不受影响 | `series_limit` |
| 下游导出失败 | 整批进入有界 FIFO 重试队列，后续周期按序重试 | （失败计数 `export_failed`） |
| 重试队列达 `RetryBuffer` | **丢弃最旧批次**为新批次腾位 | `buffer_full` |
| 关闭时仍无法导出 | 最终 flush 后仍残留的批次记为丢弃并释放 | `export_failed` |

- 下游短暂故障后恢复：队列中的失败批次自动重试成功，数据不丢
  （`TestAggregator_RecoveryAfterFailure`）。
- 取消/超时/关停：后台循环退出前会**脱离父取消信号**做一次有 3 秒时限的最终 flush，
  已被接收的数据尽力导出一次（`TestAggregator_CancelFinalFlush`）。
- 丢弃事件通过 `Aggregator.Drops()` / `Pipeline.Shutdown()` 返回，原因彼此可区分，
  供业务记录与告警。

## 五、配置项（`pipeline.Config`）

| 字段 | 含义 | 默认值 |
| --- | --- | --- |
| `SampleRate` | 根链路采样率 `[0,1]` | `1` |
| `LogLevel` | 最低日志级别 | `INFO` |
| `Attributes.MaxCardinality` | 单属性键不同取值上限，`<=0` 不限 | 不限 |
| `Attributes.MaxKeys` | 单批属性保留键数上限，`<=0` 不限 | 不限 |
| `Attributes.SensitiveKeys` | 精确敏感键集合 | 空 |
| `Attributes.SensitiveSubstrings` | 敏感子串（大小写不敏感） | `password`/`token`/`secret` |
| `Attributes.RedactionPlaceholder` | 脱敏占位符 | `[REDACTED]` |
| `Metrics.MaxSeries` | 时间序列数上限 | `10000` |
| `Metrics.ChannelBuffer` | 写入通道容量（吸收瞬时突发） | `1024` |
| `Metrics.RetryBuffer` | 待重试**批次**数上限（FIFO） | `8` |
| `Metrics.FlushIntervalMs` | 定时导出间隔 | `1000` |

## 六、验证

```bash
go test ./... -race -count=1
```

测试覆盖：父 context 超时取消后异步任务上下文延续、多链路并发交错不串线、
采样结论同链路一致且动态调率不翻转、指标并发写入聚合、通道满不阻塞、
下游持续失败的有界丢弃与原因区分、下游恢复后重试成功、取消时最终 flush、
敏感字段跨入口一致脱敏与基数/键数限制。单测日志会打印关键 TraceID、采样判定与
丢弃/脱敏的判定依据，便于复现与排查。
