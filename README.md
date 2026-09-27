# telemetry-context-pipeline-go

一套面向异步与异常场景的可观测数据（追踪 / 指标 / 日志）采集、传递与导出管线。
仅依赖 Go 标准库。

## 目标与保证

| 主题 | 保证 |
| --- | --- |
| 异步上下文延续 | 父任务取消/超时不会丢失或错配异步任务的 traceID、采样结论与 baggage |
| 采样一致性 | 同一 traceID 采样结论恒定；子 span 与跨进程下游强制继承父结论 |
| 指标并发写入 | 业务侧非阻塞入队（有界），单 worker 聚合，`-race` 下验证安全 |
| 导出降级 | 失败原因可区分（unavailable/timeout/rejected/internal），重试、熔断、丢弃均有计数与日志 |
| 属性治理 | 敏感字段脱敏、长度截断、键数限制、每键基数限制；三个入口共用同一处理器，结论一致 |
| 可解释性 | 采样依据、属性处理明细、丢弃原因全部进入日志/计数，可复核 |

## 包结构

```
config/     采样 / 属性 / 指标配置与校验
tracing/    TraceID/SpanID、采样器、Span、ContinueAsync、跨进程注入提取
attribute/  敏感脱敏 + 截断 + 键数/基数控制，产出判定 Report
metric/     有界队列、内存聚合器、带重试与熔断的导出代理
logx/       注入追踪上下文的结构化日志、JSON Sink
example/    端到端可运行示例
```

## 一、采样规则

- **根链路**：`RatioSampler` 对 `traceID` 计算 FNV-1a 哈希，映射到
  `[0, 1_000_000)` 整数桶，与阈值 `SampleRate * 1_000_000` 比较。
  同一 `traceID` 永远落入同一桶，因此判定**恒定可复算**，不随时间漂移。
- **子 span（进程内）**：无条件继承父 `Sampled`，判定 `Source=parent`，
  不重新掷骰子，保证同一条链路采样结果一致。
- **跨进程**：采样标志随 `traceparent` 头传递，下游继承；判定依据不跨
  进程传递（防止伪造），接收端生成"继承自父链路"的新依据。
- 判定明细写入 `SpanContext.SamplingSource/SamplingReason`，包含
  `bucket / threshold / rate`，可在日志中直接复核。

配置项：`Config.SampleRate`（`[0,1]`，越界在 `Validate()` 启动期报错）。

## 二、上下文传递规则

### 进程内：`tracing.ContinueAsync(ctx)`

专为"fire-and-forget 异步任务"设计：

1. **保留**全部追踪上下文（traceID、spanID 除外会新生成、采样结论、baggage）；
2. **剥离**父 `context` 的取消信号、超时与 deadline——父请求返回后异步任务
   不会被连带取消（用 `context.Background()` 作底）；
3. 返回**独立** `CancelFunc`，异步任务生命周期自行管理；
4. 传递时对 `SpanContext` 与 baggage 做整体拷贝，多个并发任务持有独立
   副本，并发写 baggage 互不串扰（copy-on-write）。

### 跨进程：Inject / Extract

- 载体为 `Carrier` 接口（可适配 `http.Header`、gRPC metadata 等）；
- 头格式对齐 W3C Trace Context：
  `traceparent: 00-<32hex traceID>-<16hex spanID>-<01|00 flags>`；
- baggage 头：`trace-baggage: k1=v1,k2=v2`；
- 头缺失或格式非法时 `Extract` **返回错误而非静默新建链路**。

## 三、属性基数控制与敏感字段过滤

`attribute.Processor` 被 tracing 属性、metric label、log 字段三个入口
**共享同一实例**（由 `Pipeline` 统一注入），保证同一属性在任何入口处理
结果一致。对 key 排序后顺序执行，结论确定：

1. **敏感脱敏**：命中 `SensitiveKeys`（大小写不敏感，支持前/后缀 `*` 与
   `*mid*` 包含匹配）→ 值整体替换为 `RedactedPlaceholder`，原值不进 Sink；
2. **长度截断**：字符串超过 `MaxValueLength` 按字节截断并追加 `...`，记 `truncated`；
3. **键数限制**：超过 `MaxKeysPerRecord` 的属性整条丢弃，记 `dropped`；
4. **基数限制**：同一 key 已登记的不同值超过 `MaxValuesPerKey` 后，新值
   统一归入 `OtherBucket`（默认 `other`），防止高基数字段撑爆存储。

每条属性产生 `ItemResult{Key, Action, Reason}`，汇总为 `Report`；
JSON 日志把明细放在 `attr_filter` 字段，说明"为什么被脱敏/截断/丢弃"。

默认敏感名单：`password, passwd, *token, authorization, secret, *secret, *key, api_key`。

## 四、指标导出与降级策略

### 数据流

```
业务 Record ──(非阻塞 select)──▶ 有界 channel ──▶ 单 worker ──▶ 内存聚合器
                                       │                    │
                                  满则立即丢弃         tick/Close ──▶ Exporter
                                  (queue_full)              │
                                                       重试 + 熔断
```

- **有界、非阻塞**：`Record` 用 `select/default` 入容量为 `QueueSize` 的
  channel；队列满立即返回 `(false, queue_full)` 并计数，**绝不阻塞业务**。
- **聚合有界**：序列数受 `WithMaxSeries` 上限约束（`Pipeline.New` 默认设为
  `MaxValuesPerKey * MaxKeysPerRecord`），超限样本记 `series_limit`。
- 单 worker 聚合，规避多写者竞争；计数全部用原子操作，`Stats()` 可随时读取。

### 失败分类（`FailureKind`，互相可区分）

| Kind | 含义 | 是否重试 |
| --- | --- | --- |
| `unavailable` | 连接失败 / 熔断 | 是 |
| `timeout` | 导出超过 `ExportTimeout`（含 ctx 取消） | 是 |
| `rejected` | 下游明确拒绝（4xx/非法数据） | 否，立即丢弃 |
| `internal` | 其它错误 | 是 |

### 重试与熔断

- 可重试失败按 `MaxRetries` 做线性退避（`FailureBackoff`）；
- `rejected` 不重试，直接 `export_failed` 丢弃；
- 连续失败（按批次计）达到 `UnavailableThreshold` → 熔断 **OPEN**，
  冷却 `UnavailableCooldown` 期间所有批次记 `exporter_down` 直接降级；
- 冷却结束 → **HALF_OPEN**，只放行一个探测批次：成功则 CLOSED，
  失败则重新 OPEN 并刷新冷却窗口；
- **区分两种丢弃**：`export_failed`（重试耗尽/被拒绝）与
  `exporter_down`（熔断降级），分别计数。
- `Close` 先抽空队列，再用独立的 `CloseTimeout`（基于 `context.Background`，
  与运行 ctx 解耦）尽力冲刷；超时剩余数据按上述策略丢弃，Close 不挂死。
- 运行 ctx 取消**只停止定时导出，不结束 worker**；异步任务仍可写入，
  关闭统一由 `Shutdown/Close` 负责。

### 运行计数 `metric.Stats`

`Received / Enqueued / Merged / ExportedPoints / ExportedBatches /
DroppedQueueFull / DroppedSeriesLimit / DroppedExporterDown /
DroppedExportFailed / ExportRetries / LastExportKind / LastExportError /
LastExportAt`，所有降级路径都有对应计数与标准日志输出。

## 五、配置项一览（`config.Config`）

| 字段 | 含义 | 默认值 |
| --- | --- | --- |
| `SampleRate` | 根链路采样率 `[0,1]` | `1.0` |
| `Attributes.MaxKeysPerRecord` | 单条最大属性数 | `64` |
| `Attributes.MaxValuesPerKey` | 每 key 不同值基数上限 | `128` |
| `Attributes.MaxValueLength` | 字符串值最大字节数 | `1024` |
| `Attributes.SensitiveKeys` | 敏感 key 模式 | 见上 |
| `Attributes.RedactedPlaceholder` | 脱敏占位符 | `[REDACTED]` |
| `Attributes.OtherBucket` | 基数超限归桶名 | `other` |
| `Metrics.QueueSize` | 有界写入队列容量 | `4096` |
| `Metrics.BatchSize` | 聚合序列数达到该值立即导出（否则等 tick） | `256` |
| `Metrics.ExportInterval` | 定时导出间隔 | `5s` |
| `Metrics.ExportTimeout` | 单次导出超时 | `2s` |
| `Metrics.MaxRetries` | 可重试失败的重试次数 | `2` |
| `Metrics.FailureBackoff` | 重试线性退避 | `200ms` |
| `Metrics.UnavailableThreshold` | 连续失败多少次熔断 | `5` |
| `Metrics.UnavailableCooldown` | 熔断冷却时间 | `10s` |
| `Metrics.CloseTimeout` | 关闭时最大冲刷时间 | `3s` |

`Config.Validate()` 在 `Pipeline.New` 启动期校验，非法值直接返回错误，
不允许静默使用错误配置。

## 六、快速上手

```go
p, _ := telemetry.New(cfg, exporter, sink) // exporter/sink 为可插拔接口
p.Start(ctx)

ctx, span := p.Tracer.Start(ctx, "op")
defer span.End()

actx, cancel := tracing.ContinueAsync(ctx) // 跨异步边界
go func() {
    defer cancel()
    p.Logs.Info(actx, "job", map[string]any{"password": "x"}) // 自动脱敏+带 trace
    p.Metrics.Record(metric.Sample{Name: "jobs", Kind: metric.Counter, Value: 1})
}()

defer p.Shutdown(ctx) // 有界冲刷
```

直接运行示例：`go run ./example`

## 七、测试

```
go test ./... -race -count=1 -cover
```

覆盖的关键场景：

- **取消/超时隔离**：父 ctx cancel/timeout 后异步上下文仍存活且保留
  traceID、采样结论、baggage；独立 CancelFunc 可单独取消；
- **并发不错配**：50 个交错异步任务各持副本，baggage 写不串扰，
  父 span/父 ctx 不被污染；
- **并发指标写入**：100 goroutine × 50 次写入聚合总数精确；
- **队列满**：小队列下超额样本立即 `queue_full` 丢弃，0 阻塞；
- **导出失败分类**：unavailable 重试耗尽、rejected 零重试、
  timeout 归类正确，失败原因互不混淆；
- **熔断**：OPEN 降级 `exporter_down`、HALF_OPEN 探测成功恢复；
- **取消后关闭冲刷**：运行 ctx cancel 后 Close 仍能冲刷已入队数据；
- **敏感字段**：log / metric label / 任意值三入口结论一致，
  序列化输出无明文。

单测通过 `t.Logf` 打印 traceID、bucket/threshold 判定依据、属性处理
明细与降级计数，失败用例的失败原因可从 `LastExportKind`/`DropReason`
直接区分。
