package metrics

import (
	"context"
	"sync"
	"time"
)

// AggregatorConfig 配置聚合导出器。零值由 NewAggregator 替换为安全默认值。
type AggregatorConfig struct {
	// MaxSeries 允许保留的时间序列数量上限（有界），超出后的新序列写入被丢弃。
	MaxSeries int
	// ChannelBuffer 写入通道容量；满时写入直接丢弃并计数，绝不阻塞业务调用。
	ChannelBuffer int
	// RetryBuffer 导出失败时待重试“批次”的数量上限（有界），满时丢最旧批次。
	RetryBuffer int
	// FlushIntervalMs 定时导出间隔（毫秒）。
	FlushIntervalMs int
}

// DefaultAggregatorConfig 返回一组安全的有界默认配置。
func DefaultAggregatorConfig() AggregatorConfig {
	return AggregatorConfig{
		MaxSeries:       10000,
		ChannelBuffer:   1024,
		RetryBuffer:     8,
		FlushIntervalMs: 1000,
	}
}

type seriesEntry struct {
	id    SeriesID
	kind  Kind
	value float64
}

// Aggregator 并发接收指标写入，聚合成快照后导出。
// 降级策略（任何路径都不会无限增长或阻塞业务调用）：
//   - 写入快于导出、通道已满：本次写入丢弃，记 channel_full；
//   - 时间序列数超过 MaxSeries：新序列写入丢弃，记 series_limit；
//   - 下游导出失败：整批进入有界 FIFO 重试队列，后续周期按序重试；
//   - 重试队列已满：丢弃最旧批次，记 buffer_full；导出仍失败时记 export_failed；
//   - 关闭/取消：用独立 context 做最后一次 flush，仍失败的批次记 export_failed 后释放。
type Aggregator struct {
	cfg      AggregatorConfig
	exporter Exporter

	ch chan Sample

	seriesMu sync.Mutex
	series   map[string]*seriesEntry

	qMu   sync.Mutex
	retry [][]Snapshot

	dropMu sync.Mutex
	drops  []DropEvent

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	startMu sync.Mutex
	started bool
}

// NewAggregator 创建聚合器并规范化配置。
func NewAggregator(exporter Exporter, cfg AggregatorConfig) *Aggregator {
	d := DefaultAggregatorConfig()
	if cfg.MaxSeries > 0 {
		d.MaxSeries = cfg.MaxSeries
	}
	if cfg.ChannelBuffer > 0 {
		d.ChannelBuffer = cfg.ChannelBuffer
	}
	if cfg.RetryBuffer > 0 {
		d.RetryBuffer = cfg.RetryBuffer
	}
	if cfg.FlushIntervalMs > 0 {
		d.FlushIntervalMs = cfg.FlushIntervalMs
	}
	return &Aggregator{
		cfg:      d,
		exporter: exporter,
		ch:       make(chan Sample, d.ChannelBuffer),
		series:   make(map[string]*seriesEntry),
	}
}

// Record 提交一次指标写入。该方法永不阻塞业务调用：
// 通道满时直接返回 DropChannelFull 并计数；已接收返回 DropNone。
// 序列上限类丢弃发生在后台消费侧，可通过 Drops() 观察。
func (a *Aggregator) Record(s Sample) DropReason {
	select {
	case a.ch <- s:
		return DropNone
	default:
		a.recordDrop(DropChannelFull, 1)
		return DropChannelFull
	}
}

// Run 启动后台聚合导出循环，直到 parent 被取消或调用 Close。
// 退出前会用“不携带取消信号”的独立 context 做最后一次 flush，
// 因此父任务取消/超时时，已采集的指标仍会尽力导出一次。
func (a *Aggregator) Run(parent context.Context) error {
	a.startMu.Lock()
	if a.started {
		a.startMu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	a.cancel = cancel
	a.started = true
	a.wg.Add(1) // 必须在发布 started=true 之前完成，避免 Close 抢先 Wait 后漏等。
	a.startMu.Unlock()
	defer a.wg.Done()
	defer cancel()

	ticker := time.NewTicker(time.Duration(a.cfg.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// 选中取消分支时通道中可能仍残留已接收的写入，先非阻塞排空再最终 flush，
			// 保证“已被 Record 接收”的数据不丢。
			a.drain()
			// 最终 flush：脱离父取消信号，但给出有限时间避免无限挂住关闭流程。
			finalCtx, finalCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			a.flush(finalCtx)
			finalCancel()
			a.abandonRemaining()
			return nil
		case <-ticker.C:
			a.flush(ctx)
		case sm := <-a.ch:
			// 非导出节拍也持续消费写入并即时聚合，使写入通道只吸收瞬时突发，
			// 突发超过通道容量时仍按 channel_full 立即丢弃（Record 永不阻塞）。
			a.apply(sm)
		}
	}
}

// drain 非阻塞地把通道中当前所有写入聚合进内存序列。
func (a *Aggregator) drain() {
	for {
		select {
		case s := <-a.ch:
			a.apply(s)
		default:
			return
		}
	}
}

// apply 把单条写入并入对应序列；计数器累加，测量器取最新值。
func (a *Aggregator) apply(s Sample) {
	key := s.ID.Key()
	a.seriesMu.Lock()
	defer a.seriesMu.Unlock()
	e, ok := a.series[key]
	if !ok {
		if len(a.series) >= a.cfg.MaxSeries {
			a.recordDrop(DropSeriesLimit, 1)
			return
		}
		e = &seriesEntry{id: s.ID, kind: s.Kind}
		a.series[key] = e
	}
	if s.Kind == KindGauge {
		e.value = s.Value
	} else {
		e.value += s.Value
	}
}

// flush 排空写入通道，把本周期聚合结果打成一个批次放入有界重试队列，
// 然后按 FIFO 顺序尝试导出所有待重试批次。
func (a *Aggregator) flush(ctx context.Context) error {
	a.drain()

	batch := a.takeBatch()
	if len(batch) > 0 {
		a.enqueueRetry(batch)
	}
	return a.exportQueued(ctx)
}

// takeBatch 取出当前所有序列的快照并把计数器清零、测量器移除，开始新的累积周期。
func (a *Aggregator) takeBatch() []Snapshot {
	a.seriesMu.Lock()
	defer a.seriesMu.Unlock()
	if len(a.series) == 0 {
		return nil
	}
	batch := make([]Snapshot, 0, len(a.series))
	for k, e := range a.series {
		batch = append(batch, Snapshot{ID: e.id, Kind: e.kind, Value: e.value})
		delete(a.series, k)
	}
	return batch
}

// enqueueRetry 把批次放入有界 FIFO 队列；队列满时丢弃最旧批次并计数。
func (a *Aggregator) enqueueRetry(batch []Snapshot) {
	a.qMu.Lock()
	defer a.qMu.Unlock()
	if len(a.retry) >= a.cfg.RetryBuffer {
		oldest := a.retry[0]
		a.retry = a.retry[1:]
		a.recordDrop(DropBufferFull, len(oldest))
	}
	a.retry = append(a.retry, batch)
}

// exportQueued 按入队顺序导出批次；某个批次失败则保留它及后续批次，等下周期重试。
func (a *Aggregator) exportQueued(ctx context.Context) error {
	for {
		a.qMu.Lock()
		if len(a.retry) == 0 {
			a.qMu.Unlock()
			return nil
		}
		batch := a.retry[0]
		a.qMu.Unlock()

		if err := a.exporter.Export(ctx, batch); err != nil {
			// 区分“因取消而失败”与“下游不可用”：取消不算降级丢弃计数，
			// 最终 flush 阶段还会再尝试一次。
			if ctx.Err() != nil {
				return err
			}
			a.recordDrop(DropExportFailed, len(batch))
			return err
		}
		// 成功：出队并继续导出后续批次。
		a.qMu.Lock()
		a.retry = a.retry[1:]
		a.qMu.Unlock()
	}
}

// abandonRemaining 在关闭流程末尾把最终仍无法导出的批次记为 export_failed 后释放，
// 保证内存有界且失败去向可审计。
func (a *Aggregator) abandonRemaining() {
	a.qMu.Lock()
	remaining := a.retry
	a.retry = nil
	a.qMu.Unlock()
	for _, b := range remaining {
		if len(b) > 0 {
			a.recordDrop(DropExportFailed, len(b))
		}
	}
}

// recordDrop 追加一次丢弃事件。
func (a *Aggregator) recordDrop(reason DropReason, count int) {
	a.dropMu.Lock()
	a.drops = append(a.drops, DropEvent{Reason: reason, Count: count})
	a.dropMu.Unlock()
}

// Drops 返回并清空累计的丢弃事件（不同原因可区分、可断言）。
func (a *Aggregator) Drops() []DropEvent {
	a.dropMu.Lock()
	defer a.dropMu.Unlock()
	out := a.drops
	a.drops = nil
	return out
}

// SnapshotSeries 返回当前各序列聚合值的副本（主要供测试观察）。
func (a *Aggregator) SnapshotSeries() map[string]float64 {
	a.seriesMu.Lock()
	defer a.seriesMu.Unlock()
	out := make(map[string]float64, len(a.series))
	for k, e := range a.series {
		out[k] = e.value
	}
	return out
}

// Close 停止后台循环并等待其完成（Run 退出时自带最终 flush）。
// 若 Run 从未启动，则直接用传入 ctx 做一次同步 flush，避免已写入数据无人消费。
func (a *Aggregator) Close(ctx context.Context) error {
	a.startMu.Lock()
	started := a.started
	cancel := a.cancel
	a.startMu.Unlock()

	if started {
		if cancel != nil {
			cancel()
		}
		a.wg.Wait()
		return nil
	}
	a.flush(ctx)
	a.abandonRemaining()
	return nil
}
