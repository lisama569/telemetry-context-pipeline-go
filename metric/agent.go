package metric

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/telemetry-context-pipeline-go/attribute"
	"github.com/lisama569/telemetry-context-pipeline-go/config"
)

// circuitState 是导出器的熔断状态。
type circuitState int

const (
	circuitClosed   circuitState = iota // 正常导出
	circuitOpen                         // 冷却中，批次直接降级丢弃
	circuitHalfOpen                     // 冷却结束，仅放行一个探测批次
)

// DropReason 区分降级丢弃的原因，出现在统计计数与日志中，保证可区分。
type DropReason string

const (
	DroppedQueueFull    DropReason = "queue_full"    // 有界队列已满，业务侧立即返回
	DroppedSeriesLimit  DropReason = "series_limit"  // 序列基数达上限
	DroppedExporterDown DropReason = "exporter_down" // 熔断打开/冷却期，批次直接丢弃
	DroppedExportFailed DropReason = "export_failed" // 重试耗尽或被下游拒绝
)

// Stats 暴露管线运行状态，所有降级/丢弃均有计数，不会静默发生。
type Stats struct {
	Received            int64
	Enqueued            int64
	Merged              int64
	DroppedSeriesLimit  int64
	ExportedPoints      int64
	ExportedBatches     int64
	DroppedQueueFull    int64
	DroppedExporterDown int64
	DroppedExportFailed int64
	ExportRetries       int64
	LastExportError     string
	LastExportKind      FailureKind
	LastExportAt        time.Time
}

// Agent 是有界、非阻塞的指标聚合导出代理。
//
// 业务侧 Record 只做一次非阻塞入队，绝不因下游问题阻塞；
// 单一后台 worker 负责聚合、批量导出、重试与熔断降级。
type Agent struct {
	cfg  config.MetricConfig
	exp  Exporter
	proc *attribute.Processor
	agg  *aggregator

	queue   chan Sample
	stop    chan struct{}
	stopped atomic.Bool
	wg      sync.WaitGroup

	exportMu sync.Mutex // 串行化 Flush 与 worker 的导出
	metaMu   sync.RWMutex

	// 熔断状态（仅持有 exportMu 时访问）
	consecutiveFails int
	circuitOpenUntil time.Time
	lastSuccessAt    time.Time
	circuitState     circuitState
	halfOpenInFlight bool

	st Stats
}

// MaxSeries 限制指标序列基数；默认与属性基数上限挂钩，防止无界增长。
func NewAgent(cfg config.MetricConfig, exp Exporter, proc *attribute.Processor) *Agent {
	return &Agent{
		cfg:   cfg,
		exp:   exp,
		proc:  proc,
		agg:   newAggregator(0),
		queue: make(chan Sample, cfg.QueueSize),
		stop:  make(chan struct{}),
	}
}

// WithMaxSeries 设置最大序列数（0 表示不限），返回自身便于链式配置。
func (a *Agent) WithMaxSeries(n int) *Agent {
	a.agg = newAggregator(n)
	return a
}

// Start 启动后台聚合导出循环。
func (a *Agent) Start(ctx context.Context) {
	a.wg.Add(1)
	go a.worker(ctx)
}

// Record 非阻塞写入：队列满时立即返回 DroppedQueueFull，绝不阻塞业务调用。
// 指标 label 在入队前先经过统一属性处理器（脱敏/基数控制），与日志、
// trace 属性的处理结果保持一致。
func (a *Agent) Record(s Sample) (accepted bool, reason DropReason) {
	atomic.AddInt64(&a.st.Received, 1)
	if s.Labels != nil && a.proc != nil {
		s.Labels, _ = a.proc.ProcessStrings(s.Labels)
	}
	select {
	case a.queue <- s:
		atomic.AddInt64(&a.st.Enqueued, 1)
		return true, ""
	default:
		atomic.AddInt64(&a.st.DroppedQueueFull, 1)
		log.Printf("[metric] drop sample name=%s kind=%d reason=%s queue_len=%d",
			s.Name, s.Kind, DroppedQueueFull, len(a.queue))
		return false, DroppedQueueFull
	}
}

func (a *Agent) worker(ctx context.Context) {
	defer a.wg.Done()
	ticker := time.NewTicker(a.cfg.ExportInterval)
	defer ticker.Stop()

	// 运行 ctx 取消仅代表调用方不再驱动定时导出（例如父请求结束），
	// 不代表管线关闭：异步任务仍可继续 Record，关闭统一由 Close 触发。
	timed := func() <-chan time.Time { return ticker.C }
	for {
		var tickCh <-chan time.Time
		if ctx.Err() == nil {
			tickCh = timed()
		}
		select {
		case s := <-a.queue:
			if !a.agg.Merge(s) {
				atomic.AddInt64(&a.st.DroppedSeriesLimit, 1)
				log.Printf("[metric] drop sample name=%s reason=%s", s.Name, DroppedSeriesLimit)
			} else {
				atomic.AddInt64(&a.st.Merged, 1)
			}
			// 达到批量阈值立即导出；否则队列里还有数据就继续抽空，避免延迟一个 tick
			if a.agg.Len() >= a.cfg.BatchSize {
				a.exportAll(ctx, false)
			} else if len(a.queue) > 0 {
				continue
			}
		case <-tickCh:
			a.exportAll(ctx, false)
		case <-a.stop:
			a.shutdownDrain()
			return
		}
	}
}

// shutdownDrain 停止后尽力冲刷：抽空队列后在 CloseTimeout 内导出，
// 超时则按降级策略丢弃剩余批次。
func (a *Agent) shutdownDrain() {
	for {
		select {
		case s := <-a.queue:
			if a.agg.Merge(s) {
				atomic.AddInt64(&a.st.Merged, 1)
			} else {
				atomic.AddInt64(&a.st.DroppedSeriesLimit, 1)
			}
		default:
			dctx, cancel := context.WithTimeout(context.Background(), a.cfg.CloseTimeout)
			a.exportAll(dctx, true)
			cancel()
			return
		}
	}
}

// exportAll 取出聚合数据并导出；force=true 时即使为空也走一次（关闭场景无副作用）。
func (a *Agent) exportAll(ctx context.Context, force bool) {
	a.exportMu.Lock()
	defer a.exportMu.Unlock()

	points := a.agg.Drain()
	if len(points) == 0 {
		return
	}
	if !force {
		log.Printf("[metric] flush scheduled points=%d", len(points))
	}
	// 调用方已持有 exportMu，直接走内部批次逻辑。
	a.exportBatchInner(ctx, points)
}

// exportBatch 是 exportBatchInner 的加锁外壳，保证与 Flush 互斥。
func (a *Agent) exportBatch(ctx context.Context, points []DataPoint) {
	a.exportMu.Lock()
	defer a.exportMu.Unlock()
	a.exportBatchInner(ctx, points)
}

func (a *Agent) discard(points []DataPoint, reason DropReason) int {
	switch reason {
	case DroppedExporterDown:
		atomic.AddInt64(&a.st.DroppedExporterDown, int64(len(points)))
	case DroppedExportFailed:
		atomic.AddInt64(&a.st.DroppedExportFailed, int64(len(points)))
	}
	return len(points)
}

func (a *Agent) recordFailure(kind FailureKind, err error) {
	now := time.Now()
	a.metaMu.Lock()
	a.st.LastExportKind = kind
	a.st.LastExportError = err.Error()
	a.st.LastExportAt = now
	a.metaMu.Unlock()
}

// classifyError 从普通 error 中提取 FailureKind；context 超时/取消归入 timeout。
func classifyError(err error) FailureKind {
	var ee *ExportError
	if errors.As(err, &ee) {
		return ee.Kind
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout
	}
	if errors.Is(err, context.Canceled) {
		return FailureTimeout
	}
	return FailureInternal
}

// Flush 立即导出当前聚合数据（与 worker 导出互斥）。
func (a *Agent) Flush(ctx context.Context) error {
	a.exportMu.Lock()
	defer a.exportMu.Unlock()

	// 先把队列里的采样并入聚合器（非阻塞抽空）。
	for {
		select {
		case s := <-a.queue:
			if !a.agg.Merge(s) {
				atomic.AddInt64(&a.st.DroppedSeriesLimit, 1)
			} else {
				atomic.AddInt64(&a.st.Merged, 1)
			}
		default:
			goto drain
		}
	}
drain:
	points := a.agg.Drain()
	if len(points) == 0 {
		return nil
	}
	// Flush 已持有 exportMu，直接执行内部批次逻辑（含重试与熔断降级）。
	a.metaMu.RLock()
	before := a.lastSuccessAt
	a.metaMu.RUnlock()
	a.exportBatchInner(ctx, points)
	a.metaMu.RLock()
	succeeded := a.lastSuccessAt.After(before)
	kind, errMsg := a.st.LastExportKind, a.st.LastExportError
	a.metaMu.RUnlock()
	if !succeeded {
		return NewExportError(kind, errors.New(errMsg))
	}
	return nil
}

// allowBatch 依据熔断状态决定批次放行还是直接降级。
// 冷却结束时转入半开，仅放行一个探测批次。
func (a *Agent) allowBatch(now time.Time) bool {
	switch a.circuitState {
	case circuitClosed:
		return true
	case circuitOpen:
		if now.Before(a.circuitOpenUntil) {
			return false
		}
		a.circuitState = circuitHalfOpen
		a.halfOpenInFlight = true
		return true
	case circuitHalfOpen:
		return !a.halfOpenInFlight
	default:
		return true
	}
}

// openCircuit 打开熔断并开始冷却。
func (a *Agent) openCircuit(now time.Time) {
	a.circuitState = circuitOpen
	a.circuitOpenUntil = now.Add(a.cfg.UnavailableCooldown)
}

// exportBatchInner 是真正的批次逻辑；调用方需持有 exportMu。
func (a *Agent) exportBatchInner(ctx context.Context, points []DataPoint) {
	if !a.allowBatch(time.Now()) {
		n := a.discard(points, DroppedExporterDown)
		log.Printf("[metric] circuit OPEN, discard batch points=%d reason=%s open_until=%s",
			n, DroppedExporterDown, a.circuitOpenUntil.Format(time.RFC3339))
		return
	}
	wasProbe := a.circuitState == circuitHalfOpen

	attempt := 0
	for {
		ectx, cancel := context.WithTimeout(ctx, a.cfg.ExportTimeout)
		err := a.exp.Export(ectx, points)
		cancel()

		if err == nil {
			a.consecutiveFails = 0
			a.circuitState = circuitClosed
			a.halfOpenInFlight = false
			now := time.Now()
			atomic.AddInt64(&a.st.ExportedBatches, 1)
			atomic.AddInt64(&a.st.ExportedPoints, int64(len(points)))
			a.metaMu.Lock()
			a.st.LastExportError = ""
			a.st.LastExportKind = ""
			a.st.LastExportAt = now
			a.lastSuccessAt = now
			a.metaMu.Unlock()
			if wasProbe {
				log.Printf("[metric] half-open probe SUCCEEDED, circuit closed")
			}
			return
		}

		kind := classifyError(err)
		a.recordFailure(kind, err)
		if !kind.Retryable() {
			n := a.discard(points, DroppedExportFailed)
			log.Printf("[metric] batch permanently rejected points=%d reason=%s kind=%s err=%v",
				n, DroppedExportFailed, kind, err)
			return
		}
		if attempt >= a.cfg.MaxRetries {
			if wasProbe {
				a.halfOpenInFlight = false
				a.openCircuit(time.Now())
				n := a.discard(points, DroppedExporterDown)
				log.Printf("[metric] half-open probe FAILED kind=%s, re-open circuit points=%d reason=%s",
					kind, n, DroppedExporterDown)
				return
			}
			a.consecutiveFails++
			if a.consecutiveFails >= a.cfg.UnavailableThreshold {
				a.openCircuit(time.Now())
				n := a.discard(points, DroppedExporterDown)
				log.Printf("[metric] retries exhausted, circuit OPEN points=%d reason=%s kind=%s err=%v",
					n, DroppedExporterDown, kind, err)
				return
			}
			n := a.discard(points, DroppedExportFailed)
			log.Printf("[metric] retries exhausted, discard batch points=%d reason=%s kind=%s err=%v",
				n, DroppedExportFailed, kind, err)
			return
		}

		attempt++
		atomic.AddInt64(&a.st.ExportRetries, 1)
		log.Printf("[metric] export failed attempt=%d kind=%s err=%v, backoff=%s",
			attempt, kind, err, a.cfg.FailureBackoff)
		select {
		case <-time.After(a.cfg.FailureBackoff):
		case <-ctx.Done():
			n := a.discard(points, DroppedExportFailed)
			log.Printf("[metric] context done during backoff, discard points=%d reason=%s",
				n, DroppedExportFailed)
			return
		}
	}
}

// Close 停止 worker 并在 CloseTimeout 内尽力冲刷；可重复调用。
func (a *Agent) Close(ctx context.Context) error {
	if a.stopped.Swap(true) {
		return nil
	}
	close(a.stop)
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats 返回运行计数快照：原子计数精确，LastExport* 为加锁快照。
func (a *Agent) Stats() Stats {
	snap := Stats{
		Received:            atomic.LoadInt64(&a.st.Received),
		Enqueued:            atomic.LoadInt64(&a.st.Enqueued),
		Merged:              atomic.LoadInt64(&a.st.Merged),
		DroppedSeriesLimit:  atomic.LoadInt64(&a.st.DroppedSeriesLimit),
		ExportedPoints:      atomic.LoadInt64(&a.st.ExportedPoints),
		ExportedBatches:     atomic.LoadInt64(&a.st.ExportedBatches),
		DroppedQueueFull:    atomic.LoadInt64(&a.st.DroppedQueueFull),
		DroppedExporterDown: atomic.LoadInt64(&a.st.DroppedExporterDown),
		DroppedExportFailed: atomic.LoadInt64(&a.st.DroppedExportFailed),
		ExportRetries:       atomic.LoadInt64(&a.st.ExportRetries),
	}
	a.metaMu.RLock()
	snap.LastExportError = a.st.LastExportError
	snap.LastExportKind = a.st.LastExportKind
	snap.LastExportAt = a.st.LastExportAt
	a.metaMu.RUnlock()
	return snap
}
