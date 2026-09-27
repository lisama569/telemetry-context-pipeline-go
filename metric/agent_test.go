package metric

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/telemetry-context-pipeline-go/attribute"
	"github.com/lisama569/telemetry-context-pipeline-go/config"
)

// mockExporter 可编程控制成功/失败/阻塞，并记录导出的数据点。
type mockExporter struct {
	mu       sync.Mutex
	batches  [][]DataPoint
	failKind FailureKind // 非空则返回该类错误
	failN    int         // 前 failN 次失败，之后成功（-1 表示恒失败）
	calls    int
	blockCh  chan struct{} // 非 nil 时阻塞直到该通道关闭
}

func (m *mockExporter) Export(ctx context.Context, points []DataPoint) error {
	m.mu.Lock()
	m.calls++
	n := m.calls
	block := m.blockCh
	kind := m.failKind
	failN := m.failN
	m.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return NewExportError(FailureTimeout, ctx.Err())
		}
	}
	// 记录入站批次（拷贝 label，避免与聚合器复用产生竞争观感）
	cp := make([]DataPoint, len(points))
	copy(cp, points)
	m.mu.Lock()
	m.batches = append(m.batches, cp)
	m.mu.Unlock()

	shouldFail := failN < 0 || n <= failN
	if shouldFail && kind != "" {
		return NewExportError(kind, errors.New("mock "+string(kind)))
	}
	return nil
}

func (m *mockExporter) pointCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, b := range m.batches {
		n += len(b)
	}
	return n
}

func init() { log.SetOutput(io.Discard) }

func testMetricCfg() config.MetricConfig {
	return config.MetricConfig{
		QueueSize:            64,
		BatchSize:            10,
		ExportInterval:       50 * time.Millisecond,
		ExportTimeout:        200 * time.Millisecond,
		MaxRetries:           2,
		FailureBackoff:       10 * time.Millisecond,
		UnavailableThreshold: 3,
		UnavailableCooldown:  300 * time.Millisecond,
		CloseTimeout:         500 * time.Millisecond,
	}
}

func newTestAgent(t *testing.T, exp Exporter) *Agent {
	t.Helper()
	proc := attribute.New(config.AttributeConfig{
		MaxKeysPerRecord: 16, MaxValuesPerKey: 64, MaxValueLength: 128,
		RedactedPlaceholder: "[REDACTED]", OtherBucket: "other",
	})
	cfg := testMetricCfg()
	cfg.QueueSize = 8192
	a := NewAgent(cfg, exp, proc).WithMaxSeries(1000)
	a.Start(context.Background())
	return a
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("等待超时: " + msg)
}

// 场景1：并发写入下聚合正确。
func TestConcurrentWriteAggregation(t *testing.T) {
	exp := &mockExporter{}
	a := newTestAgent(t, exp)

	const goroutines, perG = 100, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				a.Record(Sample{Name: "requests", Kind: Counter, Value: 1,
					Labels: Labels{"svc": "a"}})
			}
		}()
	}
	wg.Wait()

	waitFor(t, func() bool {
		return a.Stats().Merged == goroutines*perG
	}, 2*time.Second, "全部采样完成聚合")
	// 再等数据点被导出
	waitFor(t, func() bool { return exp.pointCount() >= 1 }, 2*time.Second, "聚合数据导出")

	s := a.Stats()
	total := int64(0)
	exp.mu.Lock()
	for _, b := range exp.batches {
		for _, dp := range b {
			if dp.Name == "requests" {
				total += int64(dp.Count)
			}
		}
	}
	exp.mu.Unlock()
	if total != goroutines*perG {
		t.Fatalf("聚合总数=%d want %d; stats=%+v", total, goroutines*perG, s)
	}
	t.Logf("并发写入 %d 条全部聚合导出, merged=%d exported_points=%d batches=%d",
		goroutines*perG, s.Merged, s.ExportedPoints, s.ExportedBatches)
}

// 场景2：有界队列满时非阻塞丢弃并计数，业务调用不被阻塞。
func TestQueueFullDropsNonBlocking(t *testing.T) {
	block := make(chan struct{})
	exp := &mockExporter{blockCh: block} // 导出永远阻塞，数据靠手动控制
	proc := attribute.New(config.AttributeConfig{RedactedPlaceholder: "X", OtherBucket: "o"})
	cfg := testMetricCfg()
	cfg.QueueSize = 4
	a := NewAgent(cfg, exp, proc).WithMaxSeries(1000)
	// 不 Start：队列无人消费，快速填满，纯测 Record 语义
	var accepted, dropped int64
	start := time.Now()
	for i := 0; i < 100; i++ {
		ok, reason := a.Record(Sample{Name: "m", Kind: Counter, Value: 1,
			Labels: Labels{"i": string(rune('a' + i%4))}})
		if ok {
			accepted++
		} else {
			if reason != DroppedQueueFull {
				t.Fatalf("丢弃原因=%s want %s", reason, DroppedQueueFull)
			}
			dropped++
		}
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Record 阻塞了业务调用: %v", elapsed)
	}
	if accepted != 4 || dropped != 96 {
		t.Fatalf("accepted=%d dropped=%d, want 4/96", accepted, dropped)
	}
	if a.Stats().DroppedQueueFull != 96 {
		t.Fatalf("统计 dropped_queue_full=%d want 96", a.Stats().DroppedQueueFull)
	}
	close(block)
	t.Logf("队列满时 %d 条立即降级丢弃, 0 阻塞, 耗时 %v", dropped, time.Since(start))
}

// 场景3：下游不可用（unavailable，可重试）→ 重试耗尽 → 丢弃并可区分。
func TestExportUnavailableRetryThenDrop(t *testing.T) {
	exp := &mockExporter{failKind: FailureUnavailable, failN: -1}
	a := newTestAgent(t, exp)
	a.Record(Sample{Name: "m", Kind: Counter, Value: 1})

	waitFor(t, func() bool {
		s := a.Stats()
		return s.DroppedExportFailed+s.DroppedExporterDown >= 1
	}, 2*time.Second, "重试耗尽后丢弃")

	s := a.Stats()
	if s.ExportRetries != int64(testMetricCfg().MaxRetries) {
		t.Fatalf("重试次数=%d want %d", s.ExportRetries, testMetricCfg().MaxRetries)
	}
	if s.LastExportKind != FailureUnavailable {
		t.Fatalf("失败类别=%q want unavailable", s.LastExportKind)
	}
	if s.DroppedExportFailed+s.DroppedExporterDown == 0 {
		t.Fatal("没有任何丢弃计数")
	}
	t.Logf("下游不可用: retries=%d last_kind=%s dropped_failed=%d dropped_down=%d err=%s",
		s.ExportRetries, s.LastExportKind, s.DroppedExportFailed,
		s.DroppedExporterDown, s.LastExportError)
}

// 场景4：被下游拒绝（rejected，不可重试）→ 立即丢弃，无重试。
func TestExportRejectedNoRetry(t *testing.T) {
	exp := &mockExporter{failKind: FailureRejected, failN: -1}
	a := newTestAgent(t, exp)
	a.Record(Sample{Name: "m", Kind: Counter, Value: 2})

	waitFor(t, func() bool {
		return a.Stats().DroppedExportFailed >= 1
	}, time.Second, "rejected 立即丢弃")

	s := a.Stats()
	if s.ExportRetries != 0 {
		t.Fatalf("rejected 不应重试, retries=%d", s.ExportRetries)
	}
	if s.LastExportKind != FailureRejected {
		t.Fatalf("失败类别=%q want rejected", s.LastExportKind)
	}
	t.Logf("被下游拒绝: 0 重试, dropped_failed=%d, kind 可区分=%s",
		s.DroppedExportFailed, s.LastExportKind)
}

// 场景5：导出超时 → 归类 timeout 并走重试。
func TestExportTimeoutClassified(t *testing.T) {
	exp := &mockExporter{blockCh: make(chan struct{})} // 永不释放
	a := newTestAgent(t, exp)
	a.Record(Sample{Name: "m", Kind: Counter, Value: 1})

	waitFor(t, func() bool {
		s := a.Stats()
		return s.LastExportKind == FailureTimeout
	}, 2*time.Second, "超时被归类")
	s := a.Stats()
	t.Logf("导出超时归类 kind=%s retries=%d err=%q", s.LastExportKind, s.ExportRetries, s.LastExportError)
}

// 场景6：连续失败达阈值后熔断，冷却期内批次按 exporter_down 丢弃；恢复后可半开成功。
func TestCircuitBreakerOpenAndRecover(t *testing.T) {
	// 前 30 次导出失败（覆盖熔断打开与整个冷却期），之后成功
	exp := &mockExporter{failKind: FailureUnavailable, failN: 30}
	a := newTestAgent(t, exp)

	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				a.Record(Sample{Name: "m", Kind: Counter, Value: 1})
			case <-stop:
				return
			}
		}
	}()

	waitFor(t, func() bool {
		return a.Stats().DroppedExporterDown >= 1
	}, 3*time.Second, "熔断打开并降级")
	s := a.Stats()
	t.Logf("熔断打开: dropped_down=%d dropped_failed=%d retries=%d",
		s.DroppedExporterDown, s.DroppedExportFailed, s.ExportRetries)
	if s.DroppedExporterDown == 0 {
		t.Fatal("熔断期间的批次应记为 exporter_down，与 export_failed 区分")
	}

	// mock 失败次数耗尽后，冷却结束的下一次探测应成功，熔断恢复
	waitFor(t, func() bool {
		return a.Stats().ExportedBatches >= 1
	}, 4*time.Second, "冷却后半开恢复")
	close(stop)
	s = a.Stats()
	t.Logf("下游恢复后导出成功: exported_batches=%d exported_points=%d dropped_down=%d",
		s.ExportedBatches, s.ExportedPoints, s.DroppedExporterDown)
}

// 场景7：运行 context 被取消时，Close 在超时内尽力冲刷。
func TestCloseFlushesOnCancel(t *testing.T) {
	exp := &mockExporter{}
	proc := attribute.New(config.AttributeConfig{RedactedPlaceholder: "X", OtherBucket: "o"})
	a := NewAgent(testMetricCfg(), exp, proc)
	root, cancel := context.WithCancel(context.Background())
	a.Start(root)
	a.Record(Sample{Name: "m", Kind: Counter, Value: 7})

	cancel() // 模拟整体取消
	cctx, cc := context.WithTimeout(context.Background(), time.Second)
	defer cc()
	if err := a.Close(cctx); err != nil {
		t.Fatalf("Close 返回错误: %v", err)
	}
	if exp.pointCount() == 0 {
		t.Fatal("取消后已有数据未被冲刷")
	}
	found := false
	exp.mu.Lock()
	for _, b := range exp.batches {
		for _, dp := range b {
			if dp.Name == "m" && dp.Value == 7 {
				found = true
			}
		}
	}
	exp.mu.Unlock()
	if !found {
		t.Fatal("冲刷的数据点与写入不符")
	}
	t.Logf("取消后 Close 成功冲刷 %d 个数据点", exp.pointCount())
}

// 场景8：敏感 label 在入队前已脱敏。
func TestRecordMasksSensitiveLabels(t *testing.T) {
	exp := &mockExporter{}
	cfg := testMetricCfg()
	proc := attribute.New(config.AttributeConfig{
		MaxKeysPerRecord: 8, MaxValuesPerKey: 32, MaxValueLength: 64,
		SensitiveKeys: []string{"token"}, RedactedPlaceholder: "[REDACTED]",
		OtherBucket: "other",
	})
	a := NewAgent(cfg, exp, proc)
	a.Start(context.Background())
	a.Record(Sample{Name: "m", Kind: Counter, Value: 1,
		Labels: Labels{"token": "abc123", "svc": "api"}})

	waitFor(t, func() bool { return exp.pointCount() >= 1 }, time.Second, "导出")
	exp.mu.Lock()
	defer exp.mu.Unlock()
	for _, b := range exp.batches {
		for _, dp := range b {
			if dp.Labels["token"] != "[REDACTED]" {
				t.Fatalf("敏感 label 透出: %v", dp.Labels)
			}
		}
	}
	if atomic.LoadInt64(&a.st.Received) != 1 {
		t.Fatal("received 计数错误")
	}
	t.Log("敏感 label 在指标入口同样被脱敏，与其它入口一致")
}
