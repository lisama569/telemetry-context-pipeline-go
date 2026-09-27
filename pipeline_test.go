package telemetry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/telemetry-context-pipeline-go/config"
	"github.com/lisama569/telemetry-context-pipeline-go/logx"
	"github.com/lisama569/telemetry-context-pipeline-go/metric"
	"github.com/lisama569/telemetry-context-pipeline-go/tracing"
)

type collectExporter struct {
	mu      sync.Mutex
	points  []metric.DataPoint
	failAll bool
}

func (c *collectExporter) Export(_ context.Context, pts []metric.DataPoint) error {
	if c.failAll {
		return metric.NewExportError(metric.FailureUnavailable, errors.New("down"))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.points = append(c.points, pts...)
	return nil
}

type bufSink struct {
	mu      sync.Mutex
	entries []logx.Entry
}

func (b *bufSink) Write(_ context.Context, e logx.Entry) error {
	b.mu.Lock()
	b.entries = append(b.entries, e)
	b.mu.Unlock()
	return nil
}

// 端到端：父请求取消后，异步任务仍在同一 trace 上写日志与指标，
// 敏感字段全程脱敏，指标最终被导出。
func TestPipelineAsyncTraceAfterCancel(t *testing.T) {
	cfg := config.Default()
	cfg.SampleRate = 1
	cfg.Metrics.ExportInterval = 30 * time.Millisecond
	cfg.Metrics.CloseTimeout = time.Second
	exp := &collectExporter{}
	sink := &bufSink{}

	p, err := New(cfg, exp, sink)
	if err != nil {
		t.Fatalf("构建管线失败: %v", err)
	}
	root, cancelRoot := context.WithCancel(context.Background())
	p.Start(root)

	ctx, span := p.Tracer.Start(root, "handle-request")
	traceID := span.Context().TraceIDHex()

	// 派生异步上下文：与父请求的生命周期解耦
	actx, cancelAsync := tracing.ContinueAsync(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, job := p.Tracer.Start(actx, "async-job")
		defer job.End()
		p.Logs.Info(actx, "async started", map[string]any{
			"password": "secret123", "task": "cleanup",
		})
		p.Metrics.Record(metric.Sample{
			Name: "async_jobs", Kind: metric.Counter, Value: 1,
			Labels: metric.Labels{"token": "tok-x", "kind": "cleanup"},
		})
	}()

	// 父请求立即取消（超时/客户端断开）
	cancelRoot()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("异步任务被父取消连带取消")
	}
	cancelAsync()

	// 关闭应冲刷异步任务产生的指标
	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	if err := p.Shutdown(sctx); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}

	// 断言1：异步日志与父请求同 trace，且敏感字段脱敏
	sink.mu.Lock()
	logs := append([]logx.Entry(nil), sink.entries...)
	sink.mu.Unlock()
	if len(logs) != 1 {
		t.Fatalf("日志条数=%d want 1", len(logs))
	}
	if logs[0].TraceID != traceID {
		t.Fatalf("异步日志 traceID 错配: got %s want %s", logs[0].TraceID, traceID)
	}
	if logs[0].Attrs["password"] != "[REDACTED]" {
		t.Fatalf("异步日志敏感字段透出: %v", logs[0].Attrs)
	}

	// 断言2：异步指标被导出，敏感 label 脱敏
	exp.mu.Lock()
	found := false
	for _, dp := range exp.points {
		if dp.Name == "async_jobs" {
			found = true
			if dp.Labels["token"] != "[REDACTED]" {
				t.Fatalf("敏感 label 透出: %v", dp.Labels)
			}
		}
	}
	exp.mu.Unlock()
	if !found {
		t.Fatal("异步任务指标未被导出")
	}

	t.Logf("端到端成立: trace=%s 在父取消后仍由异步任务延续; 日志/指标敏感字段均已脱敏", traceID)
}

// 端到端降级：导出器持续不可用时，管线不阻塞、不丢内存，丢弃计数与失败类别可查。
func TestPipelineDegradesWhenExporterDown(t *testing.T) {
	cfg := config.Default()
	cfg.SampleRate = 1
	cfg.Metrics.ExportInterval = 20 * time.Millisecond
	cfg.Metrics.MaxRetries = 1
	cfg.Metrics.FailureBackoff = 5 * time.Millisecond
	cfg.Metrics.ExportTimeout = 100 * time.Millisecond
	cfg.Metrics.UnavailableThreshold = 2
	cfg.Metrics.UnavailableCooldown = 80 * time.Millisecond
	cfg.Metrics.CloseTimeout = 200 * time.Millisecond

	exp := &collectExporter{failAll: true}
	p, err := New(cfg, exp, &bufSink{})
	if err != nil {
		t.Fatalf("构建失败: %v", err)
	}
	p.Start(context.Background())

	for i := 0; i < 5; i++ {
		ok, reason := p.Metrics.Record(metric.Sample{
			Name: "m", Kind: metric.Counter, Value: 1,
			Labels: metric.Labels{"i": string(rune('a' + i))},
		})
		if !ok {
			t.Fatalf("队列不应满, reason=%s", reason)
		}
		time.Sleep(40 * time.Millisecond)
	}

	// 等待熔断打开并出现 exporter_down 降级
	deadline := time.Now().Add(3 * time.Second)
	var st metric.Stats
	for time.Now().Before(deadline) {
		st = p.Metrics.Stats()
		if st.DroppedExporterDown > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st.DroppedExporterDown == 0 {
		t.Fatalf("未观察到熔断降级, stats=%+v", st)
	}
	if st.LastExportKind != metric.FailureUnavailable {
		t.Fatalf("失败类别=%q want unavailable", st.LastExportKind)
	}

	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	defer scancel()
	_ = p.Shutdown(sctx) // 下游仍挂，关闭应返回而非挂死
	st = p.Metrics.Stats()
	t.Logf("降级可解释: received=%d merged=%d retries=%d dropped_failed=%d dropped_down=%d kind=%s err=%q",
		st.Received, st.Merged, st.ExportRetries, st.DroppedExportFailed,
		st.DroppedExporterDown, st.LastExportKind, st.LastExportError)
}
