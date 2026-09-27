package pipeline

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/telemetry-context-pipeline-go/logs"
	"github.com/lisama569/telemetry-context-pipeline-go/metrics"
	"github.com/lisama569/telemetry-context-pipeline-go/trace"
)

// spanSink 收集 Span，便于核对同链路采样结论。
type spanSink struct {
	mu    sync.Mutex
	spans []trace.SpanData
}

func (s *spanSink) ExportSpan(d trace.SpanData) error {
	s.mu.Lock()
	s.spans = append(s.spans, d)
	s.mu.Unlock()
	return nil
}

func (s *spanSink) all() []trace.SpanData {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]trace.SpanData, len(s.spans))
	copy(out, s.spans)
	return out
}

// TestPipeline_EndToEnd_AsyncCancellation 模拟真实业务：
// 根任务开启若干异步 worker，父 context 超时取消后 worker 仍继续，
// 要求：Span/日志的 TraceID 与采样判定全程一致；属性经统一 Processor 脱敏。
func TestPipeline_EndToEnd_AsyncCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Metrics.FlushIntervalMs = 20
	cfg.Metrics.ChannelBuffer = 128
	cfg.Attributes.SensitiveKeys = map[string]struct{}{"api_key": {}}

	metricExp := metrics.NewFlakyExporter(false, nil)
	logSink := &logs.RecordingSink{}
	spans := &spanSink{}
	p := New(cfg, metricExp, logSink)

	pumpDone := make(chan struct{})
	go func() { _ = p.Run(context.Background()); close(pumpDone) }()

	// reqCtx 携带请求的取消信号；span 上下文只携带追踪状态（按设计脱离取消信号）。
	reqCtx, reqCancel := context.WithCancel(context.Background())
	root, rootCtx := p.StartSpan(reqCtx, "POST /checkout")
	rootTC := root.Context()
	_ = reqCancel

	// 派 8 个异步 worker，随后立即取消父任务（模拟请求超时）。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sp, spCtx := p.StartSpan(rootCtx, "async-charge")
			defer sp.End(spans)
			<-reqCtx.Done() // 等请求取消后再继续，验证追踪传播不依赖请求 context 存活

			if sp.Context().TraceID != rootTC.TraceID || sp.Context().Decision != rootTC.Decision {
				t.Errorf("worker %d 上下文错配", i)
			}
			p.Logger.Info(spCtx, "charge attempted", map[string]string{
				"api_key": "sk-live-xxx",
				"worker":  string(rune('0' + i)),
			})
			if r := p.Aggregator.Record(metrics.Sample{
				ID:    metrics.SeriesID{Name: "charges", Labels: map[string]string{"worker": string(rune('0' + i))}},
				Kind:  metrics.KindCounter,
				Value: 1,
			}); r != metrics.DropNone {
				t.Errorf("worker %d 指标写入被意外丢弃: %s", i, r)
			}
		}(i)
	}
	reqCancel() // 父请求超时取消：worker 收到信号后继续把指标/日志写完
	wg.Wait()
	if err := root.End(spans); err != nil {
		t.Fatalf("根 Span 结束失败: %v", err)
	}

	// 关停：最终 flush + 取得降级事件。
	drops, err := p.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown 失败: %v", err)
	}
	<-pumpDone

	// 1) 追踪：9 个 Span 同 TraceID、同采样判定。
	allSpans := spans.all()
	if len(allSpans) != 9 {
		t.Fatalf("应导出 1 根 + 8 worker Span，实际 %d", len(allSpans))
	}
	for _, sd := range allSpans {
		if sd.TraceID != rootTC.TraceID {
			t.Fatalf("Span %s 的 TraceID 错配", sd.Name)
		}
		if sd.Decision != rootTC.Decision {
			t.Fatalf("Span %s 采样判定错配: %s vs %s", sd.Name, sd.Decision, rootTC.Decision)
		}
	}

	// 2) 日志：8 条全部带同一 TraceID，敏感 api_key 一律脱敏。
	recs := logSink.Records()
	if len(recs) != 8 {
		t.Fatalf("应有 8 条 worker 日志，实际 %d", len(recs))
	}
	for _, r := range recs {
		if r.Trace.TraceID != rootTC.TraceID {
			t.Fatalf("日志 trace=%s 与根链路不一致", r.Trace.TraceID)
		}
		if r.Attrs["api_key"] != "[REDACTED]" {
			t.Fatalf("日志中 api_key 必须脱敏，得到 %q", r.Attrs["api_key"])
		}
	}

	// 3) 指标：8 个点在取消后的最终 flush 中全部到达下游。
	var charges float64
	for _, sn := range metricExp.LastSnapshot() {
		if sn.ID.Name == "charges" {
			charges += sn.Value
		}
	}
	log.Printf("[关键上下文] 端到端 trace=%s 采样=%s：9 Span + 8 日志同链路，api_key 全脱敏，charges 导出合计=%v，关停丢弃事件=%d",
		rootTC.TraceID, rootTC.Decision, charges, len(drops))
	if charges != 8 {
		t.Fatalf("取消后最终 flush 应导出 8 个 charges 点，实际 %v", charges)
	}
}

// TestPipeline_ShutdownReportsDrops 下游持续不可用时，Shutdown 返回的丢弃事件
// 必须带可区分原因，调用方可据此告警，而不是静默丢失。
func TestPipeline_ShutdownReportsDrops(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Metrics.FlushIntervalMs = 15
	cfg.Metrics.RetryBuffer = 1
	cfg.Metrics.ChannelBuffer = 64

	p := New(cfg, metrics.NewFlakyExporter(true, nil), &logs.RecordingSink{})
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	for i := 0; i < 5; i++ {
		p.Aggregator.Record(metrics.Sample{
			ID:    metrics.SeriesID{Name: "m", Labels: map[string]string{"i": string(rune('a' + i))}},
			Kind:  metrics.KindCounter,
			Value: 1,
		})
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	drops, _ := p.Shutdown(context.Background())

	reasonSet := map[metrics.DropReason]bool{}
	for _, d := range drops {
		reasonSet[d.Reason] = true
	}
	log.Printf("[判定依据] 下游持续失败，Shutdown 上报丢弃原因集合=%v", reasonSet)
	if !reasonSet[metrics.DropBufferFull] && !reasonSet[metrics.DropExportFailed] {
		t.Fatalf("应至少上报 buffer_full 或 export_failed，得到 %v", reasonSet)
	}
}
