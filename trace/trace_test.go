package trace

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"
)

// recordingSink 记录导出的 Span，并可配置返回错误。
type recordingSink struct {
	mu    sync.Mutex
	spans []SpanData
	err   error
}

func (s *recordingSink) ExportSpan(d SpanData) error {
	s.mu.Lock()
	s.spans = append(s.spans, d)
	s.mu.Unlock()
	return s.err
}

func (s *recordingSink) snapshot() []SpanData {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SpanData, len(s.spans))
	copy(out, s.spans)
	return out
}

// TestDeterministicSampler_RateBoundaries 验证采样率边界：0 全丢、1 全采。
func TestDeterministicSampler_RateBoundaries(t *testing.T) {
	root := Context{TraceID: NewTraceID()}

	s0 := NewDeterministicSampler(0)
	if got := s0.Sample(root, "op"); got != DecisionDrop {
		t.Fatalf("rate=0 时应全部 DROP，得到 %s", got)
	}
	log.Printf("[判定依据] rate=0 TraceID=%s -> DROP", root.TraceID)

	s1 := NewDeterministicSampler(1)
	if got := s1.Sample(root, "op"); got != DecisionRecordAndSample {
		t.Fatalf("rate=1 时应全部 RECORD_AND_SAMPLE，得到 %s", got)
	}
	log.Printf("[判定依据] rate=1 TraceID=%s -> RECORD_AND_SAMPLE", root.TraceID)

	// 超范围采样率必须被夹取。
	sBad := NewDeterministicSampler(5)
	if sBad.Rate() != 1 {
		t.Fatalf("超界采样率应夹取到 1，得到 %v", sBad.Rate())
	}
}

// TestDeterministicSampler_SameTraceSameDecision 同一 TraceID 反复判定结论必须一致，
// 不同入口（多次调用、不同 span 名）也不能改变结论。
func TestDeterministicSampler_SameTraceSameDecision(t *testing.T) {
	s := NewDeterministicSampler(0.5)
	var sampled, dropped int
	for i := 0; i < 1000; i++ {
		root := Context{TraceID: NewTraceID()}
		first := s.Sample(root, "entry-A")
		second := s.Sample(root, "entry-B")
		if first != second {
			t.Fatalf("同一 TraceID=%s 在不同入口判定不一致: %s vs %s", root.TraceID, first, second)
		}
		if first == DecisionRecordAndSample {
			sampled++
		} else {
			dropped++
		}
	}
	log.Printf("[判定依据] rate=0.5 1000 条链路 sampled=%d dropped=%d，同链路跨入口结论一致", sampled, dropped)
	if sampled == 0 || dropped == 0 {
		t.Fatalf("rate=0.5 应同时出现采样与丢弃，sampled=%d dropped=%d", sampled, dropped)
	}
}

// TestStartSpan_ChildInheritsDecision 子 Span 必须沿用父链路的 TraceID 与采样判定，
// 即使此时采样率已经被动态改掉，已存在链路也不能翻转。
func TestStartSpan_ChildInheritsDecision(t *testing.T) {
	s := NewDeterministicSampler(1)
	root, rootCtx := StartSpan(context.Background(), "root", s)
	rootTC := root.Context()

	s.SetRate(0) // 之后的新链路全部丢弃，但本链路不应受影响。
	child, _ := StartSpan(rootCtx, "child", s)
	childTC := child.Context()

	if childTC.TraceID != rootTC.TraceID {
		t.Fatalf("子 Span TraceID=%s 与父=%s 不一致", childTC.TraceID, rootTC.TraceID)
	}
	if childTC.Decision != rootTC.Decision || childTC.Decision != DecisionRecordAndSample {
		t.Fatalf("子 Span 采样判定=%s，应沿用父链路 %s", childTC.Decision, rootTC.Decision)
	}
	if childTC.ParentSpanID != rootTC.SpanID {
		t.Fatalf("子 Span ParentSpanID=%s 应指向父 SpanID=%s", childTC.ParentSpanID, rootTC.SpanID)
	}
	log.Printf("[判定依据] trace=%s 根判定=%s 在 SetRate(0) 后子 Span 仍为 %s",
		rootTC.TraceID, rootTC.Decision, childTC.Decision)
}

// TestStartSpan_PropagatesAcrossCancellation 父 context 被取消/超时后，
// 已经提取出的追踪上下文仍能在异步任务中正确延续，不丢失、不错配。
func TestStartSpan_PropagatesAcrossCancellation(t *testing.T) {
	sink := &recordingSink{}
	sampler := NewDeterministicSampler(1)
	root, rootCtx := StartSpan(context.Background(), "root", sampler)
	rootTC := root.Context()

	// 模拟父任务在派活后立刻超时取消。
	workerCtx, cancel := context.WithTimeout(rootCtx, time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	results := make(chan Context, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-workerCtx.Done() // 显式等到父 context 已取消
			// 关键：StartSpan 只在入口快照一次追踪状态，父取消不影响传播。
			sp, ctx := StartSpan(workerCtx, "async-work", sampler)
			if sp == nil {
				t.Errorf("父 context 取消后异步 Span 创建失败")
				return
			}
			tc := sp.Context()
			results <- tc
			if err := sp.End(sink); err != nil {
				t.Errorf("异步 Span 导出失败: %v", err)
			}
			// 新 context 必须不携带父取消信号，否则下游会立刻看到 ctx.Err。
			if ctx.Err() != nil {
				t.Errorf("子 Span context 不应继承父取消信号，得到 %v", ctx.Err())
			}
		}()
	}
	wg.Wait()
	close(results)

	count := 0
	for tc := range results {
		count++
		if tc.TraceID != rootTC.TraceID {
			t.Fatalf("异步任务上下文错配: 得到 trace=%s，期望 %s", tc.TraceID, rootTC.TraceID)
		}
		if tc.Decision != rootTC.Decision {
			t.Fatalf("异步任务采样判定错配: %s vs %s", tc.Decision, rootTC.Decision)
		}
	}
	if count != 20 {
		t.Fatalf("应有 20 个异步任务延续上下文，实际 %d", count)
	}
	log.Printf("[关键上下文] trace=%s 在父 context 超时取消后，%d 个并发异步任务全部正确延续判定 %s",
		rootTC.TraceID, count, rootTC.Decision)

	if err := root.End(sink); err != nil {
		t.Fatalf("根 Span 导出失败: %v", err)
	}
	if got := len(sink.snapshot()); got != 21 {
		t.Fatalf("应导出 1 根 + 20 子 Span，实际 %d", got)
	}
}

// TestStartSpan_ConcurrentInterleaving 多个不同链路并发交错时不得互相串上下文。
func TestStartSpan_ConcurrentInterleaving(t *testing.T) {
	s := NewDeterministicSampler(1)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root, ctx := StartSpan(context.Background(), "root", s)
			want := root.Context()
			child, _ := StartSpan(ctx, "child", s)
			got := child.Context()
			if got.TraceID != want.TraceID || got.ParentSpanID != want.SpanID {
				t.Errorf("并发交错导致上下文串线: got trace=%s parent=%s, want trace=%s span=%s",
					got.TraceID, got.ParentSpanID, want.TraceID, want.SpanID)
			}
		}()
	}
	wg.Wait()
	log.Printf("[关键上下文] 50 条独立链路并发交错，未发生 TraceID/ParentSpanID 串线")
}

// TestSpan_ExportErrorPropagated 导出失败必须把错误返回给调用方，不得静默吞掉；
// 未被采样的 Span 不导出；重复 End 只导出一次。
func TestSpan_ExportErrorPropagated(t *testing.T) {
	exportErr := errors.New("collector down")
	sink := &recordingSink{err: exportErr}

	s := NewDeterministicSampler(1)
	sp, _ := StartSpan(context.Background(), "sampled", s)
	if err := sp.End(sink); !errors.Is(err, exportErr) {
		t.Fatalf("导出失败应原样返回错误，得到 %v", err)
	}
	if err := sp.End(sink); err != nil {
		t.Fatalf("重复 End 不应再次导出/报错，得到 %v", err)
	}
	log.Printf("[判定依据] 导出失败原因可区分: %v；重复 End 安全", exportErr)

	dropSampler := NewDeterministicSampler(0)
	sp2, _ := StartSpan(context.Background(), "dropped", dropSampler)
	if err := sp2.End(sink); err != nil {
		t.Fatalf("未采样 Span 不应导出也不应报错，得到 %v", err)
	}
	if len(sink.snapshot()) != 1 {
		t.Fatalf("未采样 Span 不应到达 sink")
	}
	log.Printf("[判定依据] DROP 的 Span 不导出，sink 仅收到 1 条")
}
