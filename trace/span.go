package trace

import (
	"context"
	"sync"
	"time"
)

// SpanData 是一个已结束 Span 的快照，供导出使用。
type SpanData struct {
	TraceID      TraceID
	SpanID       SpanID
	ParentSpanID SpanID
	Name         string
	Start        time.Time
	End          time.Time
	Decision     Decision
	// SampleRate 记录该链路做根判定时使用的采样率，便于事后复算。
	SampleRate float64
	Status     string
}

// SpanSink 接收已结束的 Span。只有被采样（Decision != Drop）的 Span 才会发给 sink。
type SpanSink interface {
	ExportSpan(data SpanData) error
}

// Span 表示一个进行中的工作单元。内部用互斥锁保护，
// 可被多个 goroutine 并发地 SetStatus / End 而不会产生数据竞争。
type Span struct {
	mu   sync.Mutex
	data SpanData
	done bool
}

// StartSpan 从父 context 延续追踪上下文并开启一个新 Span。
//
// 跨异步边界的正确性保证：
//   - 追踪状态在入口处一次性以值类型提取，随后挂载到返回的新 context 上。
//     父 context 之后被取消、超时，只影响其取消信号，不影响已经复制出的追踪状态；
//   - 子 Span 沿用父链路的 TraceID、采样 Decision 与 SampleRate，仅更换 SpanID，
//     因此即使多个异步任务并发交错，也不会出现上下文错配或采样结论不一致；
//   - 父 context 不存在追踪上下文时，本 Span 成为链路根节点并做一次确定性采样。
func StartSpan(parent context.Context, name string, sampler Sampler) (*Span, context.Context) {
	now := time.Now()
	var data SpanData

	if tc, ok := FromContext(parent); ok {
		// 延续已有链路：复制一份值，不复用任何指针。
		data = SpanData{
			TraceID:      tc.TraceID,
			SpanID:       NewSpanID(),
			ParentSpanID: tc.SpanID,
			Name:         name,
			Start:        now,
			Decision:     tc.Decision,
			SampleRate:   tc.SampleRate,
			Status:       "OK",
		}
	} else {
		// 链路根节点：采样判定只发生在这里。
		root := Context{TraceID: NewTraceID()}
		rate := 1.0
		if sampler != nil {
			root.Decision = sampler.Sample(root, name)
			if ds, isDeterministic := sampler.(*DeterministicSampler); isDeterministic {
				rate = ds.Rate()
			}
		} else {
			root.Decision = DecisionRecordAndSample
		}
		root.SampleRate = rate
		root.SpanID = NewSpanID()
		data = SpanData{
			TraceID:    root.TraceID,
			SpanID:     root.SpanID,
			Name:       name,
			Start:      now,
			Decision:   root.Decision,
			SampleRate: rate,
			Status:     "OK",
		}
	}

	sp := &Span{data: data}
	return sp, ContextWith(context.Background(), sp.Context())
}

// Context 返回该 Span 对应的追踪上下文快照（值拷贝）。
func (sp *Span) Context() Context {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return Context{
		TraceID:      sp.data.TraceID,
		SpanID:       sp.data.SpanID,
		ParentSpanID: sp.data.ParentSpanID,
		Decision:     sp.data.Decision,
		SampleRate:   sp.data.SampleRate,
	}
}

// SetStatus 记录 Span 的结束状态（如 OK / ERROR）。
func (sp *Span) SetStatus(status string) {
	sp.mu.Lock()
	sp.data.Status = status
	sp.mu.Unlock()
}

// End 结束 Span 并尝试导出。Span 持有的追踪上下文是独立副本，
// 因此即使父任务已被取消，导出也不依赖父 context 的存活。
// 未被采样的 Span 不导出且返回 nil；重复 End 返回 nil（只导出一次）。
// sink 为 nil 表示不导出。导出失败时错误原样返回，绝不静默吞掉。
func (sp *Span) End(sink SpanSink) error {
	sp.mu.Lock()
	if sp.done {
		sp.mu.Unlock()
		return nil
	}
	sp.done = true
	if sp.data.End.IsZero() {
		sp.data.End = time.Now()
	}
	data := sp.data
	sp.mu.Unlock()

	if data.Decision == DecisionDrop {
		return nil
	}
	if sink == nil {
		return nil
	}
	return sink.ExportSpan(data)
}

// Data 返回 Span 数据快照。
func (sp *Span) Data() SpanData {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.data
}
