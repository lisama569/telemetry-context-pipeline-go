package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type (
	// TraceID 全链路唯一标识。
	TraceID [16]byte
	// SpanID 单个 span 标识。
	SpanID [8]byte
)

type ctxKey struct{}

// SpanContext 承载跨边界传递的追踪上下文。字段在派生时整体拷贝，
// 因此多个并发异步任务共享同一 traceID/采样结论，但各自持有独立副本，
// 不会因为 map 并发写而错配。
type SpanContext struct {
	TraceID        TraceID
	SpanID         SpanID
	Sampled        bool
	SamplingSource string
	SamplingReason string
	Baggage        map[string]string
}

// IsValid 表示是否持有合法的 traceID。
func (sc SpanContext) IsValid() bool {
	var zero TraceID
	return sc.TraceID != zero
}

// TraceIDHex 以十六进制输出 traceID，便于日志检索。
func (sc SpanContext) TraceIDHex() string { return hex.EncodeToString(sc.TraceID[:]) }

// SpanIDHex 以十六进制输出 spanID。
func (sc SpanContext) SpanIDHex() string { return hex.EncodeToString(sc.SpanID[:]) }

// Span 表示一次操作。
type Span struct {
	mu     sync.RWMutex
	sc     SpanContext
	parent *SpanID
	name   string
	start  time.Time
	end    time.Time
	ended  bool
}

// Tracer 创建 span 并把上下文写入 context.Context。
type Tracer struct {
	sampler Sampler
}

// NewTracer 创建追踪器。
func NewTracer(sampler Sampler) *Tracer {
	return &Tracer{sampler: sampler}
}

func randomTraceID() TraceID {
	var t TraceID
	_, _ = rand.Read(t[:])
	return t
}

func randomSpanID() SpanID {
	var s SpanID
	_, _ = rand.Read(s[:])
	return s
}

// Start 开启一个 span。
//
// 若 ctx 中存在父 SpanContext：沿用父 traceID 与采样结论，并复制一份
// baggage；否则生成新 traceID 并按采样器做根链路判定。判定结论
// （来源与依据）写入 SpanContext，随链路一并传递，保证可追溯。
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, *Span) {
	var sc SpanContext
	var parent *SpanID
	if p, ok := SpanFromContext(ctx); ok {
		d := t.sampler.Decide(p.TraceID, true, p.Sampled)
		sc = SpanContext{
			TraceID:        p.TraceID,
			SpanID:         randomSpanID(),
			Sampled:        d.Sampled,
			SamplingSource: d.Source,
			SamplingReason: d.Reason,
			Baggage:        cloneBaggage(p.Baggage),
		}
		pid := p.SpanID
		parent = &pid
	} else {
		tid := randomTraceID()
		d := t.sampler.Decide(tid, false, false)
		sc = SpanContext{
			TraceID:        tid,
			SpanID:         randomSpanID(),
			Sampled:        d.Sampled,
			SamplingSource: d.Source,
			SamplingReason: d.Reason,
			Baggage:        map[string]string{},
		}
	}
	sp := &Span{sc: sc, parent: parent, name: name, start: time.Now()}
	return context.WithValue(ctx, ctxKey{}, &sp.sc), sp
}

// ContextWithSpan 把 SpanContext 放入 context。
func ContextWithSpan(ctx context.Context, sc *SpanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, sc)
}

// SpanFromContext 从 context 取出 SpanContext。
func SpanFromContext(ctx context.Context) (*SpanContext, bool) {
	sc, ok := ctx.Value(ctxKey{}).(*SpanContext)
	if !ok || sc == nil {
		return nil, false
	}
	return sc, true
}

// ContinueAsync 派生一个适合“异步任务”使用的新 context：
//
//   - 完整保留追踪上下文（traceID / 采样结论 / baggage），因此父任务
//     被取消或超时不会导致异步任务丢失链路；
//   - 不继承父 context 的取消信号、超时与 deadline，避免父请求返回后
//     异步任务被连带取消；
//   - 返回独立的 CancelFunc，调用方可自行管理异步任务的生命周期。
//
// ctx 中不存在追踪上下文时，返回的 context 同样不含追踪上下文。
func ContinueAsync(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if sc, ok := SpanFromContext(ctx); ok {
		cp := *sc
		cp.Baggage = cloneBaggage(sc.Baggage)
		base = ContextWithSpan(base, &cp)
	}
	return context.WithCancel(base)
}

func cloneBaggage(b map[string]string) map[string]string {
	out := make(map[string]string, len(b))
	for k, v := range b {
		out[k] = v
	}
	return out
}

// Context 返回 span 上下文的一个独立副本，调用方修改不影响 span 内部状态。
func (s *Span) Context() SpanContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := s.sc
	cp.Baggage = cloneBaggage(s.sc.Baggage)
	return cp
}

// Name 返回 span 名称。
func (s *Span) Name() string { return s.name }

// SetBaggage 在 span 上写入透传 baggage（copy-on-write，并发安全）。
func (s *Span) SetBaggage(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneBaggage(s.sc.Baggage)
	next[key] = value
	s.sc.Baggage = next
}

// Baggage 读取一个 baggage 值。
func (s *Span) Baggage(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.sc.Baggage[key]
	return v, ok
}

// End 结束 span，重复调用只生效一次。
func (s *Span) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended {
		s.ended = true
		s.end = time.Now()
	}
}
