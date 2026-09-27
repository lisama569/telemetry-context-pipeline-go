package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// TraceID 与 SpanID 分别标识一条调用链与链路上的一个工作单元。
type TraceID [16]byte
type SpanID [8]byte

// Decision 表示采样判定结论。整条链路必须得到同一个结论。
type Decision int

const (
	DecisionUnknown Decision = iota
	DecisionDrop
	DecisionRecord
	DecisionRecordAndSample
)

// String 返回判定结论的可读名称。
func (d Decision) String() string {
	switch d {
	case DecisionDrop:
		return "DROP"
	case DecisionRecord:
		return "RECORD"
	case DecisionRecordAndSample:
		return "RECORD_AND_SAMPLE"
	default:
		return "UNKNOWN"
	}
}

// Context 携带跨异步边界需要延续的追踪状态。
// 该结构为纯值类型：放入 context.Context 与跨 goroutine 传递时都是副本，
// 因此父任务取消、超时或多个异步任务并发交错都不会改写彼此的状态。
type Context struct {
	TraceID      TraceID
	SpanID       SpanID
	ParentSpanID SpanID
	Decision     Decision
	// SampleRate 记录得出当前判定时使用的采样率，保证同一链路结论可追溯。
	SampleRate float64
}

// key 是 context.Context 中的键类型，避免与其他包冲突。
type key struct{}

// ContextWith 将追踪上下文挂载到 context.Context 上。
// 传入的 tc 是值拷贝，调用方后续修改不会影响 context 内的副本。
func ContextWith(ctx context.Context, tc Context) context.Context {
	return context.WithValue(ctx, key{}, tc)
}

// FromContext 取出追踪上下文；不存在时 ok 为 false，
// 调用方应把零值 Context 与“链路根节点”区分开来。
func FromContext(ctx context.Context) (Context, bool) {
	if ctx == nil {
		return Context{}, false
	}
	tc, ok := ctx.Value(key{}).(Context)
	return tc, ok
}

// NewTraceID 生成一个随机 TraceID；随机源不可用时退回全零 ID（仍可正常传播）。
func NewTraceID() TraceID {
	var id TraceID
	_, _ = rand.Read(id[:])
	return id
}

// NewSpanID 生成一个随机 SpanID。
func NewSpanID() SpanID {
	var id SpanID
	_, _ = rand.Read(id[:])
	return id
}

// String 返回 TraceID 的十六进制表示。
func (id TraceID) String() string { return hex.EncodeToString(id[:]) }

// String 返回 SpanID 的十六进制表示。
func (id SpanID) String() string { return hex.EncodeToString(id[:]) }

// IsZero 判断 SpanID 是否为全零（未设置）。
func (id SpanID) IsZero() bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}
