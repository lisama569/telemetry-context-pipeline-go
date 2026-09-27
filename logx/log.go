package logx

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/lisama569/telemetry-context-pipeline-go/attribute"
	"github.com/lisama569/telemetry-context-pipeline-go/config"
	"github.com/lisama569/telemetry-context-pipeline-go/tracing"
)

// Level 日志级别。
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// String 返回级别名。
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Entry 是落盘/下发前的日志条目，敏感字段在到达 Sink 前已被过滤。
type Entry struct {
	Level     Level
	Message   string
	TraceID   string
	SpanID    string
	Sampled   bool
	SampleSrc string
	Attrs     map[string]any
	FilterRpt attribute.Report
}

// Sink 是日志输出端。
type Sink interface {
	Write(ctx context.Context, e Entry) error
}

// Stats 日志侧的过滤与降级计数。
type Stats struct {
	Emitted        int64
	LevelFiltered  int64
	SinkFailures   int64
	AttrsMasked    int64
	AttrsDropped   int64
	AttrsTruncated int64
}

// Logger 注入追踪上下文并复用统一属性处理器。
type Logger struct {
	cfg   config.AttributeConfig
	proc  *attribute.Processor
	sink  Sink
	level Level
	st    Stats
}

// New 创建日志器。
func New(cfg config.AttributeConfig, proc *attribute.Processor, sink Sink, level Level) *Logger {
	return &Logger{cfg: cfg, proc: proc, sink: sink, level: level}
}

// Log 输出一条日志：
//   - 从 ctx 提取 trace/span 与采样结论，无上下文时字段留空；
//   - 属性先过统一处理器（敏感字段绝不可能静默透出到 Sink）；
//   - 级别低于门控时丢弃并计数；
//   - Sink 失败不 panic、不阻塞业务，仅累加 SinkFailures 并输出诊断日志。
func (l *Logger) Log(ctx context.Context, level Level, msg string, attrs map[string]any) {
	if level < l.level {
		atomic.AddInt64(&l.st.LevelFiltered, 1)
		return
	}
	safe, rpt := l.proc.Process(attrs)
	atomic.AddInt64(&l.st.AttrsMasked, int64(rpt.Masked))
	atomic.AddInt64(&l.st.AttrsDropped, int64(rpt.Dropped))
	atomic.AddInt64(&l.st.AttrsTruncated, int64(rpt.Truncated))

	e := Entry{
		Level: level, Message: msg, Attrs: safe, FilterRpt: rpt,
	}
	if sc, ok := tracing.SpanFromContext(ctx); ok {
		e.TraceID = sc.TraceIDHex()
		e.SpanID = sc.SpanIDHex()
		e.Sampled = sc.Sampled
		e.SampleSrc = sc.SamplingSource
	}

	atomic.AddInt64(&l.st.Emitted, 1)
	if err := l.sink.Write(ctx, e); err != nil {
		atomic.AddInt64(&l.st.SinkFailures, 1)
		log.Printf("[logx] sink write failed level=%s trace=%s err=%v (msg=%q)",
			level, e.TraceID, err, msg)
	}
}

// Debug 输出 DEBUG 日志。
func (l *Logger) Debug(ctx context.Context, msg string, attrs map[string]any) {
	l.Log(ctx, DebugLevel, msg, attrs)
}

// Info 输出 INFO 日志。
func (l *Logger) Info(ctx context.Context, msg string, attrs map[string]any) {
	l.Log(ctx, InfoLevel, msg, attrs)
}

// Warn 输出 WARN 日志。
func (l *Logger) Warn(ctx context.Context, msg string, attrs map[string]any) {
	l.Log(ctx, WarnLevel, msg, attrs)
}

// Error 输出 ERROR 日志。
func (l *Logger) Error(ctx context.Context, msg string, attrs map[string]any) {
	l.Log(ctx, ErrorLevel, msg, attrs)
}

// Stats 返回计数快照。
func (l *Logger) Stats() Stats {
	return Stats{
		Emitted:        atomic.LoadInt64(&l.st.Emitted),
		LevelFiltered:  atomic.LoadInt64(&l.st.LevelFiltered),
		SinkFailures:   atomic.LoadInt64(&l.st.SinkFailures),
		AttrsMasked:    atomic.LoadInt64(&l.st.AttrsMasked),
		AttrsDropped:   atomic.LoadInt64(&l.st.AttrsDropped),
		AttrsTruncated: atomic.LoadInt64(&l.st.AttrsTruncated),
	}
}
