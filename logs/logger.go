package logs

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/telemetry-context-pipeline-go/attributes"
	"github.com/lisama569/telemetry-context-pipeline-go/trace"
)

// Level 表示日志级别。
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String 返回级别的可读名称。
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Record 是一条经过处理、可安全导出的日志记录：
// Attrs 中的敏感值已被脱敏，超限字段已不存在，因此可直接发送给任何 sink。
type Record struct {
	Time  time.Time
	Level Level
	Msg   string
	Trace trace.Context
	Attrs map[string]string
}

// Sink 接收日志记录。
type Sink interface {
	Emit(r Record) error
}

// Logger 产出带追踪上下文与已过滤属性的日志，并发安全。
type Logger struct {
	mu        sync.Mutex
	sink      Sink
	processor *attributes.Processor
	level     Level
}

// NewLogger 创建日志器。p 为 nil 时属性不经过过滤（不推荐，仅用于裸用场景）。
func NewLogger(sink Sink, p *attributes.Processor, level Level) *Logger {
	return &Logger{sink: sink, processor: p, level: level}
}

// SetLevel 动态调整最低输出级别。
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	l.level = level
	l.mu.Unlock()
}

// Log 写入一条日志并返回属性处理依据。
//   - 从 ctx 提取追踪上下文；无上下文时 Trace 为零值（TraceID 全零），与“有链路”可区分；
//   - 属性经与追踪/指标完全相同的 Processor 处理，保证同一属性跨入口结论一致；
//   - 被脱敏/丢弃的字段体现在返回的 []AttrResult 中，调用方可打印审计日志，
//     Record 本身绝不包含被拦截的原始值。
func (l *Logger) Log(ctx context.Context, level Level, msg string, attrs map[string]string) []attributes.AttrResult {
	l.mu.Lock()
	minLevel := l.level
	sink := l.sink
	p := l.processor
	l.mu.Unlock()

	if level < minLevel || sink == nil {
		// 即使不输出也做一次属性判定并返回，调用方仍可据此审计“本来会发生什么”。
		if p != nil {
			_, results := p.Process(attrs)
			return results
		}
		return nil
	}

	tc, _ := trace.FromContext(ctx)

	processed := map[string]string{}
	var results []attributes.AttrResult
	if p != nil {
		processed, results = p.Process(attrs)
	} else {
		for k, v := range attrs {
			processed[k] = v
		}
	}

	_ = sink.Emit(Record{
		Time:  time.Now(),
		Level: level,
		Msg:   msg,
		Trace: tc,
		Attrs: processed,
	})
	return results
}

// Debug 便捷方法。
func (l *Logger) Debug(ctx context.Context, msg string, attrs map[string]string) []attributes.AttrResult {
	return l.Log(ctx, LevelDebug, msg, attrs)
}

// Info 便捷方法。
func (l *Logger) Info(ctx context.Context, msg string, attrs map[string]string) []attributes.AttrResult {
	return l.Log(ctx, LevelInfo, msg, attrs)
}

// Warn 便捷方法。
func (l *Logger) Warn(ctx context.Context, msg string, attrs map[string]string) []attributes.AttrResult {
	return l.Log(ctx, LevelWarn, msg, attrs)
}

// Error 便捷方法。
func (l *Logger) Error(ctx context.Context, msg string, attrs map[string]string) []attributes.AttrResult {
	return l.Log(ctx, LevelError, msg, attrs)
}

// RecordingSink 把日志保存在内存中，主要用于测试断言，并发安全。
type RecordingSink struct {
	mu      sync.Mutex
	records []Record
}

// Emit 实现 Sink。
func (s *RecordingSink) Emit(r Record) error {
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
	return nil
}

// Records 返回已接收日志的副本。
func (s *RecordingSink) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(s.records))
	copy(out, s.records)
	return out
}
