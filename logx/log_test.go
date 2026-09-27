package logx

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lisama569/telemetry-context-pipeline-go/attribute"
	"github.com/lisama569/telemetry-context-pipeline-go/config"
	"github.com/lisama569/telemetry-context-pipeline-go/tracing"
)

// captureSink 收集条目，并可按开关模拟失败。
type captureSink struct {
	entries []Entry
	fail    bool
}

func (c *captureSink) Write(_ context.Context, e Entry) error {
	if c.fail {
		return errors.New("sink unavailable")
	}
	c.entries = append(c.entries, e)
	return nil
}

func newTestLogger(sink Sink, level Level) *Logger {
	cfg := config.AttributeConfig{
		MaxKeysPerRecord:    16,
		MaxValuesPerKey:     64,
		MaxValueLength:      6,
		SensitiveKeys:       []string{"password", "*token"},
		RedactedPlaceholder: "[REDACTED]",
		OtherBucket:         "other",
	}
	return New(cfg, attribute.New(cfg), sink, level)
}

func TestLogInjectsTraceContext(t *testing.T) {
	sink := &captureSink{}
	l := newTestLogger(sink, DebugLevel)
	tr := tracing.NewTracer(tracing.NewRatioSampler(1.0))
	ctx, span := tr.Start(context.Background(), "op")

	l.Info(ctx, "hello", map[string]any{"k": "v"})
	if len(sink.entries) != 1 {
		t.Fatalf("收到 %d 条, want 1", len(sink.entries))
	}
	e := sink.entries[0]
	if e.TraceID != span.Context().TraceIDHex() {
		t.Fatalf("traceID 未注入: got %s want %s", e.TraceID, span.Context().TraceIDHex())
	}
	if e.SpanID == "" || !e.Sampled || e.SampleSrc != "root" {
		t.Fatalf("追踪上下文字段缺失: span=%s sampled=%v src=%s", e.SpanID, e.Sampled, e.SampleSrc)
	}
	t.Logf("注入成功 trace=%s span=%s sampled=%v src=%s",
		e.TraceID, e.SpanID, e.Sampled, e.SampleSrc)
}

func TestLogMasksSensitiveBeforeSink(t *testing.T) {
	sink := &captureSink{}
	l := newTestLogger(sink, InfoLevel)

	l.Info(context.Background(), "login", map[string]any{
		"password":  "hunter2",
		"api_token": "tok-123456",
		"user":      "alice-long-value",
	})
	e := sink.entries[0]
	if e.Attrs["password"] != "[REDACTED]" || e.Attrs["api_token"] != "[REDACTED]" {
		t.Fatalf("敏感字段透出到 Sink: %v", e.Attrs)
	}
	for _, it := range e.FilterRpt.SortedItems() {
		if it.Key == "password" && it.Action != attribute.ActionMasked {
			t.Fatalf("password 动作=%s", it.Action)
		}
		t.Logf("attr=%s action=%s reason=%s", it.Key, it.Action, it.Reason)
	}
	// 序列化后同样不得出现明文
	b, _ := json.Marshal(e)
	if strings.Contains(string(b), "hunter2") || strings.Contains(string(b), "tok-123456") {
		t.Fatalf("序列化日志包含敏感明文: %s", b)
	}
	st := l.Stats()
	if st.AttrsMasked != 2 {
		t.Fatalf("masked 计数=%d want 2", st.AttrsMasked)
	}
	t.Logf("敏感字段 2 个在 Sink 前完成脱敏, 明文不存在于序列化输出")
}

func TestLevelGating(t *testing.T) {
	sink := &captureSink{}
	l := newTestLogger(sink, WarnLevel)
	l.Debug(context.Background(), "d", nil)
	l.Info(context.Background(), "i", nil)
	l.Warn(context.Background(), "w", nil)
	if len(sink.entries) != 1 || sink.entries[0].Message != "w" {
		t.Fatalf("级别门控错误: %+v", sink.entries)
	}
	if l.Stats().LevelFiltered != 2 {
		t.Fatalf("filtered=%d want 2", l.Stats().LevelFiltered)
	}
	t.Log("DEBUG/INFO 被门控, WARN 正常输出, 计数=2")
}

func TestSinkFailureDoesNotPanic(t *testing.T) {
	sink := &captureSink{fail: true}
	l := newTestLogger(sink, InfoLevel)
	l.Error(context.Background(), "boom", map[string]any{"password": "x"}) // 不应 panic
	if l.Stats().SinkFailures != 1 {
		t.Fatalf("sink 失败计数=%d want 1", l.Stats().SinkFailures)
	}
	if l.Stats().Emitted != 1 {
		t.Fatalf("emitted=%d want 1", l.Stats().Emitted)
	}
	t.Log("Sink 失败被吞掉并计数，业务调用不 panic、不阻塞")
}

func TestNoTraceContextSafe(t *testing.T) {
	sink := &captureSink{}
	l := newTestLogger(sink, InfoLevel)
	l.Info(context.Background(), "no-trace", nil)
	e := sink.entries[0]
	if e.TraceID != "" || e.SpanID != "" {
		t.Fatalf("无上下文时不应捏造 trace 字段: %+v", e)
	}
	t.Log("无追踪上下文时日志安全输出，trace 字段留空")
}
