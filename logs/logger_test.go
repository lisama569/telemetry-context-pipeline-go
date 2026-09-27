package logs

import (
	"context"
	"log"
	"testing"

	"github.com/lisama569/telemetry-context-pipeline-go/attributes"
	"github.com/lisama569/telemetry-context-pipeline-go/trace"
)

func newTestLogger() (*Logger, *RecordingSink) {
	p := attributes.NewProcessor(attributes.ProcessorConfig{
		MaxCardinality:       10,
		SensitiveKeys:        map[string]struct{}{"ssn": {}},
		SensitiveSubstrings:  []string{"password"},
		RedactionPlaceholder: attributes.DefaultRedactionPlaceholder,
	})
	sink := &RecordingSink{}
	return NewLogger(sink, p, LevelInfo), sink
}

// TestLogger_CarriesTraceContext 日志必须携带 ctx 中的追踪上下文，
// 从而可与同一条 trace 关联；无上下文时 TraceID 为零值可辨。
func TestLogger_CarriesTraceContext(t *testing.T) {
	logger, sink := newTestLogger()
	sampler := trace.NewDeterministicSampler(1)
	root, ctx := trace.StartSpan(context.Background(), "checkout", sampler)
	defer root.End(nil)

	logger.Info(ctx, "order placed", map[string]string{"order_id": "o-1"})
	recs := sink.Records()
	if len(recs) != 1 {
		t.Fatalf("应收录 1 条日志，得到 %d", len(recs))
	}
	r := recs[0]
	if r.Trace.TraceID != root.Context().TraceID {
		t.Fatalf("日志 TraceID=%s 与 Span=%s 不匹配", r.Trace.TraceID, root.Context().TraceID)
	}
	if r.Trace.SpanID != root.Context().SpanID {
		t.Fatalf("日志应携带发起它的 SpanID")
	}
	log.Printf("[关键上下文] 日志自动携带 trace=%s span=%s level=%s",
		r.Trace.TraceID, r.Trace.SpanID, r.Level)

	// 无追踪上下文时为零值，调用方能区分“无链路”而不是误认为某条真实链路。
	logger2, sink2 := newTestLogger()
	logger2.Info(context.Background(), "startup", nil)
	if sink2.Records()[0].Trace.TraceID.String() != "00000000000000000000000000000000" {
		t.Fatalf("无上下文时 TraceID 应为零值，得到 %s", sink2.Records()[0].Trace.TraceID)
	}
	log.Printf("[判定依据] 无追踪上下文的日志 TraceID 为零值，与真实链路可区分")
}

// TestLogger_FiltersAttributes 日志属性必须经过与追踪/指标同一套规则过滤，
// 敏感值不得透出，判定依据通过返回值给出。
func TestLogger_FiltersAttributes(t *testing.T) {
	logger, sink := newTestLogger()
	results := logger.Error(context.Background(), "login failed", map[string]string{
		"password": "supersecret",
		"ssn":      "999-00-1111",
		"user":     "alice",
	})

	r := sink.Records()[0]
	if r.Attrs["password"] != attributes.DefaultRedactionPlaceholder || r.Attrs["ssn"] != attributes.DefaultRedactionPlaceholder {
		t.Fatalf("敏感字段应脱敏，得到 %v", r.Attrs)
	}
	for k, v := range r.Attrs {
		if v == "supersecret" || v == "999-00-1111" {
			t.Fatalf("敏感原值经日志键 %q 静默透出", k)
		}
	}
	if r.Attrs["user"] != "alice" {
		t.Fatalf("非敏感字段应原样保留")
	}
	reasons := map[string]string{}
	for _, x := range results {
		reasons[x.Key] = x.Outcome.String() + "/" + x.Reason
	}
	log.Printf("[判定依据] 日志属性过滤: password=%s ssn=%s user=%s；审计依据 %v",
		r.Attrs["password"], r.Attrs["ssn"], r.Attrs["user"], reasons)
}

// TestLogger_LevelFilter 低于最低级别的日志不输出。
func TestLogger_LevelFilter(t *testing.T) {
	logger, sink := newTestLogger()
	logger.Debug(context.Background(), "verbose", nil)
	if len(sink.Records()) != 0 {
		t.Fatalf("DEBUG 低于 INFO 阈值，不应输出")
	}
	logger.SetLevel(LevelWarn)
	logger.Info(context.Background(), "info after raise", nil)
	logger.Warn(context.Background(), "warn after raise", nil)
	if len(sink.Records()) != 1 || sink.Records()[0].Msg != "warn after raise" {
		t.Fatalf("提级到 WARN 后只应输出 WARN，得到 %+v", sink.Records())
	}
	log.Printf("[判定依据] 级别过滤生效：DEBUG/INFO 被过滤，WARN 输出")
}
