package tracing

import "testing"

func TestInjectExtractRoundtrip(t *testing.T) {
	src := SpanContext{
		TraceID:        TraceID{1, 2, 3, 4},
		SpanID:         SpanID{9, 8},
		Sampled:        true,
		SamplingSource: "root",
		Baggage:        map[string]string{"user": "alice", "region": "cn"},
	}
	c := MapCarrier{}
	Inject(src, c)

	got, err := Extract(c)
	if err != nil {
		t.Fatalf("提取失败: %v", err)
	}
	if got.TraceID != src.TraceID || got.SpanID != src.SpanID {
		t.Fatal("id 往返不一致")
	}
	if !got.Sampled {
		t.Fatal("采样标志未传递")
	}
	if got.Baggage["user"] != "alice" || got.Baggage["region"] != "cn" {
		t.Fatalf("baggage 往返不一致: %v", got.Baggage)
	}
	t.Logf("traceparent=%s baggage=%v sampled=%v reason=%s",
		c.Get(HeaderTraceparent), got.Baggage, got.Sampled, got.SamplingReason)
}

func TestExtractMalformed(t *testing.T) {
	cases := map[string]string{
		"缺失":          "",
		"字段不足":        "00-abc-def",
		"版本错误":        "01-00000000000000000000000000000001-0000000000000001-01",
		"traceID长度错误": "00-xx-0000000000000001-01",
		"采样标志非法":      "00-00000000000000000000000000000001-0000000000000001-09",
	}
	for name, val := range cases {
		c := MapCarrier{}
		if val != "" {
			c.Set(HeaderTraceparent, val)
		}
		if _, err := Extract(c); err == nil {
			t.Fatalf("用例 %q 应返回错误", name)
		} else {
			t.Logf("用例 %q 正确拒绝: %v", name, err)
		}
	}
}
