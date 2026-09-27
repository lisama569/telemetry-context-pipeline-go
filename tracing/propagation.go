package tracing

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Carrier 是跨进程传递时的载体抽象（http.Header / metadata.MD 均可适配）。
type Carrier interface {
	Get(key string) string
	Set(key, value string)
}

// MapCarrier 是基于 map 的简单 Carrier，便于测试。
type MapCarrier map[string]string

func (m MapCarrier) Get(key string) string {
	if m == nil {
		return ""
	}
	return m[strings.ToLower(key)]
}

func (m MapCarrier) Set(key, value string) {
	m[strings.ToLower(key)] = value
}

// 头部字段与 W3C Trace Context 对齐：version-traceid-spanid-flags。
const (
	HeaderTraceparent = "traceparent"
	HeaderBaggage     = "trace-baggage"
	sampledFlag       = "01"
)

// Inject 把 SpanContext 注入载体。
//
// 采样标志（flags）随上下文一并传递，下游据此沿用同一采样结论；
// baggage 以 k=v 对（逗号分隔）传递。采样判定依据不跨进程传递，
// 由接收端生成"继承自父链路"的新依据，避免伪造。
func Inject(sc SpanContext, c Carrier) {
	flag := "00"
	if sc.Sampled {
		flag = sampledFlag
	}
	c.Set(HeaderTraceparent, fmt.Sprintf("00-%s-%s-%s",
		sc.TraceIDHex(), sc.SpanIDHex(), flag))
	if len(sc.Baggage) > 0 {
		parts := make([]string, 0, len(sc.Baggage))
		for k, v := range sc.Baggage {
			parts = append(parts, k+"="+v)
		}
		c.Set(HeaderBaggage, strings.Join(parts, ","))
	}
}

// Extract 从载体提取 SpanContext；格式非法时返回错误而不是静默生成新链路。
func Extract(c Carrier) (SpanContext, error) {
	tp := c.Get(HeaderTraceparent)
	if tp == "" {
		return SpanContext{}, fmt.Errorf("traceparent 头缺失")
	}
	fields := strings.Split(tp, "-")
	if len(fields) != 4 || fields[0] != "00" {
		return SpanContext{}, fmt.Errorf("traceparent 格式非法: %q", tp)
	}
	var tid TraceID
	if err := decodeHex(fields[1], tid[:]); err != nil {
		return SpanContext{}, fmt.Errorf("trace id 解析失败: %w", err)
	}
	var sid SpanID
	if err := decodeHex(fields[2], sid[:]); err != nil {
		return SpanContext{}, fmt.Errorf("span id 解析失败: %w", err)
	}
	if fields[3] != "00" && fields[3] != sampledFlag {
		return SpanContext{}, fmt.Errorf("采样标志非法: %q", fields[3])
	}
	sc := SpanContext{
		TraceID:        tid,
		SpanID:         sid,
		Sampled:        fields[3] == sampledFlag,
		SamplingSource: "parent",
		SamplingReason: "inherited sampled flag from inbound traceparent",
		Baggage:        map[string]string{},
	}
	if raw := c.Get(HeaderBaggage); raw != "" {
		for _, pair := range strings.Split(raw, ",") {
			k, v, ok := strings.Cut(pair, "=")
			if ok {
				sc.Baggage[strings.TrimSpace(k)] = v
			}
		}
	}
	return sc, nil
}

func decodeHex(s string, dst []byte) error {
	if hex.DecodedLen(len(s)) != len(dst) {
		return fmt.Errorf("长度错误: %q", s)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	copy(dst, b)
	return nil
}
