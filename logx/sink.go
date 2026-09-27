package logx

import (
	"context"
	"encoding/json"
	"io"
	"os"
)

// JSONSink 把条目编码为单行 JSON 写到指定 Writer（默认 stderr）。
type JSONSink struct {
	w io.Writer
}

// NewJSONSink 创建 JSON 输出端。
func NewJSONSink(w io.Writer) *JSONSink {
	if w == nil {
		w = os.Stderr
	}
	return &JSONSink{w: w}
}

// Write 实现 Sink。
func (s *JSONSink) Write(_ context.Context, e Entry) error {
	rec := map[string]any{
		"level":      e.Level.String(),
		"message":    e.Message,
		"trace_id":   e.TraceID,
		"span_id":    e.SpanID,
		"sampled":    e.Sampled,
		"sample_src": e.SampleSrc,
		"attrs":      e.Attrs,
	}
	// 判定明细随日志一起输出，便于复核为何某字段被脱敏/截断/丢弃。
	if len(e.FilterRpt.Items) > 0 {
		items := make([]map[string]string, 0, len(e.FilterRpt.Items))
		for _, it := range e.FilterRpt.SortedItems() {
			items = append(items, map[string]string{
				"key": it.Key, "action": string(it.Action), "reason": it.Reason,
			})
		}
		rec["attr_filter"] = items
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = s.w.Write(b)
	return err
}
