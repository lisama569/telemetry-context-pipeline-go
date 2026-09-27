package metrics

import (
	"sort"
	"strings"
)

// Kind 表示指标类型。
type Kind int

const (
	KindCounter Kind = iota
	KindGauge
)

// String 返回指标类型的可读名称。
func (k Kind) String() string {
	switch k {
	case KindGauge:
		return "GAUGE"
	default:
		return "COUNTER"
	}
}

// SeriesID 标识一条时间序列（指标名 + 已处理后的标签）。
type SeriesID struct {
	Name   string
	Labels map[string]string
}

// Key 返回该序列的稳定字符串标识，用作内部 map 键。
// 标签按键排序后编码，保证不同入口（不同 map 迭代顺序）
// 构造出的同一序列得到同一个 Key，聚合不会被拆成多条。
func (id SeriesID) Key() string {
	if len(id.Labels) == 0 {
		return id.Name
	}
	keys := make([]string, 0, len(id.Labels))
	for k := range id.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(id.Name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(id.Labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// Sample 是一次指标写入。
type Sample struct {
	ID    SeriesID
	Kind  Kind
	Value float64
}

// Snapshot 是某条序列聚合后的导出快照。
type Snapshot struct {
	ID    SeriesID
	Kind  Kind
	Value float64
}

// DropReason 标识数据被丢弃/降级的原因，要求失败原因可区分。
type DropReason string

const (
	// DropNone 未丢弃。
	DropNone DropReason = ""
	// DropChannelFull 写入通道已满（导出协程停滞），本次写入被丢弃。
	DropChannelFull DropReason = "channel_full"
	// DropSeriesLimit 序列数已达上限，新序列被拒绝。
	DropSeriesLimit DropReason = "series_limit"
	// DropBufferFull 有界重试缓冲已满，最旧的待导出批次被丢弃。
	DropBufferFull DropReason = "buffer_full"
	// DropExportFailed 导出失败且缓冲溢出后丢弃。
	DropExportFailed DropReason = "export_failed"
)

// DropEvent 记录一次丢弃事件，供监控与测试断言。
type DropEvent struct {
	Reason DropReason
	// Count 本次事件丢弃的条数。
	Count int
}
