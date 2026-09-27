package metric

// Kind 标识指标类型。
type Kind int

const (
	Counter Kind = iota
	Gauge
	Histogram
)

// Labels 是指标维度集合。
type Labels map[string]string

// Sample 是一次业务侧写入。
type Sample struct {
	Name   string
	Kind   Kind
	Value  float64
	Labels Labels
}

// DataPoint 是聚合后的导出数据点。
type DataPoint struct {
	Name   string
	Kind   Kind
	Labels Labels
	Value  float64
	Count  int64
	Min    float64
	Max    float64
}
