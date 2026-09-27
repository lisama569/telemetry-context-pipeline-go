package metric

import (
	"sort"
	"sync"
)

// seriesKey 是一个数据序列的唯一键。
type seriesKey struct {
	name   string
	labels string // 规范化后的 label 串
}

// aggregate 是单个序列的滚动聚合状态。
type aggregate struct {
	name   string
	kind   Kind
	labels Labels
	value  float64 // counter: 累加；gauge: 最新值；histogram: 累加和
	count  int64
	min    float64
	max    float64
}

// aggregator 是并发安全的内存聚合器。
// 所有序列共享一把短临界区锁，业务侧 Record 不直接持有它
// （先入有界队列），因此锁竞争不会反压业务调用。
type aggregator struct {
	mu        sync.Mutex
	series    map[seriesKey]*aggregate
	maxSeries int
}

// newAggregator 创建聚合器。maxSeries<=0 表示不限制序列数量。
func newAggregator(maxSeries int) *aggregator {
	return &aggregator{series: map[seriesKey]*aggregate{}, maxSeries: maxSeries}
}

func canonicalLabels(l Labels) (string, Labels) {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cp := make(Labels, len(keys))
	var b []byte
	for _, k := range keys {
		cp[k] = l[k]
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, l[k]...)
		b = append(b, ',')
	}
	return string(b), cp
}

// Merge 写入一个采样；返回是否被接受（序列数达上限时拒绝，防止无限增长）。
func (a *aggregator) Merge(s Sample) bool {
	key := seriesKey{name: s.Name}
	key.labels, s.Labels = canonicalLabels(s.Labels)

	a.mu.Lock()
	defer a.mu.Unlock()
	agg, ok := a.series[key]
	if !ok {
		if a.maxSeries > 0 && len(a.series) >= a.maxSeries {
			return false
		}
		agg = &aggregate{name: s.Name, kind: s.Kind, labels: s.Labels,
			min: s.Value, max: s.Value}
		a.series[key] = agg
	}
	agg.count++
	switch s.Kind {
	case Counter:
		agg.value += s.Value
	case Gauge:
		agg.value = s.Value
	case Histogram:
		agg.value += s.Value
		if s.Value < agg.min {
			agg.min = s.Value
		}
		if s.Value > agg.max {
			agg.max = s.Value
		}
	}
	return true
}

// Len 返回当前序列数量。
func (a *aggregator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.series)
}

// Drain 取出并清空全部聚合数据点，按名称与 label 稳定排序，便于测试与排查。
func (a *aggregator) Drain() []DataPoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]DataPoint, 0, len(a.series))
	for _, agg := range a.series {
		out = append(out, DataPoint{
			Name: agg.name, Kind: agg.kind, Labels: agg.labels,
			Value: agg.value, Count: agg.count, Min: agg.min, Max: agg.max,
		})
	}
	a.series = map[seriesKey]*aggregate{}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		ki, _ := canonicalLabels(out[i].Labels)
		kj, _ := canonicalLabels(out[j].Labels)
		return ki < kj
	})
	return out
}
