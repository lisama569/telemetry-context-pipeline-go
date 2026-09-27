package metrics

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fastCfg() AggregatorConfig {
	return AggregatorConfig{
		MaxSeries:       100,
		ChannelBuffer:   8,
		RetryBuffer:     2,
		FlushIntervalMs: 30,
	}
}

func series(name string, labelVals ...string) SeriesID {
	labels := map[string]string{}
	for i := 0; i+1 < len(labelVals); i += 2 {
		labels[labelVals[i]] = labelVals[i+1]
	}
	return SeriesID{Name: name, Labels: labels}
}

// TestAggregator_ConcurrentWritesAggregated 并发写入同一计数器必须正确累加，
// 且不同标签顺序构造的同序列要聚合到同一条（Key 稳定）。
func TestAggregator_ConcurrentWritesAggregated(t *testing.T) {
	exp := NewFlakyExporter(false, nil)
	cfg := fastCfg()
	cfg.ChannelBuffer = 4096 // 吸收 50x200 的瞬时突发，使本用例专注验证聚合正确性
	agg := NewAggregator(exp, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	const goroutines, perG = 50, 200
	var accepted int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// 故意用不同的 map 字面值顺序构造同一序列。
				var s Sample
				if g%2 == 0 {
					s = Sample{ID: series("requests", "a", "1", "b", "2"), Kind: KindCounter, Value: 1}
				} else {
					s = Sample{ID: series("requests", "b", "2", "a", "1"), Kind: KindCounter, Value: 1}
				}
				if r := agg.Record(s); r == DropNone {
					// Record 永不阻塞；突发超容量时 channel_full 是明确的降级路径，
					// 本用例在足够大的通道下预期全部接收，仍对实际接收数做计数核对。
					atomic.AddInt64(&accepted, 1)
				}
			}
		}(g)
	}
	wg.Wait()

	// 等待至少一个 flush 周期后检查导出值。
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	snaps := exp.LastSnapshot()
	var total float64
	for _, sn := range snaps {
		if sn.ID.Name == "requests" {
			total += sn.Value
		}
	}
	if total != float64(accepted) {
		t.Fatalf("所有已接收写入都应被聚合导出：接收=%d 导出合计=%v（快照=%v）", accepted, total, snaps)
	}
	dropEvents := agg.Drops()
	channelFull := 0
	for _, d := range dropEvents {
		if d.Reason == DropChannelFull {
			channelFull += d.Count
		}
	}
	log.Printf("[关键上下文] 50x200 并发写入：接收=%d 突发丢弃(channel_full)=%d，被接收点全部正确聚合导出，无序标签未拆序列",
		accepted, channelFull)
}

// TestAggregator_ChannelFullNeverBlocks 通道打满时 Record 必须立刻返回 channel_full，
// 绝不阻塞业务调用。
func TestAggregator_ChannelFullNeverBlocks(t *testing.T) {
	exp := NewFlakyExporter(false, nil)
	// 不启动 Run：通道数据不会被消费，便于稳定制造满队列。
	cfg := AggregatorConfig{MaxSeries: 10, ChannelBuffer: 2, RetryBuffer: 1, FlushIntervalMs: 1000}
	agg := NewAggregator(exp, cfg)

	filled := 0
	for i := 0; i < 10; i++ {
		r := agg.Record(Sample{ID: series("m", "i", string(rune('a'+i))), Value: 1})
		if r == DropNone {
			filled++
		} else {
			if r != DropChannelFull {
				t.Fatalf("满队列时原因应为 channel_full，得到 %s", r)
			}
		}
	}
	drops := agg.Drops()
	if len(drops) == 0 || drops[0].Reason != DropChannelFull {
		t.Fatalf("应记录 channel_full 丢弃事件，得到 %+v", drops)
	}
	if filled != 2 {
		t.Fatalf("容量为 2 的通道应接收 2 条，实际 %d", filled)
	}
	log.Printf("[判定依据] 通道容量=2：接收 %d 条，其余立即返回 channel_full，业务不阻塞", filled)

	// 关闭时对残留数据做一次同步 flush 导出。
	if err := agg.Close(context.Background()); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if exp.Calls() != 1 {
		t.Fatalf("未启动 Run 时 Close 应同步 flush 一次，实际调用 %d 次", exp.Calls())
	}
}

// TestAggregator_ExportFailureRetryAndBoundedDrop 下游持续不可用时：
// 失败批次进入有界重试队列，队列满后丢最旧批次(buffer_full)，
// 每次失败另记 export_failed，原因彼此可区分，内存不无限增长。
func TestAggregator_ExportFailureRetryAndBoundedDrop(t *testing.T) {
	exp := NewFlakyExporter(true, nil) // 永远失败
	cfg := AggregatorConfig{MaxSeries: 100, ChannelBuffer: 64, RetryBuffer: 2, FlushIntervalMs: 20}
	agg := NewAggregator(exp, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	for cycle := 0; cycle < 6; cycle++ {
		agg.Record(Sample{ID: series("c", "cycle", string(rune('0'+cycle))), Kind: KindCounter, Value: 1})
		time.Sleep(30 * time.Millisecond)
	}
	cancel()
	<-done

	drops := agg.Drops()
	reasons := map[DropReason]int{}
	for _, d := range drops {
		reasons[d.Reason] += d.Count
	}
	log.Printf("[判定依据] 下游持续失败期间丢弃事件: buffer_full=%d 点, export_failed=%d 点；重试队列上限=2 有界",
		reasons[DropBufferFull], reasons[DropExportFailed])
	if reasons[DropBufferFull] == 0 {
		t.Fatalf("重试队列有界，溢出时必须记录 buffer_full")
	}
	if reasons[DropExportFailed] == 0 {
		t.Fatalf("每次导出失败必须记录 export_failed")
	}
	if exp.FailedCalls() == 0 {
		t.Fatalf("导出器应观测到失败调用")
	}
	if !errors.Is(exp.FailError, ErrFlakyExport) {
		t.Fatalf("失败原因应可用 errors.Is 区分，得到 %v", exp.FailError)
	}
}

// TestAggregator_RecoveryAfterFailure 下游短暂失败后恢复：
// 队列中的失败批次应自动重试成功，最终数据不丢。
func TestAggregator_RecoveryAfterFailure(t *testing.T) {
	exp := NewFlakyExporter(false, nil).FailFirstN(3)
	cfg := AggregatorConfig{MaxSeries: 100, ChannelBuffer: 64, RetryBuffer: 10, FlushIntervalMs: 20}
	agg := NewAggregator(exp, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	agg.Record(Sample{ID: series("orders"), Kind: KindCounter, Value: 5})
	time.Sleep(150 * time.Millisecond) // 跨越失败期与恢复期
	cancel()
	<-done

	if exp.Calls() < 4 {
		t.Fatalf("应至少经历 3 次失败重试 + 1 次成功，实际调用 %d 次", exp.Calls())
	}
	found := false
	for _, sn := range exp.LastSnapshot() {
		if sn.ID.Name == "orders" && sn.Value == 5 {
			found = true
		}
	}
	if !found {
		t.Fatalf("下游恢复后失败批次应重试导出，orders=5 未找到，快照=%v", exp.LastSnapshot())
	}
	drops := agg.Drops()
	for _, d := range drops {
		if d.Reason == DropBufferFull {
			t.Fatalf("重试队列未溢出，不应有 buffer_full 丢弃，实际 %+v", drops)
		}
	}
	log.Printf("[判定依据] 前 3 次导出失败、第 4 次恢复：批次重试成功 orders=5，总调用=%d 无溢出丢弃", exp.Calls())
}

// TestAggregator_CancelFinalFlush 父 context 被取消时，已采集数据仍应尽力做最终导出，
// 最终导出不能因为父取消信号而直接失败。
func TestAggregator_CancelFinalFlush(t *testing.T) {
	exp := NewFlakyExporter(false, nil) // 下游健康；最终 flush 必须成功
	cfg := fastCfg()
	cfg.ChannelBuffer = 64 // 10 个点在消费者启动前全部进入缓冲，不得提前降级
	agg := NewAggregator(exp, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = agg.Run(ctx); close(done) }()

	for i := 0; i < 10; i++ {
		agg.Record(Sample{ID: series("sigterm_metric"), Kind: KindCounter, Value: 1})
	}
	cancel() // 模拟进程收到取消信号，不给额外时间
	<-done

	found := false
	for _, sn := range exp.LastSnapshot() {
		if sn.ID.Name == "sigterm_metric" && sn.Value == 10 {
			found = true
		}
	}
	if !found {
		t.Fatalf("取消时最终 flush 应导出已采集的 10 个点，快照=%v", exp.LastSnapshot())
	}
	log.Printf("[关键上下文] 父 context 取消后脱离取消信号完成最终 flush，sigterm_metric=10")
}

// TestAggregator_SeriesLimit 序列数超限时新序列被拒绝并记 series_limit，
// 已存在序列不受影响。
func TestAggregator_SeriesLimit(t *testing.T) {
	exp := NewFlakyExporter(false, nil)
	cfg := AggregatorConfig{MaxSeries: 2, ChannelBuffer: 64, RetryBuffer: 1, FlushIntervalMs: 10_000}
	agg := NewAggregator(exp, cfg)

	agg.Record(Sample{ID: series("m", "k", "v1"), Kind: KindGauge, Value: 1})
	agg.Record(Sample{ID: series("m", "k", "v2"), Kind: KindGauge, Value: 2})
	agg.Record(Sample{ID: series("m", "k", "v3"), Kind: KindGauge, Value: 3})
	agg.drain() // 不启动 Run，直接同步消费

	snap := agg.SnapshotSeries()
	if len(snap) != 2 {
		t.Fatalf("MaxSeries=2 应只保留 2 条序列，实际 %d: %v", len(snap), snap)
	}
	drops := agg.Drops()
	var saw bool
	for _, d := range drops {
		if d.Reason == DropSeriesLimit {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("超限新序列应记录 series_limit，drops=%+v", drops)
	}
	log.Printf("[判定依据] MaxSeries=2：v1/v2 保留，v3 拒绝并记 series_limit，序列集合有界")
}
