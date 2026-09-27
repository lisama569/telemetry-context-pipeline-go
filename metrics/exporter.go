package metrics

import (
	"context"
	"errors"
	"sync"
)

// Exporter 把聚合快照导出到下游。
type Exporter interface {
	Export(ctx context.Context, snapshots []Snapshot) error
}

// ErrFlakyExport 是 FlakyExporter 的默认失败错误，便于测试区分失败原因。
var ErrFlakyExport = errors.New("flaky exporter: downstream unavailable")

// FlakyExporter 是用于测试/演示的可配置失败导出器。
//   - Fail=true 时所有调用都失败；
//   - FailCalls>0 时仅前 FailCalls 次调用失败（模拟下游短暂不可用后恢复）；
//   - 调用方可用 Calls() 观察实际尝试导出的次数，用 LastSnapshot() 观察最后导出内容。
type FlakyExporter struct {
	mu           sync.Mutex
	Fail         bool
	FailCalls    int
	FailError    error
	calls        int
	failedCalls  int
	lastSnapshot []Snapshot
}

// NewFlakyExporter 创建可配置导出器。failErr 为 nil 时使用 ErrFlakyExport。
func NewFlakyExporter(fail bool, failErr error) *FlakyExporter {
	if failErr == nil {
		failErr = ErrFlakyExport
	}
	return &FlakyExporter{Fail: fail, FailError: failErr}
}

// FailFirstN 配置前 n 次导出失败、之后成功，用于模拟下游恢复场景。
func (e *FlakyExporter) FailFirstN(n int) *FlakyExporter {
	e.mu.Lock()
	e.Fail = false
	e.FailCalls = n
	e.mu.Unlock()
	return e
}

// Export 实现 Exporter。返回的错误可被 errors.Is 识别为 ErrFlakyExport。
func (e *FlakyExporter) Export(ctx context.Context, snapshots []Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	cp := make([]Snapshot, len(snapshots))
	copy(cp, snapshots)
	e.lastSnapshot = cp

	shouldFail := e.Fail || e.FailCalls > 0
	if e.FailCalls > 0 {
		e.FailCalls--
	}
	if shouldFail {
		e.failedCalls++
		return e.FailError
	}
	return nil
}

// Calls 返回 Export 被调用的总次数。
func (e *FlakyExporter) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// FailedCalls 返回导出失败的次数。
func (e *FlakyExporter) FailedCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failedCalls
}

// LastSnapshot 返回最后一次导出的快照副本。
func (e *FlakyExporter) LastSnapshot() []Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]Snapshot, len(e.lastSnapshot))
	copy(cp, e.lastSnapshot)
	return cp
}
