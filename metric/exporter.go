package metric

import (
	"context"
	"fmt"
)

// Exporter 是指标下游导出接口。
type Exporter interface {
	Export(ctx context.Context, points []DataPoint) error
}

// FailureKind 区分导出失败原因，降级策略与日志判定都依赖该分类。
type FailureKind string

const (
	FailureUnavailable FailureKind = "unavailable" // 下游不可用（连接失败/熔断打开）
	FailureTimeout     FailureKind = "timeout"     // 导出超时
	FailureRejected    FailureKind = "rejected"    // 下游明确拒绝（如 4xx、非法数据）
	FailureInternal    FailureKind = "internal"    // 其它内部错误
)

// Retryable 报告该类失败是否值得重试。
func (k FailureKind) Retryable() bool {
	switch k {
	case FailureUnavailable, FailureTimeout, FailureInternal:
		return true
	case FailureRejected:
		return false // 明确拒绝：重试不会改变结果，直接丢弃该批
	default:
		return false
	}
}

// ExportError 携带可区分的失败类别。
type ExportError struct {
	Kind FailureKind
	Err  error
}

func (e *ExportError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("metric export failed kind=%s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("metric export failed kind=%s", e.Kind)
}

func (e *ExportError) Unwrap() error { return e.Err }

// NewExportError 构造导出错误。
func NewExportError(kind FailureKind, err error) *ExportError {
	return &ExportError{Kind: kind, Err: err}
}
