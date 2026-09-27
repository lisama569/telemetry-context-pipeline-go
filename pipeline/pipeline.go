// Package pipeline 把追踪、指标、日志与属性处理组装成一条
// 采集 -> 上下文延续/属性过滤 -> 聚合 -> 导出 的可观测流水线，
// 并统一管理采样率、有界缓冲等配置与启停生命周期。
package pipeline

import (
	"context"
	"sync"

	"github.com/lisama569/telemetry-context-pipeline-go/attributes"
	"github.com/lisama569/telemetry-context-pipeline-go/logs"
	"github.com/lisama569/telemetry-context-pipeline-go/metrics"
	"github.com/lisama569/telemetry-context-pipeline-go/trace"
)

// Config 是流水线的总配置。
type Config struct {
	// SampleRate 链路根节点的采样率，取值 [0,1]，默认 1。
	SampleRate float64
	// Attributes 属性过滤与基数控制配置，追踪/指标/日志共用同一个 Processor。
	Attributes attributes.ProcessorConfig
	// Metrics 指标聚合与降级配置。
	Metrics metrics.AggregatorConfig
	// LogLevel 最低日志级别，默认 INFO。
	LogLevel logs.Level
}

// DefaultConfig 返回安全的默认配置（全量采样、INFO、指标缓冲全部有界）。
func DefaultConfig() Config {
	return Config{
		SampleRate: 1,
		Attributes: attributes.ProcessorConfig{
			SensitiveSubstrings:  []string{"password", "token", "secret"},
			RedactionPlaceholder: attributes.DefaultRedactionPlaceholder,
		},
		Metrics:  metrics.DefaultAggregatorConfig(),
		LogLevel: logs.LevelInfo,
	}
}

// Pipeline 持有各子系统并统一生命周期管理。
type Pipeline struct {
	cfg        Config
	Sampler    *trace.DeterministicSampler
	Processor  *attributes.Processor
	Aggregator *metrics.Aggregator
	Logger     *logs.Logger

	runOnce sync.Once
}

// New 用给定指标导出器与日志 sink 组装流水线。
// 三个信号入口共享同一个 attributes.Processor 实例，保证同一属性
// 无论从 Span、指标标签还是日志进入，处理结论都一致。
func New(cfg Config, exp metrics.Exporter, logSink logs.Sink) *Pipeline {
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = DefaultConfig().SampleRate
	}
	processor := attributes.NewProcessor(cfg.Attributes)
	return &Pipeline{
		cfg:        cfg,
		Sampler:    trace.NewDeterministicSampler(cfg.SampleRate),
		Processor:  processor,
		Aggregator: metrics.NewAggregator(exp, cfg.Metrics),
		Logger:     logs.NewLogger(logSink, processor, cfg.LogLevel),
	}
}

// StartSpan 是 trace.StartSpan 的流水线便捷封装，自动带上流水线采样器。
func (p *Pipeline) StartSpan(ctx context.Context, name string) (*trace.Span, context.Context) {
	return trace.StartSpan(ctx, name, p.Sampler)
}

// Run 启动后台组件（指标聚合导出循环），阻塞直到 ctx 取消。可重复调用但只生效一次。
func (p *Pipeline) Run(ctx context.Context) error {
	var err error
	p.runOnce.Do(func() {
		err = p.Aggregator.Run(ctx)
	})
	return err
}

// Shutdown 优雅停止指标聚合（内部会做脱离取消信号的最终 flush），
// 并返回运行期间累计的丢弃/降级事件，供调用方记录与告警。
func (p *Pipeline) Shutdown(ctx context.Context) ([]metrics.DropEvent, error) {
	if err := p.Aggregator.Close(ctx); err != nil {
		return p.Aggregator.Drops(), err
	}
	return p.Aggregator.Drops(), nil
}
