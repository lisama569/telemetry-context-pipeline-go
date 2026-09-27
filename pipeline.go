package telemetry

import (
	"context"

	"github.com/lisama569/telemetry-context-pipeline-go/attribute"
	"github.com/lisama569/telemetry-context-pipeline-go/config"
	"github.com/lisama569/telemetry-context-pipeline-go/logx"
	"github.com/lisama569/telemetry-context-pipeline-go/metric"
	"github.com/lisama569/telemetry-context-pipeline-go/tracing"
)

// Pipeline 装配追踪、属性处理、指标与日志四个子系统，
// 共享同一份配置，保证属性处理与采样规则在各入口一致。
type Pipeline struct {
	Cfg       config.Config
	Tracer    *tracing.Tracer
	Sampler   tracing.Sampler
	Processor *attribute.Processor
	Metrics   *metric.Agent
	Logs      *logx.Logger
}

// New 根据配置与可插拔下游构建管线。sink 为 nil 时使用 JSON stderr。
func New(cfg config.Config, exp metric.Exporter, sink logx.Sink) (*Pipeline, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	sampler := tracing.NewRatioSampler(cfg.SampleRate)
	tracer := tracing.NewTracer(sampler)
	proc := attribute.New(cfg.Attributes)
	agent := metric.NewAgent(cfg.Metrics, exp, proc).
		WithMaxSeries(cfg.Attributes.MaxValuesPerKey * cfg.Attributes.MaxKeysPerRecord)
	if sink == nil {
		sink = logx.NewJSONSink(nil)
	}
	logger := logx.New(cfg.Attributes, proc, sink, logx.InfoLevel)

	return &Pipeline{
		Cfg:       cfg,
		Tracer:    tracer,
		Sampler:   sampler,
		Processor: proc,
		Metrics:   agent,
		Logs:      logger,
	}, nil
}

// Start 启动后台指标导出循环。
func (p *Pipeline) Start(ctx context.Context) {
	p.Metrics.Start(ctx)
}

// Shutdown 有序关闭：在配置的 CloseTimeout 内尽力冲刷指标数据。
// 日志为同步写入，无独立资源需要关闭。
func (p *Pipeline) Shutdown(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, p.Cfg.Metrics.CloseTimeout)
	defer cancel()
	return p.Metrics.Close(cctx)
}
