// 示例：一个请求处理在自身被取消后，把追踪上下文延续给异步任务；
// 异步任务产生的日志与指标仍归属同一 trace，敏感字段全程脱敏。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	tel "github.com/lisama569/telemetry-context-pipeline-go"
	"github.com/lisama569/telemetry-context-pipeline-go/config"
	"github.com/lisama569/telemetry-context-pipeline-go/logx"
	"github.com/lisama569/telemetry-context-pipeline-go/metric"
	"github.com/lisama569/telemetry-context-pipeline-go/tracing"
)

// stdoutExporter 简单打印导出的数据点。
type stdoutExporter struct{}

func (stdoutExporter) Export(_ context.Context, pts []metric.DataPoint) error {
	for _, p := range pts {
		fmt.Printf("[export] %s value=%.0f count=%d labels=%v\n", p.Name, p.Value, p.Count, p.Labels)
	}
	return nil
}

func main() {
	cfg := config.Default()
	cfg.ServiceName = "demo"
	cfg.SampleRate = 1.0
	cfg.Metrics.ExportInterval = 200 * time.Millisecond

	p, err := tel.New(cfg, stdoutExporter{}, logx.NewJSONSink(os.Stdout))
	if err != nil {
		log.Fatal(err)
	}
	root, cancelRoot := context.WithCancel(context.Background())
	p.Start(root)

	ctx, span := p.Tracer.Start(root, "POST /checkout")
	fmt.Println("trace:", span.Context().TraceIDHex())

	// 关键：异步任务使用 ContinueAsync，不继承父请求的取消信号
	actx, cancelAsync := tracing.ContinueAsync(ctx)
	go func() {
		defer cancelAsync()
		_, job := p.Tracer.Start(actx, "send-receipt")
		defer job.End()
		p.Logs.Info(actx, "receipt job started", map[string]any{
			"user": "alice", "password": "super-secret",
		})
		p.Metrics.Record(metric.Sample{
			Name: "receipts_sent", Kind: metric.Counter, Value: 1,
			Labels: metric.Labels{"auth_token": "abc.def.ghi", "channel": "email"},
		})
		time.Sleep(100 * time.Millisecond)
	}()

	cancelRoot() // 模拟请求超时/客户端断开
	time.Sleep(400 * time.Millisecond)

	sctx, c := context.WithTimeout(context.Background(), time.Second)
	defer c()
	if err := p.Shutdown(sctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("stats:", p.Metrics.Stats())
}
