package config

import (
	"errors"
	"fmt"
	"time"
)

// Config 是整套遥测管线的总配置。
type Config struct {
	ServiceName string
	SampleRate  float64 // 根链路采样率，取值 [0,1]
	Attributes  AttributeConfig
	Metrics     MetricConfig
}

// AttributeConfig 控制属性基数与敏感字段过滤。
//
// 该配置在 tracing / metric / log 三个入口共享，保证同一属性
// 在任何入口的处理结果（脱敏 / 截断 / 丢弃）一致。
type AttributeConfig struct {
	MaxKeysPerRecord    int      // 单条记录允许的最大属性数量，超出丢弃
	MaxValuesPerKey     int      // 每个 key 允许出现的不同 value 数量（基数上限），超出的值统一落到 OtherBucket
	MaxValueLength      int      // 单个字符串 value 的最大字节长度，超出截断
	SensitiveKeys       []string // 命中即脱敏的 key（大小写不敏感，前缀/后缀 * 通配）
	RedactedPlaceholder string   // 敏感值替换占位符
	OtherBucket         string   // 基数超限后值归入的桶名
}

// MetricConfig 控制指标缓冲、导出与降级策略。
type MetricConfig struct {
	QueueSize            int           // 有界写入队列容量，满后非阻塞丢弃
	BatchSize            int           // 触发批量导出的数据点条数
	ExportInterval       time.Duration // 定时导出间隔
	ExportTimeout        time.Duration // 单次导出超时
	MaxRetries           int           // 单批导出失败的最大重试次数
	FailureBackoff       time.Duration // 重试退避（线性）
	UnavailableThreshold int           // 连续失败多少次后判定下游不可用（熔断打开）
	UnavailableCooldown  time.Duration // 熔断打开后多久尝试半开探测
	CloseTimeout         time.Duration // Close 时的最大冲刷时间
}

// Default 返回一份可用的默认配置。
func Default() Config {
	return Config{
		ServiceName: "unknown-service",
		SampleRate:  1.0,
		Attributes: AttributeConfig{
			MaxKeysPerRecord:    64,
			MaxValuesPerKey:     128,
			MaxValueLength:      1024,
			SensitiveKeys:       []string{"password", "passwd", "*token", "authorization", "secret", "*secret", "*key", "api_key"},
			RedactedPlaceholder: "[REDACTED]",
			OtherBucket:         "other",
		},
		Metrics: MetricConfig{
			QueueSize:            4096,
			BatchSize:            256,
			ExportInterval:       5 * time.Second,
			ExportTimeout:        2 * time.Second,
			MaxRetries:           2,
			FailureBackoff:       200 * time.Millisecond,
			UnavailableThreshold: 5,
			UnavailableCooldown:  10 * time.Second,
			CloseTimeout:         3 * time.Second,
		},
	}
}

// Validate 校验配置项，非法配置必须在启动阶段暴露而不是静默失效。
func (c Config) Validate() error {
	var errs []error
	if c.SampleRate < 0 || c.SampleRate > 1 {
		errs = append(errs, fmt.Errorf("SampleRate 必须在 [0,1]，当前为 %v", c.SampleRate))
	}
	a := c.Attributes
	if a.MaxKeysPerRecord < 0 {
		errs = append(errs, errors.New("Attributes.MaxKeysPerRecord 不能为负"))
	}
	if a.MaxValuesPerKey < 0 {
		errs = append(errs, errors.New("Attributes.MaxValuesPerKey 不能为负"))
	}
	if a.MaxValueLength < 0 {
		errs = append(errs, errors.New("Attributes.MaxValueLength 不能为负"))
	}
	if a.RedactedPlaceholder == "" {
		errs = append(errs, errors.New("Attributes.RedactedPlaceholder 不能为空"))
	}
	m := c.Metrics
	if m.QueueSize <= 0 {
		errs = append(errs, errors.New("Metrics.QueueSize 必须为正数"))
	}
	if m.BatchSize <= 0 {
		errs = append(errs, errors.New("Metrics.BatchSize 必须为正数"))
	}
	if m.ExportInterval <= 0 {
		errs = append(errs, errors.New("Metrics.ExportInterval 必须为正数"))
	}
	if m.ExportTimeout <= 0 {
		errs = append(errs, errors.New("Metrics.ExportTimeout 必须为正数"))
	}
	if m.MaxRetries < 0 {
		errs = append(errs, errors.New("Metrics.MaxRetries 不能为负"))
	}
	if m.UnavailableThreshold <= 0 {
		errs = append(errs, errors.New("Metrics.UnavailableThreshold 必须为正数"))
	}
	if m.UnavailableCooldown <= 0 {
		errs = append(errs, errors.New("Metrics.UnavailableCooldown 必须为正数"))
	}
	if m.CloseTimeout <= 0 {
		errs = append(errs, errors.New("Metrics.CloseTimeout 必须为正数"))
	}
	return errors.Join(errs...)
}
