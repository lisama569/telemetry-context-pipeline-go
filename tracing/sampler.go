package tracing

import (
	"fmt"
	"hash/fnv"
)

// Decision 描述一次采样判定及其依据，保证可追溯。
type Decision struct {
	Sampled bool
	Source  string // "root" | "parent"
	Reason  string // 判定依据的可读说明（含哈希桶与阈值，便于复核）
}

// Sampler 根据 traceID 与父链路状态做采样判定。
type Sampler interface {
	Decide(traceID TraceID, hasParent bool, parentSampled bool) Decision
}

// samplingDenominator 把采样率映射到整数桶空间，避免浮点比较的不确定性。
const samplingDenominator = 1_000_000

// RatioSampler 按 traceID 的 FNV-1a 哈希做比例采样。
//
// 规则：
//   - 存在父链路时无条件沿用父链路的采样结果（保证同一链路一致）；
//   - 根链路按 rate 落桶判定，同一 traceID 永远落入同一桶，结论可复算。
type RatioSampler struct {
	rate      float64
	threshold uint32 // rate * samplingDenominator
}

// NewRatioSampler 创建比例采样器。rate 会被裁剪到 [0,1]。
func NewRatioSampler(rate float64) *RatioSampler {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return &RatioSampler{rate: rate, threshold: uint32(rate * samplingDenominator)}
}

// Rate 返回采样器配置的采样率。
func (s *RatioSampler) Rate() float64 { return s.rate }

// hashBucket 把 traceID 稳定映射到 [0, samplingDenominator)。
func hashBucket(traceID TraceID) uint32 {
	h := fnv.New64a()
	h.Write(traceID[:])
	return uint32(h.Sum64() % samplingDenominator)
}

// Decide 实现 Sampler。
func (s *RatioSampler) Decide(traceID TraceID, hasParent bool, parentSampled bool) Decision {
	if hasParent {
		return Decision{
			Sampled: parentSampled,
			Source:  "parent",
			Reason:  fmt.Sprintf("inherited sampled=%t from parent span", parentSampled),
		}
	}
	bucket := hashBucket(traceID)
	sampled := bucket < s.threshold
	return Decision{
		Sampled: sampled,
		Source:  "root",
		Reason: fmt.Sprintf("trace bucket=%d threshold=%d (rate=%.4f, mod=%d) => sampled=%t",
			bucket, s.threshold, s.rate, samplingDenominator, sampled),
	}
}
