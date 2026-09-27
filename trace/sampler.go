package trace

import (
	"encoding/binary"
	"math"
	"sync"
)

// Sampler 负责采样判定。
type Sampler interface {
	// Sample 对一个新建 Span 做采样判定。
	// 若 tc 是已有链路的延续（TraceID 非零且已有判定），必须沿用其 Decision；
	// 只有链路根节点才依据 TraceID 与采样率做稳定判定。
	Sample(tc Context, name string) Decision
}

// DeterministicSampler 是基于 TraceID 的确定性采样器：
// 同一个 TraceID 在任意入口、任意时刻（采样率不变时）得到的结论一致，
// 因此同一条链路上的所有 Span 采样结果统一且可按 TraceID 复算追溯。
type DeterministicSampler struct {
	mu   sync.RWMutex
	rate float64
}

// NewDeterministicSampler 创建采样器，rate 会被夹取到 [0,1]。
func NewDeterministicSampler(rate float64) *DeterministicSampler {
	return &DeterministicSampler{rate: clampRate(rate)}
}

// Rate 返回当前采样率。
func (s *DeterministicSampler) Rate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rate
}

// SetRate 动态调整采样率。只影响调整之后“新开启的链路根节点”，
// 已经产生的链路在延续时沿用记录在 Trace Context 上的旧判定，不会中途翻转。
func (s *DeterministicSampler) SetRate(rate float64) {
	s.mu.Lock()
	s.rate = clampRate(rate)
	s.mu.Unlock()
}

// Sample 实现 Sampler。
func (s *DeterministicSampler) Sample(tc Context, name string) Decision {
	// 延续已有链路：判定以随链路传递的 Context 为准，保证一致。
	if tc.Decision != DecisionUnknown {
		return tc.Decision
	}
	s.mu.RLock()
	rate := s.rate
	s.mu.RUnlock()
	return decisionForID(tc.TraceID, rate)
}

// decisionForID 是确定性采样的核心算法：
// 取 TraceID 前 8 字节映射到 [0,1)，小于 rate 即采样。
// rate<=0 必丢弃，rate>=1 必采样。纯函数，相同输入永远得到相同输出。
func decisionForID(id TraceID, rate float64) Decision {
	if rate <= 0 {
		return DecisionDrop
	}
	if rate >= 1 {
		return DecisionRecordAndSample
	}
	v := binary.BigEndian.Uint64(id[:8])
	// 用阈值比较代替浮点除法：v/2^64 < rate  <=>  v < rate*2^64。
	threshold := rate * float64(math.MaxUint64)
	if float64(v) < threshold {
		return DecisionRecordAndSample
	}
	return DecisionDrop
}

func clampRate(rate float64) float64 {
	if math.IsNaN(rate) || rate < 0 {
		return 0
	}
	if rate > 1 {
		return 1
	}
	return rate
}
