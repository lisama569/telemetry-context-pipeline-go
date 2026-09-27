package tracing

import "testing"

func TestRatioSamplerStable(t *testing.T) {
	s := NewRatioSampler(0.5)
	var tid TraceID
	tid[0], tid[15] = 0xab, 0xcd

	first := s.Decide(tid, false, false)
	// 同一 traceID 重复判定必须恒定，保证同链路采样结果一致
	for i := 0; i < 100; i++ {
		d := s.Decide(tid, false, false)
		if d.Sampled != first.Sampled || d.Source != "root" {
			t.Fatalf("判定不一致: got sampled=%v source=%s, want %v/root", d.Sampled, d.Source, first.Sampled)
		}
	}
	t.Logf("traceID=%s %s", tid, first.Reason)
}

func TestRatioSamplerRates(t *testing.T) {
	for _, rate := range []float64{0, 0.25, 0.5, 1} {
		s := NewRatioSampler(rate)
		var sampled, total int
		for i := 0; i < 2000; i++ {
			var tid TraceID
			tid[0] = byte(i >> 8)
			tid[1] = byte(i)
			if s.Decide(tid, false, false).Sampled {
				sampled++
			}
			total++
		}
		ratio := float64(sampled) / float64(total)
		if rate == 0 && sampled != 0 {
			t.Fatalf("rate=0 不应有采样, got %d", sampled)
		}
		if rate == 1 && sampled != total {
			t.Fatalf("rate=1 应全部采样, got %d/%d", sampled, total)
		}
		if rate > 0 && rate < 1 {
			delta := ratio - rate
			if delta < -0.05 || delta > 0.05 {
				t.Fatalf("rate=%.2f 实际比例偏差过大: %.3f", rate, ratio)
			}
		}
		t.Logf("rate=%.2f 实际采样比例=%.3f (%d/%d)", rate, ratio, sampled, total)
	}
}

func TestSamplerInheritsParent(t *testing.T) {
	s := NewRatioSampler(1.0)
	var tid TraceID
	tid[0] = 1
	// 无论父 sampled 是真是假，子链路必须原样继承
	for _, parent := range []bool{true, false} {
		d := s.Decide(tid, true, parent)
		if d.Sampled != parent || d.Source != "parent" {
			t.Fatalf("父 sampled=%v 子得到 %v/%s, 原因=%s", parent, d.Sampled, d.Source, d.Reason)
		}
		t.Logf("父 sampled=%v => %s", parent, d.Reason)
	}
}
