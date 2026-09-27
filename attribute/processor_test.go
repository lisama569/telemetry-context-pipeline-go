package attribute

import (
	"strings"
	"sync"
	"testing"

	"github.com/lisama569/telemetry-context-pipeline-go/config"
)

func testCfg() config.AttributeConfig {
	return config.AttributeConfig{
		MaxKeysPerRecord:    3,
		MaxValuesPerKey:     2,
		MaxValueLength:      5,
		SensitiveKeys:       []string{"password", "*token", "secret*"},
		RedactedPlaceholder: "[REDACTED]",
		OtherBucket:         "other",
	}
}

func findItem(r Report, key string) (ItemResult, bool) {
	for _, it := range r.Items {
		if it.Key == key {
			return it, true
		}
	}
	return ItemResult{}, false
}

func TestSensitiveMasking(t *testing.T) {
	p := New(testCfg())
	cases := []string{"password", "api_token", "secret_code", "Password"}
	in := map[string]any{}
	for _, k := range cases {
		in[k] = "do-not-leak"
	}
	out, rpt := p.Process(in)
	for _, k := range cases {
		if out[k] != "[REDACTED]" {
			t.Fatalf("key=%s 未脱敏, out=%v", k, out)
		}
		it, _ := findItem(rpt, k)
		if it.Action != ActionMasked {
			t.Fatalf("key=%s 动作=%s, want masked", k, it.Action)
		}
	}
	if rpt.Masked != len(cases) {
		t.Fatalf("masked 计数=%d, want %d", rpt.Masked, len(cases))
	}
	t.Logf("敏感字段全部命中脱敏: %d 个, 明细=%v", rpt.Masked, rpt.SortedItems())
}

func TestTruncation(t *testing.T) {
	p := New(testCfg())
	out, rpt := p.Process(map[string]any{"name": "abcdefghij"})
	got, _ := out["name"].(string)
	if !strings.HasPrefix(got, "abcde") {
		t.Fatalf("截断结果错误: %q", got)
	}
	it, _ := findItem(rpt, "name")
	if it.Action != ActionTruncated {
		t.Fatalf("动作=%s want truncated, 原因=%s", it.Action, it.Reason)
	}
	t.Logf("截断判定: %s", it.Reason)
}

func TestMaxKeysDropsExcess(t *testing.T) {
	p := New(testCfg())
	in := map[string]any{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}
	out, rpt := p.Process(in)
	if rpt.Dropped != 2 || rpt.Kept != 3 {
		t.Fatalf("kept=%d dropped=%d, want 3/2; 明细=%v", rpt.Kept, rpt.Dropped, rpt.SortedItems())
	}
	if _, exists := out["d"]; exists {
		t.Fatal("超出键数上限的字段不应透出")
	}
	for _, k := range []string{"d", "e"} {
		it, _ := findItem(rpt, k)
		if it.Action != ActionDropped {
			t.Fatalf("key=%s 动作=%s want dropped", k, it.Action)
		}
	}
	t.Logf("键数限制生效, kept=%d dropped=%d: %v", rpt.Kept, rpt.Dropped, rpt.SortedItems())
}

func TestCardinalityBucket(t *testing.T) {
	p := New(testCfg())
	// 同一 key 前 2 个不同值保留，第 3 个起归入 other 桶
	for _, v := range []string{"v1", "v1", "v2", "v3", "v4"} {
		out, _ := p.Process(map[string]any{"tenant": v})
		got := out["tenant"]
		want := v
		if v == "v3" || v == "v4" {
			want = "other"
		}
		if got != want {
			t.Fatalf("tenant=%s 输出=%v want=%s", v, got, want)
		}
	}
	if n := p.CardinalitySnapshot("tenant"); n != 2 {
		t.Fatalf("登记基数=%d want 2", n)
	}
	t.Log("高基数值稳定归入 other 桶，未见值泄漏")
}

// TestConsistencyAcrossEntries 要求任意值类型入口与字符串入口结论一致。
func TestConsistencyAcrossEntries(t *testing.T) {
	cfg := testCfg()
	p1 := New(cfg)
	p2 := New(cfg)
	inAny := map[string]any{"password": "x", "name": "abcdefghij"}
	inStr := map[string]string{"password": "x", "name": "abcdefghij"}

	o1, r1 := p1.Process(inAny)
	o2, r2 := p2.ProcessStrings(inStr)
	if o1["password"] != o2["password"] {
		t.Fatalf("脱敏结果不一致: %v vs %v", o1, o2)
	}
	if o1["name"] != o2["name"] {
		t.Fatalf("截断结果不一致: %v vs %v", o1["name"], o2["name"])
	}
	if r1.Masked != r2.Masked || r1.Truncated != r2.Truncated {
		t.Fatalf("两个入口报告不一致: %+v vs %+v", r1, r2)
	}
	t.Logf("Process 与 ProcessStrings 判定一致: %v", r1.SortedItems())
}

func TestConcurrentProcessingSafe(t *testing.T) {
	p := New(testCfg())
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p.Process(map[string]any{"k": string(rune('a' + n%5)), "password": "p"})
			p.ProcessStrings(map[string]string{"k2": string(rune('a' + n%5))})
		}(i)
	}
	wg.Wait()
	t.Log("并发处理 100 goroutine 无数据竞争")
}
