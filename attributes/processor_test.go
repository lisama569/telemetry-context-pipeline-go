package attributes

import (
	"log"
	"sync"
	"testing"
)

func testConfig() ProcessorConfig {
	return ProcessorConfig{
		MaxCardinality:       2,
		MaxKeys:              3,
		SensitiveKeys:        map[string]struct{}{"ssn": {}},
		SensitiveSubstrings:  []string{"password", "token"},
		RedactionPlaceholder: DefaultRedactionPlaceholder,
	}
}

// TestProcessor_SensitiveFiltered 精确与子串命中的敏感字段值必须脱敏透出占位符，
// 原值绝不能出现在输出中，且原因可区分。
func TestProcessor_SensitiveFiltered(t *testing.T) {
	p := NewProcessor(testConfig())
	in := map[string]string{
		"ssn":          "123-45-6789",
		"userPassword": "hunter2",
		"auth_token":   "abc.def.ghi",
		"region":       "us-east",
	}
	out, results := p.Process(in)

	if out["ssn"] != DefaultRedactionPlaceholder {
		t.Fatalf("精确敏感键应脱敏，得到 %q", out["ssn"])
	}
	if out["userPassword"] != DefaultRedactionPlaceholder {
		t.Fatalf("子串敏感键（大小写不敏感）应脱敏，得到 %q", out["userPassword"])
	}
	if out["auth_token"] != DefaultRedactionPlaceholder {
		t.Fatalf("含 token 子串的键应脱敏，得到 %q", out["auth_token"])
	}
	for k, v := range out {
		if v == "hunter2" || v == "abc.def.ghi" || v == "123-45-6789" {
			t.Fatalf("敏感原值从键 %q 静默透出: %q", k, v)
		}
	}
	byKey := map[string]AttrResult{}
	for _, r := range results {
		byKey[r.Key] = r
	}
	if byKey["ssn"].Reason != ReasonSensitiveExact {
		t.Fatalf("ssn 应标记精确匹配，得到 %s", byKey["ssn"].Reason)
	}
	if byKey["userPassword"].Reason != ReasonSensitiveSubstring {
		t.Fatalf("userPassword 应标记子串匹配，得到 %s", byKey["userPassword"].Reason)
	}
	log.Printf("[判定依据] 敏感字段 ssn=%s userPassword=%s auth_token=%s，原因分别为 %s/%s，原值未透出",
		out["ssn"], out["userPassword"], out["auth_token"],
		byKey["ssn"].Reason, byKey["userPassword"].Reason)
}

// TestProcessor_CardinalityLimit 同一键第 3 个新值必须丢弃，旧值始终放行，
// 丢弃记录可审计而不是静默消失。
func TestProcessor_CardinalityLimit(t *testing.T) {
	p := NewProcessor(testConfig())
	feed := func(v string) (string, AttrResult) {
		out, results := p.Process(map[string]string{"user_id": v})
		var r AttrResult
		for _, x := range results {
			if x.Key == "user_id" {
				r = x
			}
		}
		return out["user_id"], r
	}

	for i, v := range []string{"u1", "u2"} {
		got, r := feed(v)
		if got != v || r.Outcome != OutcomeKept {
			t.Fatalf("第 %d 个基数内应保留，got=%q outcome=%s", i+1, got, r.Outcome)
		}
	}
	got, r := feed("u3")
	if r.Outcome != OutcomeDropped || r.Reason != ReasonCardinality {
		t.Fatalf("超基数新值应丢弃并标记，got=%q outcome=%s reason=%s", got, r.Outcome, r.Reason)
	}
	if got != "" {
		t.Fatalf("超基数值不应出现在输出中，得到 %q", got)
	}
	// 旧值仍放行。
	old, _ := feed("u1")
	if old != "u1" {
		t.Fatalf("已记录的旧值应继续放行，得到 %q", old)
	}
	log.Printf("[判定依据] user_id 基数上限=2：u1/u2 保留，u3 丢弃(%s)，旧值 u1 仍保留", r.Reason)
}

// TestProcessor_MaxKeys 单批键数超过上限时，排序靠后的键被丢弃且原因可区分。
func TestProcessor_MaxKeys(t *testing.T) {
	p := NewProcessor(testConfig())
	in := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}
	out, results := p.Process(in)
	if len(out) != 3 {
		t.Fatalf("MaxKeys=3 应只保留 3 个键，得到 %d: %v", len(out), out)
	}
	if _, dropped := out["d"]; dropped {
		t.Fatalf("排序最后的 d 应被键数上限丢弃")
	}
	var droppedResult AttrResult
	for _, r := range results {
		if r.Key == "d" {
			droppedResult = r
		}
	}
	if droppedResult.Outcome != OutcomeDropped || droppedResult.Reason != ReasonKeyLimit {
		t.Fatalf("d 应标记 max_keys_exceeded，得到 %+v", droppedResult)
	}
	log.Printf("[判定依据] MaxKeys=3：保留 a,b,c，丢弃 d(%s)，无静默丢失", droppedResult.Reason)
}

// TestProcessor_ConsistentAcrossCalls 同一批属性重复处理结论必须稳定一致，
// 模拟“不同入口（span/指标/日志）共用同一 Processor”的场景。
func TestProcessor_ConsistentAcrossCalls(t *testing.T) {
	p := NewProcessor(ProcessorConfig{
		MaxCardinality:       100,
		SensitiveSubstrings:  []string{"token"},
		RedactionPlaceholder: DefaultRedactionPlaceholder,
	})
	in := map[string]string{"auth_token": "zzz", "svc": "billing"}

	var first map[string]string
	for i := 0; i < 5; i++ {
		out, results := p.Process(in)
		if i == 0 {
			first = out
			continue
		}
		if out["auth_token"] != first["auth_token"] || out["svc"] != first["svc"] {
			t.Fatalf("第 %d 次处理与首次不一致: %v vs %v", i+1, out, first)
		}
		if len(results) != 2 {
			t.Fatalf("每次处理都应返回全部输入键的判定依据")
		}
	}
	log.Printf("[关键上下文] 同批属性跨 5 次入口处理结果稳定: auth_token=%s svc=%s",
		first["auth_token"], first["svc"])
}

// TestProcessor_ConcurrentSafe 高并发处理不得产生数据竞争（需配合 -race 运行）。
func TestProcessor_ConcurrentSafe(t *testing.T) {
	p := NewProcessor(ProcessorConfig{
		MaxCardinality:       50,
		SensitiveKeys:        map[string]struct{}{"ssn": {}},
		RedactionPlaceholder: DefaultRedactionPlaceholder,
	})
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				out, results := p.Process(map[string]string{
					"ssn":    "000",
					"bucket": "v",
				})
				if out["ssn"] != DefaultRedactionPlaceholder {
					t.Errorf("并发下敏感键脱敏结论不稳定: %q", out["ssn"])
				}
				if len(results) != 2 {
					t.Errorf("并发下判定依据缺失: %d", len(results))
				}
			}
		}(g)
	}
	wg.Wait()
	log.Printf("[关键上下文] 20 goroutine x 100 次并发 Process 无竞争，敏感键始终脱敏")
}
