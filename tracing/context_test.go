package tracing

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStartSpanInheritsTraceAndSampling(t *testing.T) {
	tr := NewTracer(NewRatioSampler(0.5))
	ctx, root := tr.Start(context.Background(), "root")
	childCtx, child := tr.Start(ctx, "child")

	rc, cc := root.Context(), child.Context()
	if rc.TraceID != cc.TraceID {
		t.Fatalf("traceID 未继承: root=%x child=%x", rc.TraceID, cc.TraceID)
	}
	if rc.SpanID == cc.SpanID {
		t.Fatal("子 span 不应复用父 spanID")
	}
	if rc.Sampled != cc.Sampled || cc.SamplingSource != "parent" {
		t.Fatalf("采样结论未正确继承: parent=%v child=%v src=%s reason=%s",
			rc.Sampled, cc.Sampled, cc.SamplingSource, cc.SamplingReason)
	}
	if psc, ok := SpanFromContext(childCtx); !ok || psc.SpanID != cc.SpanID {
		t.Fatal("context 中取出的 SpanContext 与子 span 不匹配")
	}
	t.Logf("trace=%s rootSampled=%v childSampled=%v src=%s",
		cc.TraceIDHex(), rc.Sampled, cc.Sampled, cc.SamplingSource)
}

func TestContinueAsyncSurvivesParentCancel(t *testing.T) {
	tr := NewTracer(NewRatioSampler(1.0))
	parentCtx, root := tr.Start(context.Background(), "root")
	parentCtx, cancelParent := context.WithCancel(parentCtx)
	root.SetBaggage("user", "alice")

	asyncCtx, cancelAsync := ContinueAsync(parentCtx)

	// 父任务被取消（例如请求超时返回），异步任务上下文必须仍然存活且保留追踪信息
	cancelParent()
	select {
	case <-asyncCtx.Done():
		t.Fatal("父 context 取消后异步上下文被连带取消，追踪上下文会丢失")
	default:
	}

	sc, ok := SpanFromContext(asyncCtx)
	if !ok {
		t.Fatal("异步上下文中丢失了 SpanContext")
	}
	if sc.TraceID != root.Context().TraceID {
		t.Fatal("异步上下文 traceID 错配")
	}
	if v, ok := sc.Baggage["user"]; !ok || v != "alice" {
		t.Fatalf("异步上下文 baggage 未继承: %v", sc.Baggage)
	}
	if !sc.Sampled {
		t.Fatal("采样结论在异步延续后丢失")
	}

	// 异步任务可被自己独立取消
	cancelAsync()
	select {
	case <-asyncCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("异步 CancelFunc 未生效")
	}
	t.Logf("父取消后异步任务仍持有 trace=%s user=%s sampled=%v",
		sc.TraceIDHex(), sc.Baggage["user"], sc.Sampled)
}

func TestContinueAsyncSurvivesParentTimeout(t *testing.T) {
	tr := NewTracer(NewRatioSampler(1.0))
	parentCtx, _ := tr.Start(context.Background(), "root")
	parentCtx, cancel := context.WithTimeout(parentCtx, 10*time.Millisecond)
	defer cancel()

	asyncCtx, cancelAsync := ContinueAsync(parentCtx)
	defer cancelAsync()
	time.Sleep(30 * time.Millisecond) // 等父超时

	select {
	case <-asyncCtx.Done():
		t.Fatal("父超时不应取消异步任务")
	default:
	}
	if _, ok := SpanFromContext(asyncCtx); !ok {
		t.Fatal("父超时后异步追踪上下文丢失")
	}
	t.Log("父链路超时后异步上下文依然有效，traceID 与采样结论保留")
}

// TestConcurrentAsyncNoCrossTalk 验证多个并发交错的异步任务：
// 同源派生时各自持有 SpanContext 副本，对副本的修改互不影响。
func TestConcurrentAsyncNoCrossTalk(t *testing.T) {
	tr := NewTracer(NewRatioSampler(1.0))
	ctx, root := tr.Start(context.Background(), "root")
	root.SetBaggage("shared", "base")

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			actx, cancel := ContinueAsync(ctx)
			defer cancel()

			sc, ok := SpanFromContext(actx)
			if !ok || sc.TraceID != root.Context().TraceID {
				errs <- errString("traceID 错配或丢失")
				return
			}
			// 在自己的副本上写 baggage，其它 goroutine 不应看到
			sc.Baggage["task"] = taskName(id)
			time.Sleep(5 * time.Millisecond) // 制造交错
			if sc.Baggage["task"] != taskName(id) {
				errs <- errString("baggage 并发错配")
				return
			}
			if sc.Baggage["shared"] != "base" {
				errs <- errString("继承的 baggage 被破坏")
				return
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	// 原始 span 与父 ctx 的 baggage 不应被异步副本污染
	if v, _ := root.Baggage("task"); v != "" {
		t.Fatal("异步副本的 baggage 写穿透到了 root span")
	}
	if psc, _ := SpanFromContext(ctx); psc.Baggage["task"] != "" {
		t.Fatal("父 context 中的 SpanContext 被异步任务修改")
	}
	t.Logf("%d 个并发异步任务上下文全部正确隔离且 traceID 一致", n)
}

type errString string

func (e errString) Error() string { return string(e) }
func taskName(id int) string      { return "task-" + itoa(id) }
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
