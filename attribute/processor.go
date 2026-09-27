package attribute

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/lisama569/telemetry-context-pipeline-go/config"
)

// Action 描述单个属性的处理结果。
type Action string

const (
	ActionKept      Action = "kept"
	ActionMasked    Action = "masked"
	ActionTruncated Action = "truncated"
	ActionDropped   Action = "dropped"
)

// ItemResult 记录单个属性的处理动作与原因。
type ItemResult struct {
	Key    string
	Action Action
	Reason string
}

// Report 是一次属性处理的判定明细，保证结果可解释、可复核。
type Report struct {
	Items     []ItemResult
	Kept      int
	Masked    int
	Truncated int
	Dropped   int
}

// SortedItems 按 key 排序返回明细，便于日志稳定输出。
func (r Report) SortedItems() []ItemResult {
	out := append([]ItemResult(nil), r.Items...)
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Processor 是所有入口（span / metric label / log）共用的属性处理器，
// 保证同一属性在不同入口的处理结果一致。
//
// 处理规则（对排序后的 key 顺序执行，结果确定）：
//  1. 命中敏感名单 -> 整体替换为占位符（不静默透出原值）；
//  2. 字符串超过 MaxValueLength -> 截断（报告 truncated）；
//  3. key 数量超过 MaxKeysPerRecord -> 超出部分整条丢弃（dropped）；
//  4. 同一 key 已见过的不同值超过 MaxValuesPerKey（基数上限）
//     -> 新值归入固定 OtherBucket 桶，防止高基数字段撑爆存储。
type Processor struct {
	cfg config.AttributeConfig

	mu       sync.Mutex
	patterns []matcher
	seen     map[string]map[string]struct{}
	allowed  map[string]string // key -> 被保留的第一个值
}

// matcher 判断一个小写 key 是否命中敏感规则。
type matcher func(lowerKey string) bool

// New 创建处理器并预编译敏感 key 匹配规则。
func New(cfg config.AttributeConfig) *Processor {
	p := &Processor{
		cfg:     cfg,
		seen:    map[string]map[string]struct{}{},
		allowed: map[string]string{},
	}
	for _, k := range cfg.SensitiveKeys {
		p.patterns = append(p.patterns, compileMatcher(strings.ToLower(k)))
	}
	return p
}

// compileMatcher 支持精确匹配与前/后缀 * 通配。
func compileMatcher(pat string) matcher {
	switch {
	case strings.HasPrefix(pat, "*") && strings.HasSuffix(pat, "*"):
		mid := strings.Trim(pat, "*")
		return func(k string) bool { return mid == "" || strings.Contains(k, mid) }
	case strings.HasPrefix(pat, "*"):
		suf := strings.TrimPrefix(pat, "*")
		return func(k string) bool { return strings.HasSuffix(k, suf) }
	case strings.HasSuffix(pat, "*"):
		pre := strings.TrimSuffix(pat, "*")
		return func(k string) bool { return strings.HasPrefix(k, pre) }
	default:
		return func(k string) bool { return k == pat }
	}
}

func (p *Processor) isSensitive(key string) bool {
	lk := strings.ToLower(key)
	for _, m := range p.patterns {
		if m(lk) {
			return true
		}
	}
	return false
}

// cardinalityValue 决定一个值在基数控制下的最终字符串值。
// 调用方需持有 p.mu。
func (p *Processor) cardinalityValue(key, raw string) string {
	if p.cfg.MaxValuesPerKey <= 0 {
		return raw
	}
	vals := p.seen[key]
	if vals == nil {
		vals = map[string]struct{}{}
		p.seen[key] = vals
	}
	if _, known := vals[raw]; known {
		return raw
	}
	// 新值：未超限则登记并保留；超限则归入固定桶。
	if len(vals) < p.cfg.MaxValuesPerKey {
		vals[raw] = struct{}{}
		return raw
	}
	return p.cfg.OtherBucket
}

// truncate 按字节安全截断并附提示。
func (p *Processor) truncate(v string) (string, bool) {
	if p.cfg.MaxValueLength <= 0 || len(v) <= p.cfg.MaxValueLength {
		return v, false
	}
	return v[:p.cfg.MaxValueLength] + "...", true
}

func (p *Processor) note(rpt *Report, it ItemResult) {
	rpt.Items = append(rpt.Items, it)
	switch it.Action {
	case ActionKept:
		rpt.Kept++
	case ActionMasked:
		rpt.Masked++
	case ActionTruncated:
		rpt.Truncated++
	case ActionDropped:
		rpt.Dropped++
	}
}

// Process 处理任意类型属性，返回处理后的安全属性集合与判定明细。
func (p *Processor) Process(attrs map[string]any) (map[string]any, Report) {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make(map[string]any, len(attrs))
	rpt := Report{}
	p.mu.Lock()
	defer p.mu.Unlock()

	keptCount := 0
	for _, k := range keys {
		raw := attrs[k]
		switch {
		case p.isSensitive(k):
			out[k] = p.cfg.RedactedPlaceholder
			p.note(&rpt, ItemResult{k, ActionMasked, "key 命中敏感字段名单"})
		case p.cfg.MaxKeysPerRecord > 0 && keptCount >= p.cfg.MaxKeysPerRecord:
			p.note(&rpt, ItemResult{k, ActionDropped,
				fmt.Sprintf("超过单条最大属性数 %d", p.cfg.MaxKeysPerRecord)})
		default:
			value := raw
			reason := ""
			if sv, ok := raw.(string); ok {
				var wasTrunc bool
				sv, wasTrunc = p.truncate(sv)
				if wasTrunc {
					reason = fmt.Sprintf("超过最大长度 %d 已截断", p.cfg.MaxValueLength)
				}
				sv = p.cardinalityValue(k, sv)
				value = sv
			}
			out[k] = value
			keptCount++
			if reason != "" {
				p.note(&rpt, ItemResult{k, ActionTruncated, reason})
			} else {
				p.note(&rpt, ItemResult{k, ActionKept, "未命中任何限制"})
			}
		}
	}
	return out, rpt
}

// ProcessStrings 处理字符串属性（指标 label 入口），规则与 Process 完全一致。
func (p *Processor) ProcessStrings(attrs map[string]string) (map[string]string, Report) {
	anyAttrs := make(map[string]any, len(attrs))
	for k, v := range attrs {
		anyAttrs[k] = v
	}
	outAny, rpt := p.Process(anyAttrs)
	out := make(map[string]string, len(outAny))
	for k, v := range outAny {
		if sv, ok := v.(string); ok {
			out[k] = sv
		} else {
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out, rpt
}

// CardinalitySnapshot 返回某 key 当前已登记的不同值数量，仅用于测试与诊断。
func (p *Processor) CardinalitySnapshot(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.seen[key])
}
