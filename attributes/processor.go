package attributes

import (
	"sort"
	"strings"
	"sync"
)

// 默认占位符与判定原因，保证不同入口的处理输出格式一致。
const DefaultRedactionPlaceholder = "[REDACTED]"

// Outcome 表示单个属性被处理后的去向。
type Outcome int

const (
	// OutcomeKept 正常保留。
	OutcomeKept Outcome = iota
	// OutcomeRedacted 命中敏感规则，值被替换为脱敏占位符（键保留，不静默透出）。
	OutcomeRedacted
	// OutcomeDropped 超出基数限制或键数限制，被丢弃（在返回结果中可审计）。
	OutcomeDropped
)

// String 返回去向的可读名称，便于日志与测试输出判定依据。
func (o Outcome) String() string {
	switch o {
	case OutcomeRedacted:
		return "REDACTED"
	case OutcomeDropped:
		return "DROPPED"
	default:
		return "KEPT"
	}
}

// 判定原因常量，失败/拦截原因可区分。
const (
	ReasonKept               = "kept"
	ReasonSensitiveExact     = "sensitive_exact_match"
	ReasonSensitiveSubstring = "sensitive_substring_match"
	ReasonCardinality        = "cardinality_exceeded"
	ReasonKeyLimit           = "max_keys_exceeded"
)

// AttrResult 记录单个属性的处理结果与判定依据。
type AttrResult struct {
	Key     string
	Outcome Outcome
	Reason  string
}

// ProcessorConfig 配置属性处理器。零值字段按“不限制 / 默认占位符”解释。
type ProcessorConfig struct {
	// MaxCardinality 单个属性键允许的不同取值数量上限；<=0 表示不限。
	// 已见过的旧值始终放行，只有超限后的“新值”被丢弃。
	MaxCardinality int
	// MaxKeys 一次 Process 调用允许保留的属性键总数上限；<=0 表示不限。
	MaxKeys int
	// SensitiveKeys 精确匹配的敏感键名集合。
	SensitiveKeys map[string]struct{}
	// SensitiveSubstrings 键名包含任一子串即视为敏感（大小写不敏感）。
	SensitiveSubstrings []string
	// RedactionPlaceholder 脱敏后写入的值；为空时使用默认占位符。
	RedactionPlaceholder string
}

// Processor 对属性做敏感字段过滤与基数控制。
// 同一属性在任意入口（追踪/指标/日志）的处理结果必须一致，实现并发安全。
// 处理顺序按键名排序固定，保证同一批输入在任意入口得到完全一致的输出顺序与判定。
type Processor struct {
	cfg      ProcessorConfig
	mu       sync.Mutex
	seen     map[string]map[string]struct{}
	keptKeys int
}

// NewProcessor 创建处理器并规范化配置。
func NewProcessor(cfg ProcessorConfig) *Processor {
	if strings.TrimSpace(cfg.RedactionPlaceholder) == "" {
		cfg.RedactionPlaceholder = DefaultRedactionPlaceholder
	}
	return &Processor{cfg: cfg, seen: make(map[string]map[string]struct{})}
}

// Process 处理一批键值属性，返回保留下来的属性与每个输入键的判定结果。
//   - 命中敏感字段：键保留、值替换为占位符，结果标为 REDACTED，绝不透出原值；
//   - 超过单键基数：该值丢弃，结果标为 DROPPED/cardinality_exceeded；
//   - 超过单批键数：排在后面的键丢弃，结果标为 DROPPED/max_keys_exceeded。
//
// 被拦截的字段都会出现在第二个返回值中，调用方可据此打印审计日志，不存在“静默消失”。
func (p *Processor) Process(attrs map[string]string) (map[string]string, []AttrResult) {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	p.mu.Lock()
	defer p.mu.Unlock()

	out := make(map[string]string, len(keys))
	results := make([]AttrResult, 0, len(keys))
	keptInBatch := 0

	for _, k := range keys {
		v := attrs[k]

		// 1) 敏感字段优先：任何情况下原值都不得透出。
		if reason, sensitive := sensitiveReason(k, p.cfg); sensitive {
			out[k] = p.cfg.RedactionPlaceholder
			results = append(results, AttrResult{Key: k, Outcome: OutcomeRedacted, Reason: reason})
			keptInBatch++
			continue
		}

		// 2) 单批键数上限。
		if p.cfg.MaxKeys > 0 && keptInBatch >= p.cfg.MaxKeys {
			results = append(results, AttrResult{Key: k, Outcome: OutcomeDropped, Reason: ReasonKeyLimit})
			continue
		}

		// 3) 单键基数上限：旧值放行，新值超限丢弃。
		if p.cfg.MaxCardinality > 0 {
			values := p.seen[k]
			if values == nil {
				values = make(map[string]struct{})
				p.seen[k] = values
			}
			if _, known := values[v]; !known && len(values) >= p.cfg.MaxCardinality {
				results = append(results, AttrResult{Key: k, Outcome: OutcomeDropped, Reason: ReasonCardinality})
				continue
			}
			values[v] = struct{}{}
		}

		out[k] = v
		results = append(results, AttrResult{Key: k, Outcome: OutcomeKept, Reason: ReasonKept})
		keptInBatch++
	}

	p.keptKeys += keptInBatch
	return out, results
}

// Reset 清空已记录的基数状态（敏感规则等配置不变）。
func (p *Processor) Reset() {
	p.mu.Lock()
	p.seen = make(map[string]map[string]struct{})
	p.keptKeys = 0
	p.mu.Unlock()
}

// sensitiveReason 返回命中的敏感规则类型，原因可区分。
func sensitiveReason(key string, cfg ProcessorConfig) (string, bool) {
	if _, ok := cfg.SensitiveKeys[key]; ok {
		return ReasonSensitiveExact, true
	}
	if isSensitive(key, cfg) {
		return ReasonSensitiveSubstring, true
	}
	return "", false
}
