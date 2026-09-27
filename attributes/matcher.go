package attributes

import "strings"

// isSensitive 判断键名是否命中敏感规则。
// 精确匹配优先，其次做大小写不敏感的子串匹配。
// 该函数为纯函数：同一个键在追踪/指标/日志任意入口得到的结论一致。
func isSensitive(key string, cfg ProcessorConfig) bool {
	if _, ok := cfg.SensitiveKeys[key]; ok {
		return true
	}
	lk := lower(key)
	for _, sub := range cfg.SensitiveSubstrings {
		if sub == "" {
			continue
		}
		if strings.Contains(lk, lower(sub)) {
			return true
		}
	}
	return false
}

// lower 为子串匹配提供统一的小写形式。
func lower(s string) string { return strings.ToLower(s) }
