package config

import "testing"

func TestDefaultValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("默认配置应合法: %v", err)
	}
	t.Log("默认配置通过校验")
}

func TestValidateRejectsBadValues(t *testing.T) {
	bad := Default()
	bad.SampleRate = 1.5
	bad.Metrics.QueueSize = 0
	bad.Attributes.RedactedPlaceholder = ""
	err := bad.Validate()
	if err == nil {
		t.Fatal("非法配置应返回错误")
	}
	t.Logf("非法配置被拒绝: %v", err)
}
