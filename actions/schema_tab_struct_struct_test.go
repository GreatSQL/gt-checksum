package actions

import (
	"testing"
)

// TestShouldReturnNoValidTables 验证 Struct() 函数中判断是否应返回 ErrNoValidTables 的条件逻辑。
// 回归测试：当 checkObject=struct 且所有表都有结构差异时，不应返回 ErrNoValidTables。
// 原 bug：条件 len(normal) == 0 && len(abnormal) > 0 错误地将有结构差异的表视为"无有效表"。
func TestShouldReturnNoValidTables(t *testing.T) {
	tests := []struct {
		name     string
		normal   []string
		abnormal []string
		want     bool
	}{
		{
			name:     "both empty - should return true (no valid tables)",
			normal:   []string{},
			abnormal: []string{},
			want:     true,
		},
		{
			name:     "only normal tables - should return false",
			normal:   []string{"sbtest.t1", "sbtest.t2"},
			abnormal: []string{},
			want:     false,
		},
		{
			name:     "only abnormal tables - should return false (struct check should proceed)",
			normal:   []string{},
			abnormal: []string{"sbtest.indext"},
			want:     false,
		},
		{
			name:     "both normal and abnormal - should return false",
			normal:   []string{"sbtest.t1"},
			abnormal: []string{"sbtest.indext"},
			want:     false,
		},
		{
			name:     "multiple abnormal tables - should return false",
			normal:   []string{},
			abnormal: []string{"sbtest.indext", "sbtest.t8"},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldReturnNoValidTables(tt.normal, tt.abnormal)
			if got != tt.want {
				t.Errorf("shouldReturnNoValidTables(normal=%v, abnormal=%v) = %v, want %v",
					tt.normal, tt.abnormal, got, tt.want)
			}
		})
	}
}
