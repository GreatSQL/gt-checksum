package actions

import "testing"

// TestBuildChunkRangeWhere_EmptyBoundsSkipped 验证当分片下/上界为空时
// 不再生成 `col >= ''` / `col < ''` 这类在 Oracle 数值列会触发
// ORA-01722 (invalid number) 的谓词。
// 回归测例：regression-oracle-20260417-084030 中 TESTBIT(F1 NUMBER)
// 触发了 `WHERE "F1" >= ''` 的 ORA-01722 错误。
func TestBuildChunkRangeWhere_EmptyBoundsSkipped(t *testing.T) {
	cases := []struct {
		name          string
		outer         string
		col           string
		low           string
		high          string
		highInclusive bool
		want          string
	}{
		{
			name: "empty low and high returns outer only",
			col:  "F1", low: "", high: "", outer: "",
			want: "1=1",
		},
		{
			name: "empty low with outer returns outer only",
			col:  "F1", low: "", high: "", outer: "a=1",
			want: "a=1",
		},
		{
			name: "empty low keeps only upper bound",
			col:  "F1", low: "", high: "10",
			want: "`F1` < '10'",
		},
		{
			name: "empty high keeps only lower bound",
			col:  "F1", low: "1", high: "",
			want: "`F1` >= '1'",
		},
		{
			name: "both bounds combined with outer",
			col:  "F1", low: "1", high: "10", outer: "b=2",
			want: "b=2 and `F1` >= '1' and `F1` < '10'",
		},
		{
			name: "high inclusive emits <=",
			col:  "F1", low: "1", high: "10", highInclusive: true,
			want: "`F1` >= '1' and `F1` <= '10'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildChunkRangeWhere(tc.outer, tc.col, tc.low, tc.high, tc.highInclusive)
			if got != tc.want {
				t.Fatalf("buildChunkRangeWhere(%q,%q,%q,%q,%v)=%q, want %q",
					tc.outer, tc.col, tc.low, tc.high, tc.highInclusive, got, tc.want)
			}
		})
	}
}
