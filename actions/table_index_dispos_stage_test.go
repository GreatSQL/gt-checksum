package actions

import (
	"os"
	"strings"
	"testing"
)

// TestFlattenSQLToSingleLine 验证多行 SQL 被正确压平为单行，
// 且字符串字面量内部的真实换行保持不变。
func TestFlattenSQLToSingleLine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no newline returns as-is",
			in:   "INSERT INTO `db`.`t`(`id`) VALUES (1);",
			want: "INSERT INTO `db`.`t`(`id`) VALUES (1);",
		},
		{
			name: "multi-line multi-value insert flattened",
			in:   "INSERT INTO `db`.`t`(`id`, `name`) VALUES\n(1,'a'),\n(2,'b'),\n(3,'c');",
			want: "INSERT INTO `db`.`t`(`id`, `name`) VALUES (1,'a'), (2,'b'), (3,'c');",
		},
		{
			name: "newline inside string literal preserved",
			in:   "INSERT INTO `db`.`t`(`id`, `note`) VALUES\n(1,'line1\nline2');",
			want: "INSERT INTO `db`.`t`(`id`, `note`) VALUES (1,'line1\nline2');",
		},
		{
			name: "escaped quote does not end literal",
			in:   "INSERT INTO `db`.`t`(`id`, `note`) VALUES\n(1,'a\\'b\nc');",
			want: "INSERT INTO `db`.`t`(`id`, `note`) VALUES (1,'a\\'b\nc');",
		},
		{
			name: "carriage return also flattened",
			in:   "INSERT INTO `db`.`t`(`id`) VALUES\r\n(1);",
			want: "INSERT INTO `db`.`t`(`id`) VALUES  (1);",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := flattenSQLToSingleLine(tt.in)
			if got != tt.want {
				t.Fatalf("flattenSQLToSingleLine mismatch:\n in=  %q\n got= %q\n want=%q", tt.in, got, tt.want)
			}
			// 压平后除字面量内部保留的换行外，不应再有格式化换行导致的多行拆分。
			if strings.Contains(tt.name, "flattened") && strings.Contains(got, "\n") {
				t.Fatalf("expected single line, got newline: %q", got)
			}
		})
	}
}

// TestStageSQLsRoundTripMultiLineInsert 验证多行 INSERT 经 stageSQLs 写入后，
// processSQLStageFile 逐行读回时仍能还原为单条完整语句（而非被拆成多行片段）。
func TestStageSQLsRoundTripMultiLineInsert(t *testing.T) {
	stage := newTableModeSQLStage("INSERT")
	defer func() {
		if stage.path != "" {
			os.Remove(stage.path)
		}
	}()

	multiLine := "INSERT INTO `db`.`t`(`id`, `name`) VALUES\n(1,'a'),\n(2,'b'),\n(3,'c');"
	if err := stage.stageSQLs([]string{multiLine}); err != nil {
		t.Fatalf("stageSQLs error: %v", err)
	}
	if err := stage.close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	var collected []string
	handler := func(sqls []string) error {
		collected = append(collected, sqls...)
		return nil
	}
	if err := processSQLStageFile(stage.path, 1000, 4*1024*1024, handler); err != nil {
		t.Fatalf("processSQLStageFile error: %v", err)
	}

	if len(collected) != 1 {
		t.Fatalf("expected 1 statement after round-trip, got %d: %#v", len(collected), collected)
	}
	want := "INSERT INTO `db`.`t`(`id`, `name`) VALUES (1,'a'), (2,'b'), (3,'c');"
	if collected[0] != want {
		t.Fatalf("round-trip mismatch:\n got= %q\n want=%q", collected[0], want)
	}
}
