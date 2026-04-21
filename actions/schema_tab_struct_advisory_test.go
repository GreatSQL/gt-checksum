package actions

import (
	"strings"
	"testing"

	"gt-checksum/schemacompat"
)

// ---------- buildConstraintAdvisoryLines ----------

// TestBuildConstraintAdvisoryLines_ManualReviewCommented 验证 manual-review 级别的 SQL 语句
// 应以注释形式写出（-- 前缀），防止用户未经审查直接执行修复文件时触发高风险 DDL。
// manual-review 级别用于外键删除、CHECK 约束变更等操作，需人工确认后手动执行。
// C-1 安全修复：原实现将该级别语句写为可执行形式，有安全隐患，已更正为统一注释形式。
func TestBuildConstraintAdvisoryLines_ManualReviewCommented(t *testing.T) {
	suggestions := []schemacompat.ConstraintRepairSuggestion{
		{
			ConstraintName: "FK_EMP_DEPT1",
			Kind:           "FOREIGN KEY",
			Level:          schemacompat.ConstraintRepairLevelManualReview,
			Reason:         "target has an extra foreign key that does not exist on the source side",
			Statements:     []string{"ALTER TABLE `gt_checksum`.`tb_emp6` DROP FOREIGN KEY `FK_EMP_DEPT1`"},
		},
	}

	lines := buildConstraintAdvisoryLines("gt_checksum.tb_emp6 FOREIGN KEY constraints", suggestions)

	joined := strings.Join(lines, "\n")

	// manual-review 级别的 SQL 必须以注释形式输出（-- 前缀），不得为裸可执行语句
	if !strings.Contains(joined, "-- ALTER TABLE `gt_checksum`.`tb_emp6` DROP FOREIGN KEY `FK_EMP_DEPT1`") {
		t.Errorf("manual-review SQL 应以注释形式输出（-- 前缀），但实际输出:\n%s", joined)
	}

	// 确保裸可执行形式（无 -- 前缀的 ALTER TABLE）不存在
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "ALTER TABLE") {
			t.Errorf("manual-review SQL 不应以裸可执行形式写入修复文件，但发现: %s", line)
		}
	}
}

// TestBuildConstraintAdvisoryLines_AdvisoryOnlyCommented 验证 advisory-only 级别的 SQL 语句
// 仍以注释形式写出（仅供参考，不可直接执行）。
func TestBuildConstraintAdvisoryLines_AdvisoryOnlyCommented(t *testing.T) {
	suggestions := []schemacompat.ConstraintRepairSuggestion{
		{
			Kind:       "TABLE COLLATION",
			Level:      schemacompat.ConstraintRepairLevelAdvisoryOnly,
			Reason:     "collation difference is advisory only",
			Statements: []string{"ALTER TABLE `t1` CONVERT TO CHARACTER SET utf8mb4"},
		},
	}

	lines := buildConstraintAdvisoryLines("test.t1 TABLE options", suggestions)

	joined := strings.Join(lines, "\n")

	// advisory-only 语句必须以注释形式出现
	if !strings.Contains(joined, "-- ALTER TABLE `t1` CONVERT TO CHARACTER SET utf8mb4") {
		t.Errorf("advisory-only SQL 应以注释形式输出，但实际输出:\n%s", joined)
	}
}

// TestBuildConstraintAdvisoryLines_Empty 空建议返回 nil。
func TestBuildConstraintAdvisoryLines_Empty(t *testing.T) {
	lines := buildConstraintAdvisoryLines("scope", nil)
	if lines != nil {
		t.Errorf("空建议应返回 nil，got %v", lines)
	}
}
