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

// ---------- mergeAlterTableStatements COLLATE 优化 ----------

// TestMergeAlterTableStatements_CollationOnlyOptimization 验证当同时存在
// MODIFY COLUMN（仅修改 COLLATE）和 CONVERT TO CHARACTER SET 时，
// 只保留 CONVERT TO，丢弃冗余的 MODIFY COLUMN 子句
func TestMergeAlterTableStatements_CollationOnlyOptimization(t *testing.T) {
	sqls := []string{
		"ALTER TABLE `gt_checksum`.`articles` MODIFY COLUMN `doc_code` char(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文档固定编号' AFTER `id`, MODIFY COLUMN `title` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文章标题' AFTER `doc_code`, MODIFY COLUMN `author_desc` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '作者简介' AFTER `title`, MODIFY COLUMN `full_content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '文章正文' AFTER `author_desc`;",
		"ALTER TABLE `gt_checksum`.`articles` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
	}

	merged := mergeAlterTableStatements(sqls, 1)

	// 应该只保留 CONVERT TO，丢弃冗余的 MODIFY COLUMN
	if len(merged) != 1 {
		t.Errorf("期望合并后只有 1 条 SQL，实际有 %d 条", len(merged))
	}

	result := merged[0]
	// 结果应该只包含 CONVERT TO，不包含 MODIFY COLUMN
	if !strings.Contains(result, "CONVERT TO CHARACTER SET") {
		t.Errorf("合并后的 SQL 应包含 CONVERT TO CHARACTER SET，实际: %s", result)
	}
	if strings.Contains(result, "MODIFY COLUMN") {
		t.Errorf("合并后的 SQL 不应包含 MODIFY COLUMN（已被 CONVERT TO 覆盖），实际: %s", result)
	}
}

// TestMergeAlterTableStatements_MixedModifyColumn 验证当 MODIFY COLUMN
// 包含非 COLLATE 的实质性变更时，不应被优化掉
func TestMergeAlterTableStatements_MixedModifyColumn(t *testing.T) {
	sqls := []string{
		"ALTER TABLE `t1` MODIFY COLUMN `col1` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
		"ALTER TABLE `t1` MODIFY COLUMN `col2` int NOT NULL;",
		"ALTER TABLE `t1` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
	}

	merged := mergeAlterTableStatements(sqls, 1)

	if len(merged) != 1 {
		t.Errorf("期望合并后只有 1 条 SQL，实际有 %d 条", len(merged))
	}

	result := merged[0]
	// col1 的 MODIFY COLUMN 应被优化掉（仅修改 COLLATE）
	if strings.Contains(result, "MODIFY COLUMN `col1`") {
		t.Errorf("col1 的 MODIFY COLUMN 应被优化掉，实际: %s", result)
	}
	// col2 的 MODIFY COLUMN 应保留（修改了类型和 NULL 属性）
	if !strings.Contains(result, "MODIFY COLUMN `col2`") {
		t.Errorf("col2 的 MODIFY COLUMN 应保留，实际: %s", result)
	}
	// CONVERT TO 应保留
	if !strings.Contains(result, "CONVERT TO CHARACTER SET") {
		t.Errorf("CONVERT TO 应保留，实际: %s", result)
	}
}
