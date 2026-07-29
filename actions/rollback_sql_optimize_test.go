package actions

import (
	"testing"
)

func TestOptimizeDeleteSqlsForRollback(t *testing.T) {
	tests := []struct {
		name       string
		sqls       []string
		maxSqlSize int
		fixTrxNum  int
		want       []string
	}{
		{
			name:       "single delete",
			sqls:       []string{"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;"},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want:       []string{"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;"},
		},
		{
			name: "multiple deletes same table same column",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 4;",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want:       []string{"DELETE FROM `gt_checksum`.`test3` WHERE `id` IN (2,3,4);"},
		},
		{
			name: "multiple deletes same table different columns",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `name` = 'test';",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `name` = 'test';",
			},
		},
		{
			name: "multiple deletes different tables",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test4` WHERE `id` = 3;",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test4` WHERE `id` = 3;",
			},
		},
		{
			name: "multiple deletes with batch limit",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 4;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 5;",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  2,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` IN (2,3);",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` IN (4,5);",
			},
		},
		{
			name: "multiple deletes with size limit",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
			},
			maxSqlSize: 50, // Small size limit to trigger split
			fixTrxNum:  1000,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OptimizeDeleteSqls(tt.sqls, tt.maxSqlSize, tt.fixTrxNum)
			if len(got) != len(tt.want) {
				t.Errorf("OptimizeDeleteSqls() returned %d statements, want %d", len(got), len(tt.want))
				t.Errorf("Got: %v", got)
				t.Errorf("Want: %v", tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("OptimizeDeleteSqls()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestOptimizeInsertSqlsForRollback(t *testing.T) {
	tests := []struct {
		name       string
		sqls       []string
		maxSqlSize int
		fixTrxNum  int
		want       []string
	}{
		{
			name:       "single insert",
			sqls:       []string{"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test');"},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want:       []string{"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test');"},
		},
		{
			name: "multiple inserts same table same columns",
			sqls: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test1');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (3, 'test2');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (4, 'test3');",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want:       []string{"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES\n(2, 'test1'),\n(3, 'test2'),\n(4, 'test3');"},
		},
		{
			name: "multiple inserts same table different columns",
			sqls: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test1');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `age`) VALUES (3, 25);",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test1');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `age`) VALUES (3, 25);",
			},
		},
		{
			name: "multiple inserts different tables",
			sqls: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test1');",
				"INSERT INTO `gt_checksum`.`test4`(`id`, `name`) VALUES (3, 'test2');",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  1000,
			want: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test1');",
				"INSERT INTO `gt_checksum`.`test4`(`id`, `name`) VALUES (3, 'test2');",
			},
		},
		{
			name: "multiple inserts with batch limit",
			sqls: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test1');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (3, 'test2');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (4, 'test3');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (5, 'test4');",
			},
			maxSqlSize: 4 * 1024 * 1024,
			fixTrxNum:  2,
			want: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES\n(2, 'test1'),\n(3, 'test2');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES\n(4, 'test3'),\n(5, 'test4');",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OptimizeInsertSqls(tt.sqls, tt.maxSqlSize, tt.fixTrxNum)
			if len(got) != len(tt.want) {
				t.Errorf("OptimizeInsertSqls() returned %d statements, want %d", len(got), len(tt.want))
				t.Errorf("Got: %v", got)
				t.Errorf("Want: %v", tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("OptimizeInsertSqls()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestOptimizeSqlStatementsForRollback(t *testing.T) {
	tests := []struct {
		name         string
		sqls         []string
		fixTrxNum    int
		isUniqueKey  bool
		deleteSqlSize int
		insertSqlSize int
		want         []string
	}{
		{
			name: "mixed delete and insert with unique key",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (4, 'test4');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (5, 'test5');",
			},
			fixTrxNum:     1000,
			isUniqueKey:   true,
			deleteSqlSize: 4 * 1024 * 1024,
			insertSqlSize: 4 * 1024 * 1024,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` IN (2,3);",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES\n(4, 'test4'),\n(5, 'test5');",
			},
		},
		{
			name: "mixed delete and insert without unique key",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (4, 'test4');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (5, 'test5');",
			},
			fixTrxNum:     1000,
			isUniqueKey:   false,
			deleteSqlSize: 4 * 1024 * 1024,
			insertSqlSize: 4 * 1024 * 1024,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES\n(4, 'test4'),\n(5, 'test5');",
			},
		},
		{
			name: "only delete statements with unique key",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 2;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 3;",
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` = 4;",
			},
			fixTrxNum:     1000,
			isUniqueKey:   true,
			deleteSqlSize: 4 * 1024 * 1024,
			insertSqlSize: 4 * 1024 * 1024,
			want: []string{
				"DELETE FROM `gt_checksum`.`test3` WHERE `id` IN (2,3,4);",
			},
		},
		{
			name: "only insert statements",
			sqls: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (2, 'test2');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (3, 'test3');",
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES (4, 'test4');",
			},
			fixTrxNum:     1000,
			isUniqueKey:   true,
			deleteSqlSize: 4 * 1024 * 1024,
			insertSqlSize: 4 * 1024 * 1024,
			want: []string{
				"INSERT INTO `gt_checksum`.`test3`(`id`, `name`) VALUES\n(2, 'test2'),\n(3, 'test3'),\n(4, 'test4');",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optimizeSqlStatements(tt.sqls, tt.fixTrxNum, tt.isUniqueKey, tt.deleteSqlSize, tt.insertSqlSize)
			if len(got) != len(tt.want) {
				t.Errorf("optimizeSqlStatements() returned %d statements, want %d", len(got), len(tt.want))
				t.Errorf("Got: %v", got)
				t.Errorf("Want: %v", tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("optimizeSqlStatements()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestMergeDuplicateDeleteLimits 验证无主键表回滚 DELETE LIMIT 合并逻辑
// 场景：源端有 N 条相同值的行，回滚 DELETE 应生成 LIMIT N 而非 N 条 LIMIT 1
func TestMergeDuplicateDeleteLimits(t *testing.T) {
	tests := []struct {
		name string
		sqls []string
		want []string
	}{
		{
			name: "rollsql-limit: 两条相同WHERE的LIMIT 1合并为LIMIT 2",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 1;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 1;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 5 LIMIT 1;",
			},
			want: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 2;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 5 LIMIT 1;",
			},
		},
		{
			name: "三条相同WHERE的LIMIT 1合并为LIMIT 3",
			sqls: []string{
				"DELETE FROM `db`.`t` WHERE `v` = 7 LIMIT 1;",
				"DELETE FROM `db`.`t` WHERE `v` = 7 LIMIT 1;",
				"DELETE FROM `db`.`t` WHERE `v` = 7 LIMIT 1;",
			},
			want: []string{
				"DELETE FROM `db`.`t` WHERE `v` = 7 LIMIT 3;",
			},
		},
		{
			name: "无重复时原样返回",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 1;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 5 LIMIT 1;",
			},
			want: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 1;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 5 LIMIT 1;",
			},
		},
		{
			name: "单条语句原样返回",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 1;",
			},
			want: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `a2` = 4 LIMIT 1;",
			},
		},
		{
			name: "不含LIMIT的语句不受影响",
			sqls: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `id` = 1;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `id` = 1;",
			},
			want: []string{
				"DELETE FROM `gt_checksum`.`test2` WHERE `id` = 1;",
				"DELETE FROM `gt_checksum`.`test2` WHERE `id` = 1;",
			},
		},
		{
			name: "多列WHERE条件相同时合并",
			sqls: []string{
				"DELETE FROM `db`.`t` WHERE `a` = 1 AND `b` = 'x' LIMIT 1;",
				"DELETE FROM `db`.`t` WHERE `a` = 1 AND `b` = 'x' LIMIT 1;",
			},
			want: []string{
				"DELETE FROM `db`.`t` WHERE `a` = 1 AND `b` = 'x' LIMIT 2;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeDuplicateDeleteLimits(tt.sqls)
			if len(got) != len(tt.want) {
				t.Errorf("mergeDuplicateDeleteLimits() returned %d statements, want %d\nGot:  %v\nWant: %v",
					len(got), len(tt.want), got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("mergeDuplicateDeleteLimits()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
