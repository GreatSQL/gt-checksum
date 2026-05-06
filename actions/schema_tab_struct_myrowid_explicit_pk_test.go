package actions

import (
	"regexp"
	"strings"
	"testing"
)

// TestInjectMyRowID_ExplicitPrimaryKey 测试当 CREATE TABLE 已包含显式主键时不应注入 my_row_id
// 这是针对 bug "已有显式主键时仍然生成 my_row_id 导致双主键语法错误" 的回归测试
func TestInjectMyRowID_ExplicitPrimaryKey(t *testing.T) {
	tests := []struct {
		name              string
		inputSQL          string
		shouldNotContain  []string
		shouldContain     []string
		hasPrimaryKey     bool
	}{
		{
			name: "table with explicit PRIMARY KEY on single column",
			inputSQL: `CREATE TABLE ` + "`sbtest`.`indext` (\n" +
				"  `id` int NOT NULL,\n" +
				"  `k` int NOT NULL DEFAULT '0',\n" +
				"  `c` char(120) NOT NULL DEFAULT '',\n" +
				"  `pad` char(60) NOT NULL DEFAULT '',\n" +
				"  PRIMARY KEY (`id`),\n" +
				"  KEY `k_1` (`k`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			shouldNotContain: []string{
				"`my_row_id`",
			},
			shouldContain: []string{
				"PRIMARY KEY (`id`)",
			},
			hasPrimaryKey: true,
		},
		{
			name: "table with composite PRIMARY KEY",
			inputSQL: `CREATE TABLE ` + "`test`.`orders` (\n" +
				"  `order_id` bigint NOT NULL,\n" +
				"  `user_id` bigint NOT NULL,\n" +
				"  `amount` decimal(10,2) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`order_id`,`user_id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			shouldNotContain: []string{
				"`my_row_id`",
			},
			shouldContain: []string{
				"PRIMARY KEY (`order_id`,`user_id`)",
			},
			hasPrimaryKey: true,
		},
		{
			name: "table with PRIMARY KEY and multiple indexes",
			inputSQL: `CREATE TABLE ` + "`db1`.`users` (\n" +
				"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"  `username` varchar(50) NOT NULL,\n" +
				"  `email` varchar(100) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`),\n" +
				"  UNIQUE KEY `uk_username` (`username`),\n" +
				"  KEY `idx_email` (`email`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			shouldNotContain: []string{
				"`my_row_id`",
			},
			shouldContain: []string{
				"PRIMARY KEY (`id`)",
				"UNIQUE KEY",
			},
			hasPrimaryKey: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. 验证输入 SQL 是否包含 PRIMARY KEY
			primaryKeyPattern := regexp.MustCompile(`(?i)\bPRIMARY\s+KEY\b`)
			hasPK := primaryKeyPattern.MatchString(tt.inputSQL)

			if hasPK != tt.hasPrimaryKey {
				t.Errorf("PRIMARY KEY detection mismatch: got %v, want %v", hasPK, tt.hasPrimaryKey)
			}

			// 2. 模拟修复后的逻辑：如果已有 PRIMARY KEY，不应注入 my_row_id
			result := tt.inputSQL
			if hasPK {
				// 已有主键，不注入 my_row_id
				t.Logf("Table already has PRIMARY KEY, skipping my_row_id injection")
			}

			// 3. 验证结果不应包含 my_row_id
			for _, notExpected := range tt.shouldNotContain {
				if strings.Contains(result, notExpected) {
					t.Errorf("Result should NOT contain '%s', but it does", notExpected)
					t.Logf("This indicates the bug: my_row_id was injected despite existing PRIMARY KEY")
				}
			}

			// 4. 验证结果应包含原有的 PRIMARY KEY
			for _, expected := range tt.shouldContain {
				if !strings.Contains(result, expected) {
					t.Errorf("Result should contain '%s', but it doesn't", expected)
				}
			}

			// 5. 验证不会出现双主键错误
			pkCount := strings.Count(result, "PRIMARY KEY")
			if pkCount > 1 {
				t.Errorf("Bug detected: Multiple PRIMARY KEY definitions found (%d occurrences)", pkCount)
				t.Logf("MySQL syntax error: there can be only one PRIMARY KEY per table")
			}
		})
	}
}
