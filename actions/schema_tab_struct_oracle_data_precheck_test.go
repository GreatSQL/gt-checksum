package actions

import (
	"strings"
	"testing"

	"gt-checksum/global"
	"gt-checksum/inputArg"
)

// TestDetectOracleToMySQLColumnHardMismatch 验证 Oracle→MySQL data 预检新增的
// 列定义兼容性扫描：当列名一致但底层类型/字符集/排序规则硬不兼容时命中 DDL-yes。
// 覆盖四类真实现场：FLOAT/BINARY/TIMESTAMP(n)/CHAR(n) 不匹配导致 data 模式反复
// 生成同样修复 SQL 的场景。
func TestDetectOracleToMySQLColumnHardMismatch(t *testing.T) {
	identity := func(s string) string { return s }

	makeSt := func() *schemaTable {
		return &schemaTable{
			sourceDrive:   "godror",
			destDrive:     "mysql",
			sourceVersion: global.MySQLVersionInfo{Raw: "11.2.0.4", Series: "11.2", Flavor: global.DatabaseFlavorOracle},
			destVersion:   global.MySQLVersionInfo{Raw: "8.0.32", Series: "8.0", Flavor: global.DatabaseFlavorMySQL},
			checkRules:    inputArg.RulesS{},
		}
	}

	tests := []struct {
		name       string
		sourceCol  []string // Oracle attrs: [type, _, _, nullable, default, comment]
		destCol    []string // MySQL  attrs: [type, charset, collation, nullable, default, comment]
		wantHit    bool
		wantReason string
	}{
		{
			name:       "FLOAT(Oracle) vs DECIMAL(MySQL) mismatch",
			sourceCol:  []string{"FLOAT(126)", "null", "null", "Y", "null", "null"},
			destCol:    []string{"decimal(10,2)", "null", "null", "YES", "null", "null"},
			wantHit:    true,
			wantReason: "type mismatch",
		},
		{
			name:       "RAW(Oracle) vs VARCHAR(MySQL) binary mismatch",
			sourceCol:  []string{"RAW(16)", "null", "null", "Y", "null", "null"},
			destCol:    []string{"varchar(32)", "utf8mb4", "utf8mb4_general_ci", "YES", "null", "null"},
			wantHit:    true,
			wantReason: "type mismatch",
		},
		{
			name:       "TIMESTAMP(6) vs DATETIME(0) precision mismatch",
			sourceCol:  []string{"TIMESTAMP(6)", "null", "null", "Y", "null", "null"},
			destCol:    []string{"datetime(0)", "null", "null", "YES", "null", "null"},
			wantHit:    true,
			wantReason: "type mismatch",
		},
		{
			name:       "CHAR(5) vs CHAR(10) length mismatch",
			sourceCol:  []string{"CHAR(5)", "null", "null", "Y", "null", "null"},
			destCol:    []string{"char(10)", "utf8mb4", "utf8mb4_general_ci", "YES", "null", "null"},
			wantHit:    true,
			wantReason: "type mismatch",
		},
		{
			name:      "Compatible integer types should not trigger DDL-yes",
			sourceCol: []string{"NUMBER(10,0)", "null", "null", "Y", "null", "null"},
			destCol:   []string{"bigint", "null", "null", "YES", "null", "null"},
			wantHit:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := makeSt()
			srcMap := map[string][]string{"F1": tc.sourceCol}
			dstMap := map[string][]string{"F1": tc.destCol}
			col, reason, hit := st.detectOracleToMySQLColumnHardMismatch(srcMap, dstMap, identity, identity)
			if hit != tc.wantHit {
				t.Fatalf("hit = %v want %v (reason=%q)", hit, tc.wantHit, reason)
			}
			if tc.wantHit {
				if col != "F1" {
					t.Errorf("column = %q, want F1", col)
				}
				if tc.wantReason != "" && !strings.Contains(reason, tc.wantReason) {
					t.Errorf("reason %q does not contain %q", reason, tc.wantReason)
				}
			}
		})
	}
}
