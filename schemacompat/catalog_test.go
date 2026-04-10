package schemacompat

import (
	"testing"

	"gt-checksum/global"
)

func TestBuildTargetColumnRepairPlanMariaDBToMariaDB(t *testing.T) {
	mariaDB106 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 6, Series: "10.6"}
	mariaDB1011 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 11, Series: "10.11"}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0, Series: "8.0"}

	tests := []struct {
		name        string
		columnType  string
		srcInfo     global.MySQLVersionInfo
		dstInfo     global.MySQLVersionInfo
		wantContain string // 期望输出中包含该字符串（空表示不检查）
		wantStrip   string // 期望输出中不含该字符串（空表示不检查）
	}{
		{
			name:        "mariadb-to-mariadb-preserves-COMPRESSED",
			columnType:  "varchar(100) COMPRESSED",
			srcInfo:     mariaDB106,
			dstInfo:     mariaDB1011,
			wantContain: "COMPRESSED",
		},
		{
			name:        "mariadb-to-mariadb-preserves-PERSISTENT",
			columnType:  "int PERSISTENT",
			srcInfo:     mariaDB106,
			dstInfo:     mariaDB106,
			wantContain: "PERSISTENT",
		},
		{
			name:      "mariadb-to-mysql-strips-COMPRESSED",
			columnType: "varchar(100) COMPRESSED",
			srcInfo:    mariaDB106,
			dstInfo:    mysql80,
			wantStrip:  "COMPRESSED",
		},
		{
			name:      "mariadb-to-mysql-strips-PERSISTENT",
			columnType: "int PERSISTENT",
			srcInfo:    mariaDB106,
			dstInfo:    mysql80,
			wantStrip:  "PERSISTENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := BuildTargetColumnRepairPlan("col", []string{tt.columnType, "", ""}, tt.srcInfo, tt.dstInfo, "", "")
			if tt.wantContain != "" && !containsStr(plan.Type, tt.wantContain) {
				t.Fatalf("expected plan.Type %q to contain %q", plan.Type, tt.wantContain)
			}
			if tt.wantStrip != "" && containsStr(plan.Type, tt.wantStrip) {
				t.Fatalf("expected plan.Type %q NOT to contain %q (should have been stripped)", plan.Type, tt.wantStrip)
			}
		})
	}
}

func TestBuildTargetColumnRepairPlanGeneratedColumn(t *testing.T) {
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0, Series: "8.0"}
	mariaDB106 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 6, Series: "10.6"}

	tests := []struct {
		name             string
		colName          string
		columnType       string // attrs[0]，来自 INFORMATION_SCHEMA（含 STORED GENERATED 后缀，无表达式）
		createDefinition string // 来自 SHOW CREATE TABLE
		srcInfo          global.MySQLVersionInfo
		dstInfo          global.MySQLVersionInfo
		wantDirect       bool   // 期望 UseDirectDefinition = true
		wantContain      string // DirectDefinition 中必须包含
		wantNotContain   string // DirectDefinition 中不得包含
	}{
		{
			name:             "mysql-to-mysql-stored-generated-uses-direct-definition",
			colName:          "price_off_08",
			columnType:       "decimal(10,2) STORED GENERATED",
			createDefinition: "decimal(10,2) GENERATED ALWAYS AS (price * 0.8) STORED COMMENT ''",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       true,
			wantContain:      "GENERATED ALWAYS AS",
			wantNotContain:   "DEFAULT NULL",
		},
		{
			name:             "mysql-to-mysql-virtual-generated-uses-direct-definition",
			colName:          "price_off_05",
			columnType:       "decimal(10,2) VIRTUAL GENERATED",
			createDefinition: "decimal(10,2) GENERATED ALWAYS AS (price * 0.5) VIRTUAL COMMENT ''",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       true,
			wantContain:      "GENERATED ALWAYS AS",
			wantNotContain:   "DEFAULT NULL",
		},
		{
			name:             "mysql-to-mysql-stored-generated-preserves-expression",
			colName:          "total",
			columnType:       "int STORED GENERATED",
			createDefinition: "int GENERATED ALWAYS AS (a + b) STORED COMMENT ''",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       true,
			wantContain:      "a + b",
		},
		{
			name:             "mysql-to-mysql-stored-generated-preserves-stored-keyword",
			colName:          "price_off_08",
			columnType:       "decimal(10,2) STORED GENERATED",
			createDefinition: "decimal(10,2) GENERATED ALWAYS AS (price * 0.8) STORED COMMENT ''",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       true,
			wantContain:      "STORED",
		},
		{
			name:             "mysql-to-mysql-virtual-generated-preserves-virtual-keyword",
			colName:          "price_off_05",
			columnType:       "decimal(10,2) VIRTUAL GENERATED",
			createDefinition: "decimal(10,2) GENERATED ALWAYS AS (price * 0.5) VIRTUAL COMMENT ''",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       true,
			wantContain:      "VIRTUAL",
		},
		{
			name:             "mariadb-to-mysql-generated-regression",
			colName:          "calc",
			columnType:       "int PERSISTENT GENERATED",
			createDefinition: "int GENERATED ALWAYS AS (a + b) PERSISTENT",
			srcInfo:          mariaDB106,
			dstInfo:          mysql80,
			wantDirect:       true,
			wantContain:      "STORED",
			wantNotContain:   "PERSISTENT",
		},
		{
			name:             "mysql-to-mysql-no-generated-column-not-direct",
			colName:          "name",
			columnType:       "varchar(100)",
			createDefinition: "varchar(100) NOT NULL COMMENT ''",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       false,
		},
		{
			name:             "empty-create-definition-no-direct",
			colName:          "price_off_08",
			columnType:       "decimal(10,2) STORED GENERATED",
			createDefinition: "",
			srcInfo:          mysql80,
			dstInfo:          mysql80,
			wantDirect:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := []string{tt.columnType, "null", "null", "YES", "empty", ""}
			plan := BuildTargetColumnRepairPlan(tt.colName, attrs, tt.srcInfo, tt.dstInfo, tt.createDefinition, "")
			if plan.UseDirectDefinition != tt.wantDirect {
				t.Fatalf("UseDirectDefinition = %v, want %v (DirectDefinition=%q)", plan.UseDirectDefinition, tt.wantDirect, plan.DirectDefinition)
			}
			if tt.wantDirect {
				if tt.wantContain != "" && !containsStr(plan.DirectDefinition, tt.wantContain) {
					t.Fatalf("DirectDefinition %q does not contain %q", plan.DirectDefinition, tt.wantContain)
				}
				if tt.wantNotContain != "" && containsStr(plan.DirectDefinition, tt.wantNotContain) {
					t.Fatalf("DirectDefinition %q must NOT contain %q", plan.DirectDefinition, tt.wantNotContain)
				}
			}
		})
	}
}

func containsStr(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}

func TestBuildSchemaFeatureCatalogMariaDBFeatureBoundaries(t *testing.T) {
	tests := []struct {
		name                      string
		major, minor              int
		wantJSON                  bool
		wantInvisibleColumns      bool
		wantFunctionIndexes       bool
		wantEnforcesCheck         bool
		wantColumnCompression     bool
		wantInvisibleIndexes      bool
		wantGeneratedColumns      bool
		wantCheckConstraintSyntax bool
	}{
		{
			name: "mariadb-10.0",
			major: 10, minor: 0,
			wantJSON: false, wantInvisibleColumns: false, wantFunctionIndexes: false,
			wantEnforcesCheck: false, wantColumnCompression: false, wantInvisibleIndexes: false,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
		{
			name: "mariadb-10.1",
			major: 10, minor: 1,
			wantJSON: false, wantInvisibleColumns: false, wantFunctionIndexes: false,
			wantEnforcesCheck: false, wantColumnCompression: false, wantInvisibleIndexes: false,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
		{
			// JSON 数据类型从 10.2 引入（longtext+JSON_VALID alias）
			// CHECK 约束从 10.2 开始强制执行
			name: "mariadb-10.2",
			major: 10, minor: 2,
			wantJSON: true, wantInvisibleColumns: false, wantFunctionIndexes: false,
			wantEnforcesCheck: true, wantColumnCompression: false, wantInvisibleIndexes: false,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
		{
			// 不可见列、COMPRESSED 列属性、序列等从 10.3 引入
			name: "mariadb-10.3",
			major: 10, minor: 3,
			wantJSON: true, wantInvisibleColumns: true, wantFunctionIndexes: false,
			wantEnforcesCheck: true, wantColumnCompression: true, wantInvisibleIndexes: false,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
		{
			// 函数式索引从 10.4 引入
			name: "mariadb-10.4",
			major: 10, minor: 4,
			wantJSON: true, wantInvisibleColumns: true, wantFunctionIndexes: true,
			wantEnforcesCheck: true, wantColumnCompression: true, wantInvisibleIndexes: false,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
		{
			// 10.5 在 10.4 基础上无新增门控特性（IGNORED 索引是 10.6）
			name: "mariadb-10.5",
			major: 10, minor: 5,
			wantJSON: true, wantInvisibleColumns: true, wantFunctionIndexes: true,
			wantEnforcesCheck: true, wantColumnCompression: true, wantInvisibleIndexes: false,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
		{
			// IGNORED（不可见）索引从 10.6 引入
			name: "mariadb-10.6",
			major: 10, minor: 6,
			wantJSON: true, wantInvisibleColumns: true, wantFunctionIndexes: true,
			wantEnforcesCheck: true, wantColumnCompression: true, wantInvisibleIndexes: true,
			wantGeneratedColumns: true, wantCheckConstraintSyntax: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := global.MySQLVersionInfo{
				Flavor: global.DatabaseFlavorMariaDB,
				Major:  tt.major,
				Minor:  tt.minor,
			}
			c := BuildSchemaFeatureCatalog(info)

			check := func(feature string, got, want bool) {
				t.Helper()
				if got != want {
					t.Errorf("%s: %s = %v, want %v", tt.name, feature, got, want)
				}
			}
			check("SupportsJSON", c.SupportsJSON, tt.wantJSON)
			check("SupportsInvisibleColumns", c.SupportsInvisibleColumns, tt.wantInvisibleColumns)
			check("SupportsFunctionIndexes", c.SupportsFunctionIndexes, tt.wantFunctionIndexes)
			check("EnforcesCheckConstraints", c.EnforcesCheckConstraints, tt.wantEnforcesCheck)
			check("SupportsColumnCompression", c.SupportsColumnCompression, tt.wantColumnCompression)
			check("SupportsInvisibleIndexes", c.SupportsInvisibleIndexes, tt.wantInvisibleIndexes)
			check("SupportsGeneratedColumns", c.SupportsGeneratedColumns, tt.wantGeneratedColumns)
			check("SupportsCheckConstraints", c.SupportsCheckConstraints, tt.wantCheckConstraintSyntax)
		})
	}
}

func TestBuildSchemaFeatureCatalogMariaDBInvisibleIndexes(t *testing.T) {
	tests := []struct {
		name     string
		info     global.MySQLVersionInfo
		expected bool
	}{
		{
			name: "mariadb-10.5-does-not-advertise-ignored-index-capability",
			info: global.MySQLVersionInfo{
				Flavor: global.DatabaseFlavorMariaDB,
				Major:  10,
				Minor:  5,
			},
			expected: false,
		},
		{
			name: "mariadb-10.11-advertises-ignored-index-capability",
			info: global.MySQLVersionInfo{
				Flavor: global.DatabaseFlavorMariaDB,
				Major:  10,
				Minor:  11,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog := BuildSchemaFeatureCatalog(tt.info)
			if catalog.SupportsInvisibleIndexes != tt.expected {
				t.Fatalf("SupportsInvisibleIndexes = %v, want %v", catalog.SupportsInvisibleIndexes, tt.expected)
			}
		})
	}
}

// TestNormalizeMySQLColumnTypeMariaDB100Generated 验证 MariaDB 10.0 的生成列归一化行为。
// MariaDB 10.0 的 INFORMATION_SCHEMA.COLUMNS.EXTRA 对 STORED 生成列只返回 "PERSISTENT"，
// 对 VIRTUAL 生成列只返回 "VIRTUAL"（均不带 "GENERATED" 后缀）。与 MySQL 8.0 的
// "STORED GENERATED" / "VIRTUAL GENERATED" 应被识别为等价。
func TestNormalizeMySQLColumnTypeMariaDB100Generated(t *testing.T) {
	mariaDB100 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 0, Series: "10.0"}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0, Series: "8.0"}

	tests := []struct {
		name        string
		srcType     string // MariaDB 10.0 侧（EXTRA 仅含 PERSISTENT/VIRTUAL）
		dstType     string // MySQL 8.0 侧（EXTRA 含 STORED GENERATED / VIRTUAL GENERATED）
		wantCompat  bool   // 期望两者被判定为兼容（无差异）
	}{
		{
			name:       "bigint-persistent-vs-stored-generated",
			srcType:    "bigint(20) PERSISTENT",
			dstType:    "bigint STORED GENERATED",
			wantCompat: true,
		},
		{
			name:       "varchar-persistent-vs-stored-generated",
			srcType:    "varchar(5) PERSISTENT",
			dstType:    "varchar(5) STORED GENERATED",
			wantCompat: true,
		},
		{
			name:       "char-persistent-vs-stored-generated",
			srcType:    "char(5) PERSISTENT",
			dstType:    "char(5) STORED GENERATED",
			wantCompat: true,
		},
		{
			name:       "varchar-virtual-vs-virtual-generated",
			srcType:    "varchar(5) VIRTUAL",
			dstType:    "varchar(5) VIRTUAL GENERATED",
			wantCompat: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcAttrs := []string{tt.srcType, "null", "null", "YES", "null", ""}
			dstAttrs := []string{tt.dstType, "null", "null", "YES", "null", ""}
			srcCanonical := CanonicalizeMySQLColumn("col", srcAttrs, mariaDB100)
			dstCanonical := CanonicalizeMySQLColumn("col", dstAttrs, mysql80)
			decision := DecideColumnDefinitionCompatibility(srcCanonical, dstCanonical)
			if tt.wantCompat && decision.IsMismatch() {
				t.Fatalf("expected compatible, got mismatch: src=%q dst=%q reason=%s",
					tt.srcType, tt.dstType, decision.Reason)
			}
			if !tt.wantCompat && !decision.IsMismatch() {
				t.Fatalf("expected mismatch, got compatible: src=%q dst=%q", tt.srcType, tt.dstType)
			}
		})
	}
}

// TestDecideColumnDefinitionCompatibilityMariaDB100GeneratedExpression 回归验证
// MariaDB 10.0 源端与 MySQL 8.0 目标端生成列表达式的兼容性判断。
//
// MariaDB 10.0 在 SHOW CREATE TABLE 中输出的生成列表达式保留用户原始大小写、
// 且列引用不带反引号（如 CAST(num1 AS SIGNED)）；MySQL 8.0 则总是输出小写、
// 带反引号的形式（如 cast(`num1` as signed)）并在最外层再套一对括号。
// 修复前两者被误判为不同，导致不必要的 MODIFY COLUMN 修复 SQL 产生。
func TestDecideColumnDefinitionCompatibilityMariaDB100GeneratedExpression(t *testing.T) {
	mariaDB100 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 0, Series: "10.0"}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0, Series: "8.0"}

	tests := []struct {
		name          string
		srcType       string // MariaDB 10.0 EXTRA（PERSISTENT/VIRTUAL，无 GENERATED 后缀）
		srcDefinition string // MariaDB 10.0 SHOW CREATE TABLE 列定义片段（大写+无反引号）
		dstType       string // MySQL 8.0 EXTRA（STORED GENERATED / VIRTUAL GENERATED）
		dstDefinition string // MySQL 8.0 SHOW CREATE TABLE 列定义片段（小写+反引号+双层括号）
		wantCompat    bool
	}{
		{
			// good_sub.vnum: CAST 表达式，大小写+反引号差异
			name:          "mariadb100-cast-expression-vs-mysql80-lowercase-backtick",
			srcType:       "bigint(20) PERSISTENT",
			srcDefinition: "`vnum` bigint(20) AS (CAST(num1 AS SIGNED) - CAST(num2 AS SIGNED)) PERSISTENT",
			dstType:       "bigint STORED GENERATED",
			dstDefinition: "`vnum` bigint GENERATED ALWAYS AS ((cast(`num1` as signed) - cast(`num2` as signed))) STORED",
			wantCompat:    true,
		},
		{
			// good_pad.vtxt: RTRIM 函数，大小写+反引号差异
			name:          "mariadb100-rtrim-expression-vs-mysql80-lowercase-backtick",
			srcType:       "varchar(5) PERSISTENT",
			srcDefinition: "`vtxt` varchar(5) AS (RTRIM(txt)) PERSISTENT",
			dstType:       "varchar(5) STORED GENERATED",
			dstDefinition: "`vtxt` varchar(5) collate utf8mb4_general_ci GENERATED ALWAYS AS (rtrim(`txt`)) STORED",
			wantCompat:    true,
		},
		{
			// good_pad.vtxt2: 裸列引用，反引号差异
			name:          "mariadb100-bare-column-ref-vs-mysql80-backtick-virtual",
			srcType:       "varchar(5) VIRTUAL",
			srcDefinition: "`vtxt2` varchar(5) AS (txt) VIRTUAL",
			dstType:       "varchar(5) VIRTUAL GENERATED",
			dstDefinition: "`vtxt2` varchar(5) collate utf8mb4_general_ci GENERATED ALWAYS AS (`txt`) VIRTUAL",
			wantCompat:    true,
		},
		{
			// good_pad.txt2: 裸列引用 PERSISTENT vs MySQL STORED，反引号差异
			name:          "mariadb100-bare-column-ref-persistent-vs-mysql80-backtick-stored",
			srcType:       "char(5) PERSISTENT",
			srcDefinition: "`txt2` char(5) AS (txt) PERSISTENT",
			dstType:       "char(5) STORED GENERATED",
			dstDefinition: "`txt2` char(5) collate utf8mb4_general_ci GENERATED ALWAYS AS (`txt`) STORED",
			wantCompat:    true,
		},
		{
			// 真正不同的表达式，仍应判为不兼容
			name:          "mariadb100-truly-different-expression",
			srcType:       "bigint(20) PERSISTENT",
			srcDefinition: "`v` bigint(20) AS (a + b) PERSISTENT",
			dstType:       "bigint STORED GENERATED",
			dstDefinition: "`v` bigint GENERATED ALWAYS AS (`a` * `b`) STORED",
			wantCompat:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcAttrs := []string{tt.srcType, "null", "null", "YES", "null", ""}
			dstAttrs := []string{tt.dstType, "null", "null", "YES", "null", ""}

			colName := "col"
			// 从 srcDefinition 提取列名（取反引号之间的内容）
			if len(tt.srcDefinition) > 0 && tt.srcDefinition[0] == '`' {
				end := 1
				for end < len(tt.srcDefinition) && tt.srcDefinition[end] != '`' {
					end++
				}
				if end < len(tt.srcDefinition) {
					colName = tt.srcDefinition[1:end]
				}
			}

			srcCanonical := CanonicalizeColumnForComparison(colName, srcAttrs, mariaDB100, mysql80, tt.srcDefinition, "")
			dstCanonical := CanonicalizeColumnForComparison(colName, dstAttrs, mysql80, mariaDB100, tt.dstDefinition, "")

			decision := DecideColumnDefinitionCompatibility(srcCanonical, dstCanonical)
			if tt.wantCompat && decision.IsMismatch() {
				t.Fatalf("expected compatible, got mismatch:\n  src definition: %s\n  dst definition: %s\n  reason: %s\n  src expr: %q\n  dst expr: %q",
					tt.srcDefinition, tt.dstDefinition, decision.Reason,
					srcCanonical.GeneratedExpression, dstCanonical.GeneratedExpression)
			}
			if !tt.wantCompat && !decision.IsMismatch() {
				t.Fatalf("expected mismatch, got compatible:\n  src definition: %s\n  dst definition: %s",
					tt.srcDefinition, tt.dstDefinition)
			}
		})
	}
}
