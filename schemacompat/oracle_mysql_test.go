package schemacompat

import (
	"gt-checksum/global"
	"strings"
	"testing"
)

func TestNormalizeOracleColumnType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// String types
		{"VARCHAR2(100)", "varchar(100)"},
		{"VARCHAR2(4000)", "varchar(4000)"},
		{"CHAR(10)", "char(10)"},
		{"NCHAR(20)", "char(20)"},
		{"NVARCHAR2(200)", "varchar(200)"},

		// Numeric types — decimal
		{"NUMBER(10,2)", "decimal(10,2)"},
		{"NUMBER(38,10)", "decimal(38,10)"},
		{"NUMBER(5,3)", "decimal(5,3)"},

		// Numeric types — integer mapping by precision
		{"NUMBER(1,0)", "tinyint(1)"},
		{"NUMBER(2,0)", "tinyint"},
		{"NUMBER(3,0)", "smallint"},
		{"NUMBER(4,0)", "smallint"},
		{"NUMBER(5,0)", "mediumint"},
		{"NUMBER(6,0)", "mediumint"},
		{"NUMBER(7,0)", "int"},
		{"NUMBER(9,0)", "int"},
		{"NUMBER(10,0)", "bigint"},
		{"NUMBER(18,0)", "bigint"},
		{"NUMBER(19,0)", "decimal(19,0)"},
		{"NUMBER(38,0)", "decimal(38,0)"},

		// NUMBER(p) without scale
		{"NUMBER(1)", "tinyint(1)"},
		{"NUMBER(5)", "mediumint"},
		{"NUMBER(10)", "bigint"},

		// NUMBER without precision
		{"NUMBER", "decimal(38,0)"},

		// Float/Double
		{"FLOAT", "double"},
		{"FLOAT(126)", "double"},
		{"BINARY_FLOAT", "float"},
		{"BINARY_DOUBLE", "double"},
		{"DECIMAL", "decimal"},

		// Date/Time
		{"DATE", "datetime"},
		{"TIMESTAMP(6)", "datetime(6)"},
		{"TIMESTAMP(0)", "datetime"},
		{"TIMESTAMP(3)", "datetime(3)"},
		{"TIMESTAMP", "datetime(6)"},
		{"TIMESTAMP(6) WITH TIME ZONE", "datetime(6)"},
		{"TIMESTAMP(3) WITH LOCAL TIME ZONE", "datetime(3)"},
		{"TIMESTAMP WITH TIME ZONE", "datetime(6)"},

		// LOB types
		{"CLOB", "longtext"},
		{"NCLOB", "longtext"},
		{"BLOB", "longblob"},
		{"LONG", "longtext"},
		{"LONG RAW", "longblob"},
		{"XMLTYPE", "longtext"},

		// Binary
		{"RAW(16)", "varbinary(16)"},
		{"RAW(2000)", "varbinary(2000)"},

		// INTERVAL types
		{"INTERVAL YEAR TO MONTH", "varchar(30)"},
		{"INTERVAL YEAR(4) TO MONTH", "varchar(30)"},
		{"INTERVAL DAY TO SECOND", "varchar(30)"},
		{"INTERVAL DAY(3) TO SECOND(6)", "varchar(30)"},

		// ROWID
		{"ROWID", "varchar(18)"},

		// Boolean (if Oracle has it)
		{"BOOLEAN", "tinyint(1)"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := NormalizeOracleColumnType(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeOracleColumnType(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestCanonicalizeOracleColumnForComparison(t *testing.T) {
	destVersion := global.MySQLVersionInfo{Major: 8, Minor: 0, Flavor: global.DatabaseFlavorMySQL}

	tests := []struct {
		name     string
		attrs    []string
		wantType string
		wantNull bool
	}{
		{
			name:     "VARCHAR2 nullable",
			attrs:    []string{"VARCHAR2(100)", "null", "null", "Y", "", "test comment"},
			wantType: "varchar(100)",
			wantNull: true,
		},
		{
			name:     "NUMBER not nullable",
			attrs:    []string{"NUMBER(10,2)", "null", "null", "N", "0", ""},
			wantType: "decimal(10,2)",
			wantNull: false,
		},
		{
			name:     "DATE with SYSDATE default",
			attrs:    []string{"DATE", "null", "null", "Y", "SYSDATE", ""},
			wantType: "datetime",
			wantNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := CanonicalizeOracleColumnForComparison(tt.name, tt.attrs, destVersion)
			if col.NormalizedType != tt.wantType {
				t.Errorf("NormalizedType = %q, want %q", col.NormalizedType, tt.wantType)
			}
			if col.Nullable != tt.wantNull {
				t.Errorf("Nullable = %v, want %v", col.Nullable, tt.wantNull)
			}
		})
	}
}

func TestDecideOracleToMySQLTypeCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		srcRaw    string
		srcNorm   string
		dstRaw    string
		dstNorm   string
		wantState CompatibilityState
	}{
		{
			name:      "exact match",
			srcRaw:    "VARCHAR2(100)",
			srcNorm:   "varchar(100)",
			dstRaw:    "varchar(100)",
			dstNorm:   "varchar(100)",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "int display width equivalence",
			srcRaw:    "NUMBER(9,0)",
			srcNorm:   "int",
			dstRaw:    "int(11)",
			dstNorm:   "int(11)",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "NUMBER(10) source bigint vs INT target - integer cross-type equivalent",
			srcRaw:    "NUMBER(10)",
			srcNorm:   "bigint",
			dstRaw:    "int",
			dstNorm:   "int",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "NUMBER(10,0) bigint vs INT - integer cross-type equivalent",
			srcRaw:    "NUMBER(10,0)",
			srcNorm:   "bigint",
			dstRaw:    "int",
			dstNorm:   "int",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "NUMBER(5,0) mediumint vs INT - integer cross-type equivalent",
			srcRaw:    "NUMBER(5,0)",
			srcNorm:   "mediumint",
			dstRaw:    "int",
			dstNorm:   "int",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "NUMBER(10,2) decimal vs INT - NOT equivalent (decimal vs integer)",
			srcRaw:    "NUMBER(10,2)",
			srcNorm:   "decimal(10,2)",
			dstRaw:    "int",
			dstNorm:   "int",
			wantState: CompatibilityUnsupported,
		},
		// Oracle NUMBER (no precision) should be compatible with any MySQL integer type.
		// NUMBER without precision has an implicit scale=0 and users often map it to
		// an integer type that fits their actual data range.
		{
			name:      "NUMBER (no precision) vs INT - compatible",
			srcRaw:    "NUMBER",
			srcNorm:   "decimal(38,0)",
			dstRaw:    "int",
			dstNorm:   "int",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "NUMBER (no precision) vs BIGINT - compatible",
			srcRaw:    "NUMBER",
			srcNorm:   "decimal(38,0)",
			dstRaw:    "bigint",
			dstNorm:   "bigint",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "NUMBER (no precision) vs decimal(38,0) - exact canonical match",
			srcRaw:    "NUMBER",
			srcNorm:   "decimal(38,0)",
			dstRaw:    "decimal(38,0)",
			dstNorm:   "decimal(38,0)",
			wantState: CompatibilityNormalizedEqual,
		},
		{
			name:      "timestamp tz warn",
			srcRaw:    "TIMESTAMP(6) WITH TIME ZONE",
			srcNorm:   "datetime(6)",
			dstRaw:    "datetime(6)",
			dstNorm:   "datetime(6)",
			wantState: CompatibilityWarnOnly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := CanonicalColumn{RawType: tt.srcRaw, NormalizedType: tt.srcNorm}
			dst := CanonicalColumn{RawType: tt.dstRaw, NormalizedType: tt.dstNorm}
			decision := DecideOracleToMySQLTypeCompatibility(src, dst)
			if decision.State != tt.wantState {
				t.Errorf("State = %q, want %q; reason: %s", decision.State, tt.wantState, decision.Reason)
			}
		})
	}
}

func TestDecideOracleToMySQLCharsetCompatibility(t *testing.T) {
	src := CanonicalColumn{Charset: ""}
	dst := CanonicalColumn{Charset: "utf8mb4"}
	decision := DecideOracleToMySQLCharsetCompatibility(src, dst)
	if decision.State != CompatibilityNormalizedEqual {
		t.Errorf("expected NormalizedEqual for charset, got %s", decision.State)
	}
}

func TestDecideOracleToMySQLCollationCompatibility(t *testing.T) {
	src := CanonicalColumn{Collation: ""}
	dst := CanonicalColumn{Collation: "utf8mb4_general_ci"}
	decision := DecideOracleToMySQLCollationCompatibility(src, dst)
	if decision.State != CompatibilityNormalizedEqual {
		t.Errorf("expected NormalizedEqual for collation, got %s", decision.State)
	}
}

func TestDecideOracleToMySQLDefaultCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		srcDef    string
		dstDef    string
		wantState CompatibilityState
	}{
		{"both empty", "", "", CompatibilityNormalizedEqual},
		{"sysdate vs current_timestamp", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", CompatibilityNormalizedEqual},
		{"mismatch", "100", "200", CompatibilityUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := CanonicalColumn{DefaultValue: tt.srcDef}
			dst := CanonicalColumn{DefaultValue: tt.dstDef}
			decision := DecideOracleToMySQLDefaultCompatibility(src, dst)
			if decision.State != tt.wantState {
				t.Errorf("State = %q, want %q", decision.State, tt.wantState)
			}
		})
	}
}

func TestNormalizeOracleDefault(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SYSDATE", "CURRENT_TIMESTAMP"},
		{"  SYSDATE  ", "CURRENT_TIMESTAMP"},
		{"SYSTIMESTAMP", "CURRENT_TIMESTAMP"},
		{"my_seq.NEXTVAL", ""},
		{"'hello'", "hello"},
		{"'it''s'", "it's"},
		{"NULL", ""},
		{"", ""},
		{"42", "42"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeOracleDefault(tt.input)
			if got != tt.expected {
				t.Errorf("normalizeOracleDefault(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestBuildOracleToMySQLRepairPlan(t *testing.T) {
	attrs := []string{"NUMBER(10,2)", "null", "null", "Y", "", ""}
	plan := BuildOracleToMySQLRepairPlan("price", attrs, global.MySQLVersionInfo{Major: 8, Minor: 0})
	if plan.Type != "decimal(10,2)" {
		t.Errorf("RepairPlan.Type = %q, want %q", plan.Type, "decimal(10,2)")
	}
	if plan.Charset != "" {
		t.Errorf("RepairPlan.Charset = %q, want empty", plan.Charset)
	}
}

// TestNullableNormalization_OracleYEquivalentToMySQLYES 验证 Oracle "Y"/"N"
// 与 MySQL "YES"/"NO" 在 canonical 层被统一为相同的 bool 值，防止 NULL 约束误报 Diffs=yes。
// 这是 Bug 修复测例：Oracle→MySQL struct 校验因 Y vs YES 误判为不一致。
func TestNullableNormalization_OracleYEquivalentToMySQLYES(t *testing.T) {
	destVersion := global.MySQLVersionInfo{Major: 8, Minor: 0, Flavor: global.DatabaseFlavorMySQL}

	tests := []struct {
		name         string
		oracleAttrs  []string // index 3 = NULLABLE from dba_tab_columns
		mysqlAttrs   []string // index 3 = IS_NULLABLE from information_schema
		wantEqual    bool
	}{
		{
			name:        "Oracle Y equals MySQL YES (both nullable)",
			oracleAttrs: []string{"NUMBER(10,0)", "", "", "Y", "", ""},
			mysqlAttrs:  []string{"bigint", "", "", "YES", "", ""},
			wantEqual:   true,
		},
		{
			name:        "Oracle N equals MySQL NO (both not nullable)",
			oracleAttrs: []string{"NUMBER(10,0)", "", "", "N", "", ""},
			mysqlAttrs:  []string{"bigint", "", "", "NO", "", ""},
			wantEqual:   true,
		},
		{
			name:        "Oracle Y not equal MySQL NO (nullable mismatch)",
			oracleAttrs: []string{"NUMBER(10,0)", "", "", "Y", "", ""},
			mysqlAttrs:  []string{"bigint", "", "", "NO", "", ""},
			wantEqual:   false,
		},
		{
			name:        "Oracle N not equal MySQL YES (not-nullable mismatch)",
			oracleAttrs: []string{"NUMBER(10,0)", "", "", "N", "", ""},
			mysqlAttrs:  []string{"bigint", "", "", "YES", "", ""},
			wantEqual:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracleCanon := CanonicalizeOracleColumnForComparison("col", tt.oracleAttrs, destVersion)
			mysqlCanon := CanonicalizeColumnForComparison("col", tt.mysqlAttrs, destVersion, destVersion, "", "")

			got := oracleCanon.Nullable == mysqlCanon.Nullable
			if got != tt.wantEqual {
				t.Errorf("Nullable equality = %v, want %v (oracle=%v mysql=%v)",
					got, tt.wantEqual, oracleCanon.Nullable, mysqlCanon.Nullable)
			}
		})
	}
}

func TestGenerateOracleToMySQLCreateTableSQL(t *testing.T) {
	columns := []map[string]interface{}{
		{"columnName": "ID", "columnType": "NUMBER(10,0)", "isNull": "N", "columnDefault": nil, "columnComment": "Primary key"},
		{"columnName": "NAME", "columnType": "VARCHAR2(100)", "isNull": "Y", "columnDefault": nil, "columnComment": ""},
		{"columnName": "CREATED_AT", "columnType": "DATE", "isNull": "Y", "columnDefault": "SYSDATE", "columnComment": "Creation time"},
	}
	indexData := map[string][]string{
		"PRIMARY": {"ID"},
	}

	sql := GenerateOracleToMySQLCreateTableSQL("testdb", "users", columns, indexData, global.MySQLVersionInfo{Major: 8, Minor: 0, Flavor: global.DatabaseFlavorMySQL})
	if sql == "" {
		t.Fatal("expected non-empty CREATE TABLE SQL")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS") {
		t.Error("missing CREATE TABLE IF NOT EXISTS")
	}
	if !strings.Contains(sql, "PRIMARY KEY") {
		t.Error("missing PRIMARY KEY")
	}
	if !strings.Contains(sql, "bigint") {
		t.Error("expected NUMBER(10,0) to map to bigint")
	}
	if !strings.Contains(sql, "datetime") {
		t.Error("expected DATE to map to datetime")
	}
}

// TestOracleForeignKeyFormat_SingleFK 验证 Oracle Foreign() 新实现产出的格式
// 可被 CanonicalizeForeignKeyDefinitions 正确解析（对应 bug: user_constraints 跨 schema 返回空）
func TestOracleForeignKeyFormat_SingleFK(t *testing.T) {
	// 模拟 Oracle Foreign() 新实现产出的 map（key=约束名, value=定义字符串）
	oracleDefs := map[string]string{
		"FK_EMP_DEPT1": "CONSTRAINT !FK_EMP_DEPT1! FOREIGN KEY (!DEPTID!) REFERENCES !GT_CHECKSUM!.!TB_DEPT1! (!ID!)",
	}
	// 模拟 MySQL Foreign() 产出的 map
	mysqlDefs := map[string]string{
		"FK_EMP_DEPT1": "CONSTRAINT !fk_emp_dept1! FOREIGN KEY (!deptid!) REFERENCES !gt_checksum!.!tb_dept1! (!id!)",
	}

	srcFKs := CanonicalizeForeignKeyDefinitions(oracleDefs)
	dstFKs := CanonicalizeForeignKeyDefinitions(mysqlDefs)

	if len(srcFKs) != 1 {
		t.Fatalf("expected 1 oracle FK, got %d", len(srcFKs))
	}
	if len(dstFKs) != 1 {
		t.Fatalf("expected 1 mysql FK, got %d", len(dstFKs))
	}

	srcFK := srcFKs[0]
	dstFK := dstFKs[0]

	if srcFK.Name != "FK_EMP_DEPT1" {
		t.Errorf("oracle FK name: got %q, want FK_EMP_DEPT1", srcFK.Name)
	}
	if dstFK.Name != "FK_EMP_DEPT1" {
		t.Errorf("mysql FK name: got %q, want FK_EMP_DEPT1", dstFK.Name)
	}

	decision := DecideForeignKeyCompatibility(srcFK, dstFK)
	if decision.IsMismatch() {
		t.Errorf("FK should be compatible, but got mismatch: %s", decision.Reason)
	}
}

// TestOracleForeignKeyFormat_MultiFKs 验证单表多个 FK 均能被正确解析
func TestOracleForeignKeyFormat_MultiFKs(t *testing.T) {
	oracleDefs := map[string]string{
		"FK_EMP_DEPT1": "CONSTRAINT !FK_EMP_DEPT1! FOREIGN KEY (!DEPTID!) REFERENCES !GT_CHECKSUM!.!TB_DEPT1! (!ID!)",
		"FK_EMP_MGR":   "CONSTRAINT !FK_EMP_MGR! FOREIGN KEY (!MANAGER_ID!) REFERENCES !GT_CHECKSUM!.!TB_EMP6! (!ID!)",
	}
	fks := CanonicalizeForeignKeyDefinitions(oracleDefs)
	if len(fks) != 2 {
		t.Fatalf("expected 2 FKs, got %d", len(fks))
	}
	byName := make(map[string]CanonicalConstraint)
	for _, fk := range fks {
		byName[fk.Name] = fk
	}
	if _, ok := byName["FK_EMP_DEPT1"]; !ok {
		t.Error("FK_EMP_DEPT1 missing from parsed result")
	}
	if _, ok := byName["FK_EMP_MGR"]; !ok {
		t.Error("FK_EMP_MGR missing from parsed result")
	}
}

// TestOracleForeignKeyFormat_DeleteCascade 验证 ON DELETE CASCADE 规则被正确传递
func TestOracleForeignKeyFormat_DeleteCascade(t *testing.T) {
	oracleDefs := map[string]string{
		"FK_CASCADE": "CONSTRAINT !FK_CASCADE! FOREIGN KEY (!DEPTID!) REFERENCES !GT_CHECKSUM!.!TB_DEPT1! (!ID!) ON DELETE CASCADE",
	}
	fks := CanonicalizeForeignKeyDefinitions(oracleDefs)
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK, got %d", len(fks))
	}
	if fks[0].DeleteRule != "CASCADE" {
		t.Errorf("expected DeleteRule=CASCADE, got %q", fks[0].DeleteRule)
	}
}
