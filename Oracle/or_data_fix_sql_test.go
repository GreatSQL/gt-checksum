package oracle

import (
	"strings"
	"testing"
)

// ---------- oracleIdentifier 语义验证（复审 2.1 必须补充的测试） ----------

// 普通大写简单标识符：返回裸名（无引号，大写）
func TestOracleIdentifier_SimpleUpper(t *testing.T) {
	got := oracleIdentifier("IDX_NAME")
	want := "IDX_NAME"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 小写简单标识符：返回 Oracle 默认大写裸名（无引号）
// 关键：不能返回 "test"，否则 Oracle 会以大小写敏感方式查找对象
func TestOracleIdentifier_LowercaseSimple(t *testing.T) {
	got := oracleIdentifier("test")
	want := "TEST"
	if got != want {
		t.Fatalf("got %q, want %q (must be uppercase unquoted, not %q)", got, want, `"test"`)
	}
}

// 含空格标识符：需加双引号，保留原始大小写
func TestOracleIdentifier_WithSpace(t *testing.T) {
	got := oracleIdentifier("idx name")
	want := `"idx name"`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 已显式带双引号的标识符：剥去外层引号后重新转义并加回引号，不能三重引号
// 关键：`"Idx A"` → `"Idx A"`，而不是 `"""Idx A"""`
func TestOracleIdentifier_AlreadyQuoted(t *testing.T) {
	got := oracleIdentifier(`"Idx A"`)
	want := `"Idx A"`
	if got != want {
		t.Fatalf("got %q, want %q (must not triple-quote)", got, want)
	}
}

// 含双引号字符（非已引用标识符）：应转义内部双引号
func TestOracleIdentifier_WithDoubleQuoteChar(t *testing.T) {
	got := oracleIdentifier(`idx"name`)
	want := `"idx""name"`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// ---------- FixAlterIndexSqlExec（BUG-3/6）——使用 oracleIdentifier 语义 ----------

func newOracleFixStruct(indexType string) *OracleDataAbnormalFixStruct {
	return &OracleDataAbnormalFixStruct{
		Schema:    "TEST",
		Table:     "T1",
		IndexType: indexType,
	}
}

// BUG-3：DROP INDEX 应使用 Oracle 独立语法，标识符按 oracleIdentifier 处理
func TestOracleFixAlterIndexSqlExec_DropUni_OracleSyntax(t *testing.T) {
	or := newOracleFixStruct("uni")
	sqls := or.FixAlterIndexSqlExec([]string{"IDX_1"}, nil, nil, "godror", 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.HasPrefix(got, "DROP INDEX") {
		t.Errorf("Oracle DROP should use standalone DROP INDEX, got: %s", got)
	}
	if strings.Contains(strings.ToUpper(got), "ALTER TABLE") {
		t.Errorf("Oracle DROP must NOT use ALTER TABLE syntax, got: %s", got)
	}
	// 简单大写标识符：oracleIdentifier 不加引号
	if !strings.Contains(got, "TEST.IDX_1") {
		t.Errorf("simple identifiers should be unquoted uppercase: %s", got)
	}
}

func TestOracleFixAlterIndexSqlExec_DropMul_OracleSyntax(t *testing.T) {
	or := newOracleFixStruct("mul")
	sqls := or.FixAlterIndexSqlExec([]string{"IDX_2"}, nil, nil, "godror", 1)
	got := sqls[0]
	if !strings.HasPrefix(got, "DROP INDEX") {
		t.Errorf("Oracle mul DROP should use standalone DROP INDEX, got: %s", got)
	}
}

// BUG-6：ADD INDEX 应使用 Oracle CREATE INDEX 语法
func TestOracleFixAlterIndexSqlExec_AddUni_CreateIndex(t *testing.T) {
	or := newOracleFixStruct("uni")
	si := map[string][]string{"IDX_U": {"C1/*seq*/1/*type*/NUMBER"}}
	sqls := or.FixAlterIndexSqlExec(nil, []string{"IDX_U"}, si, "godror", 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.HasPrefix(strings.ToUpper(got), "CREATE UNIQUE INDEX") {
		t.Errorf("Oracle ADD uni should use CREATE UNIQUE INDEX, got: %s", got)
	}
	// 简单大写标识符：无引号
	if !strings.Contains(got, "TEST.IDX_U") {
		t.Errorf("index ident should be unquoted uppercase: %s", got)
	}
	if !strings.Contains(got, "TEST.T1") {
		t.Errorf("table ident should be unquoted uppercase: %s", got)
	}
	if !strings.Contains(got, "C1") {
		t.Errorf("column name not present: %s", got)
	}
}

func TestOracleFixAlterIndexSqlExec_AddMul_CreateIndex(t *testing.T) {
	or := newOracleFixStruct("mul")
	si := map[string][]string{"IDX_M": {"C2/*seq*/1/*type*/VARCHAR2"}}
	sqls := or.FixAlterIndexSqlExec(nil, []string{"IDX_M"}, si, "godror", 1)
	got := sqls[0]
	if !strings.HasPrefix(strings.ToUpper(got), "CREATE INDEX") {
		t.Errorf("Oracle ADD mul should use CREATE INDEX, got: %s", got)
	}
}

// DROP PRIMARY KEY 仍可用 ALTER TABLE 语法（Oracle 支持）
func TestOracleFixAlterIndexSqlExec_DropPri(t *testing.T) {
	or := newOracleFixStruct("pri")
	sqls := or.FixAlterIndexSqlExec([]string{""}, nil, nil, "godror", 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(strings.ToUpper(got), "DROP PRIMARY KEY") {
		t.Errorf("Oracle DROP pri should contain DROP PRIMARY KEY, got: %s", got)
	}
}

// 含空格 schema/table 名：oracleIdentifier 自动加双引号
func TestOracleFixAlterIndexSqlExec_QuotedIdents(t *testing.T) {
	or := &OracleDataAbnormalFixStruct{
		Schema:    "my schema",
		Table:     "my table",
		IndexType: "mul",
	}
	sqls := or.FixAlterIndexSqlExec([]string{"my idx"}, nil, nil, "godror", 1)
	got := sqls[0]
	if !strings.Contains(got, `"my schema"`) {
		t.Errorf("schema with space should be double-quoted: %s", got)
	}
	if !strings.Contains(got, `"my idx"`) {
		t.Errorf("index with space should be double-quoted: %s", got)
	}
}

// ---------- FixAlterIndexSqlGenerate（Oracle 应原样返回，不包装 ALTER TABLE） ----------

func TestOracleFixAlterIndexSqlGenerate_PassThrough(t *testing.T) {
	or := newOracleFixStruct("uni")
	ops := []string{
		`DROP INDEX TEST.IDX_1;`,
		`CREATE UNIQUE INDEX TEST.IDX_2 ON TEST.T1 (C1);`,
	}
	sqls := or.FixAlterIndexSqlGenerate(ops, 1)
	if len(sqls) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(sqls))
	}
	for i, s := range sqls {
		if strings.Contains(strings.ToUpper(s), "ALTER TABLE") {
			t.Errorf("statement[%d] should not be wrapped in ALTER TABLE: %s", i, s)
		}
	}
}
