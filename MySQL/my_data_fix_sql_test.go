package mysql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gt-checksum/global"
	log "gt-checksum/go-log/log"
)

func TestMain(m *testing.M) {
	// 为单元测试初始化一个丢弃输出的日志器，避免 global.Wlog 为 nil 时 panic
	global.Wlog = log.NewWlog(os.DevNull, "error")
	os.Exit(m.Run())
}

// ---------- mysqlQuoteIdent ----------

func TestMysqlQuoteIdent_Normal(t *testing.T) {
	got := mysqlQuoteIdent("idx_name")
	want := "`idx_name`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMysqlQuoteIdent_WithSpace(t *testing.T) {
	got := mysqlQuoteIdent("x 1")
	want := "`x 1`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMysqlQuoteIdent_WithBacktick(t *testing.T) {
	got := mysqlQuoteIdent("tab`le")
	want := "`tab``le`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMysqlQuoteIdent_ReservedWord(t *testing.T) {
	got := mysqlQuoteIdent("primary")
	want := "`primary`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMysqlQuoteIdent_WithDash(t *testing.T) {
	got := mysqlQuoteIdent("idx-name")
	want := "`idx-name`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFixUpdateSqlExec_UsesMappedDestinationColumnsCaseInsensitively(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema:      "src_db",
		Table:       "orders",
		IndexColumn: []string{"ID"},
		ColData: []map[string]string{
			{"columnName": "id", "dataType": "bigint"},
			{"columnName": "amount", "dataType": "decimal(10,2)"},
			{"columnName": "note", "dataType": "varchar(32)"},
		},
	}

	got, err := my.FixUpdateSqlExec(nil,
		"1/*go actions columnData*/10.50/*go actions columnData*/paid",
		[]string{"AMOUNT", "NOTE"},
		map[string]string{"amount": "TOTAL_AMOUNT", "note": "memo_text"},
		1,
	)
	if err != nil {
		t.Fatalf("FixUpdateSqlExec() error = %v", err)
	}
	want := "UPDATE `src_db`.`orders` SET `TOTAL_AMOUNT`=10.50,`memo_text`='paid' WHERE `ID`=1;"
	if got != want {
		t.Fatalf("FixUpdateSqlExec() = %q, want %q", got, want)
	}
}

func TestFixUpdateSqlExec_QuotesSchemaTableAndColumns(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema:      "db`1",
		Table:       "tab`1",
		IndexColumn: []string{"pk`col"},
		ColData: []map[string]string{
			{"columnName": "pk`col", "dataType": "bigint"},
			{"columnName": "val`col", "dataType": "varchar(32)"},
		},
	}

	got, err := my.FixUpdateSqlExec(nil,
		"7/*go actions columnData*/hello",
		[]string{"val`col"},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("FixUpdateSqlExec() error = %v", err)
	}
	want := "UPDATE `db``1`.`tab``1` SET `val``col`='hello' WHERE `pk``col`=7;"
	if got != want {
		t.Fatalf("FixUpdateSqlExec() = %q, want %q", got, want)
	}
}

// ---------- FixAlterIndexSqlExec（BUG-1/2/4） ----------

func newFixStruct(indexType string) *MysqlDataAbnormalFixStruct {
	return &MysqlDataAbnormalFixStruct{
		Schema:    "sbtest",
		Table:     "t9",
		IndexType: indexType,
	}
}

func TestFixAlterIndexSqlExec_AddUniIndex_Normal(t *testing.T) {
	my := newFixStruct("uni")
	sqls := my.FixAlterIndexSqlExec([]string{"k1"}, nil, map[string][]string{"k1": {"c2/*seq*/1/*type*/int", "c3/*seq*/2/*type*/int"}}, "mysql", 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "`k1`") {
		t.Errorf("index name not quoted: %s", got)
	}
}

func TestFixAlterIndexSqlExec_AddUniIndex_SpecialChar(t *testing.T) {
	my := newFixStruct("uni")
	sqls := my.FixAlterIndexSqlExec([]string{"x 1"}, nil, map[string][]string{"x 1": {"c1/*seq*/1/*type*/int"}}, "mysql", 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "`x 1`") {
		t.Errorf("space in index name not quoted: %s", got)
	}
}

func TestFixAlterIndexSqlExec_DropUniIndex_Normal(t *testing.T) {
	my := newFixStruct("uni")
	sqls := my.FixAlterIndexSqlExec(nil, []string{"x1"}, nil, "mysql", 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "DROP INDEX `x1`") {
		t.Errorf("drop index name not quoted: %s", got)
	}
}

func TestFixAlterIndexSqlExec_DropIndex_SpecialChar(t *testing.T) {
	my := newFixStruct("mul")
	sqls := my.FixAlterIndexSqlExec(nil, []string{"x 1"}, nil, "mysql", 1)
	got := sqls[0]
	if !strings.Contains(got, "DROP INDEX `x 1`") {
		t.Errorf("space in drop index name not quoted: %s", got)
	}
}

func TestFixAlterIndexSqlExec_DropIndex_ReservedWord(t *testing.T) {
	my := newFixStruct("mul")
	sqls := my.FixAlterIndexSqlExec(nil, []string{"primary"}, nil, "mysql", 1)
	got := sqls[0]
	if !strings.Contains(got, "DROP INDEX `primary`") {
		t.Errorf("reserved word not quoted: %s", got)
	}
}

// BUG-4：schema/table 名含反引号时应被正确转义
func TestFixAlterIndexSqlExec_SchemaWithBacktick(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema:    "my`db",
		Table:     "t1",
		IndexType: "mul",
	}
	sqls := my.FixAlterIndexSqlExec(nil, []string{"idx"}, nil, "mysql", 1)
	got := sqls[0]
	if !strings.Contains(got, "`my``db`") {
		t.Errorf("backtick in schema name not escaped: %s", got)
	}
}

// ---------- normalizeAlterOperationContent（BUG-5） ----------

func TestNormalizeAlterOperationContent_Normal(t *testing.T) {
	op := "ALTER TABLE `sbtest`.`t9` DROP INDEX `x1`;"
	got := normalizeAlterOperationContent(op)
	want := "DROP INDEX `x1`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeAlterOperationContent_SchemaWithSpace(t *testing.T) {
	op := "ALTER TABLE `my schema`.`my table` DROP INDEX `idx`;"
	got := normalizeAlterOperationContent(op)
	want := "DROP INDEX `idx`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeAlterOperationContent_NonAlterTable(t *testing.T) {
	op := "DROP INDEX `idx`;"
	got := normalizeAlterOperationContent(op)
	want := "DROP INDEX `idx`"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeAlterOperationContent_Empty(t *testing.T) {
	got := normalizeAlterOperationContent("")
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

// ---------- FixAlterIndexSqlGenerate（BUG-5 + BUG-4 combined） ----------

func TestFixAlterIndexSqlGenerate_CombinesOps(t *testing.T) {
	my := newFixStruct("mul")
	ops := []string{
		"ALTER TABLE `sbtest`.`t9` ADD INDEX `k1`(`c1`);",
		"ALTER TABLE `sbtest`.`t9` DROP INDEX `x1`;",
	}
	sqls := my.FixAlterIndexSqlGenerate(ops, 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 combined sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "ADD INDEX") || !strings.Contains(got, "DROP INDEX") {
		t.Errorf("combined sql missing operations: %s", got)
	}
}

func TestFixAlterIndexSqlGenerate_SchemaWithSpace(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{Schema: "my schema", Table: "t1", IndexType: "mul"}
	// 输入语句 schema 含空格（反引号已包裹），BUG-5 修复后应能正确提取操作内容
	ops := []string{
		"ALTER TABLE `my schema`.`t1` DROP INDEX `idx`;",
	}
	sqls := my.FixAlterIndexSqlGenerate(ops, 1)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d: %v", len(sqls), sqls)
	}
	got := sqls[0]
	if !strings.Contains(got, "DROP INDEX") {
		t.Errorf("operation not extracted correctly: %s", got)
	}
}

// ---------- 2.2 修复：MySQL FK / routine / trigger 标识符转义 ----------

// 外键名含反引号：DROP FOREIGN KEY 应正确转义
func TestFixAlterIndexSqlExec_DropFKWithSpecialName(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema:    "sbtest",
		Table:     "t1",
		IndexType: "uni",
		ForeignKeyDefinitions: map[string]string{
			"fk`1": "CONSTRAINT `fk``1` FOREIGN KEY (`c1`) REFERENCES `ref`.`t2` (`c1`)",
		},
	}
	// f = 需要 DROP 的 FK 名
	sqls := my.FixAlterIndexSqlExec(nil, []string{"fk`1"}, nil, "mysql", 1)
	got := sqls[0]
	if !strings.Contains(got, "`fk``1`") {
		t.Errorf("FK name with backtick not escaped in DROP: %s", got)
	}
}

// GenerateRoutineFixSQL：routine 名含反引号应正确转义
func TestGenerateRoutineFixSQL_SpecialName(t *testing.T) {
	sqls := GenerateRoutineFixSQL("src", "my`db", "proc`1", "PROCEDURE", "CREATE PROCEDURE proc1() BEGIN END")
	if len(sqls) == 0 {
		t.Fatal("expected at least 1 sql")
	}
	drop := sqls[0]
	if !strings.Contains(drop, "`my``db`") {
		t.Errorf("schema with backtick not escaped in routine DROP: %s", drop)
	}
	if !strings.Contains(drop, "`proc``1`") {
		t.Errorf("routine name with backtick not escaped in DROP: %s", drop)
	}
}

// GenerateTriggerFixSQL：trigger 名含反引号应正确转义
func TestGenerateTriggerFixSQL_SpecialName(t *testing.T) {
	sqls := GenerateTriggerFixSQL("src", "my`db", "trg`1", "CREATE TRIGGER trg1 BEFORE INSERT ON t1 FOR EACH ROW BEGIN END")
	if len(sqls) == 0 {
		t.Fatal("expected at least 1 sql")
	}
	drop := sqls[0]
	if !strings.Contains(drop, "`my``db`") {
		t.Errorf("schema with backtick not escaped in trigger DROP: %s", drop)
	}
	if !strings.Contains(drop, "`trg``1`") {
		t.Errorf("trigger name with backtick not escaped in DROP: %s", drop)
	}
}

func TestBuildForeignKeyDDLForFix_QuotesFKColumnsWithBacktick(t *testing.T) {
	ddl, ok := buildForeignKeyDDLForFix("fk_1", []foreignKeyColumn{
		{
			ordinalPosition:  2,
			columnName:       "ref`id",
			referencedSchema: "ref`db",
			referencedTable:  "parent`table",
			referencedColumn: "id`2",
		},
		{
			ordinalPosition:  1,
			columnName:       "child`id",
			referencedSchema: "ref`db",
			referencedTable:  "parent`table",
			referencedColumn: "id`1",
		},
	}, "srcdb")
	if !ok {
		t.Fatal("expected valid FK DDL")
	}
	if !strings.Contains(ddl, "(`child``id`,`ref``id`)") {
		t.Errorf("source FK columns not escaped/sorted correctly: %s", ddl)
	}
	if !strings.Contains(ddl, "REFERENCES `ref``db`.`parent``table` (`id``1`,`id``2`)") {
		t.Errorf("referenced identifiers not escaped correctly: %s", ddl)
	}
}

func TestCheckAndCleanupEmptyFixFile_RemovesEmptyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "table.appdb.empty.sql")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	if err := CheckAndCleanupEmptyFixFile(dir); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("expected empty file to be removed, stat err=%v", err)
	}
}

func TestCheckAndCleanupEmptyFixFile_RemovesPreambleOnlyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "table.appdb.preamble.sql")
	content := strings.Join([]string{
		"SET NAMES utf8mb4;",
		"SET FOREIGN_KEY_CHECKS = 0;",
		"BEGIN;",
		"COMMIT;",
		"",
	}, "\n")
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatalf("write preamble-only file: %v", err)
	}

	if err := CheckAndCleanupEmptyFixFile(dir); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("expected preamble-only file to be removed, stat err=%v", err)
	}
}

func TestCheckAndCleanupEmptyFixFile_KeepsActualFixSQL(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "table.appdb.data.sql")
	content := strings.Join([]string{
		"SET NAMES utf8mb4;",
		"BEGIN;",
		"INSERT INTO `t1` VALUES (1);",
		"COMMIT;",
		"",
	}, "\n")
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatalf("write fix file: %v", err)
	}

	if err := CheckAndCleanupEmptyFixFile(dir); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("expected file with actual fix SQL to remain, stat err=%v", err)
	}
}
