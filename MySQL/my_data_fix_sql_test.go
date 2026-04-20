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

// ---------- FixAlterIndexSqlExec（prefix index 端到端） ----------

func TestFixAlterIndexSqlExec_PrefixIndex_MulIndex(t *testing.T) {
	my := newFixStruct("mul")
	// token 格式：colName/*seq*/N/*type*/T/*prefix*/P
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_goods"},
		nil,
		map[string][]string{"idx_goods": {"goods_name/*seq*/1/*type*/varchar(50)/*prefix*/20"}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	// 必须含反引号列名和前缀长度
	if !strings.Contains(got, "`goods_name`(20)") {
		t.Errorf("prefix index DDL missing `goods_name`(20): %s", got)
	}
	if !strings.Contains(got, "ADD INDEX") {
		t.Errorf("expected ADD INDEX in DDL: %s", got)
	}
}

func TestFixAlterIndexSqlExec_PrefixIndex_MultiCol(t *testing.T) {
	my := newFixStruct("mul")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_multi"},
		nil,
		map[string][]string{"idx_multi": {
			"col_a/*seq*/1/*type*/varchar(20)/*prefix*/10",
			"col_b/*seq*/2/*type*/int/*prefix*/0",
		}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "`col_a`(10)") {
		t.Errorf("multi-col: missing `col_a`(10): %s", got)
	}
	if !strings.Contains(got, "`col_b`") {
		t.Errorf("multi-col: missing `col_b`: %s", got)
	}
	// col_b prefix=0，不应有括号
	if strings.Contains(got, "`col_b`(") {
		t.Errorf("multi-col: `col_b` should not have prefix length: %s", got)
	}
}

func TestFixAlterIndexSqlExec_PrefixIndex_OldToken(t *testing.T) {
	// 旧格式 token（无 /*prefix*/）向后兼容：prefix 视为 0，不生成括号
	my := newFixStruct("mul")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_old"},
		nil,
		map[string][]string{"idx_old": {"name/*seq*/1/*type*/varchar(100)"}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "`name`") {
		t.Errorf("old token: missing `name`: %s", got)
	}
	if strings.Contains(got, "`name`(") {
		t.Errorf("old token: `name` should not have prefix length: %s", got)
	}
}

// ---------- mysqlIndexColDDLExpr ----------

func TestMysqlIndexColDDLExpr_WithPrefix(t *testing.T) {
	token := "goods_name/*seq*/1/*type*/varchar(50)/*prefix*/20"
	got := mysqlIndexColDDLExpr(token)
	want := "`goods_name`(20)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestMysqlIndexColDDLExpr_NoPrefix(t *testing.T) {
	token := "id/*seq*/1/*type*/bigint/*prefix*/0"
	got := mysqlIndexColDDLExpr(token)
	want := "`id`"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestMysqlIndexColDDLExpr_OldTokenNoPrefix(t *testing.T) {
	// 旧格式 token（无 /*prefix*/ 段），向后兼容
	token := "name/*seq*/1/*type*/varchar(100)"
	got := mysqlIndexColDDLExpr(token)
	want := "`name`"
	if got != want {
		t.Errorf("old token: got %q, want %q", got, want)
	}
}

func TestMysqlIndexColDDLExpr_SpecialCharsInColName(t *testing.T) {
	token := "col`name/*seq*/1/*type*/varchar(50)/*prefix*/10"
	got := mysqlIndexColDDLExpr(token)
	want := "`col``name`(10)"
	if got != want {
		t.Errorf("special chars: got %q, want %q", got, want)
	}
}

// ---------- FixAlterIndexSqlExec DROP 顺序在 ADD 之前 ----------

// TestFixAlterIndexSqlExec_DropBeforeAdd 验证同时有 ADD 和 DROP 时，DROP 操作在 ADD 之前生成，
// 确保合并后的 ALTER TABLE 语句中先删后建，避免同名索引冲突。
// 断言策略：记录最后一个 DROP INDEX 位置与第一个 ADD INDEX 位置，断言 lastDrop < firstAdd，
// 防止"DROP/ADD 交错但首个 DROP 仍在首个 ADD 之前"的假通过情形。
func TestFixAlterIndexSqlExec_DropBeforeAdd(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{Schema: "gt_checksum", Table: "indext", IndexType: "mul"}
	// e: 需要 ADD 的索引（源端有，目标端没有）
	// f: 需要 DROP 的索引（目标端有，源端没有）
	e := []string{"idx 2", "idx_3"}
	f := []string{"idx_2", "idx 3"}
	si := map[string][]string{
		"idx 2": {"tenantry_id/*seq*/1/*type*/bigint/*prefix*/0", "code/*seq*/2/*type*/varchar(64)/*prefix*/0"},
		"idx_3": {"code/*seq*/1/*type*/varchar(64)/*prefix*/0", "tenantry_id/*seq*/2/*type*/bigint/*prefix*/0"},
	}
	sqls := my.FixAlterIndexSqlExec(e, f, si, "mysql", 1)
	// 期望：先 DROP，再 ADD
	if len(sqls) < 4 {
		t.Fatalf("expected at least 4 statements, got %d: %v", len(sqls), sqls)
	}
	firstAdd := -1
	lastDrop := -1
	for i, s := range sqls {
		upper := strings.ToUpper(s)
		if strings.Contains(upper, "DROP INDEX") {
			lastDrop = i
		}
		if firstAdd == -1 && strings.Contains(upper, "ADD INDEX") {
			firstAdd = i
		}
	}
	if lastDrop == -1 {
		t.Fatal("no DROP INDEX found")
	}
	if firstAdd == -1 {
		t.Fatal("no ADD INDEX found")
	}
	// 严格断言：所有 DROP 必须在任意 ADD 之前，防止交错排列
	if lastDrop > firstAdd {
		t.Errorf("all DROP INDEX must come before any ADD INDEX, but last DROP at pos %d, first ADD at pos %d\nsqls: %v", lastDrop, firstAdd, sqls)
	}
}

// ---------- 前缀索引 × uni / 可见性分支 ----------

// TestFixAlterIndexSqlExec_PrefixIndex_UniIndex 验证 uni 分支下前缀索引生成 UNIQUE INDEX DDL。
func TestFixAlterIndexSqlExec_PrefixIndex_UniIndex(t *testing.T) {
	my := newFixStruct("uni")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"uk_email"},
		nil,
		map[string][]string{"uk_email": {"email/*seq*/1/*type*/varchar(255)/*prefix*/100"}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "ADD UNIQUE INDEX") {
		t.Errorf("uni prefix: expected ADD UNIQUE INDEX, got: %s", got)
	}
	if !strings.Contains(got, "`email`(100)") {
		t.Errorf("uni prefix: expected `email`(100), got: %s", got)
	}
}

// TestFixAlterIndexSqlExec_PrefixIndex_UniInvisible 验证 uni + prefix + INVISIBLE 组合生成正确 DDL（MySQL）。
func TestFixAlterIndexSqlExec_PrefixIndex_UniInvisible(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema:    "sbtest",
		Table:     "t9",
		IndexType: "uni",
		IndexVisibilityMap: map[string]string{
			"uk_name": "NO",
		},
	}
	sqls := my.FixAlterIndexSqlExec(
		[]string{"uk_name"},
		nil,
		map[string][]string{"uk_name": {"name/*seq*/1/*type*/varchar(200)/*prefix*/50"}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "ADD UNIQUE INDEX") {
		t.Errorf("uni+invisible: expected ADD UNIQUE INDEX, got: %s", got)
	}
	if !strings.Contains(got, "`name`(50)") {
		t.Errorf("uni+invisible: expected `name`(50), got: %s", got)
	}
	if !strings.Contains(strings.ToUpper(got), "INVISIBLE") {
		t.Errorf("uni+invisible: expected INVISIBLE clause, got: %s", got)
	}
}

// TestFixAlterIndexSqlExec_PrefixIndex_MariaDBIgnored 验证 MariaDB 下 mul + prefix + IGNORED 生成正确 DDL。
func TestFixAlterIndexSqlExec_PrefixIndex_MariaDBIgnored(t *testing.T) {
	my := &MysqlDataAbnormalFixStruct{
		Schema:     "sbtest",
		Table:      "t9",
		IndexType:  "mul",
		DestFlavor: global.DatabaseFlavorMariaDB,
		IndexVisibilityMap: map[string]string{
			"idx_title": "IGNORED",
		},
	}
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_title"},
		nil,
		map[string][]string{"idx_title": {"title/*seq*/1/*type*/varchar(500)/*prefix*/30"}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d", len(sqls))
	}
	got := sqls[0]
	if !strings.Contains(got, "ADD INDEX") {
		t.Errorf("mariadb+ignored: expected ADD INDEX, got: %s", got)
	}
	if !strings.Contains(got, "`title`(30)") {
		t.Errorf("mariadb+ignored: expected `title`(30), got: %s", got)
	}
	if !strings.Contains(strings.ToUpper(got), "IGNORED") {
		t.Errorf("mariadb+ignored: expected IGNORED clause, got: %s", got)
	}
}

// ---------- 函数索引（Functional Index）----------

// makeFuncToken 构造函数索引 token，格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0
func makeFuncToken(expr, seq string) string {
	return "/*expr*/" + expr + "/*seq*/" + seq + "/*type*//*prefix*/0"
}

func TestMysqlIndexColDDLExpr_FuncIndex(t *testing.T) {
	token := makeFuncToken("(ABS(`price`))", "1")
	got := mysqlIndexColDDLExpr(token)
	want := "(ABS(`price`))"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestMysqlIndexColDDLExpr_FuncIndex_Multiword(t *testing.T) {
	token := makeFuncToken("(LOWER(`email`))", "1")
	got := mysqlIndexColDDLExpr(token)
	want := "(LOWER(`email`))"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFixAlterIndexSqlExec_FuncIndex_AddIndex(t *testing.T) {
	my := newFixStruct("mul")
	token := makeFuncToken("(ABS(`price`))", "1")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_5"},
		nil,
		map[string][]string{"idx_5": {token}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d: %v", len(sqls), sqls)
	}
	got := sqls[0]
	// 修复 SQL 必须含函数表达式
	if !strings.Contains(got, "(ABS(`price`))") {
		t.Errorf("functional index DDL missing expression: %s", got)
	}
	if !strings.Contains(got, "ADD INDEX") {
		t.Errorf("expected ADD INDEX in DDL: %s", got)
	}
	if !strings.Contains(got, "`idx_5`") {
		t.Errorf("expected index name `idx_5` in DDL: %s", got)
	}
}

func TestFixAlterIndexSqlExec_FuncIndex_DropAndAdd(t *testing.T) {
	// 模拟目标端有旧索引需 DROP，源端有函数索引需 ADD
	my := newFixStruct("mul")
	token := makeFuncToken("(ABS(`price`))", "1")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_5"},  // ADD
		[]string{"idx_5"},  // DROP
		map[string][]string{"idx_5": {token}},
		"mysql", 1,
	)
	if len(sqls) != 2 {
		t.Fatalf("expected 2 sqls (DROP+ADD), got %d: %v", len(sqls), sqls)
	}
	// DROP 在前
	if !strings.Contains(strings.ToUpper(sqls[0]), "DROP INDEX") {
		t.Errorf("first sql should be DROP INDEX, got: %s", sqls[0])
	}
	// ADD 在后，含函数表达式
	if !strings.Contains(sqls[1], "(ABS(`price`))") {
		t.Errorf("second sql missing expression: %s", sqls[1])
	}
}

// TestMysqlIndexColDDLExpr_FuncIndex_NoOuterParens 验证 MySQL 实际存储格式：
// information_schema.statistics.EXPRESSION 不含外层括号，DDL 生成时必须补全。
func TestMysqlIndexColDDLExpr_FuncIndex_NoOuterParens(t *testing.T) {
	// MySQL 实际格式：表达式不带外层括号
	token := makeFuncToken("abs(`price`)", "1")
	got := mysqlIndexColDDLExpr(token)
	want := "(abs(`price`))"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestMysqlIndexColDDLExpr_FuncIndex_NoOuterParens_Multiword(t *testing.T) {
	token := makeFuncToken("lower(`email`)", "1")
	got := mysqlIndexColDDLExpr(token)
	want := "(lower(`email`))"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFixAlterIndexSqlExec_FuncIndex_NoOuterParens(t *testing.T) {
	// 验证 MySQL 实际格式（无外层括号）生成的修复 SQL 语法正确
	my := newFixStruct("mul")
	token := makeFuncToken("abs(`price`)", "1")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_5"},
		nil,
		map[string][]string{"idx_5": {token}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d: %v", len(sqls), sqls)
	}
	got := sqls[0]
	// 必须含双括号形式：ADD INDEX `idx_5`((abs(`price`)))
	want := "ADD INDEX `idx_5`((abs(`price`)))"
	if !strings.Contains(got, want) {
		t.Errorf("functional index DDL wrong, got: %s\nwant substring: %s", got, want)
	}
}

// TestFixAlterIndexSqlExec_FuncIndex_JSONWithSingleQuotes 验证含单引号的 JSON 函数索引
// 生成的 DDL 中不包含反斜杠转义的单引号（修复 INFORMATION_SCHEMA.EXPRESSION 转义问题）。
func TestFixAlterIndexSqlExec_FuncIndex_JSONWithSingleQuotes(t *testing.T) {
	my := newFixStruct("mul")
	// 模拟 EXPRESSION 列反转义后的表达式（单引号已还原）
	jsonExpr := "cast(json_extract(`f9`,_utf8mb4'$.tags') as char(50) array)"
	token := makeFuncToken(jsonExpr, "1")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_multi_tags"},
		nil,
		map[string][]string{"idx_multi_tags": {token}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d: %v", len(sqls), sqls)
	}
	got := sqls[0]
	// DDL 中必须包含正常单引号，不能有反斜杠转义
	if strings.Contains(got, `\'`) {
		t.Errorf("DDL must NOT contain escaped single quotes (\\'), got: %s", got)
	}
	if !strings.Contains(got, "_utf8mb4'$.tags'") {
		t.Errorf("DDL should contain unescaped single quotes, got: %s", got)
	}
	if !strings.Contains(got, "ADD INDEX `idx_multi_tags`") {
		t.Errorf("DDL missing index name, got: %s", got)
	}
}

// TestFixAlterIndexSqlExec_FuncIndex_JSONUnquoteWithSingleQuotes 验证 json_unquote 函数索引。
func TestFixAlterIndexSqlExec_FuncIndex_JSONUnquoteWithSingleQuotes(t *testing.T) {
	my := newFixStruct("mul")
	jsonExpr := "cast(json_unquote(json_extract(`f9`,_utf8mb4'$.status')) as char(50) charset utf8mb4)"
	token := makeFuncToken(jsonExpr, "1")
	sqls := my.FixAlterIndexSqlExec(
		[]string{"idx_v_f9"},
		nil,
		map[string][]string{"idx_v_f9": {token}},
		"mysql", 1,
	)
	if len(sqls) != 1 {
		t.Fatalf("expected 1 sql, got %d: %v", len(sqls), sqls)
	}
	got := sqls[0]
	if strings.Contains(got, `\'`) {
		t.Errorf("DDL must NOT contain escaped single quotes (\\'), got: %s", got)
	}
	if !strings.Contains(got, "_utf8mb4'$.status'") {
		t.Errorf("DDL should contain unescaped single quotes, got: %s", got)
	}
}

// TestBuildFloatDeletePredicate_BareFloat 验证裸 FLOAT 列 DELETE WHERE 使用 ROUND(.,7)。
// Bug：之前使用 `CAST(... AS FLOAT)` 只在 MySQL 8.0.17+ 支持，在 MySQL 5.7 /
// MySQL 8.0.0-8.0.16 / MariaDB 10.x 上会报 Syntax error 导致 fix 脚本失败。
// 现改为 ROUND(col,7) = ROUND(val,7) —— 7 有效数字匹配 IEEE754 单精度。
func TestBuildFloatDeletePredicate_BareFloat(t *testing.T) {
	cases := []struct {
		dataType    string
		value       string
		wantContain string // 期望 predicate 包含的关键字
		wantAvoid   string // 期望 predicate 不包含的关键字
	}{
		// 裸 FLOAT → 使用 ROUND(col,7) = ROUND(val,7)，不再使用 CAST
		{"FLOAT", "123.449997", "ROUND(`F1`, 7) = ROUND(123.449997, 7)", "CAST"},
		{"float", "123.449997", "ROUND(`F1`, 7) = ROUND(123.449997, 7)", "CAST"},
		// FLOAT(M,D) → 使用 ROUND
		{"FLOAT(5,2)", "123.449997", "ROUND(", ""},
		// DOUBLE → fallback，不需要 CAST AS FLOAT
		{"DOUBLE", "123.449997", "123.449997", "CAST"},
		// DOUBLE(M,D) → 不是 FLOAT 前缀，走 plain 路径
		{"DOUBLE(10,4)", "123.4499", "123.4499", "CAST"},
	}
	for _, c := range cases {
		pred, ok := buildFloatDeletePredicate("F1", c.value, c.dataType)
		if !ok {
			t.Errorf("buildFloatDeletePredicate(F1, %q, %q) returned ok=false", c.value, c.dataType)
			continue
		}
		if !strings.Contains(pred, c.wantContain) {
			t.Errorf("buildFloatDeletePredicate(F1, %q, %q) = %q; want to contain %q",
				c.value, c.dataType, pred, c.wantContain)
		}
		if c.wantAvoid != "" && strings.Contains(pred, c.wantAvoid) {
			t.Errorf("buildFloatDeletePredicate(F1, %q, %q) = %q; must not contain %q",
				c.value, c.dataType, pred, c.wantAvoid)
		}
	}

	// 非 float 类型返回 ok=false
	if _, ok := buildFloatDeletePredicate("F1", "123.45", "VARCHAR(32)"); ok {
		t.Error("buildFloatDeletePredicate should return ok=false for VARCHAR")
	}
}

// ---------- formatMySQLInsertLiteral: BIT 列归一化 ----------
// 回归用例：commit 37f7e4286 将 BIT 列 SELECT 改为 CAST(col AS UNSIGNED) 后，
// 返回值变为十进制字符串（如 "0"、"16"），原实现仍走 `0x%X` 把 ASCII 字节编码
// 成 0x30/0x3136，对 BIT(1)/BIT(5) 插入时会触发 ERROR 1406 Data too long。
// 这里锁定修复后应输出整数字面量。
func TestFormatMySQLInsertLiteral_BitColumnNormalization(t *testing.T) {
	cases := []struct {
		dataType string
		value    string
		want     string
	}{
		{"BIT", "0", "0"},
		{"BIT(1)", "1", "1"},
		{"BIT(5)", "16", "16"},
		{"BIT(64)", "34", "34"},
		{"bit(8)", "255", "255"},
	}
	for _, c := range cases {
		got := formatMySQLInsertLiteral(c.value, c.dataType)
		if got != c.want {
			t.Errorf("formatMySQLInsertLiteral(%q, %q) = %q; want %q",
				c.value, c.dataType, got, c.want)
		}
	}
}

// BINARY/VARBINARY/BLOB 仍走 hex 编码，避免回归到 BIT 修复误伤其它二进制列。
func TestFormatMySQLInsertLiteral_BinaryColumnsStillHex(t *testing.T) {
	cases := []struct {
		dataType string
		value    string
		want     string
	}{
		{"BINARY(4)", "abcd", "0x61626364"},
		{"VARBINARY(10)", "ab", "0x6162"},
		{"BLOB", "x", "0x78"},
	}
	for _, c := range cases {
		got := formatMySQLInsertLiteral(c.value, c.dataType)
		if got != c.want {
			t.Errorf("formatMySQLInsertLiteral(%q, %q) = %q; want %q",
				c.value, c.dataType, got, c.want)
		}
	}
}

// BIT 列若拿到非数字字符串（异常路径），兜底走 hex 编码避免产生非法 SQL。
func TestFormatMySQLInsertLiteral_BitFallbackToHex(t *testing.T) {
	got := formatMySQLInsertLiteral("not-a-number", "BIT(8)")
	if got != "0x6E6F742D612D6E756D626572" {
		t.Errorf("bit fallback = %q", got)
	}
}
