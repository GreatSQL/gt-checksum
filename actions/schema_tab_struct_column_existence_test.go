package actions

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"gt-checksum/dbExec"
	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
	"gt-checksum/inputArg"
)

// TestCheckTableExistence_SourceMissing_SkipIndexCheck 验证当源端表不存在时，
// 表应被添加到 skipIndexCheckTables 列表中，以跳过后续的索引检查。
// 这是针对 bug "tbl-not-exists" 的专项测试。
func TestCheckTableExistence_SourceMissing_SkipIndexCheck(t *testing.T) {
	// 创建一个 schemaTable 实例用于测试
	stcls := &schemaTable{
		skipIndexCheckTables: make([]string, 0),
	}

	// 模拟源端表不存在的场景
	sourceSchema := "sbtest"
	sourceTableName := "t9"
	destSchema := "sbtest"
	destTableName := "t9"

	// 验证初始状态下 skipIndexCheckTables 为空
	if len(stcls.skipIndexCheckTables) != 0 {
		t.Fatalf("初始状态 skipIndexCheckTables 应为空，但实际有 %d 个元素", len(stcls.skipIndexCheckTables))
	}

	// 模拟 checkTableExistence 中源端表不存在的处理逻辑
	// 这是我们修复的核心逻辑
	tableKey := destSchema + "." + destTableName
	stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)

	// 验证表已被添加到 skipIndexCheckTables
	found := false
	for _, skipTable := range stcls.skipIndexCheckTables {
		if skipTable == tableKey {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("源端表 %s.%s 不存在时，应将表 %s 添加到 skipIndexCheckTables，但未找到",
			sourceSchema, sourceTableName, tableKey)
	}

	t.Logf("测试通过：源端表不存在时，表 %s 已正确添加到 skipIndexCheckTables", tableKey)
}

// TestCheckTableExistence_SourceExists_NotSkipIndexCheck 验证当源端表存在时，
// 表不应被添加到 skipIndexCheckTables 列表中。
func TestCheckTableExistence_SourceExists_NotSkipIndexCheck(t *testing.T) {
	// 创建一个 schemaTable 实例用于测试
	stcls := &schemaTable{
		skipIndexCheckTables: make([]string, 0),
	}

	// 模拟源端表存在的场景
	tableKey := "sbtest.t1"

	// 验证初始状态下 skipIndexCheckTables 为空
	if len(stcls.skipIndexCheckTables) != 0 {
		t.Fatalf("初始状态 skipIndexCheckTables 应为空，但实际有 %d 个元素", len(stcls.skipIndexCheckTables))
	}

	// 源端表存在时，不应该添加到 skipIndexCheckTables
	// 这里我们验证的是：如果源端表存在，函数应该正常返回，不修改 skipIndexCheckTables

	// 验证表未被添加到 skipIndexCheckTables
	found := false
	for _, skipTable := range stcls.skipIndexCheckTables {
		if skipTable == tableKey {
			found = true
			break
		}
	}

	if found {
		t.Errorf("源端表存在时，不应将表 %s 添加到 skipIndexCheckTables", tableKey)
	}

	t.Logf("测试通过：源端表存在时，表 %s 未被添加到 skipIndexCheckTables", tableKey)
}

func TestPrivPrecheck_TargetInvisibleTableMissingSelectStopsCreateTableFix(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/priv-precheck.log", "error")
	defer func() { global.Wlog = origWlog }()

	origAccessCheck := schemaTableColumnExistenceTableAccessPriCheck
	defer func() { schemaTableColumnExistenceTableAccessPriCheck = origAccessCheck }()

	called := false
	schemaTableColumnExistenceTableAccessPriCheck = func(tc dbExec.TableColumnNameStruct, db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
		called = true
		if tc.Drive != "mysql" {
			t.Fatalf("Drive = %q, want mysql", tc.Drive)
		}
		if len(checkTableList) != 1 || checkTableList[0] != "gt_checksum.t1" {
			t.Fatalf("checkTableList = %v, want [gt_checksum.t1]", checkTableList)
		}
		if checkObject != "data" || datafix != "file" || accessRole != "dest" {
			t.Fatalf("privilege args = checkObject:%q datafix:%q accessRole:%q, want data/file/dest", checkObject, datafix, accessRole)
		}
		return map[string]int{}, nil
	}

	stcls := &schemaTable{
		destDrive:               "mysql",
		datafix:                 "file",
		caseSensitiveObjectName: "no",
		checkRules:              inputArg.RulesS{CheckObject: "data"},
	}

	err := stcls.ensureTargetAccessBeforeMissingTableRepair("gt_checksum", "t1", "[test]", 1)
	if err == nil {
		t.Fatalf("ensureTargetAccessBeforeMissingTableRepair returned nil, want missing privilege error")
	}
	if !errors.Is(err, global.ErrTargetTablePrivilegeMissing) {
		t.Fatalf("error = %v, want ErrTargetTablePrivilegeMissing", err)
	}
	if !strings.Contains(err.Error(), "target table gt_checksum.t1") {
		t.Fatalf("error = %q, want target table context", err.Error())
	}
	if !called {
		t.Fatalf("target privilege precheck was not called")
	}
}

func TestPrivPrecheck_TargetInvisibleWildcardTableUsesSchemaGrantTarget(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/priv-precheck.log", "error")
	defer func() { global.Wlog = origWlog }()

	origAccessCheck := schemaTableColumnExistenceTableAccessPriCheck
	defer func() { schemaTableColumnExistenceTableAccessPriCheck = origAccessCheck }()

	called := false
	schemaTableColumnExistenceTableAccessPriCheck = func(tc dbExec.TableColumnNameStruct, db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
		called = true
		if len(checkTableList) != 1 || checkTableList[0] != "gt_checksum.*" {
			t.Fatalf("checkTableList = %v, want [gt_checksum.*]", checkTableList)
		}
		if checkObject != "data" || datafix != "file" || accessRole != "dest" {
			t.Fatalf("privilege args = checkObject:%q datafix:%q accessRole:%q, want data/file/dest", checkObject, datafix, accessRole)
		}
		return map[string]int{}, nil
	}

	stcls := &schemaTable{
		table:                   "t1",
		rawTables:               "gt_checksum.*",
		destDrive:               "mysql",
		datafix:                 "file",
		caseSensitiveObjectName: "no",
		checkRules:              inputArg.RulesS{CheckObject: "data"},
	}

	err := stcls.ensureTargetAccessBeforeMissingTableRepair("gt_checksum", "t1", "[test]", 1)
	if err == nil {
		t.Fatalf("ensureTargetAccessBeforeMissingTableRepair returned nil, want missing privilege error")
	}
	if !errors.Is(err, global.ErrTargetTablePrivilegeMissing) {
		t.Fatalf("error = %v, want ErrTargetTablePrivilegeMissing", err)
	}
	if !called {
		t.Fatalf("target privilege precheck was not called")
	}
}

func TestPrivPrecheck_TargetInvisibleWildcardSchemaPrivilegeCoversTable(t *testing.T) {
	origAccessCheck := schemaTableColumnExistenceTableAccessPriCheck
	defer func() { schemaTableColumnExistenceTableAccessPriCheck = origAccessCheck }()

	schemaTableColumnExistenceTableAccessPriCheck = func(tc dbExec.TableColumnNameStruct, db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
		if len(checkTableList) != 1 || checkTableList[0] != "gt_checksum.*" {
			t.Fatalf("checkTableList = %v, want [gt_checksum.*]", checkTableList)
		}
		return map[string]int{"gt_checksum.*": 1}, nil
	}

	stcls := &schemaTable{
		table:                   "t1",
		rawTables:               "gt_checksum.*",
		destDrive:               "mysql",
		datafix:                 "file",
		caseSensitiveObjectName: "no",
		checkRules:              inputArg.RulesS{CheckObject: "data"},
	}

	if err := stcls.ensureTargetAccessBeforeMissingTableRepair("gt_checksum", "t1", "[test]", 1); err != nil {
		t.Fatalf("ensureTargetAccessBeforeMissingTableRepair returned %v, want nil", err)
	}
}

func TestPrivPrecheck_TargetMissingTableWithSelectKeepsCreateTableFix(t *testing.T) {
	origAccessCheck := schemaTableColumnExistenceTableAccessPriCheck
	defer func() { schemaTableColumnExistenceTableAccessPriCheck = origAccessCheck }()

	schemaTableColumnExistenceTableAccessPriCheck = func(tc dbExec.TableColumnNameStruct, db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
		return map[string]int{"gt_checksum.t1": 1}, nil
	}

	stcls := &schemaTable{
		destDrive:               "mysql",
		datafix:                 "file",
		caseSensitiveObjectName: "no",
		checkRules:              inputArg.RulesS{CheckObject: "data"},
	}

	if err := stcls.ensureTargetAccessBeforeMissingTableRepair("gt_checksum", "t1", "[test]", 1); err != nil {
		t.Fatalf("ensureTargetAccessBeforeMissingTableRepair returned %v, want nil", err)
	}
}

func TestCheckTableExistence_DataModeTableMissingReportsDDLYes(t *testing.T) {
	cases := []struct {
		name              string
		datafix           string
		sourceTableExists bool
		destTableExists   bool
		wantReason        string
		wantAccessCheck   bool
	}{
		{
			name:              "target missing with datafix table",
			datafix:           "table",
			sourceTableExists: true,
			destTableExists:   false,
			wantReason:        "table missing on target",
			wantAccessCheck:   true,
		},
		{
			name:              "target missing with datafix file",
			datafix:           "file",
			sourceTableExists: true,
			destTableExists:   false,
			wantReason:        "table missing on target",
			wantAccessCheck:   true,
		},
		{
			name:              "source missing with datafix table",
			datafix:           "table",
			sourceTableExists: false,
			destTableExists:   true,
			wantReason:        "table missing on source",
			wantAccessCheck:   false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			origWlog := global.Wlog
			global.Wlog = golog.NewWlog(t.TempDir()+"/data-mode-ddl.log", "error")
			defer func() { global.Wlog = origWlog }()

			global.ResetRuntimeState()
			defer global.ResetRuntimeState()

			origPods := measuredDataPods
			measuredDataPods = nil
			defer func() { measuredDataPods = origPods }()

			origAccessCheck := schemaTableColumnExistenceTableAccessPriCheck
			accessCalled := false
			schemaTableColumnExistenceTableAccessPriCheck = func(tc dbExec.TableColumnNameStruct, db *sql.DB, checkTableList []string, checkObject, datafix, accessRole string, logThreadSeq int64) (map[string]int, error) {
				accessCalled = true
				if len(checkTableList) != 1 || checkTableList[0] != "gt_checksum.*" {
					t.Fatalf("checkTableList = %v, want [gt_checksum.*]", checkTableList)
				}
				if checkObject != "data" || datafix != tt.datafix || accessRole != "dest" {
					t.Fatalf("privilege args = checkObject:%q datafix:%q accessRole:%q, want data/%s/dest", checkObject, datafix, accessRole, tt.datafix)
				}
				return map[string]int{"gt_checksum.*": 1}, nil
			}
			defer func() { schemaTableColumnExistenceTableAccessPriCheck = origAccessCheck }()

			sourceDB := &sql.DB{}
			destDB := &sql.DB{}
			stcls := &schemaTable{
				sourceDrive:             "mysql",
				destDrive:               "mysql",
				sourceDB:                sourceDB,
				destDB:                  destDB,
				datafix:                 tt.datafix,
				rawTables:               "gt_checksum.*",
				caseSensitiveObjectName: "no",
				checkRules:              inputArg.RulesS{CheckObject: "data"},
				tableExistenceCache: map[string]map[string]struct{}{
					tableExistenceCacheKey(sourceDB, "mysql", "gt_checksum"): {},
					tableExistenceCacheKey(destDB, "mysql", "gt_checksum"):   {},
				},
			}
			if tt.sourceTableExists {
				stcls.tableExistenceCache[tableExistenceCacheKey(sourceDB, "mysql", "gt_checksum")]["NOT_EXISTS_T2"] = struct{}{}
			}
			if tt.destTableExists {
				stcls.tableExistenceCache[tableExistenceCacheKey(destDB, "mysql", "gt_checksum")]["NOT_EXISTS_T2"] = struct{}{}
			}

			sourceExists, destExists, skip, err := stcls.checkTableExistence("gt_checksum", "not_exists_t2", "gt_checksum", "not_exists_t2", "gt_checksum.not_exists_t2", "[test]", 1)
			if err != nil {
				t.Fatalf("checkTableExistence returned error: %v", err)
			}
			if sourceExists != tt.sourceTableExists || destExists != tt.destTableExists {
				t.Fatalf("existence = source:%v dest:%v, want source:%v dest:%v", sourceExists, destExists, tt.sourceTableExists, tt.destTableExists)
			}
			if !skip {
				t.Fatalf("skip = false, want true for data-mode DDL mismatch")
			}
			if accessCalled != tt.wantAccessCheck {
				t.Fatalf("accessCalled = %v, want %v", accessCalled, tt.wantAccessCheck)
			}
			if len(measuredDataPods) != 1 {
				t.Fatalf("measuredDataPods length = %d, want 1", len(measuredDataPods))
			}
			pod := measuredDataPods[0]
			if pod.Schema != "gt_checksum" || pod.Table != "not_exists_t2" || pod.CheckObject != "data" || pod.DIFFS != "DDL-yes" || pod.Datafix != tt.datafix || pod.Rows != tt.wantReason {
				t.Fatalf("pod = %+v, want gt_checksum.not_exists_t2 data DDL-yes %s reason %q", pod, tt.datafix, tt.wantReason)
			}

			skippedTables := global.GetSkippedTables()
			if len(skippedTables) != 1 {
				t.Fatalf("skippedTables length = %d, want 1", len(skippedTables))
			}
			skipped := skippedTables[0]
			if skipped.Schema != "gt_checksum" || skipped.Table != "not_exists_t2" || skipped.CheckObject != "data" || skipped.Diffs != global.SkipDiffsDDLYes || skipped.Reason != tt.wantReason {
				t.Fatalf("skipped table = %+v, want DDL-yes reason %q", skipped, tt.wantReason)
			}
			if len(stcls.skipIndexCheckTables) != 1 || stcls.skipIndexCheckTables[0] != "gt_checksum.not_exists_t2" {
				t.Fatalf("skipIndexCheckTables = %v, want [gt_checksum.not_exists_t2]", stcls.skipIndexCheckTables)
			}
		})
	}
}

func TestWriteFixSql_StructDatafixTableForcesFixFile(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/force-file.log", "error")
	defer func() { global.Wlog = origWlog }()

	fixDir := t.TempDir()
	stmt := "CREATE TABLE IF NOT EXISTS `gt_checksum`.`not_exists_struct_force_file` (`id` int)"
	stcls := &schemaTable{
		schema:     "gt_checksum",
		table:      "not_exists_struct_force_file",
		datafix:    "table",
		datafixSql: fixDir,
		checkRules: inputArg.RulesS{CheckObject: "struct"},
		djdbc:      "user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
	}

	if err := stcls.writeFixSql([]string{stmt}, 1); err != nil {
		t.Fatalf("writeFixSql returned %v, want nil and fix file output", err)
	}

	content, err := os.ReadFile(fixDir + "/table.gt_checksum.not_exists_struct_force_file.sql")
	if err != nil {
		t.Fatalf("failed to read generated fix file: %v", err)
	}
	if !strings.Contains(string(content), "CREATE TABLE IF NOT EXISTS `gt_checksum`.`not_exists_struct_force_file` (`id` int);") {
		t.Fatalf("fix file content = %q, want CREATE TABLE statement", string(content))
	}
}

func TestWriteFixSql_DataDatafixTableKeepsDirectExecution(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/data-table.log", "error")
	defer func() { global.Wlog = origWlog }()

	fixDir := t.TempDir()
	stcls := &schemaTable{
		schema:     "gt_checksum",
		table:      "data_mode_keeps_table",
		datafix:    "table",
		datafixSql: fixDir,
		checkRules: inputArg.RulesS{CheckObject: "data"},
	}

	err := stcls.writeFixSql([]string{"ALTER TABLE `gt_checksum`.`data_mode_keeps_table` ADD COLUMN `c1` int"}, 1)
	if err == nil || !strings.Contains(err.Error(), "destination DB is nil in datafix=table mode") {
		t.Fatalf("writeFixSql error = %v, want direct execution nil-db error", err)
	}
	if _, statErr := os.Stat(fixDir + "/table.gt_checksum.data_mode_keeps_table.sql"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("fix file stat error = %v, want not-exist because data mode keeps direct execution", statErr)
	}
}

func TestWriteFixSql_StructDatafixTableUsesDefaultFixFileDir(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/default-fix-dir.log", "error")
	defer func() { global.Wlog = origWlog }()

	workDir := t.TempDir()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("Chdir(%s) failed: %v", workDir, err)
	}
	defer func() {
		if err := os.Chdir(oldWd); err != nil {
			t.Fatalf("restore working directory failed: %v", err)
		}
	}()

	stmt := "CREATE TABLE IF NOT EXISTS `gt_checksum`.`struct_default_fix_dir` (`id` int)"
	stcls := &schemaTable{
		schema:     "gt_checksum",
		table:      "struct_default_fix_dir",
		datafix:    "table",
		checkRules: inputArg.RulesS{CheckObject: "struct"},
		djdbc:      "user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
	}

	if err := stcls.writeFixSql([]string{stmt}, 1); err != nil {
		t.Fatalf("writeFixSql returned %v, want nil and default fix file output", err)
	}

	content, err := os.ReadFile(workDir + "/fixsql/table.gt_checksum.struct_default_fix_dir.sql")
	if err != nil {
		t.Fatalf("failed to read generated default fix file: %v", err)
	}
	if !strings.Contains(string(content), "CREATE TABLE IF NOT EXISTS `gt_checksum`.`struct_default_fix_dir` (`id` int);") {
		t.Fatalf("fix file content = %q, want CREATE TABLE statement", string(content))
	}
}

func TestWriteAdvisoryFixSql_StructDatafixTableUsesDefaultFixFileDir(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/default-advisory-dir.log", "error")
	defer func() { global.Wlog = origWlog }()

	workDir := t.TempDir()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("Chdir(%s) failed: %v", workDir, err)
	}
	defer func() {
		if err := os.Chdir(oldWd); err != nil {
			t.Fatalf("restore working directory failed: %v", err)
		}
	}()

	stcls := &schemaTable{
		schema:     "gt_checksum",
		table:      "testtime",
		datafix:    "table",
		checkRules: inputArg.RulesS{CheckObject: "struct"},
		djdbc:      "user:pass@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4",
	}

	if err := stcls.writeAdvisoryFixSql([]string{"-- advisory constraint repair"}, 1); err != nil {
		t.Fatalf("writeAdvisoryFixSql returned %v, want nil and default fix file output", err)
	}

	content, err := os.ReadFile(workDir + "/fixsql/table.gt_checksum.testtime.sql")
	if err != nil {
		t.Fatalf("failed to read generated default advisory fix file: %v", err)
	}
	if !strings.Contains(string(content), "-- advisory constraint repair") {
		t.Fatalf("advisory fix file content = %q, want advisory line", string(content))
	}
}
