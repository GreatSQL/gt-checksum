package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestPrivGrantsPrecheck_MySQLGlobalGrantSQL(t *testing.T) {
	missingPrivileges := sortedPrivilegeKeys(map[string]int{
		"SESSION_VARIABLES_ADMIN": 0,
		"REPLICATION CLIENT":      0,
	})
	wantPrivileges := []string{"REPLICATION CLIENT", "SESSION_VARIABLES_ADMIN"}
	if !reflect.DeepEqual(missingPrivileges, wantPrivileges) {
		t.Fatalf("missing privileges = %v, want %v", missingPrivileges, wantPrivileges)
	}

	grantSQL := mysqlGlobalGrantSQL(missingPrivileges, "'yejr'@'%'")
	wantGrantSQL := "GRANT REPLICATION CLIENT, SESSION_VARIABLES_ADMIN ON *.* TO 'yejr'@'%';"
	if grantSQL != wantGrantSQL {
		t.Fatalf("grant SQL = %q, want %q", grantSQL, wantGrantSQL)
	}
}

func TestPrivGrantsPrecheck_NormalizeAccessRole(t *testing.T) {
	if got := normalizePrivilegeAccessRole(" Source "); got != "source" {
		t.Fatalf("normalizePrivilegeAccessRole returned %q, want source", got)
	}
	if got := normalizePrivilegeAccessRole(""); got != "unknown" {
		t.Fatalf("normalizePrivilegeAccessRole returned %q, want unknown", got)
	}
}

func TestPrivGrantsPrecheck_NormalizeCheckObject(t *testing.T) {
	if got := normalizePrivilegeCheckObject(""); got != "data" {
		t.Fatalf("normalizePrivilegeCheckObject returned %q, want data", got)
	}
	if got := normalizePrivilegeCheckObject(" Procedure "); got != "routine" {
		t.Fatalf("normalizePrivilegeCheckObject returned %q, want routine", got)
	}
	if got := normalizePrivilegeCheckObject("TRIGGER"); got != "trigger" {
		t.Fatalf("normalizePrivilegeCheckObject returned %q, want trigger", got)
	}
}

func TestPrivGrantsPrecheck_MySQLRequiresReplicationClientPrivilege(t *testing.T) {
	for _, checkObject := range []string{"", "data", "struct", "routine", "trigger"} {
		if mysqlRequiresReplicationClientPrivilege(checkObject) {
			t.Fatalf("mysqlRequiresReplicationClientPrivilege(%q) returned true, want false", checkObject)
		}
	}

	for _, checkObject := range []string{"binlog", "inc", "increment", "incremental"} {
		if !mysqlRequiresReplicationClientPrivilege(checkObject) {
			t.Fatalf("mysqlRequiresReplicationClientPrivilege(%q) returned false, want true", checkObject)
		}
	}
}

func TestPrivGrantsPrecheck_MySQLRequiredTablePrivilegesByCheckObject(t *testing.T) {
	sourcePrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("data", "table", "source"))
	if !reflect.DeepEqual(sourcePrivileges, []string{"SELECT"}) {
		t.Fatalf("source privileges = %v, want [SELECT]", sourcePrivileges)
	}

	destDataPrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("data", "table", "dest"))
	wantDestDataPrivileges := []string{"DELETE", "INSERT", "SELECT"}
	if !reflect.DeepEqual(destDataPrivileges, wantDestDataPrivileges) {
		t.Fatalf("dest data privileges = %v, want %v", destDataPrivileges, wantDestDataPrivileges)
	}

	destStructPrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("struct", "table", "dest"))
	wantDestStructPrivileges := []string{"ALTER", "SELECT"}
	if !reflect.DeepEqual(destStructPrivileges, wantDestStructPrivileges) {
		t.Fatalf("dest struct privileges = %v, want %v", destStructPrivileges, wantDestStructPrivileges)
	}

	triggerPrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("trigger", "file", "dest"))
	if !reflect.DeepEqual(triggerPrivileges, []string{"TRIGGER"}) {
		t.Fatalf("trigger privileges = %v, want [TRIGGER]", triggerPrivileges)
	}

	routinePrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("routine", "table", "dest"))
	if len(routinePrivileges) != 0 {
		t.Fatalf("routine privileges = %v, want [] because routine uses version-specific definition precheck", routinePrivileges)
	}

	filePrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("data", "file", "dest"))
	if !reflect.DeepEqual(filePrivileges, []string{"SELECT"}) {
		t.Fatalf("file privileges = %v, want [SELECT]", filePrivileges)
	}
}

func TestPrivGrantsPrecheck_MySQLMissingTablePrivilegesAndGrantSQL(t *testing.T) {
	requiredPrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("data", "table", "dest"))
	globalGranted := map[string]int{}
	schemaGranted := map[string]map[string]int{}
	tableGranted := map[string]map[string]int{}

	mysqlAddObjectPrivilege(schemaGranted, "sbtest", "SELECT")
	missingByTable := mysqlMissingTablePrivileges(
		[]string{"sbtest.t1", "sbtest.t3"},
		requiredPrivileges,
		globalGranted,
		schemaGranted,
		tableGranted,
	)

	wantMissing := map[string][]string{
		"sbtest.t1": {"DELETE", "INSERT"},
		"sbtest.t3": {"DELETE", "INSERT"},
	}
	if !reflect.DeepEqual(missingByTable, wantMissing) {
		t.Fatalf("missingByTable = %v, want %v", missingByTable, wantMissing)
	}

	grantSQL := mysqlTableGrantSQL(missingByTable["sbtest.t1"], "sbtest.t1", "'yejr'@'%'")
	wantGrantSQL := "GRANT DELETE, INSERT ON `sbtest`.`t1` TO 'yejr'@'%';"
	if grantSQL != wantGrantSQL {
		t.Fatalf("grant SQL = %q, want %q", grantSQL, wantGrantSQL)
	}
}

func TestPrivGrantsPrecheck_StructTableFixMissingAlter(t *testing.T) {
	requiredPrivileges := sortedPrivilegeKeys(mysqlRequiredTablePrivileges("struct", "table", "dest"))
	globalGranted := map[string]int{}
	schemaGranted := map[string]map[string]int{}
	tableGranted := map[string]map[string]int{}

	// 复现用户场景：目标端具备 SELECT/INSERT/DELETE，但缺少结构修复所需 ALTER。
	mysqlAddObjectPrivilege(schemaGranted, "sbtest", "SELECT")
	mysqlAddObjectPrivilege(schemaGranted, "sbtest", "INSERT")
	mysqlAddObjectPrivilege(schemaGranted, "sbtest", "DELETE")

	missingByTable := mysqlMissingTablePrivileges(
		[]string{"sbtest.t1"},
		requiredPrivileges,
		globalGranted,
		schemaGranted,
		tableGranted,
	)

	wantMissing := map[string][]string{
		"sbtest.t1": {"ALTER"},
	}
	if !reflect.DeepEqual(missingByTable, wantMissing) {
		t.Fatalf("missingByTable = %v, want %v", missingByTable, wantMissing)
	}

	grantSQL := mysqlTableGrantSQL(missingByTable["sbtest.t1"], "sbtest.t1", "'yejr'@'%'")
	wantGrantSQL := "GRANT ALTER ON `sbtest`.`t1` TO 'yejr'@'%';"
	if grantSQL != wantGrantSQL {
		t.Fatalf("grant SQL = %q, want %q", grantSQL, wantGrantSQL)
	}
}

func TestPrivGrantsPrecheck_StructTableFixMixedSchemaAndTablePrivileges(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"sbtest.t1"}, "struct", "table", "dest", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	want := map[string]int{"sbtest.t1": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TableAccessPriCheck returned %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_StructTableFixWildcardSchemaRequiresSchemaAlter(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"sbtest.*"}, "struct", "table", "dest", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("TableAccessPriCheck returned %v, want empty because table-level ALTER must not satisfy sbtest.*", got)
	}
}

func TestPrivGrantsPrecheck_TriggerWildcardSchemaMissingTriggerPrivilege(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"gt_checksum.*"}, "trigger", "file", "source", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("TableAccessPriCheck returned %v, want empty because gt_checksum.* lacks TRIGGER", got)
	}
}

func TestPrivGrantsPrecheck_TriggerWildcardSchemaWithSchemaTriggerPrivilege(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"gt_trig_ok.*"}, "trigger", "file", "source", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	want := map[string]int{"gt_trig_ok.*": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TableAccessPriCheck returned %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_RoutineMySQL80MissingShowRoutinePrivilege(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "8.0.32")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"gt_checksum.*"}, "routine", "file", "source", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("TableAccessPriCheck returned %v, want empty because MySQL 8.0.20+ lacks SHOW_ROUTINE/global SELECT", got)
	}
}

func TestPrivGrantsPrecheck_RoutineMySQL80WithShowRoutinePrivilege(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "8.0.32-show-routine")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"gt_checksum.*"}, "routine", "file", "source", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	want := map[string]int{"gt_checksum.*": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TableAccessPriCheck returned %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_RoutineMySQL57WithMysqlProcSelect(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "5.7.38-mysql-proc-select")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"gt_checksum.*"}, "routine", "file", "source", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	want := map[string]int{"gt_checksum.*": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TableAccessPriCheck returned %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_RoutineMySQL8019RequiresGlobalSelect(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(os.DevNull, "error")
	defer func() { global.Wlog = origWlog }()

	db, err := sql.Open(mysqlPrivilegePrecheckDriverName, "8.0.19")
	if err != nil {
		t.Fatalf("open privilege precheck test db failed: %v", err)
	}
	defer db.Close()

	got, err := (&QueryTable{}).TableAccessPriCheck(db, []string{"gt_checksum.*"}, "routine", "file", "source", 1)
	if err != nil {
		t.Fatalf("TableAccessPriCheck returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("TableAccessPriCheck returned %v, want empty because MySQL 8.0.19 lacks global SELECT", got)
	}
}

const mysqlPrivilegePrecheckDriverName = "gt_checksum_mysql_privilege_precheck_test"

func init() {
	sql.Register(mysqlPrivilegePrecheckDriverName, mysqlPrivilegePrecheckDriver{})
}

type mysqlPrivilegePrecheckDriver struct{}

func (mysqlPrivilegePrecheckDriver) Open(name string) (driver.Conn, error) {
	return mysqlPrivilegePrecheckConn{name: name}, nil
}

type mysqlPrivilegePrecheckConn struct {
	name string
}

func (conn mysqlPrivilegePrecheckConn) Prepare(query string) (driver.Stmt, error) {
	return mysqlPrivilegePrecheckStmt{query: query, name: conn.name}, nil
}

func (mysqlPrivilegePrecheckConn) Close() error { return nil }

func (mysqlPrivilegePrecheckConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported in privilege precheck tests")
}

type mysqlPrivilegePrecheckStmt struct {
	query string
	name  string
}

func (stmt mysqlPrivilegePrecheckStmt) Close() error { return nil }

func (stmt mysqlPrivilegePrecheckStmt) NumInput() int { return -1 }

func (stmt mysqlPrivilegePrecheckStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("exec is not supported in privilege precheck tests")
}

func (stmt mysqlPrivilegePrecheckStmt) Query(args []driver.Value) (driver.Rows, error) {
	return mysqlPrivilegeRowsForQuery(stmt.query, stmt.name), nil
}

type mysqlPrivilegePrecheckRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (rows *mysqlPrivilegePrecheckRows) Columns() []string { return rows.columns }

func (rows *mysqlPrivilegePrecheckRows) Close() error { return nil }

func (rows *mysqlPrivilegePrecheckRows) Next(dest []driver.Value) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(dest, rows.values[rows.index])
	rows.index++
	return nil
}

func mysqlPrivilegeRowsForQuery(query, name string) driver.Rows {
	upperQuery := strings.ToUpper(query)
	switch {
	case strings.Contains(upperQuery, "CURRENT_USER()"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"user"}, values: [][]driver.Value{{"yejr@%"}}}
	case strings.Contains(upperQuery, "SELECT VERSION()"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"VERSION"}, values: [][]driver.Value{{mysqlPrivilegeVersionForName(name)}}}
	case strings.Contains(upperQuery, "INFORMATION_SCHEMA.USER_PRIVILEGES"):
		return mysqlPrivilegeUserPrivilegesForName(name)
	case strings.Contains(upperQuery, "INFORMATION_SCHEMA.SCHEMA_PRIVILEGES") && strings.Contains(query, "TABLE_SCHEMA='mysql'") && strings.Contains(name, "mysql-schema-select"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"databaseName", "privileges"}, values: [][]driver.Value{{"mysql", "SELECT"}}}
	case strings.Contains(upperQuery, "INFORMATION_SCHEMA.SCHEMA_PRIVILEGES") && strings.Contains(query, "TABLE_SCHEMA='gt_trig_ok'"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"databaseName", "privileges"}, values: [][]driver.Value{{"gt_trig_ok", "TRIGGER"}}}
	case strings.Contains(upperQuery, "INFORMATION_SCHEMA.SCHEMA_PRIVILEGES") && strings.Contains(query, "TABLE_SCHEMA='sbtest'"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"databaseName", "privileges"}, values: [][]driver.Value{{"sbtest", "SELECT"}}}
	case strings.Contains(upperQuery, "INFORMATION_SCHEMA.TABLE_PRIVILEGES") && strings.Contains(query, "TABLE_SCHEMA='mysql'") && strings.Contains(query, "TABLE_NAME='proc'") && strings.Contains(name, "mysql-proc-select"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"tableName", "privileges"}, values: [][]driver.Value{{"proc", "SELECT"}}}
	case strings.Contains(upperQuery, "INFORMATION_SCHEMA.TABLE_PRIVILEGES") && strings.Contains(query, "TABLE_SCHEMA='sbtest'"):
		return &mysqlPrivilegePrecheckRows{columns: []string{"tableName", "privileges"}, values: [][]driver.Value{{"t1", "ALTER"}}}
	default:
		return &mysqlPrivilegePrecheckRows{columns: []string{"ignored"}}
	}
}

func mysqlPrivilegeVersionForName(name string) string {
	switch {
	case strings.Contains(name, "5.6"):
		return "5.6.51"
	case strings.Contains(name, "5.7"):
		return "5.7.38"
	case strings.Contains(name, "8.0.19"):
		return "8.0.19"
	case strings.Contains(name, "8.4"):
		return "8.4.0"
	default:
		return "8.0.32"
	}
}

func mysqlPrivilegeUserPrivilegesForName(name string) driver.Rows {
	values := make([][]driver.Value, 0, 2)
	if strings.Contains(name, "show-routine") {
		values = append(values, []driver.Value{"SHOW_ROUTINE"})
	}
	if strings.Contains(name, "global-select") {
		values = append(values, []driver.Value{"SELECT"})
	}
	return &mysqlPrivilegePrecheckRows{columns: []string{"privileges"}, values: values}
}
