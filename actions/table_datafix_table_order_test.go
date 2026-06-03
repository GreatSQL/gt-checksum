package actions

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"

	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
)

const datafixOrderDriverName = "datafix_table_order_driver"

var datafixOrderDriverOnce sync.Once
var datafixOrderDriverState struct {
	sync.Mutex
	queries []string
}

func registerDatafixOrderDriver() {
	datafixOrderDriverOnce.Do(func() {
		sql.Register(datafixOrderDriverName, datafixOrderDriver{})
	})
}

func resetDatafixOrderQueries() {
	datafixOrderDriverState.Lock()
	defer datafixOrderDriverState.Unlock()
	datafixOrderDriverState.queries = nil
}

func datafixOrderQueries() []string {
	datafixOrderDriverState.Lock()
	defer datafixOrderDriverState.Unlock()
	queries := make([]string, len(datafixOrderDriverState.queries))
	copy(queries, datafixOrderDriverState.queries)
	return queries
}

type datafixOrderDriver struct{}

func (datafixOrderDriver) Open(string) (driver.Conn, error) {
	return datafixOrderConn{}, nil
}

type datafixOrderConn struct{}

func (datafixOrderConn) Prepare(string) (driver.Stmt, error) {
	return datafixOrderStmt{}, nil
}

func (datafixOrderConn) Close() error {
	return nil
}

func (datafixOrderConn) Begin() (driver.Tx, error) {
	return datafixOrderTx{}, nil
}

func (datafixOrderConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	datafixOrderDriverState.Lock()
	datafixOrderDriverState.queries = append(datafixOrderDriverState.queries, strings.TrimSpace(query))
	datafixOrderDriverState.Unlock()
	return driver.RowsAffected(1), nil
}

type datafixOrderStmt struct{}

func (datafixOrderStmt) Close() error {
	return nil
}

func (datafixOrderStmt) NumInput() int {
	return -1
}

func (datafixOrderStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (datafixOrderStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, nil
}

type datafixOrderTx struct{}

func (datafixOrderTx) Commit() error {
	return nil
}

func (datafixOrderTx) Rollback() error {
	return nil
}

func withDatafixOrderTestLogger(t *testing.T) {
	t.Helper()
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(t.TempDir()+"/datafix-table-order.log", "debug")
	t.Cleanup(func() { global.Wlog = origWlog })
}

func assertDeletesBeforeInserts(t *testing.T, queries []string) {
	t.Helper()
	lastDelete := -1
	firstInsert := -1
	for i, query := range queries {
		upper := strings.ToUpper(query)
		if strings.HasPrefix(upper, "DELETE") {
			lastDelete = i
		}
		if strings.HasPrefix(upper, "INSERT") && firstInsert == -1 {
			firstInsert = i
		}
	}
	if lastDelete == -1 || firstInsert == -1 {
		t.Fatalf("expected DELETE and INSERT statements, got %#v", queries)
	}
	if firstInsert < lastDelete {
		t.Fatalf("INSERT executed before all DELETE statements: queries=%#v", queries)
	}
}

func TestDataFixDispos_TableModeExecutesAllDeletesBeforeStagedInserts(t *testing.T) {
	registerDatafixOrderDriver()
	resetDatafixOrderQueries()

	withDatafixOrderTestLogger(t)

	origMeasured := measuredDataPods
	measuredDataPods = nil
	t.Cleanup(func() { measuredDataPods = origMeasured })

	sp := &SchedulePlan{
		schema:        "sbtest",
		table:         "t2",
		destSchema:    "sbtest",
		datafixType:   "table",
		ddrive:        datafixOrderDriverName,
		djdbc:         "",
		fixTrxNum:     2,
		fixTrxSize:    4,
		deleteSqlSize: 1024 * 1024,
		insertSqlSize: 1024 * 1024,
		pods:          &Pod{Schema: "sbtest", Table: "t2", CheckObject: "data", Datafix: "table"},
	}

	fixSQL := make(chanFixSQLItem, 4)
	fixSQL <- fixSQLItem{ChunkSeq: 1, SQL: "DELETE FROM `sbtest`.`t2` WHERE `id` = 1;"}
	fixSQL <- fixSQLItem{ChunkSeq: 1, SQL: "INSERT INTO `sbtest`.`t2`(`id`) VALUES (1);"}
	fixSQL <- fixSQLItem{ChunkSeq: 1, SQL: "INSERT INTO `sbtest`.`t2`(`id`) VALUES (2);"}
	// 旧逻辑会在第二条 INSERT 到达时直接 flush INSERT batch，导致这条后续 DELETE 晚于 INSERT 执行。
	fixSQL <- fixSQLItem{ChunkSeq: 2, SQL: "DELETE FROM `sbtest`.`t2` WHERE `id` = 2;"}
	close(fixSQL)

	sp.DataFixDispos(fixSQL, 42)

	assertDeletesBeforeInserts(t, datafixOrderQueries())
	if sp.pods.Fixed != "yes" {
		t.Fatalf("Fixed = %q, want yes", sp.pods.Fixed)
	}
}

func TestFixSqlExecTableModeOrdered_NoIndexExecutesDeleteStageFirst(t *testing.T) {
	registerDatafixOrderDriver()
	resetDatafixOrderQueries()
	withDatafixOrderTestLogger(t)

	sp := &SchedulePlan{
		schema:        "sbtest",
		table:         "noidx",
		destSchema:    "sbtest",
		datafixType:   "table",
		ddrive:        datafixOrderDriverName,
		djdbc:         "",
		fixTrxNum:     2,
		fixTrxSize:    4,
		deleteSqlSize: 1024 * 1024,
		insertSqlSize: 1024 * 1024,
	}

	sqlStrExec := make(chan string, 4)
	sqlStrExec <- "DELETE FROM `sbtest`.`noidx` WHERE `id` = 1 LIMIT 1;"
	sqlStrExec <- "INSERT INTO `sbtest`.`noidx`(`id`) VALUES (1);"
	sqlStrExec <- "INSERT INTO `sbtest`.`noidx`(`id`) VALUES (2);"
	sqlStrExec <- "DELETE FROM `sbtest`.`noidx` WHERE `id` = 2 LIMIT 1;"
	close(sqlStrExec)

	sp.FixSqlExec(sqlStrExec, 43)

	assertDeletesBeforeInserts(t, datafixOrderQueries())
	if got := sp.fixedValueForDatafixTable(); got != "yes" {
		t.Fatalf("fixedValueForDatafixTable() = %q, want yes", got)
	}
}
