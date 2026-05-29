package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestExecuteSQLFile_DuplicateKeySplitContinues(t *testing.T) {
	registerRepairDBDupKeyDriver()
	resetRepairDBDupKeyDriverState("")

	dir := t.TempDir()
	sqlFile := filepath.Join(dir, "table.db1.t1.sql")
	content := "BEGIN;\n" +
		"INSERT INTO `db1`.`t1`(`id`,`name`) VALUES (1,'ok'),(2,'dup'),(3,'ok');\n" +
		"INSERT INTO `db1`.`t1`(`id`,`name`) VALUES (4,'after');\n" +
		"COMMIT;\n"
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create SQL file: %v", err)
	}

	db, err := sql.Open(repairDBDupKeyDriverName, "")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	result, err := executeSQLFile(context.Background(), db, sqlFile)
	if err != nil {
		t.Fatalf("executeSQLFile returned error: %v", err)
	}
	if result.InsertSuccess != 3 {
		t.Fatalf("InsertSuccess = %d, want 3", result.InsertSuccess)
	}
	if result.InsertFailure != 1 {
		t.Fatalf("InsertFailure = %d, want 1", result.InsertFailure)
	}

	after, err := os.ReadFile(sqlFile)
	if err != nil {
		t.Fatalf("failed to read SQL file: %v", err)
	}
	if string(after) != content {
		t.Fatal("executeSQLFile modified original SQL file")
	}

	queries := repairDBDupKeyQueries()
	assertQueryContains(t, queries, "VALUES (1,'ok'),(2,'dup'),(3,'ok')")
	assertQueryContains(t, queries, "VALUES (1,'ok')")
	assertQueryContains(t, queries, "VALUES (2,'dup')")
	assertQueryContains(t, queries, "VALUES (3,'ok')")
	assertQueryContains(t, queries, "VALUES (4,'after')")
}

func TestExecuteSQLFile_DuplicateKeySplitNonDuplicateErrorStops(t *testing.T) {
	registerRepairDBDupKeyDriver()
	resetRepairDBDupKeyDriverState("(3,'bad')")

	dir := t.TempDir()
	sqlFile := filepath.Join(dir, "table.db1.t1.sql")
	content := "INSERT INTO `db1`.`t1`(`id`,`name`) VALUES (1,'ok'),(2,'dup'),(3,'bad');\n"
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create SQL file: %v", err)
	}

	db, err := sql.Open(repairDBDupKeyDriverName, "")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	result, err := executeSQLFile(context.Background(), db, sqlFile)
	if err == nil {
		t.Fatal("expected split INSERT non-duplicate error")
	}
	if !strings.Contains(err.Error(), "split INSERT tuple #3/3 failed") {
		t.Fatalf("expected split tuple failure, got %v", err)
	}
	if result.InsertSuccess != 0 {
		t.Fatalf("InsertSuccess = %d, want 0", result.InsertSuccess)
	}
	if result.InsertFailure != 1 {
		t.Fatalf("InsertFailure = %d, want 1", result.InsertFailure)
	}
}

func TestSplitSQLStatementsWithLocation_Multiline(t *testing.T) {
	content := "-- generated\n" +
		"BEGIN;\n" +
		"INSERT INTO `db1`.`t1`(`id`,`name`) VALUES\n" +
		"(1,'a'),\n" +
		"(2,'b');\n" +
		"COMMIT;\n"
	statements := splitSQLStatementsWithLocation(content)
	if len(statements) != 3 {
		t.Fatalf("got %d statements, want 3: %#v", len(statements), statements)
	}
	insertStmt := statements[1]
	if insertStmt.startLine != 3 || insertStmt.endLine != 5 {
		t.Fatalf("insert statement line range = %d-%d, want 3-5", insertStmt.startLine, insertStmt.endLine)
	}
}

const repairDBDupKeyDriverName = "repairdb_dupkey_driver"

var repairDBDupKeyDriverOnce sync.Once
var repairDBDupKeyDriverState struct {
	sync.Mutex
	queries               []string
	failNonDuplicateTuple string
}

func registerRepairDBDupKeyDriver() {
	repairDBDupKeyDriverOnce.Do(func() {
		sql.Register(repairDBDupKeyDriverName, repairDBDupKeyDriver{})
	})
}

func resetRepairDBDupKeyDriverState(failNonDuplicateTuple string) {
	repairDBDupKeyDriverState.Lock()
	defer repairDBDupKeyDriverState.Unlock()
	repairDBDupKeyDriverState.queries = nil
	repairDBDupKeyDriverState.failNonDuplicateTuple = failNonDuplicateTuple
}

func repairDBDupKeyQueries() []string {
	repairDBDupKeyDriverState.Lock()
	defer repairDBDupKeyDriverState.Unlock()
	queries := make([]string, len(repairDBDupKeyDriverState.queries))
	copy(queries, repairDBDupKeyDriverState.queries)
	return queries
}

type repairDBDupKeyDriver struct{}

func (repairDBDupKeyDriver) Open(string) (driver.Conn, error) {
	return repairDBDupKeyConn{}, nil
}

type repairDBDupKeyConn struct{}

func (repairDBDupKeyConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not supported")
}

func (repairDBDupKeyConn) Close() error {
	return nil
}

func (repairDBDupKeyConn) Begin() (driver.Tx, error) {
	return repairDBDupKeyTx{}, nil
}

func (repairDBDupKeyConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return repairDBDupKeyTx{}, nil
}

func (repairDBDupKeyConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	trimmed := strings.TrimSpace(query)
	if strings.HasPrefix(strings.ToUpper(trimmed), "SET ") {
		return driver.RowsAffected(0), nil
	}

	repairDBDupKeyDriverState.Lock()
	repairDBDupKeyDriverState.queries = append(repairDBDupKeyDriverState.queries, trimmed)
	failNonDuplicateTuple := repairDBDupKeyDriverState.failNonDuplicateTuple
	repairDBDupKeyDriverState.Unlock()

	compact := strings.Join(strings.Fields(trimmed), " ")
	if strings.Contains(compact, "),(") || strings.Contains(compact, "), (") {
		return nil, fmt.Errorf("Error 1062: Duplicate entry '2' for key 't1.PRIMARY'")
	}
	if strings.Contains(compact, "(2,'dup')") {
		return nil, fmt.Errorf("Error 1062: Duplicate entry '2' for key 't1.PRIMARY'")
	}
	if failNonDuplicateTuple != "" && strings.Contains(compact, failNonDuplicateTuple) {
		return nil, fmt.Errorf("Error 1406: Data too long for column 'name'")
	}
	return driver.RowsAffected(1), nil
}

type repairDBDupKeyTx struct{}

func (repairDBDupKeyTx) Commit() error {
	return nil
}

func (repairDBDupKeyTx) Rollback() error {
	return nil
}

func assertQueryContains(t *testing.T, queries []string, want string) {
	t.Helper()
	for _, query := range queries {
		if strings.Contains(query, want) {
			return
		}
	}
	t.Fatalf("expected executed query containing %q, got %#v", want, queries)
}
