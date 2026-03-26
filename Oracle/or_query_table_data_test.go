package oracle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
)

type oracleQueryExpectation struct {
	query   string
	columns []string
	rows    [][]driver.Value
}

type oracleQueryState struct {
	mu           sync.Mutex
	expectations []oracleQueryExpectation
	queries      []string
}

type oracleQueryDriver struct {
	state *oracleQueryState
}

type oracleQueryConn struct {
	state *oracleQueryState
}

type oracleQueryRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (d *oracleQueryDriver) Open(name string) (driver.Conn, error) {
	return &oracleQueryConn{state: d.state}, nil
}

func (c *oracleQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare is not implemented in oracleQueryConn")
}

func (c *oracleQueryConn) Close() error {
	return nil
}

func (c *oracleQueryConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not implemented in oracleQueryConn")
}

func (c *oracleQueryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	c.state.queries = append(c.state.queries, query)
	if len(c.state.expectations) == 0 {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}

	exp := c.state.expectations[0]
	c.state.expectations = c.state.expectations[1:]
	if query != exp.query {
		return nil, fmt.Errorf("query mismatch: got %q want %q", query, exp.query)
	}
	return &oracleQueryRows{columns: exp.columns, rows: exp.rows}, nil
}

func (c *oracleQueryConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return c.QueryContext(context.Background(), query, named)
}

func (r *oracleQueryRows) Columns() []string {
	return r.columns
}

func (r *oracleQueryRows) Close() error {
	return nil
}

func (r *oracleQueryRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	r.index++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		}
	}
	return nil
}

func newOracleQueryTestDB(t *testing.T, expectations []oracleQueryExpectation) (*sql.DB, *oracleQueryState) {
	t.Helper()

	state := &oracleQueryState{expectations: append([]oracleQueryExpectation(nil), expectations...)}
	driverName := fmt.Sprintf("oracle-query-test-%s", strings.ReplaceAll(t.Name(), "/", "-"))
	sql.Register(driverName, &oracleQueryDriver{state: state})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open() failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func initOracleQueryTestLogger(t *testing.T) {
	t.Helper()
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "oracle-query-test.log"), "debug")
}

func TestTableRows_OracleUsesCountQueryWithoutDBMSStats(t *testing.T) {
	initOracleQueryTestLogger(t)

	expectedQuery := `SELECT COUNT(1) AS "sum" FROM TEST.BOOK`
	db, state := newOracleQueryTestDB(t, []oracleQueryExpectation{
		{
			query:   expectedQuery,
			columns: []string{"sum"},
			rows:    [][]driver.Value{{"7"}},
		},
	})

	q := &QueryTable{Schema: "TEST", Table: "BOOK"}
	got, err := q.TableRows(db, 1)
	if err != nil {
		t.Fatalf("TableRows() error = %v", err)
	}
	if got != 7 {
		t.Fatalf("TableRows() = %d, want 7", got)
	}
	if len(state.expectations) != 0 {
		t.Fatalf("unconsumed expectations remain: %d", len(state.expectations))
	}
	if len(state.queries) != 1 || state.queries[0] != expectedQuery {
		t.Fatalf("unexpected queries: %#v", state.queries)
	}
	if strings.Contains(strings.ToLower(state.queries[0]), "dbms_stats.gather_table_stats") {
		t.Fatalf("query must not call dbms_stats.gather_table_stats: %q", state.queries[0])
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(state.queries[0])), "exec ") {
		t.Fatalf("query must not use exec command: %q", state.queries[0])
	}
}

func TestTmpTableIndexColumnRowsCount_OracleUsesCountQueryWithoutDBMSStats(t *testing.T) {
	initOracleQueryTestLogger(t)

	expectedQuery := `SELECT COUNT(1) AS "sum" FROM TEST.BOOK`
	db, state := newOracleQueryTestDB(t, []oracleQueryExpectation{
		{
			query:   expectedQuery,
			columns: []string{"sum"},
			rows:    [][]driver.Value{{"11"}},
		},
	})

	q := &QueryTable{Schema: "TEST", Table: "BOOK"}
	got, err := q.TmpTableIndexColumnRowsCount(db, 1)
	if err != nil {
		t.Fatalf("TmpTableIndexColumnRowsCount() error = %v", err)
	}
	if got != 11 {
		t.Fatalf("TmpTableIndexColumnRowsCount() = %d, want 11", got)
	}
	if len(state.expectations) != 0 {
		t.Fatalf("unconsumed expectations remain: %d", len(state.expectations))
	}
	if len(state.queries) != 1 || state.queries[0] != expectedQuery {
		t.Fatalf("unexpected queries: %#v", state.queries)
	}
	if strings.Contains(strings.ToLower(state.queries[0]), "dbms_stats.gather_table_stats") {
		t.Fatalf("query must not call dbms_stats.gather_table_stats: %q", state.queries[0])
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(state.queries[0])), "exec ") {
		t.Fatalf("query must not use exec command: %q", state.queries[0])
	}
}
