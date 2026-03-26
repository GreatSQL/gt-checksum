package actions

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
	"gt-checksum/inputArg"
)

type actionQueryExpectation struct {
	query   string
	columns []string
	rows    [][]driver.Value
}

type actionQueryState struct {
	mu           sync.Mutex
	expectations []actionQueryExpectation
	queries      []string
}

type actionQueryDriver struct {
	state *actionQueryState
}

type actionQueryConn struct {
	state *actionQueryState
}

type actionQueryRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (d *actionQueryDriver) Open(name string) (driver.Conn, error) {
	return &actionQueryConn{state: d.state}, nil
}

func (c *actionQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("Prepare is not implemented in actionQueryConn")
}

func (c *actionQueryConn) Close() error {
	return nil
}

func (c *actionQueryConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not implemented in actionQueryConn")
}

func (c *actionQueryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
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
	return &actionQueryRows{columns: exp.columns, rows: exp.rows}, nil
}

func (c *actionQueryConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return c.QueryContext(context.Background(), query, named)
}

func (r *actionQueryRows) Columns() []string {
	return r.columns
}

func (r *actionQueryRows) Close() error {
	return nil
}

func (r *actionQueryRows) Next(dest []driver.Value) error {
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

func newActionQueryTestDB(t *testing.T, expectations []actionQueryExpectation) (*sql.DB, *actionQueryState) {
	t.Helper()

	state := &actionQueryState{expectations: append([]actionQueryExpectation(nil), expectations...)}
	driverName := fmt.Sprintf("actions-query-test-%s", strings.ReplaceAll(t.Name(), "/", "-"))
	sql.Register(driverName, &actionQueryDriver{state: state})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open() failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func initActionQueryTestLogger(t *testing.T) {
	t.Helper()
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "actions-query-test.log"), "debug")
}

func writeShowActualRowsTestConfig(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gc.conf")
	if err := os.WriteFile(path, []byte("showActualRows="+value+"\n"), 0600); err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}
	return path
}

func withShowActualRowsConfig(t *testing.T, value string) {
	t.Helper()
	cfg := inputArg.GetGlobalConfig()
	oldConfig := cfg.Config
	oldCli := cfg.CliShowActualRows
	oldWhere := cfg.SecondaryL.SchemaV.SqlWhere
	oldRules := cfg.SecondaryL.RulesV.ShowActualRows

	cfg.Config = writeShowActualRowsTestConfig(t, value)
	cfg.CliShowActualRows = ""
	cfg.SecondaryL.SchemaV.SqlWhere = ""
	cfg.SecondaryL.RulesV.ShowActualRows = value

	t.Cleanup(func() {
		cfg.Config = oldConfig
		cfg.CliShowActualRows = oldCli
		cfg.SecondaryL.SchemaV.SqlWhere = oldWhere
		cfg.SecondaryL.RulesV.ShowActualRows = oldRules
	})
}

func resetRowCountCache() {
	tableRowCountCache = sync.Map{}
}

func TestQueryEstimatedTableRows_OracleUsesAllTablesNUM_ROWS(t *testing.T) {
	initActionQueryTestLogger(t)

	expectedQuery := "SELECT NVL(MAX(NUM_ROWS),0) FROM all_tables WHERE UPPER(owner)=UPPER('TEST') AND UPPER(table_name)=UPPER('BOOK')"
	db, state := newActionQueryTestDB(t, []actionQueryExpectation{
		{
			query:   expectedQuery,
			columns: []string{"NUM_ROWS"},
			rows:    [][]driver.Value{{int64(123)}},
		},
	})

	sp := &SchedulePlan{}
	got, ok := sp.queryEstimatedTableRows(db, "TEST", "BOOK", "godror")
	if !ok || got != 123 {
		t.Fatalf("queryEstimatedTableRows() = (%d, %v), want (123, true)", got, ok)
	}
	if len(state.expectations) != 0 {
		t.Fatalf("unconsumed expectations remain: %d", len(state.expectations))
	}
	if len(state.queries) != 1 || state.queries[0] != expectedQuery {
		t.Fatalf("unexpected queries: %#v", state.queries)
	}
	if strings.Contains(strings.ToLower(state.queries[0]), "dbms_stats.gather_table_stats") {
		t.Fatalf("estimated row query must not call dbms_stats: %q", state.queries[0])
	}
}

func TestQueryEstimatedIndexCardinality_OracleUsesAllIndexesDistinctKeys(t *testing.T) {
	initActionQueryTestLogger(t)

	expectedQuery := "SELECT NVL(MAX(DISTINCT_KEYS),0) FROM all_indexes WHERE UPPER(owner)=UPPER('TEST') AND UPPER(table_name)=UPPER('BOOK')"
	db, state := newActionQueryTestDB(t, []actionQueryExpectation{
		{
			query:   expectedQuery,
			columns: []string{"DISTINCT_KEYS"},
			rows:    [][]driver.Value{{int64(456)}},
		},
	})

	sp := &SchedulePlan{}
	got, ok := sp.queryEstimatedIndexCardinality(db, "TEST", "BOOK", "godror")
	if !ok || got != 456 {
		t.Fatalf("queryEstimatedIndexCardinality() = (%d, %v), want (456, true)", got, ok)
	}
	if len(state.expectations) != 0 {
		t.Fatalf("unconsumed expectations remain: %d", len(state.expectations))
	}
	if len(state.queries) != 1 || state.queries[0] != expectedQuery {
		t.Fatalf("unexpected queries: %#v", state.queries)
	}
	if strings.Contains(strings.ToLower(state.queries[0]), "dbms_stats.gather_table_stats") {
		t.Fatalf("estimated index query must not call dbms_stats: %q", state.queries[0])
	}
}

func TestGetExactRowCount_OracleShowActualRowsOffUsesMetadataEstimate(t *testing.T) {
	initActionQueryTestLogger(t)
	withShowActualRowsConfig(t, "OFF")
	resetRowCountCache()

	expectedQuery := "SELECT NVL(MAX(NUM_ROWS),0) FROM all_tables WHERE UPPER(owner)=UPPER('TEST') AND UPPER(table_name)=UPPER('BOOK')"
	db, state := newActionQueryTestDB(t, []actionQueryExpectation{
		{
			query:   expectedQuery,
			columns: []string{"NUM_ROWS"},
			rows:    [][]driver.Value{{int64(88)}},
		},
	})

	pool := global.NewPool(1, []*sql.DB{db}, 1, "godror")
	sp := &SchedulePlan{
		sdbPool:       pool,
		sdrive:        "godror",
		sourceSchema:  "TEST",
		tableMappings: make(map[string]string),
	}

	got, exact := sp.getExactRowCount(sp.sdbPool, "TEST", "BOOK", 1)
	if exact {
		t.Fatalf("getExactRowCount() exact = true, want false when showActualRows=OFF")
	}
	if got != 88 {
		t.Fatalf("getExactRowCount() = %d, want 88", got)
	}
	if len(state.expectations) != 0 {
		t.Fatalf("unconsumed expectations remain: %d", len(state.expectations))
	}
	if len(state.queries) != 1 || state.queries[0] != expectedQuery {
		t.Fatalf("unexpected queries: %#v", state.queries)
	}
	if strings.Contains(strings.ToLower(state.queries[0]), "count(*)") {
		t.Fatalf("showActualRows=OFF must not execute exact COUNT query: %q", state.queries[0])
	}
}

func TestGetExactRowCount_OracleShowActualRowsOnUsesCountQuery(t *testing.T) {
	initActionQueryTestLogger(t)
	withShowActualRowsConfig(t, "ON")
	resetRowCountCache()

	expectedQuery := "SELECT COUNT(*) FROM TEST.BOOK"
	db, state := newActionQueryTestDB(t, []actionQueryExpectation{
		{
			query:   expectedQuery,
			columns: []string{"COUNT"},
			rows:    [][]driver.Value{{int64(66)}},
		},
	})

	pool := global.NewPool(1, []*sql.DB{db}, 1, "godror")
	sp := &SchedulePlan{
		sdbPool:       pool,
		sdrive:        "godror",
		sourceSchema:  "TEST",
		tableMappings: make(map[string]string),
	}

	got, exact := sp.getExactRowCount(sp.sdbPool, "TEST", "BOOK", 1)
	if !exact {
		t.Fatalf("getExactRowCount() exact = false, want true when showActualRows=ON")
	}
	if got != 66 {
		t.Fatalf("getExactRowCount() = %d, want 66", got)
	}
	if len(state.expectations) != 0 {
		t.Fatalf("unconsumed expectations remain: %d", len(state.expectations))
	}
	if len(state.queries) != 1 || state.queries[0] != expectedQuery {
		t.Fatalf("unexpected queries: %#v", state.queries)
	}
	if strings.Contains(strings.ToLower(state.queries[0]), "dbms_stats.gather_table_stats") {
		t.Fatalf("exact row count query must not call dbms_stats: %q", state.queries[0])
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(state.queries[0])), "exec ") {
		t.Fatalf("exact row count query must not use exec command: %q", state.queries[0])
	}
}
