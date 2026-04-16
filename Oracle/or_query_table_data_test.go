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

// TestIndexDisposF_OracleSystemGeneratedPKName 验证 IndexDisposF 能正确识别
// Oracle 系统生成约束名（如 SYS_C0014700）的主键索引，不会将其误归入普通索引。
// 修复前，"SYS_C0014700" != "PRIMARY" 导致主键落入 smul，生成错误的
// DROP PRIMARY KEY + ADD INDEX SYS_C0014700(ID) 修复语句。
func TestIndexDisposF_OracleSystemGeneratedPKName(t *testing.T) {
	initOracleQueryTestLogger(t)

	// 模拟 Oracle tb_emp6 的索引数据：
	//   SYS_C0014700: PRIMARY KEY on ID (columnKey=1, UNIQUE)
	//   fk_emp_dept1: FOREIGN KEY on DEPTID — 外键约束无 backing index，不出现在 all_indexes
	queryData := []map[string]interface{}{
		{
			"columnName": "ID",
			"columnType": "NUMBER(11)",
			"columnKey":  "1", // DECODE(co.constraint_type,'P','1','0') = '1' 表示主键
			"nonUnique":  "UNIQUE",
			"indexName":  "SYS_C0014700",
			"IndexSeq":   "1",
			"columnSeq":  "1",
		},
	}

	q := &QueryTable{Schema: "GT_CHECKSUM", Table: "TB_EMP6"}
	spri, suni, smul, _ := q.IndexDisposF(queryData, 1)

	// 主键应落入 spri["pri"]，不应出现在 smul 或 suni
	if _, ok := spri["pri"]; !ok {
		t.Errorf("primary key not found in spri; got spri=%v smul=%v suni=%v", spri, smul, suni)
	}
	if len(smul) != 0 {
		t.Errorf("smul should be empty, got %v (SYS_C0014700 must not fall into regular index map)", smul)
	}
	if len(suni) != 0 {
		t.Errorf("suni should be empty, got %v", suni)
	}

	// 主键列应包含 ID
	cols := spri["pri"]
	if len(cols) == 0 || !strings.Contains(cols[0], "ID") {
		t.Errorf("spri[pri] = %v, expected column to contain 'ID'", cols)
	}
}

// TestIndexDisposF_OracleUniqueNonPKIndex 验证 Oracle UNIQUE 索引（非主键）
// 被正确归入 suni，而不是 smul。修复前 isUnique 判断用 "0"（MySQL 风格），
// Oracle 返回 "UNIQUE" 导致所有 UNIQUE 索引均落入 smul。
func TestIndexDisposF_OracleUniqueNonPKIndex(t *testing.T) {
	initOracleQueryTestLogger(t)

	queryData := []map[string]interface{}{
		{
			"columnName": "EMAIL",
			"columnType": "VARCHAR2(100)",
			"columnKey":  "0",
			"nonUnique":  "UNIQUE",
			"indexName":  "UK_EMP_EMAIL",
			"IndexSeq":   "1",
			"columnSeq":  "2",
		},
		{
			"columnName": "DEPTID",
			"columnType": "NUMBER(11)",
			"columnKey":  "0",
			"nonUnique":  "NONUNIQUE",
			"indexName":  "IDX_EMP_DEPT",
			"IndexSeq":   "1",
			"columnSeq":  "3",
		},
	}

	q := &QueryTable{Schema: "GT_CHECKSUM", Table: "TB_EMP"}
	spri, suni, smul, _ := q.IndexDisposF(queryData, 1)

	if len(spri) != 0 {
		t.Errorf("spri should be empty, got %v", spri)
	}
	if _, ok := suni["UK_EMP_EMAIL"]; !ok {
		t.Errorf("unique index UK_EMP_EMAIL not found in suni; got suni=%v", suni)
	}
	if _, ok := smul["IDX_EMP_DEPT"]; !ok {
		t.Errorf("regular index IDX_EMP_DEPT not found in smul; got smul=%v", smul)
	}
}

// TestIndexDisposF_OraclePKAndUniqueCoexist 验证同时有主键（系统名）和唯一索引时分类正确。
func TestIndexDisposF_OraclePKAndUniqueCoexist(t *testing.T) {
	initOracleQueryTestLogger(t)

	queryData := []map[string]interface{}{
		{
			"columnName": "ID",
			"columnType": "NUMBER(11)",
			"columnKey":  "1",
			"nonUnique":  "UNIQUE",
			"indexName":  "SYS_C0099999",
			"IndexSeq":   "1",
			"columnSeq":  "1",
		},
		{
			"columnName": "CODE",
			"columnType": "VARCHAR2(50)",
			"columnKey":  "0",
			"nonUnique":  "UNIQUE",
			"indexName":  "UQ_CODE",
			"IndexSeq":   "1",
			"columnSeq":  "2",
		},
		{
			"columnName": "NAME",
			"columnType": "VARCHAR2(100)",
			"columnKey":  "0",
			"nonUnique":  "NONUNIQUE",
			"indexName":  "IDX_NAME",
			"IndexSeq":   "1",
			"columnSeq":  "3",
		},
	}

	q := &QueryTable{Schema: "S", Table: "T"}
	spri, suni, smul, _ := q.IndexDisposF(queryData, 1)

	if _, ok := spri["pri"]; !ok {
		t.Errorf("primary key not in spri: %v", spri)
	}
	if _, ok := suni["UQ_CODE"]; !ok {
		t.Errorf("unique index UQ_CODE not in suni: %v", suni)
	}
	if _, ok := smul["IDX_NAME"]; !ok {
		t.Errorf("regular index IDX_NAME not in smul: %v", smul)
	}
	// SYS_C0099999 不能落入 suni 或 smul
	if _, ok := suni["SYS_C0099999"]; ok {
		t.Errorf("PK SYS_C0099999 must not appear in suni")
	}
	if _, ok := smul["SYS_C0099999"]; ok {
		t.Errorf("PK SYS_C0099999 must not appear in smul")
	}
}

// TestOracleComparableColumnExpr_CharNCharRTRIM 验证 CHAR/NCHAR 列被包装为 RTRIM(...)，
// 消除 Oracle 固定宽度右填充与 MySQL 自动剥离尾部空格的行为差异，避免修复SQL死循环。
func TestOracleComparableColumnExpr_CharNCharRTRIM(t *testing.T) {
	cases := []struct {
		col      string
		dtype    string
		wantRTRM bool // 期望表达式包含 RTRIM(
	}{
		{"C_CHAR", "CHAR(10)", true},
		{"C_NCHAR", "NCHAR(10)", true},
		{"c_char", "char", true},
		{"c_nchar", "nchar", true},
		{"C_CHAR", "CHAR", true},
		{"C_NCHAR", "NCHAR", true},
		// 非 CHAR/NCHAR 类型不应被 RTRIM 包装
		{"C_VARCHAR2", "VARCHAR2(100)", false},
		{"C_NUMBER", "NUMBER(10,2)", false},
		{"C_DATE", "DATE", false},
		{"C_TIMESTAMP", "TIMESTAMP(6)", false},
		{"C_CLOB", "CLOB", false},
	}

	for _, tc := range cases {
		expr := oracleComparableColumnExpr(tc.col, tc.dtype)
		hasRTRIM := strings.HasPrefix(expr, "RTRIM(")
		if hasRTRIM != tc.wantRTRM {
			t.Errorf("oracleComparableColumnExpr(%q, %q) = %q, wantRTRM=%v", tc.col, tc.dtype, expr, tc.wantRTRM)
		}
	}
}

// TestOracleComparableColumnExpr_CharNCharRTRIM_EliminatesTrailingSpace 验证 RTRIM 能真正消除
// Oracle 尾部填充空格与 MySQL 剥离空格之间的差异场景。
// 即两端真实数据：Oracle 返回 "A         "（CHAR(10) 填充），MySQL 存储 "A"。
// 经 RTRIM 后 Oracle 侧返回 "A"，与 MySQL 侧一致，不再产生 Diffs。
func TestOracleComparableColumnExpr_CharNCharRTRIM_EliminatesTrailingSpace(t *testing.T) {
	expr := oracleComparableColumnExpr("C_CHAR", "CHAR(10)")
	if !strings.HasPrefix(expr, "RTRIM(") {
		t.Fatalf("CHAR(10) expression should start with RTRIM(, got %q", expr)
	}
	expr2 := oracleComparableColumnExpr("C_NCHAR", "NCHAR(5)")
	if !strings.HasPrefix(expr2, "RTRIM(") {
		t.Fatalf("NCHAR(5) expression should start with RTRIM(, got %q", expr2)
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
