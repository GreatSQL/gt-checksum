package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeRepairDBConfigForTest(t *testing.T, body string) string {
	t.Helper()
	confFile := filepath.Join(t.TempDir(), "gc.conf")
	if err := os.WriteFile(confFile, []byte(body), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	return confFile
}

func TestParseConfigSplitInsertOnDupKeyDefaultOn(t *testing.T) {
	old := config
	t.Cleanup(func() { config = old })
	config = Config{}

	confFile := writeRepairDBConfigForTest(t, "dstDSN=mysql|user:pass@tcp(127.0.0.1:3306)/db\n")
	if err := parseConfig(confFile); err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if !config.SplitInsertOnDupKey {
		t.Fatal("SplitInsertOnDupKey = false, want true by default")
	}
}

func TestParseConfigSplitInsertOnDupKeyOff(t *testing.T) {
	old := config
	t.Cleanup(func() { config = old })
	config = Config{}

	confFile := writeRepairDBConfigForTest(t, "dstDSN=mysql|user:pass@tcp(127.0.0.1:3306)/db\nsplitInsertOnDupKey=OFF\n")
	if err := parseConfig(confFile); err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if config.SplitInsertOnDupKey {
		t.Fatal("SplitInsertOnDupKey = true, want false")
	}
}

func TestParseConfigSplitInsertOnDupKeyInvalid(t *testing.T) {
	old := config
	t.Cleanup(func() { config = old })
	config = Config{}

	confFile := writeRepairDBConfigForTest(t, "dstDSN=mysql|user:pass@tcp(127.0.0.1:3306)/db\nsplitInsertOnDupKey=INVALID\n")
	err := parseConfig(confFile)
	if err == nil {
		t.Fatal("expected invalid splitInsertOnDupKey error")
	}
	if !strings.Contains(err.Error(), "invalid value for splitInsertOnDupKey") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// sliceEqual reports whether two string slices have identical contents in identical order.
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sortedCopy returns a sorted copy of s without modifying s.
func sortedCopy(s []string) []string {
	out := make([]string, len(s))
	copy(out, s)
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// detectObjectStage
// ---------------------------------------------------------------------------

func TestDetectObjectStage(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		// TABLE: bare filename and path variants
		{"table.db1.t1.sql", "TABLE"},
		{"/fixsql/table.appdb.orders.sql", "TABLE"},
		{"./fixsql/table.db1.t1-1.sql", "TABLE"},
		// VIEW
		{"view.db1.v1.sql", "VIEW"},
		{"/fixsql/view.appdb.v_orders.sql", "VIEW"},
		// ROUTINE
		{"routine.db1.p1.sql", "ROUTINE"},
		{"/fixsql/routine.appdb.proc_calc.sql", "ROUTINE"},
		// TRIGGER
		{"trigger.db1.trg1.sql", "TRIGGER"},
		{"/fixsql/trigger.appdb.trg_audit.sql", "TRIGGER"},
		// DELETE takes priority over any type prefix
		{"table.db1.t1-DELETE-1.sql", "DELETE"},
		{"view.db1.v1-DELETE-1.sql", "DELETE"},
		{"routine.db1.p1-DELETE-1.sql", "DELETE"},
		{"trigger.db1.trg1-DELETE-1.sql", "DELETE"},
		// UNKNOWN: unrecognised prefixes and hand-crafted filenames
		{"manual.sql", "UNKNOWN"},
		{"fix_data.sql", "UNKNOWN"},
		{"index.db1.t1.sql", "UNKNOWN"}, // index.* is not a recognised prefix
		{"data.db1.t1.sql", "UNKNOWN"},
	}

	for _, tc := range cases {
		got := detectObjectStage(tc.path)
		if got != tc.want {
			t.Errorf("detectObjectStage(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// classifySQLFiles
// ---------------------------------------------------------------------------

func TestDetectObjectStage_RollbackFiles(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"rollsql/table.db1.t1.rollback-DELETE-1.sql", "DELETE"},
		{"rollsql/table.db1.t1.rollback-INSERT-1.sql", "TABLE"},
		{"rollsql/table.db1.t1.rollback-TRUNCATE-1.sql", "TABLE"},
	}
	for _, tc := range cases {
		if got := detectObjectStage(tc.path); got != tc.want {
			t.Errorf("detectObjectStage(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestClassifySQLFiles_Mixed(t *testing.T) {
	input := []string{
		"table.db1.t1.sql",
		"view.db1.v1.sql",
		"routine.db1.p1.sql",
		"trigger.db1.trg1.sql",
		"table.db1.t1-DELETE-1.sql",
		"manual.sql",
	}
	cf := classifySQLFiles(input)

	check := func(label string, got []string, wantLen int, wantElem string) {
		t.Helper()
		if len(got) != wantLen {
			t.Errorf("%s: len=%d, want %d", label, len(got), wantLen)
			return
		}
		if wantLen > 0 && got[0] != wantElem {
			t.Errorf("%s[0]=%q, want %q", label, got[0], wantElem)
		}
	}

	check("Delete", cf.Delete, 1, "table.db1.t1-DELETE-1.sql")
	check("Table", cf.Table, 1, "table.db1.t1.sql")
	check("View", cf.View, 1, "view.db1.v1.sql")
	check("Routine", cf.Routine, 1, "routine.db1.p1.sql")
	check("Trigger", cf.Trigger, 1, "trigger.db1.trg1.sql")
	check("Unknown", cf.Unknown, 1, "manual.sql")
}

func TestClassifySQLFiles_Empty(t *testing.T) {
	cf := classifySQLFiles(nil)
	if cf.Delete != nil || cf.Table != nil || cf.View != nil ||
		cf.Routine != nil || cf.Trigger != nil || cf.Unknown != nil {
		t.Error("classifySQLFiles(nil): expected all fields to be nil")
	}
}

func TestClassifySQLFiles_MultiplePerStage(t *testing.T) {
	input := []string{
		"table.db1.t1.sql",
		"table.db1.t2.sql",
		"view.db1.v1.sql",
		"view.db1.v2.sql",
	}
	cf := classifySQLFiles(input)
	if len(cf.Table) != 2 {
		t.Errorf("Table count=%d, want 2", len(cf.Table))
	}
	if len(cf.View) != 2 {
		t.Errorf("View count=%d, want 2", len(cf.View))
	}
	if len(cf.Delete)+len(cf.Routine)+len(cf.Trigger)+len(cf.Unknown) != 0 {
		t.Error("unexpected files in other stages")
	}
}

// ---------------------------------------------------------------------------
// buildExecutionStages
// ---------------------------------------------------------------------------

func TestBuildExecutionStages_FixedOrder(t *testing.T) {
	// All six stage types present; verify order is always DELETE→TABLE→VIEW→ROUTINE→TRIGGER→UNKNOWN.
	input := []string{
		"trigger.db1.trg1.sql",
		"table.db1.t1.sql",
		"view.db1.v1.sql",
		"routine.db1.p1.sql",
		"table.db1.t1-DELETE-1.sql",
		"manual.sql",
	}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	want := []string{"DELETE", "TABLE", "VIEW", "ROUTINE", "TRIGGER", "UNKNOWN"}
	if len(stages) != len(want) {
		t.Fatalf("stage count=%d, want %d", len(stages), len(want))
	}
	for i, s := range stages {
		if s.Name != want[i] {
			t.Errorf("stages[%d].Name=%q, want %q", i, s.Name, want[i])
		}
	}
}

func TestBuildExecutionStages_ShuffleFlags(t *testing.T) {
	input := []string{
		"table.db1.t1.sql",
		"view.db1.v1.sql",
		"routine.db1.p1.sql",
		"trigger.db1.trg1.sql",
		"table.db1.t1-DELETE-1.sql",
	}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	for _, s := range stages {
		wantShuffle := s.Name == "TABLE"
		if s.Shuffle != wantShuffle {
			t.Errorf("stage %q: Shuffle=%v, want %v", s.Name, s.Shuffle, wantShuffle)
		}
	}
}

func TestBuildExecutionStages_RollbackFiles(t *testing.T) {
	input := []string{
		"rollsql/table.db1.t1.rollback-INSERT-1.sql",
		"rollsql/table.db1.t1.rollback-DELETE-1.sql",
		"rollsql/table.db1.t1.rollback-TRUNCATE-1.sql",
	}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	if len(stages) != 2 {
		t.Fatalf("stage count=%d, want 2", len(stages))
	}
	if stages[0].Name != "DELETE" {
		t.Fatalf("first rollback stage=%q, want DELETE", stages[0].Name)
	}
	if stages[1].Name != "TABLE" {
		t.Fatalf("second rollback stage=%q, want TABLE", stages[1].Name)
	}
	if len(stages[0].Files) != 1 || stages[0].Files[0] != "rollsql/table.db1.t1.rollback-DELETE-1.sql" {
		t.Fatalf("DELETE stage files=%v, want rollback DELETE file", stages[0].Files)
	}
	if !stages[1].Shuffle {
		t.Fatal("TABLE rollback stage should retain existing shuffle behavior")
	}
	wantTableFiles := []string{
		"rollsql/table.db1.t1.rollback-INSERT-1.sql",
		"rollsql/table.db1.t1.rollback-TRUNCATE-1.sql",
	}
	if !sliceEqual(sortedCopy(stages[1].Files), sortedCopy(wantTableFiles)) {
		t.Fatalf("TABLE stage files=%v, want %v", stages[1].Files, wantTableFiles)
	}
}

func TestBuildExecutionStages_EmptyStagesOmitted(t *testing.T) {
	// Only TABLE and VIEW; DELETE/ROUTINE/TRIGGER/UNKNOWN must not appear.
	input := []string{
		"table.db1.t1.sql",
		"view.db1.v1.sql",
	}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	if len(stages) != 2 {
		t.Fatalf("stage count=%d, want 2", len(stages))
	}
	if stages[0].Name != "TABLE" {
		t.Errorf("stages[0].Name=%q, want TABLE", stages[0].Name)
	}
	if stages[1].Name != "VIEW" {
		t.Errorf("stages[1].Name=%q, want VIEW", stages[1].Name)
	}
}

func TestBuildExecutionStages_UnknownOmittedWhenAbsent(t *testing.T) {
	input := []string{"table.db1.t1.sql"}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	for _, s := range stages {
		if s.Name == "UNKNOWN" {
			t.Error("UNKNOWN stage must not appear when there are no unknown files")
		}
	}
}

func TestBuildExecutionStages_UnknownPresentAndLast(t *testing.T) {
	input := []string{
		"table.db1.t1.sql",
		"manual.sql",
	}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	if len(stages) == 0 {
		t.Fatal("expected at least one stage")
	}
	last := stages[len(stages)-1]
	if last.Name != "UNKNOWN" {
		t.Errorf("last stage=%q, want UNKNOWN", last.Name)
	}
}

func TestBuildExecutionStages_AllFilesPresent(t *testing.T) {
	input := []string{
		"table.db1.t1.sql",
		"view.db1.v1.sql",
		"table.db1.t1-DELETE-1.sql",
	}
	cf := classifySQLFiles(input)
	stages := buildExecutionStages(cf)

	totalFiles := 0
	for _, s := range stages {
		totalFiles += len(s.Files)
	}
	if totalFiles != len(input) {
		t.Errorf("total files across stages=%d, want %d", totalFiles, len(input))
	}
}

// ---------------------------------------------------------------------------
// prepareStageFiles
// ---------------------------------------------------------------------------

func TestPrepareStageFiles_NonTableSorted(t *testing.T) {
	for _, name := range []string{"DELETE", "VIEW", "ROUTINE", "TRIGGER", "UNKNOWN"} {
		files := []string{"c.sql", "a.sql", "b.sql"}
		stage := executionStage{Name: name, Files: files, Shuffle: false}
		result := prepareStageFiles(stage)
		want := sortedCopy(files)
		if !sliceEqual(result, want) {
			t.Errorf("stage %q: result not sorted: got %v, want %v", name, result, want)
		}
	}
}

func TestPrepareStageFiles_NonTableDoesNotModifyInput(t *testing.T) {
	files := []string{"c.sql", "a.sql", "b.sql"}
	original := append([]string(nil), files...)
	stage := executionStage{Name: "VIEW", Files: files, Shuffle: false}
	_ = prepareStageFiles(stage)
	if !sliceEqual(files, original) {
		t.Error("prepareStageFiles modified the input slice for a non-TABLE stage")
	}
}

func TestPrepareStageFiles_TableContainsSameFiles(t *testing.T) {
	files := []string{"table.db1.t2.sql", "table.db1.t1.sql", "table.db1.t3.sql"}
	stage := executionStage{Name: "TABLE", Files: files, Shuffle: true}
	result := prepareStageFiles(stage)

	if len(result) != len(files) {
		t.Fatalf("TABLE stage: result len=%d, want %d", len(result), len(files))
	}
	if !sliceEqual(sortedCopy(result), sortedCopy(files)) {
		t.Error("TABLE stage: result contains different files than input")
	}
}

func TestPrepareStageFiles_TableDoesNotModifyInput(t *testing.T) {
	files := []string{"table.db1.t2.sql", "table.db1.t1.sql", "table.db1.t3.sql"}
	original := append([]string(nil), files...)
	stage := executionStage{Name: "TABLE", Files: files, Shuffle: true}
	_ = prepareStageFiles(stage)
	if !sliceEqual(files, original) {
		t.Error("prepareStageFiles modified the input slice for a TABLE stage")
	}
}

func TestPrepareStageFiles_TableShuffles(t *testing.T) {
	// Build 20 files with a clear alphabetical order; run up to 20 times and
	// expect at least one result that differs from the sorted order.
	// P(all 20 runs produce sorted output) ≈ (1/20!)^20 — effectively impossible.
	files := make([]string, 20)
	for i := range files {
		files[i] = fmt.Sprintf("table.db1.t%02d.sql", i)
	}
	sorted := sortedCopy(files)
	stage := executionStage{Name: "TABLE", Files: files, Shuffle: true}

	shuffleDetected := false
	for attempt := 0; attempt < 20; attempt++ {
		result := prepareStageFiles(stage)
		if !sliceEqual(result, sorted) {
			shuffleDetected = true
			break
		}
	}
	if !shuffleDetected {
		t.Error("TABLE stage: after 20 runs prepareStageFiles never produced a shuffled order")
	}
}

// TestResultCollector_Concurrent verifies resultCollector is safe for concurrent use.
func TestResultCollector_Concurrent(t *testing.T) {
	collector := &resultCollector{}
	const numGoroutines = 50
	const resultsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < resultsPerGoroutine; i++ {
				collector.append(FileExecResult{
					FilePath:      fmt.Sprintf("table.db.t%d.sql", gid),
					InsertSuccess: int64(i),
				})
			}
		}(g)
	}
	wg.Wait()

	snapshot := collector.snapshot()
	expected := numGoroutines * resultsPerGoroutine
	if len(snapshot) != expected {
		t.Errorf("snapshot length = %d, expected %d", len(snapshot), expected)
	}
}

func TestRepairDB_BreakResumeStopsSchedulingAfterInterrupt(t *testing.T) {
	oldParallelThds := config.ParallelThds
	oldFixFileDir := config.FixFileDir
	defer func() {
		config.ParallelThds = oldParallelThds
		config.FixFileDir = oldFixFileDir
	}()

	dir := t.TempDir()
	config.ParallelThds = 1
	config.FixFileDir = dir

	sqlFile := filepath.Join(dir, "table.db1.t1.sql")
	if err := os.WriteFile(sqlFile, []byte("INSERT INTO t VALUES (1);"), 0644); err != nil {
		t.Fatalf("failed to create SQL file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := parallelExecuteSQLFiles(ctx, nil, []string{sqlFile}, "TABLE", nil)
	if err == nil {
		t.Fatal("expected interrupted error")
	}
	if !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("expected interrupted error, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no executed result, got %d", len(results))
	}
}

func TestRepairDB_RepeatedInterruptKeepsSignalHandlerActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	stop := make(chan struct{})
	done := make(chan struct{})
	go handleRepairSignals(cancel, signals, stop, done)

	signals <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		close(stop)
		<-done
		t.Fatal("expected first interrupt to cancel context")
	}

	signals <- os.Interrupt
	select {
	case <-done:
		t.Fatal("signal handler exited after repeated interrupt")
	case <-time.After(20 * time.Millisecond):
	}

	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signal handler did not stop")
	}
}

func TestRepairDB_InterruptDoesNotCancelInFlightSQL(t *testing.T) {
	registerRepairDBInterruptDriver()

	oldParallelThds := config.ParallelThds
	oldFixFileDir := config.FixFileDir
	defer func() {
		config.ParallelThds = oldParallelThds
		config.FixFileDir = oldFixFileDir
	}()

	dir := t.TempDir()
	config.ParallelThds = 1
	config.FixFileDir = dir

	sqlFile := filepath.Join(dir, "table.db1.t1.sql")
	if err := os.WriteFile(sqlFile, []byte("INSERT INTO t VALUES (1);"), 0644); err != nil {
		t.Fatalf("failed to create SQL file: %v", err)
	}

	db, err := sql.Open(repairDBInterruptDriverName, "")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ctxErrCh := make(chan error, 1)
	repairDBInterruptDriverState.Lock()
	repairDBInterruptDriverState.cancel = cancel
	repairDBInterruptDriverState.ctxErrCh = ctxErrCh
	repairDBInterruptDriverState.Unlock()
	defer func() {
		repairDBInterruptDriverState.Lock()
		repairDBInterruptDriverState.cancel = nil
		repairDBInterruptDriverState.ctxErrCh = nil
		repairDBInterruptDriverState.Unlock()
	}()

	results, err := parallelExecuteSQLFiles(ctx, db, []string{sqlFile}, "TABLE", nil)
	if err == nil {
		t.Fatal("expected interrupted error")
	}
	if !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("expected interrupted error, got %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one in-flight result, got %d", len(results))
	}
	if results[0].InsertSuccess != 1 {
		t.Fatalf("expected insert success count 1, got %d", results[0].InsertSuccess)
	}

	select {
	case execCtxErr := <-ctxErrCh:
		if execCtxErr != nil {
			t.Fatalf("in-flight SQL context was canceled: %v", execCtxErr)
		}
	default:
		t.Fatal("test driver did not observe INSERT execution")
	}
}

const repairDBInterruptDriverName = "repairdb_interrupt_driver"

var repairDBInterruptDriverOnce sync.Once
var repairDBInterruptDriverState struct {
	sync.Mutex
	cancel   func()
	ctxErrCh chan error
}

func registerRepairDBInterruptDriver() {
	repairDBInterruptDriverOnce.Do(func() {
		sql.Register(repairDBInterruptDriverName, repairDBInterruptDriver{})
	})
}

type repairDBInterruptDriver struct{}

func (repairDBInterruptDriver) Open(string) (driver.Conn, error) {
	return repairDBInterruptConn{}, nil
}

type repairDBInterruptConn struct{}

func (repairDBInterruptConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not supported")
}

func (repairDBInterruptConn) Close() error {
	return nil
}

func (repairDBInterruptConn) Begin() (driver.Tx, error) {
	return repairDBInterruptTx{}, nil
}

func (repairDBInterruptConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "SET ") {
		return driver.RowsAffected(0), nil
	}

	repairDBInterruptDriverState.Lock()
	cancel := repairDBInterruptDriverState.cancel
	ctxErrCh := repairDBInterruptDriverState.ctxErrCh
	repairDBInterruptDriverState.Unlock()
	if cancel != nil {
		cancel()
	}
	if ctxErrCh != nil {
		ctxErrCh <- ctx.Err()
	}
	return driver.RowsAffected(1), nil
}

type repairDBInterruptTx struct{}

func (repairDBInterruptTx) Commit() error {
	return nil
}

func (repairDBInterruptTx) Rollback() error {
	return nil
}
