package main

import (
	"fmt"
	"sort"
	"testing"
)

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
