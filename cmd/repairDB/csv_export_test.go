package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestWriteRepairCSVReport_BOM verifies UTF-8 BOM is written.
func TestWriteRepairCSVReport_BOM(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-bom.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 10, InsertFailure: 2, Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, 2*time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	if len(data) < 3 || data[0] != 0xEF || data[1] != 0xBB || data[2] != 0xBF {
		t.Fatalf("missing UTF-8 BOM, got: % X", data[:min(3, len(data))])
	}
}

// TestWriteRepairCSVReport_SummaryAtTop verifies summary section comes before detail rows.
func TestWriteRepairCSVReport_SummaryAtTop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-summary-top.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 10, Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	lines := strings.Split(strings.TrimSpace(content), "\n")

	// First line should be summary header
	if !strings.HasPrefix(lines[0], "统计项,数值") {
		t.Fatalf("expected summary header at top, got: %s", lines[0])
	}
	// Summary should include 总文件数
	foundTotal := false
	for _, line := range lines {
		if strings.HasPrefix(line, "总文件数,") {
			foundTotal = true
			break
		}
	}
	if !foundTotal {
		t.Fatal("missing 总文件数 in summary section")
	}
}

// TestWriteRepairCSVReport_UnifiedHeader verifies the unified header is present.
func TestWriteRepairCSVReport_UnifiedHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-unified-header.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 100, DeleteSuccess: 50, Elapsed: time.Second},
		{FilePath: "view.db.v1.sql", Schema: "db", ObjectName: "v1", ObjectType: "view", Stage: "VIEW", CreateSuccess: 1, Elapsed: time.Millisecond * 100},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	expectedHeader := strings.Join(unifiedHeader, ",")
	if !strings.Contains(content, expectedHeader) {
		t.Fatalf("missing unified header in CSV content:\n%s", content)
	}
}

// TestWriteRepairCSVReport_ObjectType verifies ObjectType column values.
func TestWriteRepairCSVReport_ObjectType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-objecttype.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", Elapsed: time.Second},
		{FilePath: "view.db.v1.sql", Schema: "db", ObjectName: "v1", ObjectType: "view", Stage: "VIEW", Elapsed: time.Second},
		{FilePath: "routine.db.p1.sql", Schema: "db", ObjectName: "p1", ObjectType: "procedure", Stage: "ROUTINE", Elapsed: time.Second},
		{FilePath: "routine.db.f1.sql", Schema: "db", ObjectName: "f1", ObjectType: "function", Stage: "ROUTINE", Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	r := csv.NewReader(strings.NewReader(content))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv read failed: %v", err)
	}

	// Find detail rows and check ObjectType column (index 2)
	objectTypes := make(map[string]bool)
	for _, row := range records {
		if len(row) >= 3 && row[0] == "db" {
			objectTypes[row[2]] = true
		}
	}
	for _, expected := range []string{"table", "view", "procedure", "function"} {
		if !objectTypes[expected] {
			t.Errorf("missing ObjectType %q in detail rows", expected)
		}
	}
}

// TestWriteRepairCSVReport_RowCount verifies correct number of CSV rows.
func TestWriteRepairCSVReport_RowCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-rows.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 10, Elapsed: time.Second},
		{FilePath: "table.db.t2.sql", Schema: "db", ObjectName: "t2", ObjectType: "table", Stage: "TABLE", InsertSuccess: 20, Elapsed: 2 * time.Second},
		{FilePath: "view.db.v1.sql", Schema: "db", ObjectName: "v1", ObjectType: "view", Stage: "VIEW", CreateSuccess: 1, Elapsed: time.Millisecond * 500},
	}
	if err := writeRepairCSVReport(results, 5*time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	r := csv.NewReader(strings.NewReader(readCSVContent(t, path)))
	r.FieldsPerRecord = -1 // allow variable-length rows (summary=2, detail=15, blank=0)
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv read failed: %v", err)
	}

	// Expected: 1 summary header + 14 summary rows + blank(skipped) + 1 detail header + 3 detail rows
	expectedRows := 1 + 14 + 1 + 3
	if len(records) != expectedRows {
		t.Fatalf("expected %d CSV rows, got %d", expectedRows, len(records))
	}
}

// TestWriteRepairCSVReport_SummaryValues verifies summary values.
func TestWriteRepairCSVReport_SummaryValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-summary.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 100, InsertFailure: 5, DeleteSuccess: 20, DeleteFailure: 3, Elapsed: time.Second},
		{FilePath: "table.db.t2.sql", Schema: "db", ObjectName: "t2", ObjectType: "table", Stage: "TABLE", InsertSuccess: 50, ErrorReason: "Error 1213: Deadlock", Elapsed: 2 * time.Second},
		{FilePath: "view.db.v1.sql", Schema: "db", ObjectName: "v1", ObjectType: "view", Stage: "VIEW", CreateSuccess: 1, Elapsed: time.Second},
		{FilePath: "routine.db.p1.sql", Schema: "db", ObjectName: "p1", ObjectType: "procedure", Stage: "ROUTINE", CreateSuccess: 1, AlterSuccess: 2, Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, 10*time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	checks := []string{
		"总文件数,4",
		"成功,3",
		"失败,1",
		"INSERT成功行数,150",
		"INSERT失败行数,5",
		"DELETE成功行数,20",
		"DELETE失败行数,3",
		"ALTER成功数,2",
		"ALTER失败数,0",
		"CREATE成功数,2",
		"CREATE失败数,0",
		"DROP成功数,0",
		"DROP失败数,0",
	}
	for _, c := range checks {
		if !strings.Contains(content, c) {
			t.Errorf("missing expected summary line: %s", c)
		}
	}
}

// TestWriteRepairCSVReport_DropColumns verifies DROP success/failure columns.
func TestWriteRepairCSVReport_DropColumns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-drop.csv")

	results := []FileExecResult{
		{FilePath: "view.db.v1.sql", Schema: "db", ObjectName: "v1", ObjectType: "view", Stage: "VIEW", DropSuccess: 2, DropFailure: 1, CreateSuccess: 1, Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	// Check summary includes DROP stats
	if !strings.Contains(content, "DROP成功数,2") {
		t.Errorf("missing DROP成功数 in summary")
	}
	if !strings.Contains(content, "DROP失败数,1") {
		t.Errorf("missing DROP失败数 in summary")
	}

	// Check detail row has DROP columns
	r := csv.NewReader(strings.NewReader(content))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv read failed: %v", err)
	}
	for _, row := range records {
		if len(row) >= 15 && row[0] == "db" && row[1] == "v1" {
			// DROP success is column 11, DROP failure is column 12
			if row[11] != "2" {
				t.Errorf("expected DROP success=2, got %s", row[11])
			}
			if row[12] != "1" {
				t.Errorf("expected DROP failure=1, got %s", row[12])
			}
		}
	}
}

// TestWriteRepairCSVReport_AutoCreateDir verifies parent directory is auto-created.
func TestWriteRepairCSVReport_AutoCreateDir(t *testing.T) {
	dir := t.TempDir()
	nestedDir := filepath.Join(dir, "sub1", "sub2")
	path := filepath.Join(nestedDir, "test.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 1, Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatalf("CSV file was not created at %s", path)
	}
}

// TestWriteRepairCSVReport_FilePermission0600 verifies file permission is 0600.
func TestWriteRepairCSVReport_FilePermission0600(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-perm.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 1, Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("expected file permission 0600, got 0%o", info.Mode().Perm())
	}
}

// TestWriteRepairCSVReport_CommaEscaping verifies fields with commas are properly escaped.
func TestWriteRepairCSVReport_CommaEscaping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-comma.csv")

	results := []FileExecResult{
		{FilePath: "table.db.t1.sql", Schema: "db", ObjectName: "t1", ObjectType: "table", Stage: "TABLE", InsertSuccess: 1, ErrorReason: "Error 1064: syntax error, near 'SELECT'", Elapsed: time.Second},
	}
	if err := writeRepairCSVReport(results, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	r := csv.NewReader(strings.NewReader(content))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv read failed: %v", err)
	}

	// Find the detail row and verify error reason is correctly quoted
	for _, row := range records {
		if len(row) >= 15 && row[0] == "db" && row[1] == "t1" {
			if row[14] != results[0].ErrorReason {
				t.Errorf("error reason mismatch: %q != %q", row[14], results[0].ErrorReason)
			}
		}
	}
}

// TestWriteRepairCSVReport_NoRows tests empty results (no data, only summary).
func TestWriteRepairCSVReport_NoRows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-empty.csv")

	if err := writeRepairCSVReport(nil, time.Second, path); err != nil {
		t.Fatalf("writeRepairCSVReport failed: %v", err)
	}

	content := readCSVContent(t, path)
	if !strings.Contains(content, "总文件数,0") {
		t.Errorf("empty report should still have summary with 0 files")
	}
	// No detail header when results are empty
	if strings.Contains(content, "ObjectName") {
		t.Errorf("empty report should not have detail header")
	}
}

// TestResolveRepairResultFilePath_Default verifies default path format.
func TestResolveRepairResultFilePath_Default(t *testing.T) {
	path := resolveRepairResultFilePath("")
	if !strings.HasPrefix(path, "result/repairDB-result-") {
		t.Errorf("default path should start with 'result/repairDB-result-', got: %s", path)
	}
	if !strings.HasSuffix(path, ".csv") {
		t.Errorf("default path should end with '.csv', got: %s", path)
	}
}

// TestResolveRepairResultFilePath_Custom verifies basedir extraction from resultFile.
func TestResolveRepairResultFilePath_Custom(t *testing.T) {
	// With a custom path containing a directory, repairDB extracts basedir
	// and generates its own filename there.
	path := resolveRepairResultFilePath("/tmp/my-report.csv")
	if !strings.HasPrefix(path, "/tmp/repairDB-result-") {
		t.Errorf("expected /tmp/repairDB-result-*, got: %s", path)
	}
	if !strings.HasSuffix(path, ".csv") {
		t.Errorf("expected .csv suffix, got: %s", path)
	}

	// Bare filename → current directory.
	path2 := resolveRepairResultFilePath("my-report.csv")
	if strings.Contains(path2, "/") {
		t.Errorf("bare filename should produce path in current directory, got: %s", path2)
	}
	if !strings.HasPrefix(path2, "repairDB-result-") {
		t.Errorf("expected repairDB-result-*, got: %s", path2)
	}
}

// TestBuildExecSummary verifies summary aggregation.
func TestBuildExecSummary(t *testing.T) {
	results := []FileExecResult{
		{InsertSuccess: 10, InsertFailure: 2, DeleteSuccess: 5, DropSuccess: 1, ErrorReason: ""},
		{InsertSuccess: 20, InsertFailure: 0, DeleteSuccess: 3, DropFailure: 2, ErrorReason: "some error"},
		{CreateSuccess: 1, AlterSuccess: 3, ErrorReason: ""},
	}
	s := buildExecSummary(results, 5*time.Second)

	if s.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, expected 3", s.TotalFiles)
	}
	if s.SuccessFiles != 2 {
		t.Errorf("SuccessFiles = %d, expected 2", s.SuccessFiles)
	}
	if s.FailureFiles != 1 {
		t.Errorf("FailureFiles = %d, expected 1", s.FailureFiles)
	}
	if s.TotalInsertOk != 30 {
		t.Errorf("TotalInsertOk = %d, expected 30", s.TotalInsertOk)
	}
	if s.TotalInsertFail != 2 {
		t.Errorf("TotalInsertFail = %d, expected 2", s.TotalInsertFail)
	}
	if s.TotalCreateOk != 1 {
		t.Errorf("TotalCreateOk = %d, expected 1", s.TotalCreateOk)
	}
	if s.TotalAlterOk != 3 {
		t.Errorf("TotalAlterOk = %d, expected 3", s.TotalAlterOk)
	}
	if s.TotalDropOk != 1 {
		t.Errorf("TotalDropOk = %d, expected 1", s.TotalDropOk)
	}
	if s.TotalDropFail != 2 {
		t.Errorf("TotalDropFail = %d, expected 2", s.TotalDropFail)
	}
}

// readCSVContent reads a CSV file and strips BOM, returning the content as string.
func readCSVContent(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	// Strip BOM
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		data = data[3:]
	}
	return string(data)
}
