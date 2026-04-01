package actions

import (
	"bytes"
	"encoding/csv"
	"gt-checksum/inputArg"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// csvHeader
// ---------------------------------------------------------------------------

func TestCSVHeader_columnCount(t *testing.T) {
	h := csvHeader()
	if len(h) != 13 {
		t.Errorf("expected 13 columns, got %d: %v", len(h), h)
	}
}

func TestCSVHeader_columnOrder(t *testing.T) {
	h := csvHeader()
	want := []string{
		"RunID", "CheckTime", "CheckObject",
		"Schema", "Table", "ObjectName", "ObjectType",
		"IndexColumn", "Rows", "Diffs", "Datafix",
		"Mapping", "Definer",
	}
	for i, col := range want {
		if h[i] != col {
			t.Errorf("column[%d]: got %q, want %q", i, h[i], col)
		}
	}
}

// ---------------------------------------------------------------------------
// recordToCSVRow
// ---------------------------------------------------------------------------

func TestRecordToCSVRow_fieldOrder(t *testing.T) {
	rec := ResultRecord{
		RunID:       "20260323120000",
		CheckTime:   "2026-03-23 12:00:00",
		CheckObject: "data",
		Schema:      "db1",
		Table:       "t1",
		ObjectName:  "t1",
		ObjectType:  "table",
		IndexColumn: "id",
		Rows:        "100",
		Diffs:       "no",
		Datafix:     "file",
		Mapping:     "",
		Definer:     "",
	}
	row := recordToCSVRow(rec)
	if len(row) != 13 {
		t.Errorf("expected 13 fields, got %d", len(row))
	}
	if row[0] != "20260323120000" {
		t.Errorf("row[0] (RunID) = %q", row[0])
	}
	if row[9] != "no" {
		t.Errorf("row[9] (Diffs) = %q", row[9])
	}
}

// ---------------------------------------------------------------------------
// ResolveResultFilePath
// ---------------------------------------------------------------------------

func TestResolveResultFilePath_defaultNaming(t *testing.T) {
	m := &inputArg.ConfigParameter{}
	m.RunID = "20260323120000"
	m.SecondaryL.RulesV.ResultFile = ""
	path := ResolveResultFilePath(m)
	want := "result/gt-checksum-result-20260323120000.csv"
	if path != want {
		t.Errorf("got %q, want %q", path, want)
	}
}

func TestResolveResultFilePath_explicitPath(t *testing.T) {
	m := &inputArg.ConfigParameter{}
	m.RunID = "20260323120000"
	m.SecondaryL.RulesV.ResultFile = "./out/my-result.csv"
	path := ResolveResultFilePath(m)
	if path != "./out/my-result.csv" {
		t.Errorf("got %q, want ./out/my-result.csv", path)
	}
}

func TestResolveResultFilePath_trimSpace(t *testing.T) {
	m := &inputArg.ConfigParameter{}
	m.RunID = "20260323120000"
	m.SecondaryL.RulesV.ResultFile = "  ./result.csv  "
	path := ResolveResultFilePath(m)
	if path != "./result.csv" {
		t.Errorf("got %q, expected trimmed path", path)
	}
}

// ---------------------------------------------------------------------------
// WriteCSVResults
// ---------------------------------------------------------------------------

func TestWriteCSVResults_UTF8BOM(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")

	records := []ResultRecord{
		{RunID: "20260323120000", CheckObject: "data", Schema: "db1", Table: "t1", Diffs: "no"},
	}
	if err := WriteCSVResults(path, records); err != nil {
		t.Fatalf("WriteCSVResults error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.HasPrefix(data, []byte{0xEF, 0xBB, 0xBF}) {
		t.Errorf("file does not start with UTF-8 BOM")
	}
}

func TestWriteCSVResults_headerPresent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")

	if err := WriteCSVResults(path, nil); err != nil {
		t.Fatalf("WriteCSVResults error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	// Strip BOM
	content := strings.TrimPrefix(string(data), "\xEF\xBB\xBF")
	r := csv.NewReader(strings.NewReader(content))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v", err)
	}
	if len(rows) < 1 {
		t.Fatal("no rows in CSV")
	}
	if rows[0][0] != "RunID" {
		t.Errorf("first column header = %q, want RunID", rows[0][0])
	}
}

func TestWriteCSVResults_commaEscaping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")

	records := []ResultRecord{
		{ObjectName: `name,with,commas`, Diffs: "yes"},
	}
	if err := WriteCSVResults(path, records); err != nil {
		t.Fatalf("WriteCSVResults error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	content := strings.TrimPrefix(string(data), "\xEF\xBB\xBF")
	r := csv.NewReader(strings.NewReader(content))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv parse error (field not escaped?): %v", err)
	}
	// Find the ObjectName column index
	header := rows[0]
	objNameIdx := -1
	for i, h := range header {
		if h == "ObjectName" {
			objNameIdx = i
			break
		}
	}
	if objNameIdx < 0 {
		t.Fatal("ObjectName column not found")
	}
	if rows[1][objNameIdx] != "name,with,commas" {
		t.Errorf("ObjectName = %q, want name,with,commas", rows[1][objNameIdx])
	}
}

func TestWriteCSVResults_quoteEscaping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")

	records := []ResultRecord{
		{ObjectName: `name"quoted"`, Diffs: "yes"},
	}
	if err := WriteCSVResults(path, records); err != nil {
		t.Fatalf("WriteCSVResults error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	content := strings.TrimPrefix(string(data), "\xEF\xBB\xBF")
	r := csv.NewReader(strings.NewReader(content))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv parse error: %v", err)
	}
	header := rows[0]
	objNameIdx := -1
	for i, h := range header {
		if h == "ObjectName" {
			objNameIdx = i
			break
		}
	}
	if rows[1][objNameIdx] != `name"quoted"` {
		t.Errorf("ObjectName = %q", rows[1][objNameIdx])
	}
}

func TestWriteCSVResults_rowCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")

	records := []ResultRecord{
		{Schema: "db1", Table: "t1", Diffs: "no"},
		{Schema: "db1", Table: "t2", Diffs: "yes"},
		{Schema: "db1", Table: "t3", Diffs: "DDL-yes"},
	}
	if err := WriteCSVResults(path, records); err != nil {
		t.Fatalf("WriteCSVResults error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	content := strings.TrimPrefix(string(data), "\xEF\xBB\xBF")
	r := csv.NewReader(strings.NewReader(content))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v", err)
	}
	// 1 header + 3 data rows
	if len(rows) != 4 {
		t.Errorf("expected 4 rows (1 header + 3 data), got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// ExportResultsIfNeeded
// ---------------------------------------------------------------------------

func TestExportResultsIfNeeded_OFF(t *testing.T) {
	m := &inputArg.ConfigParameter{}
	m.RunID = "20260323120000"
	m.SecondaryL.RulesV.ResultExport = "OFF"
	m.SecondaryL.RulesV.ResultFile = filepath.Join(t.TempDir(), "should-not-exist.csv")

	if err := ExportResultsIfNeeded(m, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(m.SecondaryL.RulesV.ResultFile); !os.IsNotExist(err) {
		t.Error("CSV file should not have been created when resultExport=OFF")
	}
}

func TestExportResultsIfNeeded_csvCreated(t *testing.T) {
	dir := t.TempDir()
	m := &inputArg.ConfigParameter{}
	m.RunID = "20260323120000"
	m.SecondaryL.RulesV.ResultExport = "csv"
	m.SecondaryL.RulesV.ResultFile = filepath.Join(dir, "result.csv")

	records := []ResultRecord{{Schema: "db1", Table: "t1", Diffs: "no"}}
	if err := ExportResultsIfNeeded(m, records); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(m.SecondaryL.RulesV.ResultFile); err != nil {
		t.Errorf("CSV file not created: %v", err)
	}
}

// ---------------------------------------------------------------------------
// WriteCSVResults — 目录自动创建 + 文件权限
// ---------------------------------------------------------------------------

func TestWriteCSVResults_autoCreatesParentDir(t *testing.T) {
	// resultFile 指向一个不存在的子目录，WriteCSVResults 应当自动创建
	dir := filepath.Join(t.TempDir(), "subdir", "nested")
	path := filepath.Join(dir, "result.csv")
	if err := WriteCSVResults(path, nil); err != nil {
		t.Fatalf("WriteCSVResults failed with non-existent parent dir: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("CSV file not found after auto-create: %v", err)
	}
}

func TestWriteCSVResults_filePermission0600(t *testing.T) {
	const want = os.FileMode(0600)

	t.Run("new file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "result.csv")
		if err := WriteCSVResults(path, nil); err != nil {
			t.Fatalf("WriteCSVResults failed: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat failed: %v", err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("file permission = %04o, want %04o", got, want)
		}
	})

	t.Run("existing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "result.csv")
		if err := os.WriteFile(path, []byte("legacy"), 0644); err != nil {
			t.Fatalf("seed file failed: %v", err)
		}
		if err := WriteCSVResults(path, nil); err != nil {
			t.Fatalf("WriteCSVResults failed: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat failed: %v", err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("file permission = %04o, want %04o", got, want)
		}
	})
}
