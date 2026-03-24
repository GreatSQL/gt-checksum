package actions

import (
	"encoding/csv"
	"fmt"
	"gt-checksum/inputArg"
	"os"
	"path/filepath"
	"strings"
)

// utf8BOM is the byte-order mark that makes Excel open UTF-8 CSV files correctly.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// csvHeader returns the fixed, ordered column headers for the result CSV.
// The order must stay stable across releases to avoid breaking downstream consumers.
func csvHeader() []string {
	return []string{
		"RunID",
		"CheckTime",
		"CheckObject",
		"Schema",
		"Table",
		"ObjectName",
		"ObjectType",
		"IndexColumn",
		"Rows",
		"Diffs",
		"Datafix",
		"Mapping",
		"Definer",
	}
}

// recordToCSVRow converts a ResultRecord into an ordered string slice matching csvHeader().
func recordToCSVRow(r ResultRecord) []string {
	return []string{
		r.RunID,
		r.CheckTime,
		r.CheckObject,
		r.Schema,
		r.Table,
		r.ObjectName,
		r.ObjectType,
		r.IndexColumn,
		r.Rows,
		r.Diffs,
		r.Datafix,
		r.Mapping,
		r.Definer,
	}
}

// ResolveResultFilePath returns the absolute or relative file path for the CSV output.
// If m.SecondaryL.RulesV.ResultFile is non-empty it is returned as-is; otherwise the
// default naming convention gt-checksum-result-<RunID>.csv is applied.
func ResolveResultFilePath(m *inputArg.ConfigParameter) string {
	if v := strings.TrimSpace(m.SecondaryL.RulesV.ResultFile); v != "" {
		return v
	}
	return fmt.Sprintf("gt-checksum-result-%s.csv", m.RunID)
}

// WriteCSVResults writes records to path as a UTF-8 BOM CSV file using the fixed
// column schema defined in csvHeader(). The file is created (or truncated) at path.
// Parent directories are created automatically if they do not exist.
// Fields containing commas, double-quotes, or newlines are escaped by encoding/csv.
func WriteCSVResults(path string, records []ResultRecord) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("result csv: mkdir %q: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("result csv: open %q: %w", path, err)
	}
	defer f.Close()

	// Write UTF-8 BOM so that Excel auto-detects the encoding.
	if _, err := f.Write(utf8BOM); err != nil {
		return fmt.Errorf("result csv: write BOM: %w", err)
	}

	w := csv.NewWriter(f)
	if err := w.Write(csvHeader()); err != nil {
		return fmt.Errorf("result csv: write header: %w", err)
	}
	for _, r := range records {
		if err := w.Write(recordToCSVRow(r)); err != nil {
			return fmt.Errorf("result csv: write row: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("result csv: flush: %w", err)
	}
	return nil
}

// ExportResultsIfNeeded is the top-level entry point called from the main program.
// It honours the resultExport setting and does nothing when set to "OFF".
// On write failure it returns an error; the caller decides whether to treat it as fatal.
func ExportResultsIfNeeded(m *inputArg.ConfigParameter, records []ResultRecord) error {
	if strings.ToUpper(strings.TrimSpace(m.SecondaryL.RulesV.ResultExport)) == "OFF" {
		return nil
	}
	path := ResolveResultFilePath(m)
	if err := WriteCSVResults(path, records); err != nil {
		return err
	}
	fmt.Printf("Result exported to: %s\n", path)
	return nil
}
