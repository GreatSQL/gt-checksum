package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// unifiedHeader is the single CSV header for all result rows.
var unifiedHeader = []string{
	"Schema",
	"ObjectName",
	"ObjectType",
	"INSERT成功行数",
	"INSERT失败行数",
	"DELETE成功行数",
	"DELETE失败行数",
	"ALTER成功数",
	"ALTER失败数",
	"CREATE成功数",
	"CREATE失败数",
	"DROP成功数",
	"DROP失败数",
	"耗时",
	"执行失败原因",
}

// resultToUnifiedRow converts a FileExecResult to a unified CSV row.
func resultToUnifiedRow(r FileExecResult) []string {
	return []string{
		r.Schema,
		r.ObjectName,
		r.ObjectType,
		fmt.Sprintf("%d", r.InsertSuccess),
		fmt.Sprintf("%d", r.InsertFailure),
		fmt.Sprintf("%d", r.DeleteSuccess),
		fmt.Sprintf("%d", r.DeleteFailure),
		fmt.Sprintf("%d", r.AlterSuccess),
		fmt.Sprintf("%d", r.AlterFailure),
		fmt.Sprintf("%d", r.CreateSuccess),
		fmt.Sprintf("%d", r.CreateFailure),
		fmt.Sprintf("%d", r.DropSuccess),
		fmt.Sprintf("%d", r.DropFailure),
		formatDuration(r.Elapsed),
		r.ErrorReason,
	}
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	// Use milliseconds precision for sub-second, seconds otherwise
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}

// resolveRepairResultFilePath returns the output path for the repairDB CSV report.
// It derives the directory from the shared resultFile parameter (used by gt-checksum):
//   - resultFile not set / "result" / "." → result/repairDB-result-<timestamp>.csv
//   - resultFile=./xx.csv                → ./repairDB-result-<timestamp>.csv
//   - resultFile=./dir/res.csv           → ./dir/repairDB-result-<timestamp>.csv
//
// The generated filename never collides with the gt-checksum result file.
func resolveRepairResultFilePath(resultFileValue string) string {
	ts := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("repairDB-result-%s.csv", ts)

	dir := resolveResultDir(resultFileValue)
	if dir == "" || dir == "." {
		return filename
	}
	return filepath.Join(dir, filename)
}

// resolveResultDir extracts the output directory from the resultFile parameter.
// Mirrors the logic in gt-checksum's checksumProgressResultDir.
func resolveResultDir(resultFileValue string) string {
	v := strings.TrimSpace(resultFileValue)
	if v == "" {
		return "result"
	}
	cleaned := filepath.Clean(v)
	if cleaned == "." {
		return "result"
	}
	// Trailing separator means explicit directory.
	if strings.HasSuffix(v, string(os.PathSeparator)) || strings.HasSuffix(v, "/") {
		return cleaned
	}
	// "result" (the default value) is a directory name.
	if cleaned == "result" {
		return cleaned
	}
	// Existing directory on disk.
	if info, err := os.Stat(cleaned); err == nil && info.IsDir() {
		return cleaned
	}
	// Has a directory component (e.g. "./output/res.csv" → "./output").
	if dir := filepath.Dir(cleaned); dir != "." && dir != "" {
		return dir
	}
	// Bare filename with no directory (e.g. "my.csv") → current directory.
	return "."
}

// buildExecSummary aggregates results into an ExecSummary.
func buildExecSummary(results []FileExecResult, totalTime time.Duration) ExecSummary {
	s := ExecSummary{}
	s.TotalFiles = len(results)
	for _, r := range results {
		if r.ErrorReason == "" {
			s.SuccessFiles++
		} else {
			s.FailureFiles++
		}
		s.TotalInsertOk += r.InsertSuccess
		s.TotalInsertFail += r.InsertFailure
		s.TotalDeleteOk += r.DeleteSuccess
		s.TotalDeleteFail += r.DeleteFailure
		s.TotalAlterOk += r.AlterSuccess
		s.TotalAlterFail += r.AlterFailure
		s.TotalCreateOk += r.CreateSuccess
		s.TotalCreateFail += r.CreateFailure
		s.TotalDropOk += r.DropSuccess
		s.TotalDropFail += r.DropFailure
	}
	return s
}

// writeRepairCSVReport writes the repairDB execution report as a UTF-8 BOM CSV file.
// Format: summary at top, then blank line, then unified detail table.
func writeRepairCSVReport(results []FileExecResult, totalTime time.Duration, path string) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("repair csv: mkdir %q: %w", dir, err)
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("repair csv: open %q: %w", path, err)
	}
	defer f.Close()
	if err := f.Chmod(0600); err != nil {
		return fmt.Errorf("repair csv: chmod %q: %w", path, err)
	}

	if _, err := f.Write(utf8BOM); err != nil {
		return fmt.Errorf("repair csv: write BOM: %w", err)
	}

	w := csv.NewWriter(f)

	// --- Summary block (at top) ---
	summary := buildExecSummary(results, totalTime)
	if err := w.Write([]string{"统计项", "数值"}); err != nil {
		return fmt.Errorf("repair csv: write summary header: %w", err)
	}
	summaryRows := [][]string{
		{"总文件数", fmt.Sprintf("%d", summary.TotalFiles)},
		{"成功", fmt.Sprintf("%d", summary.SuccessFiles)},
		{"失败", fmt.Sprintf("%d", summary.FailureFiles)},
		{"INSERT成功行数", fmt.Sprintf("%d", summary.TotalInsertOk)},
		{"INSERT失败行数", fmt.Sprintf("%d", summary.TotalInsertFail)},
		{"DELETE成功行数", fmt.Sprintf("%d", summary.TotalDeleteOk)},
		{"DELETE失败行数", fmt.Sprintf("%d", summary.TotalDeleteFail)},
		{"ALTER成功数", fmt.Sprintf("%d", summary.TotalAlterOk)},
		{"ALTER失败数", fmt.Sprintf("%d", summary.TotalAlterFail)},
		{"CREATE成功数", fmt.Sprintf("%d", summary.TotalCreateOk)},
		{"CREATE失败数", fmt.Sprintf("%d", summary.TotalCreateFail)},
		{"DROP成功数", fmt.Sprintf("%d", summary.TotalDropOk)},
		{"DROP失败数", fmt.Sprintf("%d", summary.TotalDropFail)},
		{"总耗时", formatDuration(totalTime)},
	}
	for _, row := range summaryRows {
		if err := w.Write(row); err != nil {
			return fmt.Errorf("repair csv: write summary row: %w", err)
		}
	}

	// Blank separator line between summary and detail
	if len(results) > 0 {
		if err := w.Write([]string{}); err != nil {
			return fmt.Errorf("repair csv: write separator: %w", err)
		}
	}

	// --- Detail block ---
	if len(results) > 0 {
		if err := w.Write(unifiedHeader); err != nil {
			return fmt.Errorf("repair csv: write detail header: %w", err)
		}
		for _, r := range results {
			if err := w.Write(resultToUnifiedRow(r)); err != nil {
				return fmt.Errorf("repair csv: write detail row: %w", err)
			}
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("repair csv: flush: %w", err)
	}
	return nil
}
