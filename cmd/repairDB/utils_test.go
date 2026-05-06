package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFormatSize tests file size formatting
func TestFormatSize(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{100, "100 B"},
		{1024, "1.00 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
		{2147483648, "2.00 GB"},
		{512, "512 B"},
		{2048, "2.00 KB"},
		{5242880, "5.00 MB"},
	}

	for _, tt := range tests {
		result := formatSize(tt.input)
		if result != tt.expected {
			t.Errorf("formatSize(%d) = %s; expected %s", tt.input, result, tt.expected)
		}
	}
}

// TestFormatNumber tests number formatting with thousand separators
func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{100, "100"},
		{1000, "1,000"},
		{1000000, "1,000,000"},
		{123456789, "123,456,789"},
		{999, "999"},
		{1001, "1,001"},
	}

	for _, tt := range tests {
		result := formatNumber(tt.input)
		if result != tt.expected {
			t.Errorf("formatNumber(%d) = %s; expected %s", tt.input, result, tt.expected)
		}
	}
}

// TestIdentifyStatementType tests SQL statement type identification
func TestIdentifyStatementType(t *testing.T) {
	tests := []struct {
		input        string
		expectedType string
		expectedRows int64
	}{
		{"INSERT INTO t VALUES (1)", "INSERT", 1},
		{"INSERT INTO t VALUES (1), (2), (3)", "INSERT", 3},
		{"UPDATE t SET a=1 WHERE id=1", "UPDATE", 1},
		{"DELETE FROM t WHERE id=1", "DELETE", 1},
		{"DROP TABLE t", "DROP", 1},
		{"ALTER TABLE t ADD COLUMN c INT", "ALTER", 1},
		{"CREATE TABLE t (id INT)", "CREATE", 1},
		{"SELECT * FROM t", "UNKNOWN", 0},
		{"", "UNKNOWN", 0},
	}

	for _, tt := range tests {
		stmtType, rows := identifyStatementType(tt.input)
		if stmtType != tt.expectedType || rows != tt.expectedRows {
			t.Errorf("identifyStatementType(%q) = (%s, %d); expected (%s, %d)",
				tt.input, stmtType, rows, tt.expectedType, tt.expectedRows)
		}
	}
}

// TestEstimateBinlogSize tests binlog size estimation
func TestEstimateBinlogSize(t *testing.T) {
	tests := []struct {
		totalSize   int64
		deleteRows  int64
		insertRows  int64
		expectedMin int64
		expectedMax int64
	}{
		{1000000, 0, 1000, 1300000, 1300000},     // More inserts: 1.3x
		{1000000, 1000, 0, 1100000, 1100000},     // More deletes: 1.1x
		{1000000, 500, 500, 1100000, 1300000},    // Equal: could be either
	}

	for _, tt := range tests {
		result := estimateBinlogSize(tt.totalSize, tt.deleteRows, tt.insertRows)
		if result < tt.expectedMin || result > tt.expectedMax {
			t.Errorf("estimateBinlogSize(%d, %d, %d) = %d; expected between %d and %d",
				tt.totalSize, tt.deleteRows, tt.insertRows, result, tt.expectedMin, tt.expectedMax)
		}
	}
}

// TestCollectFixSQLStatistics tests the statistics collection for fix SQL files
func TestCollectFixSQLStatistics(t *testing.T) {
	// Use the actual fixsql directory
	fixsqlDir := "/home/yejr/gitee/gt-checksum/fixsql"

	stats, err := collectFixSQLStatistics(fixsqlDir)
	if err != nil {
		t.Fatalf("collectFixSQLStatistics failed: %v", err)
	}

	t.Logf("Total files: %d", stats.TotalFiles)
	t.Logf("Table files: %d", stats.TableFiles)
	t.Logf("View files: %d", stats.ViewFiles)
	t.Logf("ALTER count: %d", stats.AlterCount)
	t.Logf("DROP count: %d", stats.DropCount)
	t.Logf("CREATE count: %d", stats.CreateCount)

	// Basic sanity checks
	if stats.TotalFiles == 0 {
		t.Skip("No SQL files found in fixsql directory - skipping test")
	}

	// Verify that table + view files don't exceed total files
	if stats.TableFiles+stats.ViewFiles > stats.TotalFiles {
		t.Errorf("Table files (%d) + View files (%d) exceeds total files (%d)",
			stats.TableFiles, stats.ViewFiles, stats.TotalFiles)
	}

	// Verify statement counts are non-negative
	if stats.AlterCount < 0 || stats.DropCount < 0 || stats.CreateCount < 0 {
		t.Errorf("Statement counts should be non-negative: ALTER=%d, DROP=%d, CREATE=%d",
			stats.AlterCount, stats.DropCount, stats.CreateCount)
	}
}

// TestDebugAlterCount debugs which files have ALTER statements
func TestDebugAlterCount(t *testing.T) {
	fixsqlDir := "/home/yejr/gitee/gt-checksum/fixsql"

	files, _ := filepath.Glob(filepath.Join(fixsqlDir, "*.sql"))

	alterCount := 0
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		stage := detectObjectStage(file)
		t.Logf("File: %s, Stage: %s", filepath.Base(file), stage)

		if stage == "TABLE" || stage == "DELETE" {
			statements := splitSQLStatements(string(content))
			t.Logf("  Total statements parsed: %d", len(statements))
			fileAlterCount := 0
			for i, stmt := range statements {
				stmtType, _ := identifyStatementType(stmt)
				if stmtType == "ALTER" {
					fileAlterCount++
					alterCount++
					t.Logf("  Statement #%d is ALTER: %s", i, stmt[:min(len(stmt), 80)])
				} else if stmtType == "UNKNOWN" && len(stmt) > 0 {
					// Log unknown statements that might be ALTER
					if len(stmt) > 5 && stmt[:5] == "ALTER" {
						t.Logf("  Statement #%d marked as UNKNOWN but starts with ALTER: %s", i, stmt[:min(len(stmt), 80)])
					}
				}
			}
			if fileAlterCount > 0 {
				t.Logf("  Total ALTER in this file: %d", fileAlterCount)
			} else {
				// Check if file contains ALTER in raw content
				if strings.Contains(string(content), "ALTER TABLE") {
					t.Logf("  WARNING: File contains 'ALTER TABLE' but no ALTER statements were parsed!")
				}
			}
		}
	}

	t.Logf("Total ALTER statements counted: %d", alterCount)
}

// TestDebugSpecificFile debugs a specific file's SQL parsing
func TestDebugSpecificFile(t *testing.T) {
	testFiles := []string{
		"/home/yejr/gitee/gt-checksum/fixsql/table.gt_checksum.test1.sql",
		"/home/yejr/gitee/gt-checksum/fixsql/table.gt_checksum.testint.sql",
	}

	filesFound := false
	for _, file := range testFiles {
		t.Logf("\n=== Analyzing %s ===", filepath.Base(file))
		content, err := os.ReadFile(file)
		if err != nil {
			t.Logf("Skipping file (not found): %v", err)
			continue
		}
		filesFound = true

		t.Logf("File size: %d bytes", len(content))

		statements := splitSQLStatements(string(content))
		t.Logf("Total statements parsed: %d", len(statements))

		for i, stmt := range statements {
			stmtType, _ := identifyStatementType(stmt)
			t.Logf("Statement #%d: type=%s, length=%d", i, stmtType, len(stmt))
			if len(stmt) > 0 {
				preview := stmt
				if len(preview) > 100 {
					preview = preview[:100] + "..."
				}
				t.Logf("  Content: %s", preview)
			}
		}
	}

	if !filesFound {
		t.Skip("Test files not found in fixsql directory - skipping test")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestExtractTableName tests table name extraction from SQL statements
func TestExtractTableName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"INSERT INTO users VALUES (1)", "USERS"},
		{"INSERT INTO `users` VALUES (1)", "USERS"},
		{"UPDATE users SET name='test'", "USERS"},
		{"DELETE FROM users WHERE id=1", "USERS"},
		{"INSERT INTO db.users VALUES (1)", "USERS"},
		{"SELECT * FROM users", ""},
		// Test cases for table name followed by column list (no space)
		{"INSERT INTO `gt_checksum`.`articles`(`id`,`title`) VALUES (1,'test')", "ARTICLES"},
		{"INSERT INTO `testbit`(`f1`,`f2`) VALUES (1,2)", "TESTBIT"},
		{"DELETE FROM `gt_checksum`.`articles` WHERE `id` IN (1,2)", "ARTICLES"},
	}

	for _, tt := range tests {
		result := extractTableName(tt.input)
		if result != tt.expected {
			t.Errorf("extractTableName(%q) = %s; expected %s", tt.input, result, tt.expected)
		}
	}
}

// TestExtractSchemaAndObject tests schema and object name extraction from file paths.
func TestExtractSchemaAndObject(t *testing.T) {
	tests := []struct {
		filePath       string
		expectedSchema string
		expectedObject string
	}{
		// Standard table files
		{"table.mydb.orders.sql", "mydb", "orders"},
		{"view.appdb.v_orders.sql", "appdb", "v_orders"},
		{"routine.db1.p_calc.sql", "db1", "p_calc"},
		{"trigger.testdb.trg_audit.sql", "testdb", "trg_audit"},

		// DELETE-pattern files
		{"table.mydb.orders-DELETE-1.sql", "mydb", "orders"},
		{"table.testdb.users-DELETE-5.sql", "testdb", "users"},

		// Non-standard files (no recognized prefix)
		{"manual.sql", "", "manual"},
		{"some_random_file.sql", "", "some_random_file"},
		{"db.table.sql", "db", "table"},

		// Full path (should use basename)
		{"/path/to/fixsql/table.mydb.orders.sql", "mydb", "orders"},
		{"./fixsql/view.appdb.v_orders.sql", "appdb", "v_orders"},

		// Edge cases
		{"table..empty.sql", "", "empty"},
		{"script.sql", "", "script"},
	}

	for _, tt := range tests {
		schema, object := extractSchemaAndObject(tt.filePath)
		if schema != tt.expectedSchema {
			t.Errorf("extractSchemaAndObject(%q) schema = %q; expected %q", tt.filePath, schema, tt.expectedSchema)
		}
		if object != tt.expectedObject {
			t.Errorf("extractSchemaAndObject(%q) object = %q; expected %q", tt.filePath, object, tt.expectedObject)
		}
	}
}

