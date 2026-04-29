package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"database/sql"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"

	_ "github.com/go-sql-driver/mysql"
)

// Config stores configuration information
type Config struct {
	DstDSN       string
	ParallelThds int
	FixFileDir   string
	LogFile      string
	LogBin       bool   // true = keep sql_log_bin ON (default); false = SET sql_log_bin=0 per connection
	ResultFile   string // custom output path for CSV report, empty = use default
}

// FixSQLStatistics stores statistics about fix SQL files
type FixSQLStatistics struct {
	// Basic statistics
	TotalFiles int
	TotalSize  int64

	// Classification statistics
	DeleteFiles  int
	TableFiles   int
	ViewFiles    int
	RoutineFiles int
	TriggerFiles int
	UnknownFiles int

	// Data mode statistics
	TableCount int
	InsertRows int64
	UpdateRows int64
	DeleteRows int64

	// Struct/routine/trigger mode statistics
	DropCount   int
	AlterCount  int
	CreateCount int

	// Binlog estimation
	EstimatedBinlogSize int64
}

// unitExecOutcome captures statement-level execution results for a single execution unit.
type unitExecOutcome struct {
	InsertSuccess int64
	InsertFailure int64
	DeleteSuccess int64
	DeleteFailure int64
	AlterSuccess  int64
	AlterFailure  int64
	CreateSuccess int64
	CreateFailure int64
	DropSuccess   int64
	DropFailure   int64
}

func (o *unitExecOutcome) add(other unitExecOutcome) {
	o.InsertSuccess += other.InsertSuccess
	o.InsertFailure += other.InsertFailure
	o.DeleteSuccess += other.DeleteSuccess
	o.DeleteFailure += other.DeleteFailure
	o.AlterSuccess += other.AlterSuccess
	o.AlterFailure += other.AlterFailure
	o.CreateSuccess += other.CreateSuccess
	o.CreateFailure += other.CreateFailure
	o.DropSuccess += other.DropSuccess
	o.DropFailure += other.DropFailure
}

// FileExecResult contains the execution result of a single SQL file.
type FileExecResult struct {
	FilePath   string
	Schema     string
	ObjectName string
	Stage      string // DELETE, TABLE, VIEW, ROUTINE, TRIGGER, UNKNOWN
	ObjectType string // table, view, procedure, function, trigger, unknown

	InsertSuccess int64
	InsertFailure int64
	DeleteSuccess int64
	DeleteFailure int64
	AlterSuccess  int64
	AlterFailure  int64
	CreateSuccess int64
	CreateFailure int64
	DropSuccess   int64
	DropFailure   int64

	Elapsed     time.Duration
	ErrorReason string // empty = success
}

// mergeFromOutcome aggregates a unitExecOutcome into this file-level result.
func (r *FileExecResult) mergeFromOutcome(o unitExecOutcome) {
	r.InsertSuccess += o.InsertSuccess
	r.InsertFailure += o.InsertFailure
	r.DeleteSuccess += o.DeleteSuccess
	r.DeleteFailure += o.DeleteFailure
	r.AlterSuccess += o.AlterSuccess
	r.AlterFailure += o.AlterFailure
	r.CreateSuccess += o.CreateSuccess
	r.CreateFailure += o.CreateFailure
	r.DropSuccess += o.DropSuccess
	r.DropFailure += o.DropFailure
}

// ExecSummary aggregates execution statistics across all files.
type ExecSummary struct {
	TotalFiles      int
	SuccessFiles    int
	FailureFiles    int
	TotalInsertOk   int64
	TotalInsertFail int64
	TotalDeleteOk   int64
	TotalDeleteFail int64
	TotalAlterOk    int64
	TotalAlterFail  int64
	TotalCreateOk   int64
	TotalCreateFail int64
	TotalDropOk     int64
	TotalDropFail   int64
}

// resultCollector provides thread-safe collection of FileExecResult.
type resultCollector struct {
	mu      sync.Mutex
	results []FileExecResult
}

func (c *resultCollector) append(r FileExecResult) {
	c.mu.Lock()
	c.results = append(c.results, r)
	c.mu.Unlock()
}

func (c *resultCollector) snapshot() []FileExecResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]FileExecResult, len(c.results))
	copy(out, c.results)
	return out
}

// Global variables
var (
	config Config
)

var delimiterDirectivePattern = regexp.MustCompile(`(?i)^\s*DELIMITER\s+(.+?)\s*;?\s*$`)

// Parse config file
func parseConfig(confFile string) error {
	// Read config file content
	content, err := os.ReadFile(confFile)
	if err != nil {
		return fmt.Errorf("Failed to read config file: %v", err)
	}

	// Parse config parameters
	logbinSet := false
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Skip comments and empty lines
		if strings.HasPrefix(line, ";") || line == "" {
			continue
		}

		// Parse parameters
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Set config parameters
		switch key {
		case "dstDSN":
			config.DstDSN = value
		case "parallelThds":
			fmt.Sscanf(value, "%d", &config.ParallelThds)
		case "fixFileDir":
			config.FixFileDir = value
		case "logbin":
			logbinSet = true
			switch strings.ToUpper(value) {
			case "OFF":
				config.LogBin = false
			case "ON":
				config.LogBin = true
			default:
				return fmt.Errorf("invalid value for logbin: %q (must be ON or OFF)", value)
			}
		case "resultFile":
			config.ResultFile = value
		}
	}

	// Set default values
	if config.ParallelThds <= 0 {
		config.ParallelThds = 4
	}
	if config.FixFileDir == "" {
		config.FixFileDir = "./fixsql"
	}
	if !logbinSet {
		config.LogBin = true // default: keep sql_log_bin ON
	}
	config.LogFile = "repairDB.log"

	// Validate config
	if config.DstDSN == "" {
		return fmt.Errorf("Missing dstDSN parameter in config file")
	}

	return nil
}

// formatSize formats file size in human-readable format
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// identifyStatementType identifies SQL statement type and estimates affected rows
func identifyStatementType(stmt string) (stmtType string, affectedRows int64) {
	// Remove leading SQL comments (lines starting with --)
	// This handles cases where comments are bundled with the actual SQL statement
	lines := strings.Split(stmt, "\n")
	var cleanedLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip comment lines and empty lines
		if !strings.HasPrefix(trimmed, "--") && trimmed != "" {
			cleanedLines = append(cleanedLines, line)
		}
	}

	// Rejoin non-comment lines
	cleanedStmt := strings.Join(cleanedLines, "\n")

	// Normalize statement: trim and convert to uppercase for matching
	normalized := strings.TrimSpace(strings.ToUpper(cleanedStmt))
	if normalized == "" {
		return "UNKNOWN", 0
	}

	// Identify statement type
	switch {
	case strings.HasPrefix(normalized, "INSERT"):
		// Count rows by counting "(" after VALUES
		// Look for VALUES keyword first
		valuesIdx := strings.Index(normalized, "VALUES")
		if valuesIdx == -1 {
			return "INSERT", 1
		}
		// Count opening parentheses after VALUES
		afterValues := cleanedStmt[valuesIdx:]
		rows := int64(strings.Count(afterValues, "("))
		if rows == 0 {
			rows = 1
		}
		return "INSERT", rows
	case strings.HasPrefix(normalized, "UPDATE"):
		return "UPDATE", 1
	case strings.HasPrefix(normalized, "DELETE"):
		return "DELETE", 1
	case strings.HasPrefix(normalized, "DROP"):
		return "DROP", 1
	case strings.HasPrefix(normalized, "ALTER"):
		return "ALTER", 1
	case strings.HasPrefix(normalized, "CREATE"):
		return "CREATE", 1
	default:
		return "UNKNOWN", 0
	}
}

// collectFixSQLStatistics collects statistics about fix SQL files
func collectFixSQLStatistics(fixFileDir string) (*FixSQLStatistics, error) {
	stats := &FixSQLStatistics{}

	// Traverse fixFileDir directory to find all .sql files
	var sqlFiles []string
	err := filepath.Walk(fixFileDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".sql") {
			sqlFiles = append(sqlFiles, path)
			// Accumulate file size
			stats.TotalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to traverse directory: %v", err)
	}

	// Set total file count
	stats.TotalFiles = len(sqlFiles)

	// Track unique table names
	tableSet := make(map[string]bool)

	// Classify files by type and parse SQL statements
	for _, file := range sqlFiles {
		stage := detectObjectStage(file)
		switch stage {
		case "DELETE":
			stats.DeleteFiles++
		case "TABLE":
			stats.TableFiles++
		case "VIEW":
			stats.ViewFiles++
		case "ROUTINE":
			stats.RoutineFiles++
		case "TRIGGER":
			stats.TriggerFiles++
		default:
			stats.UnknownFiles++
		}

		// Parse SQL statements for data mode files (table.* files)
		if stage == "TABLE" || stage == "DELETE" {
			content, err := os.ReadFile(file)
			if err != nil {
				continue // Skip files that cannot be read
			}

			// Split SQL statements
			statements := splitSQLStatements(string(content))
			for _, stmt := range statements {
				stmtType, rows := identifyStatementType(stmt)
				switch stmtType {
				case "INSERT":
					stats.InsertRows += rows
				case "UPDATE":
					stats.UpdateRows += rows
				case "DELETE":
					stats.DeleteRows += rows
				case "DROP":
					stats.DropCount++
				case "ALTER":
					stats.AlterCount++
				case "CREATE":
					stats.CreateCount++
				}

				// Extract table name for table count
				if stmtType == "INSERT" || stmtType == "UPDATE" || stmtType == "DELETE" {
					tableName := extractTableName(stmt)
					if tableName != "" {
						tableSet[tableName] = true
					}
				}
			}
		}

		// Parse SQL statements for struct/routine/trigger mode files
		if stage == "VIEW" || stage == "ROUTINE" || stage == "TRIGGER" {
			content, err := os.ReadFile(file)
			if err != nil {
				continue
			}

			statements := splitSQLStatements(string(content))
			for _, stmt := range statements {
				stmtType, _ := identifyStatementType(stmt)
				switch stmtType {
				case "DROP":
					stats.DropCount++
				case "ALTER":
					stats.AlterCount++
				case "CREATE":
					stats.CreateCount++
				}
			}
		}
	}

	// Set table count
	stats.TableCount = len(tableSet)

	// Estimate binlog size
	stats.EstimatedBinlogSize = estimateBinlogSize(stats.TotalSize, stats.DeleteRows, stats.InsertRows)

	return stats, nil
}

// extractTableName extracts table name from SQL statement (simple implementation)
func extractTableName(stmt string) string {
	// Normalize statement for parsing
	normalized := strings.TrimSpace(stmt)
	upper := strings.ToUpper(normalized)

	// Extract table name from INSERT/UPDATE/DELETE statements
	var tableName string
	if strings.HasPrefix(upper, "INSERT") {
		// INSERT INTO table_name ...
		parts := strings.Fields(normalized)
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "INTO" {
			tableName = parts[2]
		}
	} else if strings.HasPrefix(upper, "UPDATE") {
		// UPDATE table_name SET ...
		parts := strings.Fields(normalized)
		if len(parts) >= 2 {
			tableName = parts[1]
		}
	} else if strings.HasPrefix(upper, "DELETE") {
		// DELETE FROM table_name ...
		parts := strings.Fields(normalized)
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "FROM" {
			tableName = parts[2]
		}
	}

	// Remove backticks
	tableName = strings.Trim(tableName, "`")

	// Remove parentheses and everything after (for cases like `table`(`col1`,`col2`))
	if idx := strings.Index(tableName, "("); idx != -1 {
		tableName = tableName[:idx]
		// Remove trailing backticks again
		tableName = strings.Trim(tableName, "`")
	}

	// Remove schema prefix (db.table -> table)
	if idx := strings.Index(tableName, "."); idx != -1 {
		tableName = tableName[idx+1:]
		// Remove backticks again after schema removal
		tableName = strings.Trim(tableName, "`")
	}

	// Convert to uppercase for consistency
	return strings.ToUpper(tableName)
}

// estimateBinlogSize estimates binlog size based on SQL file size and operations
func estimateBinlogSize(totalFileSize int64, deleteRows, insertRows int64) int64 {
	// Base coefficient: binlog is approximately 1.3x of SQL file size
	coefficient := 1.3

	// If DELETE operations are more than INSERT, reduce coefficient
	// (binlog only records primary keys for DELETE)
	if deleteRows > insertRows {
		coefficient = 1.1
	}

	return int64(float64(totalFileSize) * coefficient)
}

// printStatisticsReport prints statistics report with table format and colors
func printStatisticsReport(stats *FixSQLStatistics) {
	fmt.Println()
	fmt.Println(color.CyanString("========== repairDB 预执行报告 =========="))

	table := uitable.New()
	table.MaxColWidth = 80
	table.Wrap = true

	// Add header
	table.AddRow(color.YellowString("统计项"), color.YellowString("数值"))
	table.AddRow("---", "---")

	// Basic statistics
	table.AddRow("修复 SQL 文件总数", color.GreenString("%d", stats.TotalFiles))
	table.AddRow("文件总大小", color.GreenString("%s", formatSize(stats.TotalSize)))

	// File classification
	if stats.DeleteFiles > 0 {
		table.AddRow("DELETE 文件数", color.RedString("%d", stats.DeleteFiles))
	}
	if stats.TableFiles > 0 {
		table.AddRow("TABLE 文件数", color.GreenString("%d", stats.TableFiles))
	}
	if stats.ViewFiles > 0 {
		table.AddRow("VIEW 文件数", color.BlueString("%d", stats.ViewFiles))
	}
	if stats.RoutineFiles > 0 {
		table.AddRow("ROUTINE 文件数", color.BlueString("%d", stats.RoutineFiles))
	}
	if stats.TriggerFiles > 0 {
		table.AddRow("TRIGGER 文件数", color.BlueString("%d", stats.TriggerFiles))
	}
	if stats.UnknownFiles > 0 {
		table.AddRow("UNKNOWN 文件数", color.MagentaString("%d", stats.UnknownFiles))
	}

	table.AddRow("---", "---")

	// Data mode statistics
	if stats.TableCount > 0 {
		table.AddRow("涉及表数量", color.CyanString("%d", stats.TableCount))
	}
	if stats.InsertRows > 0 {
		table.AddRow("INSERT 行数", color.GreenString("%s", formatNumber(stats.InsertRows)))
	}
	if stats.UpdateRows > 0 {
		table.AddRow("UPDATE 行数", color.YellowString("%s", formatNumber(stats.UpdateRows)))
	}
	if stats.DeleteRows > 0 {
		table.AddRow("DELETE 行数", color.RedString("%s", formatNumber(stats.DeleteRows)))
	}

	// Struct/routine/trigger mode statistics
	if stats.DropCount > 0 {
		table.AddRow("DROP 对象数", color.RedString("%d", stats.DropCount))
	}
	if stats.AlterCount > 0 {
		table.AddRow("ALTER 对象数", color.YellowString("%d", stats.AlterCount))
	}
	if stats.CreateCount > 0 {
		table.AddRow("CREATE 对象数", color.GreenString("%d", stats.CreateCount))
	}

	// Binlog estimation
	if stats.EstimatedBinlogSize > 0 {
		table.AddRow("预估 binlog 大小", color.MagentaString("%s (预估)", formatSize(stats.EstimatedBinlogSize)))
	}

	fmt.Println(table)
	fmt.Println(color.CyanString("=========================================="))
	fmt.Println()
}

// formatNumber formats number with thousand separators
func formatNumber(n int64) string {
	str := strconv.FormatInt(n, 10)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// promptUserConfirmation prompts user for confirmation to continue execution
func promptUserConfirmation() (bool, error) {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print(color.YellowString("是否继续执行修复操作？(yes/no): "))
		input, err := reader.ReadString('\n')
		if err != nil {
			return false, fmt.Errorf("读取用户输入失败: %v", err)
		}

		// Trim whitespace and convert to lowercase
		input = strings.TrimSpace(strings.ToLower(input))

		switch input {
		case "yes", "y":
			return true, nil
		case "no", "n":
			return false, nil
		default:
			fmt.Println(color.RedString("无效输入，请输入 yes 或 no"))
		}
	}
}

// Extract connection parameters from MySQL DSN
func parseDSN(dsn string) string {
	// Format: mysql|user:password@tcp(host:port)/db?params
	parts := strings.Split(dsn, "|")
	if len(parts) != 2 {
		return dsn
	}
	return parts[1]
}

// extractSchemaAndObject parses schema and object name from a repair SQL file path.
// Examples:
//
//	table.db1.orders.sql           → schema=db1, object=orders
//	table.db1.orders-DELETE-1.sql  → schema=db1, object=orders
//	view.appdb.v_orders.sql        → schema=appdb, object=v_orders
//	manual.sql                     → schema="", object=manual
func extractSchemaAndObject(filePath string) (schema, object string) {
	base := filepath.Base(filePath)
	name := strings.TrimSuffix(base, ".sql")

	// Strip -DELETE-N suffix for DELETE-pattern files
	if deleteFileNameRegex.MatchString(base) {
		dashIdx := strings.LastIndex(name, "-DELETE-")
		if dashIdx != -1 {
			name = name[:dashIdx]
		}
	}

	parts := strings.SplitN(name, ".", 3)
	if len(parts) == 3 && (parts[0] == "table" || parts[0] == "view" || parts[0] == "routine" || parts[0] == "trigger") {
		return parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", name
}

// Execute SQL file
func executeSQLFile(db *sql.DB, sqlFile string) (FileExecResult, error) {
	startTime := time.Now()
	schema, obj := extractSchemaAndObject(sqlFile)
	stage := detectObjectStage(sqlFile)

	result := FileExecResult{
		FilePath:   sqlFile,
		Schema:     schema,
		ObjectName: obj,
		Stage:      stage,
	}

	// Read SQL file content
	content, err := os.ReadFile(sqlFile)
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = fmt.Sprintf("Failed to read SQL file: %v", err)
		return result, err
	}

	// Detect object type from stage and SQL content
	result.ObjectType = detectObjectTypeFromContent(stage, string(content))

	statements := splitSQLStatements(string(content))
	units, err := buildSQLExecutionUnits(statements)
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = err.Error()
		return result, err
	}

	// Keep one connection for the whole file so session-level SET statements
	// (for example UNIQUE_CHECKS/FOREIGN_KEY_CHECKS) apply to all subsequent SQL.
	conn, err := db.Conn(context.Background())
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = fmt.Sprintf("Failed to get database connection: %v", err)
		return result, err
	}
	defer conn.Close()

	// Always set sql_log_bin explicitly: 1 (default) or 0 (when logbin=OFF).
	// Setting logbin=OFF requires SUPER or BINLOG ADMIN privilege on the target instance.
	logBinVal := "1"
	if !config.LogBin {
		logBinVal = "0"
	}
	if _, err := conn.ExecContext(context.Background(), "SET sql_log_bin = "+logBinVal); err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = fmt.Sprintf("Failed to SET sql_log_bin=%s: %v", logBinVal, err)
		return result, err
	}

	for _, unit := range units {
		outcome, err := executeUnitWithDeadlockRetry(conn, sqlFile, unit)
		result.mergeFromOutcome(outcome)
		if err != nil {
			result.Elapsed = time.Since(startTime)
			result.ErrorReason = err.Error()
			return result, err
		}
	}

	result.Elapsed = time.Since(startTime)
	return result, nil
}

type sqlExecutionUnit struct {
	index         int
	transactional bool
	statements    []string
}

func buildSQLExecutionUnits(statements []string) ([]sqlExecutionUnit, error) {
	var units []sqlExecutionUnit
	unitIndex := 1
	i := 0
	for i < len(statements) {
		stmt := strings.TrimSpace(statements[i])
		if stmt == "" {
			i++
			continue
		}

		switch {
		case isBeginStatement(stmt):
			var txStatements []string
			foundEnd := false
			for j := i + 1; j < len(statements); j++ {
				nextStmt := strings.TrimSpace(statements[j])
				if nextStmt == "" {
					continue
				}
				if isCommitOrRollbackStatement(nextStmt) {
					units = append(units, sqlExecutionUnit{
						index:         unitIndex,
						transactional: true,
						statements:    txStatements,
					})
					unitIndex++
					i = j + 1
					foundEnd = true
					break
				}
				txStatements = append(txStatements, nextStmt)
			}
			if !foundEnd {
				return nil, fmt.Errorf("SQL file contains BEGIN without matching COMMIT/ROLLBACK")
			}
		case isCommitOrRollbackStatement(stmt):
			// Ignore standalone COMMIT/ROLLBACK to keep parser robust.
			i++
		default:
			units = append(units, sqlExecutionUnit{
				index:         unitIndex,
				transactional: false,
				statements:    []string{stmt},
			})
			unitIndex++
			i++
		}
	}

	return units, nil
}

func executeUnitWithDeadlockRetry(conn *sql.Conn, sqlFile string, unit sqlExecutionUnit) (unitExecOutcome, error) {
	var lastErr error
	var lastOutcome unitExecOutcome
	for retryRound := 0; retryRound <= maxDeadlockRetries; retryRound++ {
		if retryRound > 0 {
			backoff := time.Duration(1<<uint(retryRound)) * time.Second
			log.Printf("Deadlock retry in SQL file %s unit #%d: round=%d wait=%v\n", sqlFile, unit.index, retryRound, backoff)
			time.Sleep(backoff)
		}

		outcome, err := executeUnit(conn, unit)
		if err == nil {
			return outcome, nil
		}
		lastErr = err
		lastOutcome = outcome

		if !isDeadlockError(err) {
			return outcome, err
		}

		log.Printf("DEADLOCK detected in SQL file %s unit #%d (retry round %d): %v\n", sqlFile, unit.index, retryRound, err)
	}
	return lastOutcome, fmt.Errorf("deadlock unresolved after %d retries in SQL file %s unit #%d: %v", maxDeadlockRetries, sqlFile, unit.index, lastErr)
}

func executeUnit(conn *sql.Conn, unit sqlExecutionUnit) (unitExecOutcome, error) {
	var outcome unitExecOutcome

	if unit.transactional {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return outcome, fmt.Errorf("Failed to start transaction: %v", err)
		}

		var txOutcome unitExecOutcome
		for _, stmt := range unit.statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			stmt = normalizeMySQLDateFormatLiteralInSQLForExec(stmt)
			stmtType, _ := identifyStatementType(stmt)
			result, execErr := tx.ExecContext(context.Background(), stmt)
			if execErr != nil {
				recordStmtFailure(&txOutcome, stmtType)
				_ = tx.Rollback()
				return outcome, fmt.Errorf("Failed to execute SQL statement in transaction (rolled back): %v", execErr)
			}
			recordStmtSuccess(&txOutcome, stmtType, result)
		}

		if err := tx.Commit(); err != nil {
			return outcome, fmt.Errorf("Failed to commit transaction: %v", err)
		}
		// Only merge tx outcome after successful commit; rollback discards all changes.
		outcome.add(txOutcome)
		return outcome, nil
	}

	for _, stmt := range unit.statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmt = normalizeMySQLDateFormatLiteralInSQLForExec(stmt)
		stmtType, _ := identifyStatementType(stmt)
		result, execErr := conn.ExecContext(context.Background(), stmt)
		if execErr != nil {
			recordStmtFailure(&outcome, stmtType)
			return outcome, fmt.Errorf("Failed to execute SQL statement: %v", execErr)
		}
		recordStmtSuccess(&outcome, stmtType, result)
	}
	return outcome, nil
}

func recordStmtSuccess(o *unitExecOutcome, stmtType string, result sql.Result) {
	rows, err := result.RowsAffected()
	if err != nil {
		// Some statement types (e.g., DDL) may not support RowsAffected.
		// For DML statements, use 1 as fallback; for DDL, the switch handles it.
		rows = 1
	}
	switch stmtType {
	case "INSERT":
		o.InsertSuccess += rows
	case "DELETE":
		o.DeleteSuccess += rows
	case "ALTER":
		o.AlterSuccess++
	case "CREATE":
		o.CreateSuccess++
	case "DROP":
		o.DropSuccess++
	}
}

func recordStmtFailure(o *unitExecOutcome, stmtType string) {
	switch stmtType {
	case "INSERT":
		o.InsertFailure++
	case "DELETE":
		o.DeleteFailure++
	case "ALTER":
		o.AlterFailure++
	case "CREATE":
		o.CreateFailure++
	case "DROP":
		o.DropFailure++
	}
}

func isBeginStatement(stmt string) bool {
	s := strings.ToUpper(strings.TrimSpace(stmt))
	return s == "BEGIN" || s == "START TRANSACTION"
}

func isCommitOrRollbackStatement(stmt string) bool {
	s := strings.ToUpper(strings.TrimSpace(stmt))
	return s == "COMMIT" || s == "ROLLBACK"
}

func isMySQLDashCommentStart(content string, idx int) bool {
	if idx+1 >= len(content) || content[idx] != '-' || content[idx+1] != '-' {
		return false
	}
	// MySQL treats "--" as a comment starter only when the second dash is
	// followed by at least one whitespace/control character.
	if idx+2 >= len(content) {
		return false
	}
	switch content[idx+2] {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

func trimLeadingSQLWhitespaceAndComments(content string) string {
	i := 0
	for i < len(content) {
		switch {
		case content[i] == ' ' || content[i] == '\t' || content[i] == '\n' || content[i] == '\r' || content[i] == '\f' || content[i] == '\v':
			i++
		case isMySQLDashCommentStart(content, i):
			i += 2
			for i < len(content) && content[i] != '\n' {
				i++
			}
		case content[i] == '#':
			i++
			for i < len(content) && content[i] != '\n' {
				i++
			}
		case i+1 < len(content) && content[i] == '/' && content[i+1] == '*':
			i += 2
			for i+1 < len(content) && !(content[i] == '*' && content[i+1] == '/') {
				i++
			}
			if i+1 < len(content) {
				i += 2
			}
		default:
			return content[i:]
		}
	}
	return ""
}

// parseDelimiterDirective parses lines like:
// DELIMITER $$
// DELIMITER $$;
func parseDelimiterDirective(line string) (string, bool) {
	matches := delimiterDirectivePattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 2 {
		return "", false
	}
	raw := strings.TrimSpace(matches[1])
	if raw == "" {
		return "", false
	}
	// Special case: DELIMITER ; means switch back to default semicolon.
	if raw == ";" {
		return ";", true
	}

	delimiter := raw
	// Compatibility: allow DELIMITER $$; style with trailing statement terminator.
	delimiter = strings.TrimSuffix(delimiter, ";")
	delimiter = strings.TrimSpace(delimiter)
	if delimiter == "" {
		return "", false
	}
	return delimiter, true
}

// extractStatementsByDelimiter extracts completed SQL statements from content
// with the provided delimiter, skipping delimiters inside literals/comments.
func extractStatementsByDelimiter(content, delimiter string) ([]string, string) {
	if delimiter == "" {
		delimiter = ";"
	}

	var statements []string
	start := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	escaped := false

	for i := 0; i < len(content); {
		c := content[i]
		var next byte
		if i+1 < len(content) {
			next = content[i+1]
		}

		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			i++
			continue
		}
		if inBlockComment {
			if c == '*' && next == '/' {
				inBlockComment = false
				i += 2
				continue
			}
			i++
			continue
		}

		if escaped {
			escaped = false
			i++
			continue
		}
		if inSingleQuote {
			if c == '\\' {
				escaped = true
				i++
				continue
			}
			if c == '\'' {
				inSingleQuote = false
			}
			i++
			continue
		}
		if inDoubleQuote {
			if c == '\\' {
				escaped = true
				i++
				continue
			}
			if c == '"' {
				inDoubleQuote = false
			}
			i++
			continue
		}
		if inBacktick {
			if c == '`' {
				inBacktick = false
			}
			i++
			continue
		}

		switch {
		case isMySQLDashCommentStart(content, i):
			inLineComment = true
			i += 2
			continue
		case c == '#':
			inLineComment = true
			i++
			continue
		case c == '/' && next == '*':
			inBlockComment = true
			i += 2
			continue
		case c == '\'':
			inSingleQuote = true
			i++
			continue
		case c == '"':
			inDoubleQuote = true
			i++
			continue
		case c == '`':
			inBacktick = true
			i++
			continue
		}

		if strings.HasPrefix(content[i:], delimiter) {
			stmt := strings.TrimSpace(content[start:i])
			if stmt != "" {
				statements = append(statements, stmt)
			}
			i += len(delimiter)
			// Compatibility: some generated files use "$$;" instead of "$$".
			if i < len(content) && content[i] == ';' {
				i++
			}
			start = i
			continue
		}

		i++
	}

	return statements, trimLeadingSQLWhitespaceAndComments(content[start:])
}

// splitSQLStatements splits SQL statements and supports MySQL DELIMITER directive.
func splitSQLStatements(content string) []string {
	var statements []string
	delimiter := ";"
	var currentStmt strings.Builder
	lines := strings.Split(content, "\n")

	for i, line := range lines {
		if newDelimiter, ok := parseDelimiterDirective(line); ok {
			ready, rest := extractStatementsByDelimiter(currentStmt.String(), delimiter)
			statements = append(statements, ready...)
			currentStmt.Reset()
			currentStmt.WriteString(rest)
			delimiter = newDelimiter
			continue
		}

		currentStmt.WriteString(line)
		if i < len(lines)-1 {
			currentStmt.WriteString("\n")
		}

		ready, rest := extractStatementsByDelimiter(currentStmt.String(), delimiter)
		if len(ready) > 0 {
			statements = append(statements, ready...)
			currentStmt.Reset()
			currentStmt.WriteString(rest)
		}
	}

	lastStmt := strings.TrimSpace(trimLeadingSQLWhitespaceAndComments(currentStmt.String()))
	if lastStmt != "" {
		statements = append(statements, lastStmt)
	}

	return statements
}

var mysqlDateFormatLiteralForExecPattern = regexp.MustCompile(`(?i)DATE_FORMAT\(\s*'((?:\\'|[^'])*)'\s*,\s*'%Y-%m-%d %H:%i:%s'\s*\)`)
var mysqlDateTimePrefixForExecPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d{1,6})?`)

func normalizeMySQLDateTimeLiteralForExec(value string) string {
	s := strings.TrimSpace(value)
	if s == "" {
		return s
	}
	matches := mysqlDateTimePrefixForExecPattern.FindStringSubmatch(s)
	if len(matches) >= 3 {
		frac := ""
		if len(matches) >= 4 {
			frac = matches[3]
		}
		return matches[1] + " " + matches[2] + frac
	}
	if len(s) >= 19 && s[10] == 'T' {
		return s[:10] + " " + s[11:]
	}
	return s
}

func normalizeMySQLDateFormatLiteralInSQLForExec(sql string) string {
	if !strings.Contains(strings.ToUpper(sql), "DATE_FORMAT(") {
		return sql
	}
	return mysqlDateFormatLiteralForExecPattern.ReplaceAllStringFunc(sql, func(segment string) string {
		matches := mysqlDateFormatLiteralForExecPattern.FindStringSubmatch(segment)
		if len(matches) < 2 {
			return segment
		}
		raw := strings.ReplaceAll(matches[1], `\'`, `'`)
		normalized := normalizeMySQLDateTimeLiteralForExec(raw)
		escaped := strings.ReplaceAll(normalized, `'`, `\'`)
		return fmt.Sprintf("'%s'", escaped)
	})
}

// Maximum number of retry attempts for deadlocked SQL files
const maxDeadlockRetries = 3

// dbConnMaxLifetime caps how long a pooled connection may be reused.
// Set conservatively below common MySQL/RDS wait_timeout values (often 600s–28800s)
// so the driver recycles stale connections before the server closes them.
const dbConnMaxLifetime = 10 * time.Minute

// isDeadlockError checks if an error is a MySQL deadlock error (Error 1213)
func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Error 1213") ||
		strings.Contains(err.Error(), "Deadlock found when trying to get lock")
}

var deleteFileNameRegex = regexp.MustCompile(`^.+-DELETE-.+\.sql$`)
var numberedSQLFileRegex = regexp.MustCompile(`^(.+?-)(\d+)(\.sql)$`)

// stageOrder defines the fixed execution order for object-type stages.
// buildExecutionStages always follows this order; do not range over a map to determine phase order.
var stageOrder = []string{"DELETE", "TABLE", "VIEW", "ROUTINE", "TRIGGER", "UNKNOWN"}

// executionStage describes a single phase of the repairDB execution pipeline.
type executionStage struct {
	Name    string
	Files   []string
	Shuffle bool // true: shuffle before execution (DML hotspot reduction); false: sort for audit readability
}

// classifiedFiles holds SQL file paths grouped by their execution stage.
type classifiedFiles struct {
	Delete  []string
	Table   []string
	View    []string
	Routine []string
	Trigger []string
	Unknown []string
}

// detectObjectStage returns the execution stage name for a single SQL file path.
// Detection is performed on filepath.Base(path) to support both absolute and relative paths.
// Priority: DELETE (-DELETE- pattern) > type prefix (table./view./routine./trigger.) > UNKNOWN.
func detectObjectStage(path string) string {
	base := filepath.Base(path)
	if deleteFileNameRegex.MatchString(base) {
		return "DELETE"
	}
	switch {
	case strings.HasPrefix(base, "table."):
		return "TABLE"
	case strings.HasPrefix(base, "view."):
		return "VIEW"
	case strings.HasPrefix(base, "routine."):
		return "ROUTINE"
	case strings.HasPrefix(base, "trigger."):
		return "TRIGGER"
	default:
		return "UNKNOWN"
	}
}

// detectObjectTypeFromContent returns the human-readable object type for a SQL file.
// For ROUTINE stage files it inspects the SQL content to distinguish procedure vs function.
func detectObjectTypeFromContent(stage, content string) string {
	switch stage {
	case "TABLE", "DELETE":
		return "table"
	case "VIEW":
		return "view"
	case "TRIGGER":
		return "trigger"
	case "ROUTINE":
		upper := strings.ToUpper(content)
		if strings.Contains(upper, "CREATE FUNCTION") ||
			(strings.Contains(upper, "CREATE DEFINER") && strings.Contains(upper, " FUNCTION ")) {
			return "function"
		}
		return "procedure"
	default:
		return "unknown"
	}
}

// classifySQLFiles distributes SQL file paths into their respective execution stages.
func classifySQLFiles(files []string) classifiedFiles {
	var cf classifiedFiles
	for _, f := range files {
		switch detectObjectStage(f) {
		case "DELETE":
			cf.Delete = append(cf.Delete, f)
		case "TABLE":
			cf.Table = append(cf.Table, f)
		case "VIEW":
			cf.View = append(cf.View, f)
		case "ROUTINE":
			cf.Routine = append(cf.Routine, f)
		case "TRIGGER":
			cf.Trigger = append(cf.Trigger, f)
		default:
			cf.Unknown = append(cf.Unknown, f)
		}
	}
	return cf
}

// buildExecutionStages constructs the ordered stage table from classified files.
// Stages with no files are omitted. Order is authoritative from stageOrder.
func buildExecutionStages(cf classifiedFiles) []executionStage {
	filesByStage := map[string][]string{
		"DELETE":  cf.Delete,
		"TABLE":   cf.Table,
		"VIEW":    cf.View,
		"ROUTINE": cf.Routine,
		"TRIGGER": cf.Trigger,
		"UNKNOWN": cf.Unknown,
	}
	shuffleByStage := map[string]bool{
		"TABLE": true,
	}
	var stages []executionStage
	for _, name := range stageOrder {
		files := filesByStage[name]
		if len(files) > 0 {
			stages = append(stages, executionStage{
				Name:    name,
				Files:   files,
				Shuffle: shuffleByStage[name],
			})
		}
	}
	return stages
}

// prepareStageFiles returns a copy of the stage's files in their execution order.
// TABLE stages are shuffled to reduce lock contention hotspots on high-concurrency DML.
// All other stages are sorted for audit readability and deterministic replay.
// The original stage.Files slice is never modified.
func prepareStageFiles(stage executionStage) []string {
	out := make([]string, len(stage.Files))
	copy(out, stage.Files)
	if stage.Shuffle {
		shuffleSQLFiles(out)
	} else {
		sort.Strings(out)
	}
	return out
}

func uniqueFiles(files []string) []string {
	seen := make(map[string]struct{}, len(files))
	result := make([]string, 0, len(files))
	for _, f := range files {
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		result = append(result, f)
	}
	return result
}

func shuffleSQLFiles(files []string) {
	if len(files) <= 1 {
		return
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
}

type rangeSegment struct {
	start int
	end   int
}

func splitConsecutiveRanges(numbers []int) []rangeSegment {
	if len(numbers) == 0 {
		return nil
	}
	sort.Ints(numbers)
	uniq := make([]int, 0, len(numbers))
	prev := numbers[0] - 1
	for _, n := range numbers {
		if n != prev {
			uniq = append(uniq, n)
		}
		prev = n
	}
	if len(uniq) == 0 {
		return nil
	}
	var segments []rangeSegment
	start := uniq[0]
	last := uniq[0]
	for i := 1; i < len(uniq); i++ {
		if uniq[i] == last+1 {
			last = uniq[i]
			continue
		}
		segments = append(segments, rangeSegment{start: start, end: last})
		start = uniq[i]
		last = uniq[i]
	}
	segments = append(segments, rangeSegment{start: start, end: last})
	return segments
}

func normalizePlanPath(file, fixFileDir string) string {
	rel, err := filepath.Rel(fixFileDir, file)
	if err == nil && !strings.HasPrefix(rel, "..") && rel != "." {
		baseDir := filepath.Base(filepath.Clean(fixFileDir))
		joined := filepath.ToSlash(filepath.Join(baseDir, rel))
		return strings.TrimPrefix(joined, "./")
	}
	return strings.TrimPrefix(filepath.ToSlash(file), "./")
}

func buildCompressedPlanEntries(files []string, fixFileDir string) []string {
	type groupKey struct {
		dir    string
		prefix string
		suffix string
	}
	type groupValue struct {
		order   int
		numbers []int
	}
	type plainEntry struct {
		order int
		path  string
	}
	type mergedEntry struct {
		order int
		path  string
		start int
	}

	grouped := make(map[groupKey]*groupValue)
	var groupOrder []groupKey
	var plain []plainEntry
	orderCounter := 0

	for _, file := range files {
		normalized := normalizePlanPath(file, fixFileDir)
		base := filepath.Base(normalized)
		dir := filepath.ToSlash(filepath.Dir(normalized))
		if dir == "." {
			dir = ""
		}
		matches := numberedSQLFileRegex.FindStringSubmatch(base)
		if len(matches) != 4 {
			plain = append(plain, plainEntry{order: orderCounter, path: normalized})
			orderCounter++
			continue
		}
		num, err := strconv.Atoi(matches[2])
		if err != nil {
			plain = append(plain, plainEntry{order: orderCounter, path: normalized})
			orderCounter++
			continue
		}
		key := groupKey{dir: dir, prefix: matches[1], suffix: matches[3]}
		if _, ok := grouped[key]; !ok {
			grouped[key] = &groupValue{order: orderCounter}
			groupOrder = append(groupOrder, key)
			orderCounter++
		}
		grouped[key].numbers = append(grouped[key].numbers, num)
	}

	var merged []mergedEntry
	for _, key := range groupOrder {
		segments := splitConsecutiveRanges(grouped[key].numbers)
		for _, seg := range segments {
			name := fmt.Sprintf("%s(%d-%d)%s", key.prefix, seg.start, seg.end, key.suffix)
			if key.dir != "" {
				name = key.dir + "/" + name
			}
			merged = append(merged, mergedEntry{
				order: grouped[key].order,
				path:  name,
				start: seg.start,
			})
		}
	}

	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].order != merged[j].order {
			return merged[i].order < merged[j].order
		}
		if merged[i].start != merged[j].start {
			return merged[i].start < merged[j].start
		}
		return merged[i].path < merged[j].path
	})
	sort.SliceStable(plain, func(i, j int) bool {
		if plain[i].order != plain[j].order {
			return plain[i].order < plain[j].order
		}
		return plain[i].path < plain[j].path
	})

	type finalEntry struct {
		order int
		path  string
	}
	var final []finalEntry
	for _, item := range merged {
		final = append(final, finalEntry{order: item.order, path: item.path})
	}
	for _, item := range plain {
		final = append(final, finalEntry{order: item.order, path: item.path})
	}
	sort.SliceStable(final, func(i, j int) bool {
		return final[i].order < final[j].order
	})

	result := make([]string, 0, len(final))
	for _, item := range final {
		result = append(result, item.path)
	}
	return result
}

func logExecutionPlan(stageName string, files []string, fixFileDir string) {
	if len(files) == 0 {
		return
	}
	log.Printf("[%s] planned execution order (%d files):", stageName, len(files))
	entries := buildCompressedPlanEntries(files, fixFileDir)
	for idx, file := range entries {
		log.Printf("[%s] #%d %s", stageName, idx+1, file)
	}
}

// openExecutionDB opens and validates a MySQL connection pool for one execution stage.
// Each stage opens its own pool and closes it upon completion, so session-level variables
// set inside one stage's SQL files cannot leak into subsequent stages via pooled connections.
// The pool parameters are tuned to match config.ParallelThds concurrency and to
// recycle connections before server-side idle timeouts (dbConnMaxLifetime).
func openExecutionDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %v", err)
	}
	db.SetMaxOpenConns(config.ParallelThds)
	db.SetMaxIdleConns(config.ParallelThds)
	db.SetConnMaxLifetime(dbConnMaxLifetime)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("database ping failed: %v", err)
	}
	return db, nil
}

// parallelExecuteSQLFiles executes files concurrently using the provided connection pool.
// Concurrency is bounded by config.ParallelThds. All goroutines run to completion before
// returning (wait-all-then-report); a failure in one file does not cancel others in the
// same stage, but the returned error prevents subsequent stages from starting.
func parallelExecuteSQLFiles(db *sql.DB, files []string, stageName string) ([]FileExecResult, error) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, config.ParallelThds)
	errCh := make(chan error, len(files))
	var executionSeq uint64
	collector := &resultCollector{}

	for _, sqlFile := range files {
		file := sqlFile
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			startTime := time.Now()
			seq := atomic.AddUint64(&executionSeq, 1)
			log.Printf("[%s] execution sequence #%d: %s\n", stageName, seq, file)

			result, err := executeSQLFile(db, file)
			if result.FilePath != "" {
				collector.append(result)
			}
			if err != nil {
				errCh <- fmt.Errorf("Failed to execute SQL file %s: %v", file, err)
				log.Printf("Failed to execute SQL file %s: %v\n", file, err)
				return
			}

			elapsed := time.Since(startTime)
			log.Printf("Successfully executed SQL file %s, time taken: %v\n", file, elapsed)
		}()
	}

	wg.Wait()
	close(errCh)

	var firstErr error
	errCount := 0
	for err := range errCh {
		if err == nil {
			continue
		}
		errCount++
		if firstErr == nil {
			firstErr = err
		}
	}

	results := collector.snapshot()
	// Sort results: by stage order, then filepath for deterministic output
	sort.SliceStable(results, func(i, j int) bool {
		si, sj := stageIndex(results[i].Stage), stageIndex(results[j].Stage)
		if si != sj {
			return si < sj
		}
		return results[i].FilePath < results[j].FilePath
	})

	if errCount > 0 {
		return results, fmt.Errorf("%s failed: %d file(s) execution error, first error: %v", stageName, errCount, firstErr)
	}
	return results, nil
}

func stageIndex(stage string) int {
	for i, s := range stageOrder {
		if s == stage {
			return i
		}
	}
	return len(stageOrder)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "repairDB execution failed:", err)
		os.Exit(1)
	}
}

// run contains all repairDB logic and returns an error on failure.
// Using a dedicated run() function ensures deferred cleanup (logFile.Close) executes
// correctly on both success and error paths, since os.Exit bypasses deferred calls.
// Stage-level db.Close() is called explicitly (not via defer) inside the stage loop.
func run() error {
	// Parse command line arguments
	confFile := flag.String("conf", "gc.conf", "Config file path")
	force := flag.Bool("f", false, "Force execution without confirmation")
	forceLong := flag.Bool("force", false, "Force execution without confirmation")
	dryRun := flag.Bool("dry-run", false, "Dry run mode: show statistics only")
	resultFile := flag.String("result-file", "", "Custom output path for CSV report (default: result/repairDB-result-<timestamp>.csv)")
	flag.Parse()

	forceMode := *force || *forceLong

	// Check if command line argument is provided for fixFileDir
	var specifiedFixFileDir string
	if len(flag.Args()) > 0 {
		specifiedFixFileDir = flag.Args()[0]
	}

	// If command line argument is provided, use it directly
	if specifiedFixFileDir != "" {
		// Parse config file to get DstDSN and other parameters
		if err := parseConfig(*confFile); err != nil {
			return fmt.Errorf("failed to parse config file: %v", err)
		}
		// Override fixFileDir with command line argument
		config.FixFileDir = specifiedFixFileDir
	} else {
		// Parse config file
		if err := parseConfig(*confFile); err != nil {
			return fmt.Errorf("failed to parse config file: %v", err)
		}
		// Check if fixFileDir is set in config file
		if config.FixFileDir == "" {
			return fmt.Errorf("no fixFileDir specified in command line or config file")
		}
	}

	// Configure log file; use MultiWriter so all log.Printf calls reach both
	// the log file and stdout without needing paired fmt.Printf duplicates.
	logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	// Print configuration information
	logBinStr := "ON"
	if !config.LogBin {
		logBinStr = "OFF"
	}
	log.Printf("Configuration information:")
	log.Printf("  DstDSN: %s\n", config.DstDSN)
	log.Printf("  ParallelThds: %d\n", config.ParallelThds)
	log.Printf("  FixFileDir: %s\n", config.FixFileDir)
	log.Printf("  LogBin: %s\n", logBinStr)
	log.Printf("  LogFile: %s\n", config.LogFile)

	// Check if fixFileDir directory exists
	if _, err := os.Stat(config.FixFileDir); os.IsNotExist(err) {
		return fmt.Errorf("fixFileDir directory does not exist: %s", config.FixFileDir)
	}

	// Quick check if fixFileDir directory is empty
	entries, err := os.ReadDir(config.FixFileDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}
	if len(entries) == 0 {
		log.Printf("fixFileDir directory is empty, exiting\n")
		log.Printf("repairDB executed successfully\n")
		return nil
	}

	// Traverse fixFileDir directory to find all .sql files
	var sqlFiles []string
	err = filepath.Walk(config.FixFileDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".sql") {
			sqlFiles = append(sqlFiles, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to traverse directory: %v", err)
	}

	// Check if there are any SQL files
	if len(sqlFiles) == 0 {
		log.Printf("No .sql files found in fixFileDir directory\n")
		log.Printf("repairDB executed successfully\n")
		return nil
	}

	// Remove duplicated paths to guarantee each SQL file is scheduled once.
	sqlFiles = uniqueFiles(sqlFiles)

	// Classify SQL files into typed execution stages.
	cf := classifySQLFiles(sqlFiles)
	stages := buildExecutionStages(cf)

	// Print classification summary.
	log.Printf("Stage classification: DELETE=%d TABLE=%d VIEW=%d ROUTINE=%d TRIGGER=%d UNKNOWN=%d\n",
		len(cf.Delete), len(cf.Table), len(cf.View), len(cf.Routine), len(cf.Trigger), len(cf.Unknown))

	// Warn about files that could not be classified by type prefix.
	// They will execute last in the UNKNOWN stage; their relative ordering within
	// that stage is not guaranteed to be dependency-safe.
	if len(cf.Unknown) > 0 {
		sample := cf.Unknown
		if len(sample) > 3 {
			sample = sample[:3]
		}
		log.Printf("[WARN] %d file(s) could not be classified by type prefix and will execute last in UNKNOWN stage.\n", len(cf.Unknown))
		log.Printf("[WARN] UNKNOWN file examples: %s\n", strings.Join(sample, ", "))
	}

	// ========== Safety Pre-check Logic ==========
	// Collect statistics about fix SQL files
	stats, err := collectFixSQLStatistics(config.FixFileDir)
	if err != nil {
		return fmt.Errorf("收集统计信息失败: %v", err)
	}

	// Print statistics report
	printStatisticsReport(stats)

	// Handle dry-run mode
	if *dryRun {
		log.Printf("Dry-run 模式，不执行修复操作\n")
		return nil
	}

	// Handle interactive confirmation (unless force mode is enabled)
	if !forceMode {
		confirmed, err := promptUserConfirmation()
		if err != nil {
			return err
		}
		if !confirmed {
			log.Printf("用户取消操作，退出\n")
			return nil
		}
	}

	log.Printf("开始执行修复操作...\n")
	// ========== End of Safety Pre-check Logic ==========

	// Start timing.
	startTime := time.Now()

	// Execute stages in fixed order: DELETE → TABLE → VIEW → ROUTINE → TRIGGER → UNKNOWN.
	// Each stage runs to completion (wait-all-then-report) before the next stage starts.
	// A failure in any stage stops all subsequent stages.
	//
	// Each stage opens its own connection pool and closes it upon completion.
	// This guarantees that session-level variables set inside one stage's SQL files
	// (e.g. FOREIGN_KEY_CHECKS=0, UNIQUE_CHECKS=0) cannot leak into subsequent stages
	// via pooled connections, because database/sql does not reset session state on conn.Close().
	dsn := parseDSN(config.DstDSN)
	var allResults []FileExecResult

	for _, stage := range stages {
		files := prepareStageFiles(stage)
		logExecutionPlan(stage.Name, files, config.FixFileDir)
		log.Printf("[%s] starting execution (%d files), concurrency: %d\n", stage.Name, len(files), config.ParallelThds)

		db, err := openExecutionDB(dsn)
		if err != nil {
			// Write CSV with results collected so far before returning error
			if len(allResults) > 0 {
				writeCSVIfPossible(allResults, time.Since(startTime), *resultFile)
			}
			return fmt.Errorf("[%s] failed to connect to database: %v", stage.Name, err)
		}
		stageResults, stageErr := parallelExecuteSQLFiles(db, files, stage.Name)
		allResults = append(allResults, stageResults...)
		db.Close()
		if stageErr != nil {
			// Write CSV with results collected so far (including failed stage) before returning error
			writeCSVIfPossible(allResults, time.Since(startTime), *resultFile)
			return fmt.Errorf("[%s] execution failed: %v", stage.Name, stageErr)
		}

		log.Printf("[%s] execution completed\n", stage.Name)
	}

	// Calculate total time
	totalTime := time.Since(startTime)

	// Write CSV report
	writeCSVIfPossible(allResults, totalTime, *resultFile)

	// Format total time to match the required format (e.g., 9m43.936s)
	minutes := int(totalTime.Minutes())
	seconds := totalTime.Seconds() - float64(minutes*60)
	formattedTime := fmt.Sprintf("%dm%.3fs", minutes, seconds)

	log.Printf("All SQL files execution completed, total time taken: %s\n", formattedTime)
	log.Printf("repairDB executed successfully\n")
	return nil
}

// writeCSVIfPossible writes the CSV report and logs a warning on failure.
// It does not return an error because CSV writing is non-fatal.
func writeCSVIfPossible(results []FileExecResult, totalTime time.Duration, cliResultFile string) {
	if len(results) == 0 {
		return
	}
	// Priority: CLI flag > config file > default
	var resultPath string
	if cliResultFile != "" {
		resultPath = resolveRepairResultFilePath(cliResultFile)
	} else if config.ResultFile != "" {
		resultPath = resolveRepairResultFilePath(config.ResultFile)
	} else {
		resultPath = resolveRepairResultFilePath("")
	}
	if err := writeRepairCSVReport(results, totalTime, resultPath); err != nil {
		log.Printf("[WARN] Failed to write CSV report: %v\n", err)
	} else {
		log.Printf("CSV report written to: %s\n", resultPath)
	}
}

