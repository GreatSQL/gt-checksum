package main

import (
	"context"
	"flag"
	"fmt"
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

	_ "github.com/go-sql-driver/mysql"
)

// Config stores configuration information
type Config struct {
	DstDSN       string
	ParallelThds int
	FixFileDir   string
	LogFile      string
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
		}
	}

	// Set default values
	if config.ParallelThds <= 0 {
		config.ParallelThds = 4
	}
	if config.FixFileDir == "" {
		config.FixFileDir = "./fixsql"
	}
	config.LogFile = "repairDB.log"

	// Validate config
	if config.DstDSN == "" {
		return fmt.Errorf("Missing dstDSN parameter in config file")
	}

	return nil
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

// Execute SQL file
func executeSQLFile(db *sql.DB, sqlFile string) error {
	// Read SQL file content
	content, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("Failed to read SQL file: %v", err)
	}

	statements := splitSQLStatements(string(content))
	units, err := buildSQLExecutionUnits(statements)
	if err != nil {
		return err
	}

	// Keep one connection for the whole file so session-level SET statements
	// (for example UNIQUE_CHECKS/FOREIGN_KEY_CHECKS) apply to all subsequent SQL.
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("Failed to get database connection: %v", err)
	}
	defer conn.Close()

	for _, unit := range units {
		err = executeUnitWithDeadlockRetry(conn, sqlFile, unit)
		if err != nil {
			return err
		}
	}

	return nil
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

func executeUnitWithDeadlockRetry(conn *sql.Conn, sqlFile string, unit sqlExecutionUnit) error {
	var lastErr error
	for retryRound := 0; retryRound <= maxDeadlockRetries; retryRound++ {
		if retryRound > 0 {
			backoff := time.Duration(1<<uint(retryRound)) * time.Second
			log.Printf("Deadlock retry in SQL file %s unit #%d: round=%d wait=%v\n", sqlFile, unit.index, retryRound, backoff)
			time.Sleep(backoff)
		}

		err := executeUnit(conn, unit)
		if err == nil {
			return nil
		}
		lastErr = err

		if !isDeadlockError(err) {
			return err
		}

		log.Printf("DEADLOCK detected in SQL file %s unit #%d (retry round %d): %v\n", sqlFile, unit.index, retryRound, err)
	}
	return fmt.Errorf("deadlock unresolved after %d retries in SQL file %s unit #%d: %v", maxDeadlockRetries, sqlFile, unit.index, lastErr)
}

func executeUnit(conn *sql.Conn, unit sqlExecutionUnit) error {
	if unit.transactional {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("Failed to start transaction: %v", err)
		}

		for _, stmt := range unit.statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			stmt = normalizeMySQLDateFormatLiteralInSQLForExec(stmt)
			_, err = tx.ExecContext(context.Background(), stmt)
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("Failed to execute SQL statement: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("Failed to commit transaction: %v", err)
		}
		return nil
	}

	for _, stmt := range unit.statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmt = normalizeMySQLDateFormatLiteralInSQLForExec(stmt)
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			return fmt.Errorf("Failed to execute SQL statement: %v", err)
		}
	}
	return nil
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

func isDeleteStageFile(path string) bool {
	return deleteFileNameRegex.MatchString(filepath.Base(path))
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

// Execute SQL files in random parallel mode with bounded concurrency.
func parallelExecuteSQLFiles(files []string, dsn, stageName string) error {
	// Create database connection pool
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("Failed to create database connection: %v", err)
	}
	defer db.Close()

	// Test database connection
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("Database connection failed: %v", err)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, config.ParallelThds)
	errCh := make(chan error, len(files))
	var executionSeq uint64

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
			fmt.Printf("Starting to execute SQL file: %s\n", file)

			err := executeSQLFile(db, file)
			if err != nil {
				errCh <- fmt.Errorf("Failed to execute SQL file %s: %v", file, err)
				log.Printf("Failed to execute SQL file %s: %v\n", file, err)
				fmt.Printf("Failed to execute SQL file %s: %v\n", file, err)
				return
			}

			elapsed := time.Since(startTime)
			log.Printf("Successfully executed SQL file %s, time taken: %v\n", file, elapsed)
			fmt.Printf("Successfully executed SQL file %s, time taken: %v\n", file, elapsed)
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
	if errCount > 0 {
		return fmt.Errorf("%s failed: %d file(s) execution error, first error: %v", stageName, errCount, firstErr)
	}
	return nil
}

// Main function
func main() {
	// Parse command line arguments
	confFile := flag.String("conf", "gc.conf", "Config file path")
	flag.Parse()

	// Check if command line argument is provided for fixFileDir
	var specifiedFixFileDir string
	if len(flag.Args()) > 0 {
		specifiedFixFileDir = flag.Args()[0]
	}

	// If command line argument is provided, use it directly
	if specifiedFixFileDir != "" {
		// Parse config file to get DstDSN and other parameters
		err := parseConfig(*confFile)
		if err != nil {
			log.Printf("Failed to parse config file: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}
		// Override fixFileDir with command line argument
		config.FixFileDir = specifiedFixFileDir
	} else {
		// Parse config file
		err := parseConfig(*confFile)
		if err != nil {
			log.Printf("Failed to parse config file: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}
		// Check if fixFileDir is set in config file
		if config.FixFileDir == "" {
			log.Printf("No fixFileDir specified in command line or config file\n")
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}
	}

	// Configure log file
	logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Failed to create log file: %v\n", err)
		fmt.Println("repairDB execution failed")
		os.Exit(1)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Print configuration information
	log.Printf("Configuration information:")
	log.Printf("  DstDSN: %s\n", config.DstDSN)
	log.Printf("  ParallelThds: %d\n", config.ParallelThds)
	log.Printf("  FixFileDir: %s\n", config.FixFileDir)
	log.Printf("  LogFile: %s\n", config.LogFile)

	// Check if fixFileDir directory exists
	if _, err := os.Stat(config.FixFileDir); os.IsNotExist(err) {
		log.Printf("fixFileDir directory does not exist: %s\n", config.FixFileDir)
		fmt.Println("repairDB execution failed")
		os.Exit(1)
	}

	// Quick check if fixFileDir directory is empty
	entries, err := os.ReadDir(config.FixFileDir)
	if err != nil {
		log.Printf("Failed to read directory: %v\n", err)
		fmt.Println("repairDB execution failed")
		os.Exit(1)
	}
	if len(entries) == 0 {
		log.Printf("fixFileDir directory is empty, exiting\n")
		fmt.Println("repairDB executed successfully")
		return
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
		log.Printf("Failed to traverse directory: %v\n", err)
		fmt.Println("repairDB execution failed")
		os.Exit(1)
	}

	// Check if there are any SQL files
	if len(sqlFiles) == 0 {
		log.Printf("No .sql files found in fixFileDir directory\n")
		fmt.Println("repairDB executed successfully")
		return
	}

	// Check if there is only one datafix.sql file
	if len(sqlFiles) == 1 && strings.HasSuffix(sqlFiles[0], "datafix.sql") {
		log.Printf("Found only one datafix.sql file, executing directly\n")

		// Parse DSN
		dsn := parseDSN(config.DstDSN)

		// Create database connection
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Failed to create database connection: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}
		defer db.Close()

		// Test database connection
		err = db.Ping()
		if err != nil {
			log.Printf("Database connection failed: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}

		// Execute SQL file
		err = executeSQLFile(db, sqlFiles[0])
		if err != nil {
			log.Printf("Failed to execute datafix.sql file: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}

		log.Printf("Successfully executed datafix.sql file\n")
		return
	}

	// Remove duplicated paths to guarantee each SQL file is scheduled once.
	sqlFiles = uniqueFiles(sqlFiles)

	// Separate DELETE files and other files
	var deleteFiles []string
	var otherFiles []string

	for _, file := range sqlFiles {
		if isDeleteStageFile(file) {
			deleteFiles = append(deleteFiles, file)
		} else {
			otherFiles = append(otherFiles, file)
		}
	}

	// Phase-1 keeps deterministic order for audit readability.
	sort.Strings(deleteFiles)
	// Phase-2 uses randomized order to reduce lock contention hotspots.
	shuffleSQLFiles(otherFiles)

	// Print file information
	log.Printf("Found %d DELETE files, %d other SQL files\n", len(deleteFiles), len(otherFiles))
	fmt.Printf("Found %d DELETE files, %d other SQL files\n", len(deleteFiles), len(otherFiles))

	// Log planned execution order for audit/review.
	if len(deleteFiles) > 0 {
		logExecutionPlan("PHASE-1-DELETE", deleteFiles, config.FixFileDir)
	}
	if len(otherFiles) > 0 {
		logExecutionPlan("PHASE-2-OTHER", otherFiles, config.FixFileDir)
	}

	// Start timing
	startTime := time.Now()

	// Execute DELETE files
	if len(deleteFiles) > 0 {
		log.Printf("Starting parallel execution of DELETE files, concurrency: %d\n", config.ParallelThds)
		fmt.Printf("Starting parallel execution of DELETE files, concurrency: %d\n", config.ParallelThds)
		dsn := parseDSN(config.DstDSN)
		err = parallelExecuteSQLFiles(deleteFiles, dsn, "PHASE-1-DELETE")
		if err != nil {
			log.Printf("DELETE phase execution failed: %v\n", err)
			fmt.Printf("DELETE phase execution failed: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}
		log.Printf("DELETE files execution completed\n")
		fmt.Printf("DELETE files execution completed\n")
	}

	// Execute other SQL files
	if len(otherFiles) > 0 {
		log.Printf("Starting parallel execution of other SQL files, concurrency: %d\n", config.ParallelThds)
		fmt.Printf("Starting parallel execution of other SQL files, concurrency: %d\n", config.ParallelThds)
		dsn := parseDSN(config.DstDSN)
		err = parallelExecuteSQLFiles(otherFiles, dsn, "PHASE-2-OTHER")
		if err != nil {
			log.Printf("OTHER phase execution failed: %v\n", err)
			fmt.Printf("OTHER phase execution failed: %v\n", err)
			fmt.Println("repairDB execution failed")
			os.Exit(1)
		}
		log.Printf("Other SQL files execution completed\n")
		fmt.Printf("Other SQL files execution completed\n")
	}

	// Calculate total time
	totalTime := time.Since(startTime)

	// Format total time to match the required format (e.g., 9m43.936s)
	minutes := int(totalTime.Minutes())
	seconds := totalTime.Seconds() - float64(minutes*60)
	formattedTime := fmt.Sprintf("%dm%.3fs", minutes, seconds)

	log.Printf("All SQL files execution completed, total time taken: %s\n", formattedTime)
	fmt.Printf("All SQL files execution completed, total time taken: %s\n", formattedTime)
	fmt.Println("repairDB executed successfully")
}
