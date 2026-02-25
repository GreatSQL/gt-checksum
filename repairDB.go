package main

import (
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
	wg     sync.WaitGroup
	sem    chan struct{}
)

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

	// Execute SQL statements using a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Failed to start transaction: %v", err)
	}

	// Split SQL statements by semicolon, considering string literals
	statements := splitSQLStatements(string(content))

	// Execute each statement individually
	for _, stmt := range statements {
		// Trim whitespace and skip empty statements
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Execute the statement
		_, err = tx.Exec(stmt)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("Failed to execute SQL statement: %v", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("Failed to commit transaction: %v", err)
	}

	return nil
}

// splitSQLStatements splits SQL statements by semicolon, considering string literals
func splitSQLStatements(content string) []string {
	var statements []string
	var currentStmt strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	escaped := false

	for _, c := range content {
		if escaped {
			currentStmt.WriteRune(c)
			escaped = false
			continue
		}

		switch c {
		case '\\':
			escaped = true
			currentStmt.WriteRune(c)
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			currentStmt.WriteRune(c)
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			currentStmt.WriteRune(c)
		case ';':
			if !inSingleQuote && !inDoubleQuote {
				statements = append(statements, currentStmt.String())
				currentStmt.Reset()
			} else {
				currentStmt.WriteRune(c)
			}
		default:
			currentStmt.WriteRune(c)
		}
	}

	// Add the last statement if it's not empty
	lastStmt := strings.TrimSpace(currentStmt.String())
	if lastStmt != "" {
		statements = append(statements, lastStmt)
	}

	return statements
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

// deadlockResult holds the result of executing a SQL file that may have deadlocked
type deadlockResult struct {
	file string
	err  error
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

// Execute SQL files in parallel with deadlock retry support
func parallelExecuteSQLFiles(files []string, dsn, stageName string) {
	// Create database connection pool
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Failed to create database connection: %v\n", err)
		return
	}
	defer db.Close()

	// Test database connection
	err = db.Ping()
	if err != nil {
		log.Printf("Database connection failed: %v\n", err)
		return
	}

	// Execute files and collect deadlocked ones for retry
	pendingFiles := files
	retryRound := 0
	var executionSeq uint64

	for len(pendingFiles) > 0 {
		if retryRound > 0 {
			// Exponential backoff: 2s, 4s, 8s, ...
			backoff := time.Duration(1<<uint(retryRound)) * time.Second
			log.Printf("Deadlock retry round %d: waiting %v before retrying %d file(s)\n",
				retryRound, backoff, len(pendingFiles))
			fmt.Printf("Deadlock retry round %d: waiting %v before retrying %d file(s)\n",
				retryRound, backoff, len(pendingFiles))
			time.Sleep(backoff)
		}

		if retryRound > maxDeadlockRetries {
			// Max retries exceeded — report and stop
			log.Printf("ERROR: Max deadlock retry limit (%d) exceeded. The following %d file(s) still failed:\n",
				maxDeadlockRetries, len(pendingFiles))
			fmt.Printf("ERROR: Max deadlock retry limit (%d) exceeded. The following %d file(s) still failed:\n",
				maxDeadlockRetries, len(pendingFiles))
			for _, f := range pendingFiles {
				log.Printf("  DEADLOCK_UNRESOLVED: %s\n", f)
				fmt.Printf("  DEADLOCK_UNRESOLVED: %s\n", f)
			}
			log.Printf("Please manually execute these SQL files after resolving lock contention.\n")
			fmt.Printf("Please manually execute these SQL files after resolving lock contention.\n")
			return
		}

		// Channel to collect deadlock results
		resultChan := make(chan deadlockResult, len(pendingFiles))

		// Execute current batch
		for _, file := range pendingFiles {
			wg.Add(1)
			sem <- struct{}{} // Acquire semaphore

			go func(sqlFile string) {
				defer wg.Done()
				defer func() { <-sem }() // Release semaphore

				// Record start time
				startTime := time.Now()
				seq := atomic.AddUint64(&executionSeq, 1)
				log.Printf("[%s] execution sequence #%d (round %d): %s\n", stageName, seq, retryRound, sqlFile)
				if retryRound == 0 {
					log.Printf("Starting to execute SQL file: %s\n", sqlFile)
					fmt.Printf("Starting to execute SQL file: %s\n", sqlFile)
				} else {
					log.Printf("Retrying SQL file (round %d): %s\n", retryRound, sqlFile)
					fmt.Printf("Retrying SQL file (round %d): %s\n", retryRound, sqlFile)
				}

				// Execute SQL file
				err := executeSQLFile(db, sqlFile)
				if err != nil {
					if isDeadlockError(err) {
						log.Printf("DEADLOCK detected in SQL file %s (retry round %d): %v\n", sqlFile, retryRound, err)
						fmt.Printf("DEADLOCK detected in SQL file %s, will retry\n", sqlFile)
						resultChan <- deadlockResult{file: sqlFile, err: err}
					} else {
						log.Printf("Failed to execute SQL file %s: %v\n", sqlFile, err)
						fmt.Printf("Failed to execute SQL file %s: %v\n", sqlFile, err)
					}
				} else {
					elapsed := time.Since(startTime)
					if retryRound > 0 {
						log.Printf("Successfully executed SQL file %s on retry round %d, time taken: %v\n", sqlFile, retryRound, elapsed)
						fmt.Printf("Successfully executed SQL file %s on retry round %d, time taken: %v\n", sqlFile, retryRound, elapsed)
					} else {
						log.Printf("Successfully executed SQL file %s, time taken: %v\n", sqlFile, elapsed)
						fmt.Printf("Successfully executed SQL file %s, time taken: %v\n", sqlFile, elapsed)
					}
				}
			}(file)
		}

		// Wait for all goroutines in this round to complete
		wg.Wait()
		close(resultChan)

		// Collect deadlocked files for next retry round
		var deadlockedFiles []string
		for result := range resultChan {
			deadlockedFiles = append(deadlockedFiles, result.file)
		}

		if len(deadlockedFiles) > 0 {
			// Keep delete stage deterministic, but randomize non-delete stage on retry.
			if stageName == "PHASE-2-OTHER" {
				shuffleSQLFiles(deadlockedFiles)
			} else {
				sort.Strings(deadlockedFiles)
			}
			log.Printf("Round %d completed: %d file(s) deadlocked, will retry\n", retryRound, len(deadlockedFiles))
		}

		pendingFiles = deadlockedFiles
		retryRound++
	}
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

	// Initialize semaphore
	sem = make(chan struct{}, config.ParallelThds)

	// Start timing
	startTime := time.Now()

	// Execute DELETE files
	if len(deleteFiles) > 0 {
		log.Printf("Starting parallel execution of DELETE files, concurrency: %d\n", config.ParallelThds)
		fmt.Printf("Starting parallel execution of DELETE files, concurrency: %d\n", config.ParallelThds)
		dsn := parseDSN(config.DstDSN)
		parallelExecuteSQLFiles(deleteFiles, dsn, "PHASE-1-DELETE")
		log.Printf("DELETE files execution completed\n")
		fmt.Printf("DELETE files execution completed\n")
	}

	// Execute other SQL files
	if len(otherFiles) > 0 {
		log.Printf("Starting parallel execution of other SQL files, concurrency: %d\n", config.ParallelThds)
		fmt.Printf("Starting parallel execution of other SQL files, concurrency: %d\n", config.ParallelThds)
		dsn := parseDSN(config.DstDSN)
		parallelExecuteSQLFiles(otherFiles, dsn, "PHASE-2-OTHER")
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
