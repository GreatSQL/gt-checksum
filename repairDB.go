package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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

// Execute SQL files in parallel with deadlock retry support
func parallelExecuteSQLFiles(files []string, dsn string) {
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
			// Sort deadlocked files for consistent retry order
			sortSQLFilesByNumericSuffix(deadlockedFiles)
			log.Printf("Round %d completed: %d file(s) deadlocked, will retry\n", retryRound, len(deadlockedFiles))
		}

		pendingFiles = deadlockedFiles
		retryRound++
	}
}

// numericSuffixRegex extracts the trailing number from a filename before .sql extension
// e.g., "table-99.sql" → "99", "lineitem-DELETE-101.sql" → "101"
var numericSuffixRegex = regexp.MustCompile(`-(\d+)\.sql$`)

// extractNumericSuffix extracts the trailing numeric suffix from a SQL filename.
// Returns the number and true if found, or 0 and false if no numeric suffix.
func extractNumericSuffix(filename string) (int, bool) {
	base := filepath.Base(filename)
	matches := numericSuffixRegex.FindStringSubmatch(base)
	if len(matches) >= 2 {
		num, err := strconv.Atoi(matches[1])
		if err == nil {
			return num, true
		}
	}
	return 0, false
}

// sortSQLFilesByNumericSuffix sorts SQL file paths by the numeric suffix in their filenames.
// Files with numeric suffixes (e.g., "a-99.sql", "a-101.sql") are sorted numerically.
// Files without numeric suffixes are sorted lexicographically and placed after numbered files.
func sortSQLFilesByNumericSuffix(files []string) {
	sort.SliceStable(files, func(i, j int) bool {
		numI, hasI := extractNumericSuffix(files[i])
		numJ, hasJ := extractNumericSuffix(files[j])

		if hasI && hasJ {
			if numI != numJ {
				return numI < numJ
			}
			// Same number — sort by full path as tiebreaker
			return files[i] < files[j]
		}
		if hasI && !hasJ {
			return true // numbered files first
		}
		if !hasI && hasJ {
			return false
		}
		// Neither has a number — lexicographic
		return files[i] < files[j]
	})
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

	// Separate DELETE files and other files
	var deleteFiles []string
	var otherFiles []string

	for _, file := range sqlFiles {
		if strings.Contains(file, "-DELETE") {
			deleteFiles = append(deleteFiles, file)
		} else {
			otherFiles = append(otherFiles, file)
		}
	}

	// Sort files by numeric suffix to ensure correct execution order
	// e.g., a-99.sql before a-101.sql (not lexicographic order)
	sortSQLFilesByNumericSuffix(deleteFiles)
	sortSQLFilesByNumericSuffix(otherFiles)

	// Print file information
	log.Printf("Found %d DELETE files, %d other SQL files\n", len(deleteFiles), len(otherFiles))
	fmt.Printf("Found %d DELETE files, %d other SQL files\n", len(deleteFiles), len(otherFiles))

	// Log sorted file order for debugging
	if len(deleteFiles) > 0 {
		log.Printf("DELETE files execution order:")
		for i, f := range deleteFiles {
			log.Printf("  [%d] %s", i+1, f)
		}
	}
	if len(otherFiles) > 0 {
		log.Printf("Other SQL files execution order:")
		for i, f := range otherFiles {
			log.Printf("  [%d] %s", i+1, f)
		}
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
		parallelExecuteSQLFiles(deleteFiles, dsn)
		log.Printf("DELETE files execution completed\n")
		fmt.Printf("DELETE files execution completed\n")
	}

	// Execute other SQL files
	if len(otherFiles) > 0 {
		log.Printf("Starting parallel execution of other SQL files, concurrency: %d\n", config.ParallelThds)
		fmt.Printf("Starting parallel execution of other SQL files, concurrency: %d\n", config.ParallelThds)
		dsn := parseDSN(config.DstDSN)
		parallelExecuteSQLFiles(otherFiles, dsn)
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
