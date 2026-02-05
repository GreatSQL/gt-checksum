package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
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

	// Split SQL statements by semicolon
	statements := strings.Split(string(content), ";")

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

// Execute SQL files in parallel
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

	// Execute files
	for _, file := range files {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore

		go func(sqlFile string) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Record start time
			startTime := time.Now()
			log.Printf("Starting to execute SQL file: %s\n", sqlFile)
			fmt.Printf("Starting to execute SQL file: %s\n", sqlFile)

			// Execute SQL file
			err := executeSQLFile(db, sqlFile)
			if err != nil {
				log.Printf("Failed to execute SQL file %s: %v\n", sqlFile, err)
			} else {
				log.Printf("Successfully executed SQL file %s, time taken: %v\n", sqlFile, time.Since(startTime))
				fmt.Printf("Successfully executed SQL file %s, time taken: %v\n", sqlFile, time.Since(startTime))
			}
		}(file)
	}

	// Wait for all goroutines to complete
	wg.Wait()
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
		if strings.Contains(file, "-DELETE.sql") {
			deleteFiles = append(deleteFiles, file)
		} else {
			otherFiles = append(otherFiles, file)
		}
	}

	// Sort files
	sort.Strings(deleteFiles)
	sort.Strings(otherFiles)

	// Print file information
	log.Printf("Found %d DELETE files, %d other SQL files\n", len(deleteFiles), len(otherFiles))
	fmt.Printf("Found %d DELETE files, %d other SQL files\n", len(deleteFiles), len(otherFiles))

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

	log.Printf("All SQL files execution completed, total time taken: %v\n", totalTime)
	fmt.Printf("All SQL files execution completed, total time taken: %v\n", totalTime)
	fmt.Println("repairDB executed successfully")
}
