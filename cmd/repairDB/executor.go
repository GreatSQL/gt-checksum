package main

import (
	"context"
	"database/sql"
	"fmt"
	"gt-checksum/progress"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const maxDeadlockRetries = 3

// dbConnMaxLifetime caps how long a pooled connection may be reused.
const dbConnMaxLifetime = 10 * time.Minute

// fileKey returns a unique identifier for a SQL file relative to the fix file directory.
// This avoids collisions when different subdirectories contain files with the same name.
func fileKey(sqlFile string) string {
	// Try to get relative path from FixFileDir
	if config.FixFileDir != "" {
		rel, err := filepath.Rel(config.FixFileDir, sqlFile)
		if err == nil && !strings.HasPrefix(rel, "..") {
			return rel
		}
	}
	// Fallback to base name
	return filepath.Base(sqlFile)
}

type sqlExecutionUnit struct {
	index         int
	transactional bool
	statements    []string
}

// isDeadlockError checks if an error is a MySQL deadlock error (Error 1213)
func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Error 1213") ||
		strings.Contains(err.Error(), "Deadlock found when trying to get lock")
}

// executeSQLFile executes a single SQL file against the provided connection pool.
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

	content, err := os.ReadFile(sqlFile)
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = fmt.Sprintf("Failed to read SQL file: %v", err)
		return result, err
	}

	result.ObjectType = detectObjectTypeFromContent(stage, string(content))

	statements := splitSQLStatements(string(content))
	units, err := buildSQLExecutionUnits(statements)
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = err.Error()
		return result, err
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = fmt.Sprintf("Failed to get database connection: %v", err)
		return result, err
	}
	defer conn.Close()

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

// openExecutionDB opens and validates a MySQL connection pool for one execution stage.
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
// If repairProgress is not nil, it records the status of each file after execution
// and skips files that have already been executed successfully.
func parallelExecuteSQLFiles(db *sql.DB, files []string, stageName string, repairProgress *progress.RepairProgress) ([]FileExecResult, error) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, config.ParallelThds)
	errCh := make(chan error, len(files))
	var executionSeq uint64
	collector := &resultCollector{}

	// 断点续传：过滤掉已成功执行的文件
	filesToExecute := make([]string, 0, len(files))
	skippedCount := 0
	for _, sqlFile := range files {
		if repairProgress != nil && repairProgress.IsFileSuccess(fileKey(sqlFile)) {
			skippedCount++
			log.Printf("[RESUME] Skipping already executed file: %s\n", sqlFile)
			continue
		}
		filesToExecute = append(filesToExecute, sqlFile)
	}
	if skippedCount > 0 {
		log.Printf("[RESUME] Skipped %d already executed file(s) in stage %s\n", skippedCount, stageName)
	}

	if len(filesToExecute) == 0 {
		log.Printf("[%s] all files already executed, skipping stage\n", stageName)
		return nil, nil
	}

	for _, sqlFile := range filesToExecute {
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

			// 断点续传：记录文件执行状态
			if repairProgress != nil {
				fileStatus := "success"
				if err != nil {
					fileStatus = "failed"
				}
				if markErr := repairProgress.MarkFile(fileKey(file), fileStatus); markErr != nil {
					log.Printf("[WARN] Failed to save progress for file %s: %v\n", file, markErr)
				}
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
