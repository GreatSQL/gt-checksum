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
	statements    []sqlStatement
}

type sqlContextExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
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
func executeSQLFile(ctx context.Context, db *sql.DB, sqlFile string) (FileExecResult, error) {
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

	statements := splitSQLStatementsWithLocation(string(content))
	units, err := buildSQLExecutionUnits(statements)
	if err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = err.Error()
		return result, err
	}

	conn, err := db.Conn(ctx)
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
	if _, err := conn.ExecContext(ctx, "SET sql_log_bin = "+logBinVal); err != nil {
		result.Elapsed = time.Since(startTime)
		result.ErrorReason = fmt.Sprintf("Failed to SET sql_log_bin=%s: %v", logBinVal, err)
		return result, err
	}

	for _, unit := range units {
		outcome, err := executeUnitWithDeadlockRetry(ctx, conn, sqlFile, unit)
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

func buildSQLExecutionUnits(statements []sqlStatement) ([]sqlExecutionUnit, error) {
	var units []sqlExecutionUnit
	unitIndex := 1
	i := 0
	for i < len(statements) {
		stmt := strings.TrimSpace(statements[i].sql)
		if stmt == "" {
			i++
			continue
		}

		switch {
		case isBeginStatement(stmt):
			var txStatements []sqlStatement
			foundEnd := false
			for j := i + 1; j < len(statements); j++ {
				nextStmt := strings.TrimSpace(statements[j].sql)
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
				txStatements = append(txStatements, sqlStatement{
					sql:       nextStmt,
					startLine: statements[j].startLine,
					endLine:   statements[j].endLine,
				})
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
				statements: []sqlStatement{{
					sql:       stmt,
					startLine: statements[i].startLine,
					endLine:   statements[i].endLine,
				}},
			})
			unitIndex++
			i++
		}
	}

	return units, nil
}

func executeUnitWithDeadlockRetry(ctx context.Context, conn *sql.Conn, sqlFile string, unit sqlExecutionUnit) (unitExecOutcome, error) {
	var lastErr error
	var lastOutcome unitExecOutcome
	for retryRound := 0; retryRound <= maxDeadlockRetries; retryRound++ {
		if retryRound > 0 {
			backoff := time.Duration(1<<uint(retryRound)) * time.Second
			log.Printf("Deadlock retry in SQL file %s unit #%d: round=%d wait=%v\n", sqlFile, unit.index, retryRound, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return lastOutcome, fmt.Errorf("execution interrupted before deadlock retry in SQL file %s unit #%d: %w", sqlFile, unit.index, ctx.Err())
			}
		}

		outcome, err := executeUnit(ctx, conn, sqlFile, unit)
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

func executeUnit(ctx context.Context, conn *sql.Conn, sqlFile string, unit sqlExecutionUnit) (unitExecOutcome, error) {
	var outcome unitExecOutcome

	if unit.transactional {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return outcome, fmt.Errorf("Failed to start transaction: %v", err)
		}

		var txOutcome unitExecOutcome
		for _, statement := range unit.statements {
			stmtOutcome, execErr := executeStatementWithDupKeySplit(ctx, tx, sqlFile, unit, statement)
			txOutcome.add(stmtOutcome)
			if execErr != nil {
				_ = tx.Rollback()
				return outcome, fmt.Errorf("Failed to execute SQL statement in transaction (rolled back): %v", execErr)
			}
		}

		if err := tx.Commit(); err != nil {
			return outcome, fmt.Errorf("Failed to commit transaction: %v", err)
		}
		outcome.add(txOutcome)
		return outcome, nil
	}

	for _, statement := range unit.statements {
		stmtOutcome, execErr := executeStatementWithDupKeySplit(ctx, conn, sqlFile, unit, statement)
		outcome.add(stmtOutcome)
		if execErr != nil {
			return outcome, fmt.Errorf("Failed to execute SQL statement: %v", execErr)
		}
	}
	return outcome, nil
}

func executeStatementWithDupKeySplit(ctx context.Context, execer sqlContextExecer, sqlFile string, unit sqlExecutionUnit, statement sqlStatement) (unitExecOutcome, error) {
	var outcome unitExecOutcome
	stmt := strings.TrimSpace(statement.sql)
	if stmt == "" {
		return outcome, nil
	}

	stmt = normalizeMySQLDateFormatLiteralInSQLForExec(stmt)
	stmtType, _ := identifyStatementType(stmt)
	result, execErr := execer.ExecContext(ctx, stmt)
	if execErr == nil {
		recordStmtSuccess(&outcome, stmtType, result)
		return outcome, nil
	}

	if isDuplicateKeyError(execErr) {
		splitOutcome, handled, splitErr := executeSplitInsertOnDuplicateKey(ctx, execer, sqlFile, unit, statement, stmt, execErr)
		if handled {
			return splitOutcome, splitErr
		}
	}

	recordStmtFailure(&outcome, stmtType)
	return outcome, execErr
}

func executeSplitInsertOnDuplicateKey(ctx context.Context, execer sqlContextExecer, sqlFile string, unit sqlExecutionUnit, statement sqlStatement, stmt string, duplicateErr error) (unitExecOutcome, bool, error) {
	var outcome unitExecOutcome
	splitStatements, ok, err := splitMultiValueInsert(stmt)
	if err != nil {
		log.Printf("[DUPKEY-SPLIT] SQL file %s unit #%d line %s duplicate key split skipped: parse error=%v originalError=%v\n", sqlFile, unit.index, statementLineRange(statement), err, duplicateErr)
		return outcome, false, nil
	}
	if !ok {
		return outcome, false, nil
	}

	if conn, ok := execer.(*sql.Conn); ok {
		tx, txErr := conn.BeginTx(ctx, nil)
		if txErr != nil {
			return outcome, true, fmt.Errorf("Failed to start split INSERT retry transaction: %v", txErr)
		}
		splitOutcome, splitErr := executeSplitInsertStatements(ctx, tx, sqlFile, unit, statement, splitStatements, duplicateErr)
		if splitErr != nil {
			_ = tx.Rollback()
			var failedOutcome unitExecOutcome
			recordStmtFailure(&failedOutcome, "INSERT")
			return failedOutcome, true, splitErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return outcome, true, fmt.Errorf("Failed to commit split INSERT retry transaction: %v", commitErr)
		}
		return splitOutcome, true, nil
	}

	outcome, err = executeSplitInsertStatements(ctx, execer, sqlFile, unit, statement, splitStatements, duplicateErr)
	return outcome, true, err
}

func executeSplitInsertStatements(ctx context.Context, execer sqlContextExecer, sqlFile string, unit sqlExecutionUnit, statement sqlStatement, splitStatements []splitInsertStatement, duplicateErr error) (unitExecOutcome, error) {
	var outcome unitExecOutcome
	startTime := time.Now()
	var successRows int64
	var skippedDuplicates int64
	log.Printf("[DUPKEY-SPLIT] SQL file %s unit #%d line %s duplicate key detected, split multi-values INSERT into %d single INSERT statements: %v\n", sqlFile, unit.index, statementLineRange(statement), len(splitStatements), duplicateErr)

	for _, splitStmt := range splitStatements {
		result, execErr := execer.ExecContext(ctx, splitStmt.sql)
		if execErr == nil {
			rows, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				rows = 1
			}
			successRows += rows
			recordStmtSuccess(&outcome, "INSERT", result)
			log.Printf("[DUPKEY-SPLIT] SQL file %s unit #%d line %s tuple #%d/%d executed successfully, rowsAffected=%d\n", sqlFile, unit.index, statementLineRange(statement), splitStmt.tupleIndex, len(splitStatements), rows)
			continue
		}

		if isDuplicateKeyError(execErr) {
			skippedDuplicates++
			recordStmtFailure(&outcome, "INSERT")
			log.Printf("[DUPKEY-SPLIT] SQL file %s unit #%d line %s tuple #%d/%d skipped duplicate: %v\n", sqlFile, unit.index, statementLineRange(statement), splitStmt.tupleIndex, len(splitStatements), execErr)
			continue
		}

		recordStmtFailure(&outcome, "INSERT")
		log.Printf("[DUPKEY-SPLIT] SQL file %s unit #%d line %s tuple #%d/%d failed with non-duplicate error: %v\n", sqlFile, unit.index, statementLineRange(statement), splitStmt.tupleIndex, len(splitStatements), execErr)
		return outcome, fmt.Errorf("split INSERT tuple #%d/%d failed: %v", splitStmt.tupleIndex, len(splitStatements), execErr)
	}

	log.Printf("[DUPKEY-SPLIT] SQL file %s unit #%d line %s split retry completed, tupleTotal=%d successRows=%d skippedDuplicates=%d elapsed=%v\n", sqlFile, unit.index, statementLineRange(statement), len(splitStatements), successRows, skippedDuplicates, time.Since(startTime))
	return outcome, nil
}

func statementLineRange(statement sqlStatement) string {
	if statement.startLine <= 0 || statement.endLine <= 0 {
		return "unknown"
	}
	if statement.startLine == statement.endLine {
		return fmt.Sprintf("%d", statement.startLine)
	}
	return fmt.Sprintf("%d-%d", statement.startLine, statement.endLine)
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
func parallelExecuteSQLFiles(ctx context.Context, db *sql.DB, files []string, stageName string, repairProgress *progress.RepairProgress) ([]FileExecResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

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

	interrupted := false
scheduleLoop:
	for _, sqlFile := range filesToExecute {
		if ctx.Err() != nil {
			interrupted = true
			log.Printf("[%s] interrupt received, stop scheduling new SQL files\n", stageName)
			break scheduleLoop
		}

		select {
		case <-ctx.Done():
			interrupted = true
			log.Printf("[%s] interrupt received, stop scheduling new SQL files\n", stageName)
			break scheduleLoop
		case sem <- struct{}{}:
		}

		file := sqlFile
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			startTime := time.Now()
			seq := atomic.AddUint64(&executionSeq, 1)
			log.Printf("[%s] execution sequence #%d: %s\n", stageName, seq, file)

			// Let in-flight files finish so file-level resume never replays a partial file.
			result, err := executeSQLFile(context.Background(), db, file)
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

	if interrupted || ctx.Err() != nil {
		ctxErr := ctx.Err()
		if ctxErr == nil {
			ctxErr = context.Canceled
		}
		if errCount > 0 {
			return results, fmt.Errorf("%s interrupted: %w; %d in-flight file(s) failed, first error: %v", stageName, ctxErr, errCount, firstErr)
		}
		return results, fmt.Errorf("%s interrupted: %w", stageName, ctxErr)
	}

	if errCount > 0 {
		return results, fmt.Errorf("%s failed: %d file(s) execution error, first error: %v", stageName, errCount, firstErr)
	}
	return results, nil
}
