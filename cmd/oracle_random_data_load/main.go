package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/godror/godror"
)

func main() {
	cfg, err := parseFlags()
	if err != nil {
		log.Fatalf("Invalid arguments: %v", err)
	}
	logCloser, err := initLogger(cfg.LogFile)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	if logCloser != nil {
		defer func() {
			if closeErr := logCloser.Close(); closeErr != nil {
				log.Printf("WARN close log file failed: %v", closeErr)
			}
		}()
	}

	startTime := time.Now()
	log.Printf("oracle-random-data-load starting: schema=%s table=%s rows=%d workers=%d batch_size=%d max_retries=%d",
		cfg.Schema, cfg.Table, cfg.Rows, cfg.Workers, cfg.BatchSize, cfg.MaxRetries)

	db, err := sql.Open("godror", cfg.DSN)
	if err != nil {
		log.Fatalf("Failed to create Oracle DB handle: %v", err)
	}
	defer db.Close()
	applyDBPoolSettings(db, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	var interrupted atomic.Bool
	go func() {
		select {
		case sig := <-sigCh:
			interrupted.Store(true)
			log.Printf("Received signal %s, canceling running workers...", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to connect Oracle: %v", err)
	}

	columns, err := loadTableColumns(ctx, db, cfg.Schema, cfg.Table)
	if err != nil {
		log.Fatalf("Failed to load table columns: %v", err)
	}
	if len(columns) == 0 {
		log.Fatalf("No columns found for table %s.%s", cfg.Schema, cfg.Table)
	}

	pkCols, err := loadPrimaryKeyColumns(ctx, db, cfg.Schema, cfg.Table)
	if err != nil {
		log.Fatalf("Failed to load primary key metadata: %v", err)
	}
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, name := range pkCols {
		pkSet[strings.ToUpper(strings.TrimSpace(name))] = struct{}{}
	}

	insertColumns, skipped, err := filterInsertColumns(columns, pkSet, cfg.ExcludeColumns)
	if err != nil {
		log.Fatalf("Invalid table metadata for random load: %v", err)
	}
	for _, msg := range skipped {
		log.Printf("WARN %s", msg)
	}
	if len(insertColumns) == 0 {
		log.Fatalf("No writable columns remain for table %s.%s", cfg.Schema, cfg.Table)
	}
	pkPlans, pkPlanErr := buildPrimaryKeyPlans(ctx, db, cfg.Schema, cfg.Table, insertColumns, cfg.Rows, cfg.Seed)
	if pkPlanErr != nil {
		log.Fatalf("Failed to build primary key generation plans: %v", pkPlanErr)
	}
	cfg.PKPlans = pkPlans
	cappedBatchSize, capErr := capBatchSizeByBindLimit(cfg.BatchSize, len(insertColumns))
	if capErr != nil {
		log.Fatalf("Batch size validation failed: %v", capErr)
	}
	if cappedBatchSize != cfg.BatchSize {
		log.Printf("WARN batch-size %d exceeds Oracle bind-variable limit for %d columns, auto-capped to %d",
			cfg.BatchSize, len(insertColumns), cappedBatchSize)
		cfg.BatchSize = cappedBatchSize
	}
	log.Printf("Resolved columns: total=%d insertable=%d pk=%d", len(columns), len(insertColumns), len(pkCols))

	tableRef := fmt.Sprintf("%s.%s", oracleIdentifier(cfg.Schema), oracleIdentifier(cfg.Table))
	sqlCache := newBatchSQLCache(tableRef, insertColumns)

	taskCh := make(chan int, cfg.Workers*2)
	errCh := make(chan error, cfg.Workers)
	var wg sync.WaitGroup
	var st stats

	for workerID := 1; workerID <= cfg.Workers; workerID++ {
		wg.Add(1)
		seed := cfg.Seed + int64(workerID*100003)
		go worker(ctx, &wg, workerID, db, cfg, insertColumns, sqlCache, taskCh, errCh, &st, seed)
	}

	progressDone := make(chan struct{})
	go monitorProgress(cfg, &st, cfg.Rows, startTime, progressDone)

	go func() {
		defer close(taskCh)
		remaining := cfg.Rows
		for remaining > 0 {
			batchRows := minInt64(int64(cfg.BatchSize), remaining)
			select {
			case <-ctx.Done():
				return
			case taskCh <- int(batchRows):
				remaining -= batchRows
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var firstErr error
	for err := range errCh {
		if err == nil {
			continue
		}
		if firstErr == nil {
			firstErr = err
			if !cfg.ContinueOnError {
				cancel()
			}
		}
	}
	if firstErr == nil && interrupted.Load() {
		firstErr = errors.New("interrupted by signal")
	}

	close(progressDone)
	printSummary(cfg, &st, startTime, firstErr)
	if firstErr != nil && !cfg.ContinueOnError {
		os.Exit(1)
	}
}
