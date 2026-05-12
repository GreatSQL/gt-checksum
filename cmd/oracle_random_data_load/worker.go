package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	workerID int,
	db *sql.DB,
	cfg config,
	columns []columnMeta,
	sqlCache *batchSQLCache,
	taskCh <-chan int,
	errCh chan<- error,
	st *stats,
	seed int64,
) {
	defer wg.Done()
	r := rand.New(rand.NewSource(seed))

	for batchRows := range taskCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rows := make([][]interface{}, 0, batchRows)
		for i := 0; i < batchRows; i++ {
			rowSeq := atomic.AddUint64(&st.GeneratedRows, 1)
			vals, err := generateRow(columns, rowSeq, r, cfg.NullRate, cfg.TimeRangeDays, cfg.PKPlans)
			if err != nil {
				atomic.AddUint64(&st.FailedRows, 1)
				log.Printf("[worker-%d] generate row failed (seq=%d): %v", workerID, rowSeq, err)
				if !cfg.ContinueOnError {
					nonBlockingSendErr(errCh, err)
					return
				}
				continue
			}
			rows = append(rows, vals)
		}
		if len(rows) == 0 {
			continue
		}

		err := execBatchWithRetry(ctx, db, cfg, rows, sqlCache, st, false)
		if err == nil {
			atomic.AddUint64(&st.OKBatches, 1)
			atomic.AddUint64(&st.InsertedRows, uint64(len(rows)))
			continue
		}

		atomic.AddUint64(&st.FailBatches, 1)
		log.Printf("[worker-%d] batch insert failed after retry, fallback to row-by-row, rows=%d err=%v sample_row=%s",
			workerID, len(rows), err, formatRowForLog(columns, rows[0]))

		// Batch success and row fallback are mutually exclusive paths.
		// InsertedRows is aggregated once for each path, so no double counting.
		inserted, failed := execRowsOneByOne(ctx, db, cfg, rows, columns, sqlCache, st)
		atomic.AddUint64(&st.InsertedRows, uint64(inserted))
		atomic.AddUint64(&st.FailedRows, uint64(failed))
		if failed > 0 && !cfg.ContinueOnError {
			nonBlockingSendErr(errCh, fmt.Errorf("worker-%d row-by-row fallback still failed", workerID))
			return
		}
	}
}

func execBatchWithRetry(ctx context.Context, db *sql.DB, cfg config, rows [][]interface{}, sqlCache *batchSQLCache, st *stats, rowFallback bool) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			atomic.AddUint64(&st.Retries, 1)
			if rowFallback {
				atomic.AddUint64(&st.RowRetries, 1)
			} else {
				atomic.AddUint64(&st.BatchRetries, 1)
			}
			time.Sleep(time.Duration(attempt*150) * time.Millisecond)
		}
		if err := execInsertAll(ctx, db, cfg, rows, sqlCache); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func execRowsOneByOne(ctx context.Context, db *sql.DB, cfg config, rows [][]interface{}, columns []columnMeta, sqlCache *batchSQLCache, st *stats) (inserted, failed int) {
	for _, row := range rows {
		one := [][]interface{}{row}
		err := execBatchWithRetry(ctx, db, cfg, one, sqlCache, st, true)
		if err != nil {
			failed++
			log.Printf("row fallback insert failed: err=%v row=%s", err, formatRowForLog(columns, row))
		} else {
			inserted++
		}
	}
	return inserted, failed
}

func execInsertAll(ctx context.Context, db *sql.DB, cfg config, rows [][]interface{}, sqlCache *batchSQLCache) error {
	if len(rows) == 0 {
		return nil
	}
	sqlText := sqlCache.get(len(rows))
	if cfg.PrintSQL {
		log.Printf("SQL template(batch=%d): %s", len(rows), sqlText)
	}
	args := make([]interface{}, 0, len(rows)*len(rows[0]))
	for _, row := range rows {
		args = append(args, row...)
	}

	execCtx, cancel := context.WithTimeout(ctx, cfg.ExecTimeout)
	defer cancel()
	_, err := db.ExecContext(execCtx, sqlText, args...)
	return err
}