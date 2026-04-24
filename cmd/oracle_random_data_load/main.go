package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	_ "github.com/godror/godror"
)

const (
	defaultRows             int64   = 10000
	defaultWorkers          int     = 4
	defaultBatchSize        int     = 200
	maxConfiguredBatchSize  int     = 100000
	defaultMaxRetries       int     = 2
	defaultNullRate         float64 = 0.10
	defaultProgressInterval int     = 2
	defaultExecTimeoutSec   int     = 30
	defaultTimeRangeDays    int     = 3650
	defaultConnLifetimeMin  int     = 30
	oracleMaxBindVariables  int     = 65535
)

var simpleOracleIdentifierPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)

type config struct {
	DSN              string
	Schema           string
	Table            string
	Rows             int64
	Workers          int
	BatchSize        int
	MaxRetries       int
	NullRate         float64
	ProgressInterval time.Duration
	ExecTimeout      time.Duration
	TimeRangeDays    int
	Seed             int64
	LogFile          string
	ContinueOnError  bool
	ExcludeColumns   map[string]struct{}
	PrintSQL         bool
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	PKPlans          map[string]pkGenerationPlan
}

type columnMeta struct {
	Name           string
	DataType       string
	Length         int64
	CharLength     int64
	CharDeclLength int64
	CharUsed       string
	Precision      sql.NullInt64
	Scale          sql.NullInt64
	Nullable       bool
	IsPK           bool
}

type stats struct {
	GeneratedRows uint64
	InsertedRows  uint64
	FailedRows    uint64
	OKBatches     uint64
	FailBatches   uint64
	Retries       uint64
	BatchRetries  uint64
	RowRetries    uint64
}

type pkGenerationPlan struct {
	Kind      string
	BaseInt   int64
	StepInt   int64
	BaseFloat float64
	StepFloat float64
	Prefix    string
	MaxLen    int
}

type batchSQLCache struct {
	mu       sync.RWMutex
	colNames []string
	tableRef string
	cache    map[int]string
}

func newBatchSQLCache(tableRef string, columns []columnMeta) *batchSQLCache {
	colNames := make([]string, 0, len(columns))
	for _, c := range columns {
		colNames = append(colNames, oracleIdentifier(c.Name))
	}
	return &batchSQLCache{
		colNames: colNames,
		tableRef: tableRef,
		cache:    make(map[int]string),
	}
}

func (c *batchSQLCache) get(rowCount int) string {
	c.mu.RLock()
	if sqlText, ok := c.cache[rowCount]; ok {
		c.mu.RUnlock()
		return sqlText
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if sqlText, ok := c.cache[rowCount]; ok {
		return sqlText
	}
	sqlText := buildInsertAllSQL(c.tableRef, c.colNames, rowCount)
	c.cache[rowCount] = sqlText
	return sqlText
}

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

func parseFlags() (config, error) {
	var cfg config
	var tableWithSchema string
	var excludeColumnsRaw string
	flag.StringVar(&cfg.DSN, "dsn", "", "Oracle DSN (godror format), e.g. user=\"u\" password=\"p\" connectString=\"127.0.0.1:1521/orclpdb\"")
	flag.StringVar(&cfg.Schema, "schema", "", "Oracle schema/owner")
	flag.StringVar(&cfg.Table, "table", "", "Oracle table name; can be TABLE or SCHEMA.TABLE")
	flag.StringVar(&tableWithSchema, "table-full", "", "Optional alias of table in SCHEMA.TABLE format")
	flag.Int64Var(&cfg.Rows, "rows", defaultRows, "Total number of rows to insert")
	flag.IntVar(&cfg.Workers, "workers", defaultWorkers, "Concurrent worker count")
	flag.IntVar(&cfg.BatchSize, "batch-size", defaultBatchSize, "Rows per batch INSERT ALL")
	flag.IntVar(&cfg.MaxRetries, "max-retries", defaultMaxRetries, "Retry count for failed batch inserts")
	flag.Float64Var(&cfg.NullRate, "null-rate", defaultNullRate, "NULL generation ratio for nullable columns (0~1)")
	progressSeconds := flag.Int("progress-interval", defaultProgressInterval, "Progress log interval in seconds")
	execTimeoutSeconds := flag.Int("exec-timeout", defaultExecTimeoutSec, "Per-batch execution timeout in seconds")
	flag.IntVar(&cfg.TimeRangeDays, "time-range-days", defaultTimeRangeDays, "Random datetime range in days counting backward from now")
	flag.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "Random seed")
	flag.StringVar(&cfg.LogFile, "log-file", "", "Optional log file path")
	flag.BoolVar(&cfg.ContinueOnError, "continue-on-error", true, "Continue loading when row or batch fails")
	flag.StringVar(&excludeColumnsRaw, "exclude-columns", "", "Comma-separated columns to skip, e.g. ID,CREATE_TIME")
	flag.BoolVar(&cfg.PrintSQL, "print-sql", false, "Print generated INSERT ALL SQL template")
	flag.IntVar(&cfg.MaxOpenConns, "db-max-open-conns", 0, "Database max open connections (0 means auto)")
	flag.IntVar(&cfg.MaxIdleConns, "db-max-idle-conns", 0, "Database max idle connections (0 means auto)")
	connLifeMinutes := flag.Int("db-conn-max-lifetime-minutes", defaultConnLifetimeMin, "Database connection max lifetime in minutes")
	flag.Parse()

	if tableWithSchema != "" {
		cfg.Table = tableWithSchema
	}
	if cfg.DSN == "" {
		return cfg, errors.New("dsn is required")
	}
	if cfg.Table == "" {
		return cfg, errors.New("table is required")
	}
	if cfg.Rows <= 0 {
		return cfg, errors.New("rows must be greater than 0")
	}
	if cfg.Workers <= 0 {
		return cfg, errors.New("workers must be greater than 0")
	}
	if cfg.BatchSize <= 0 {
		return cfg, errors.New("batch-size must be greater than 0")
	}
	if cfg.BatchSize > maxConfiguredBatchSize {
		return cfg, fmt.Errorf("batch-size must be <= %d", maxConfiguredBatchSize)
	}
	if cfg.MaxRetries < 0 {
		return cfg, errors.New("max-retries cannot be negative")
	}
	if cfg.NullRate < 0 || cfg.NullRate > 1 {
		return cfg, errors.New("null-rate must be in [0,1]")
	}
	if *progressSeconds <= 0 {
		return cfg, errors.New("progress-interval must be greater than 0")
	}
	if *execTimeoutSeconds <= 0 {
		return cfg, errors.New("exec-timeout must be greater than 0")
	}
	if cfg.TimeRangeDays <= 0 {
		return cfg, errors.New("time-range-days must be greater than 0")
	}
	if cfg.MaxOpenConns < 0 {
		return cfg, errors.New("db-max-open-conns cannot be negative")
	}
	if cfg.MaxIdleConns < 0 {
		return cfg, errors.New("db-max-idle-conns cannot be negative")
	}
	if *connLifeMinutes <= 0 {
		return cfg, errors.New("db-conn-max-lifetime-minutes must be greater than 0")
	}
	cfg.ProgressInterval = time.Duration(*progressSeconds) * time.Second
	cfg.ExecTimeout = time.Duration(*execTimeoutSeconds) * time.Second
	cfg.ConnMaxLifetime = time.Duration(*connLifeMinutes) * time.Minute

	schema, table := normalizeSchemaAndTable(cfg.Schema, cfg.Table)
	if schema == "" || table == "" {
		return cfg, errors.New("unable to resolve schema/table, use -schema and -table or -table SCHEMA.TABLE")
	}
	cfg.Schema, cfg.Table = schema, table
	cfg.ExcludeColumns = parseExcludeColumns(excludeColumnsRaw)
	return cfg, nil
}

func normalizeSchemaAndTable(schema, table string) (string, string) {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if table == "" {
		return "", ""
	}
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		schema = parts[0]
		table = parts[1]
	}
	schema = strings.Trim(schema, `"`)
	table = strings.Trim(table, `"`)
	return strings.ToUpper(schema), strings.ToUpper(table)
}

func parseExcludeColumns(raw string) map[string]struct{} {
	result := make(map[string]struct{})
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result
	}
	for _, item := range strings.Split(raw, ",") {
		name := strings.ToUpper(strings.TrimSpace(item))
		if name != "" {
			result[name] = struct{}{}
		}
	}
	return result
}

func initLogger(path string) (io.Closer, error) {
	if strings.TrimSpace(path) == "" {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	log.SetOutput(io.MultiWriter(os.Stdout, f))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return f, nil
}

func loadTableColumns(ctx context.Context, db *sql.DB, schema, table string) ([]columnMeta, error) {
	const sqlText = `
SELECT
  column_name,
  data_type,
  data_length,
  char_length,
  char_col_decl_length,
  char_used,
  data_precision,
  data_scale,
  nullable
FROM all_tab_columns
WHERE owner = :1
  AND table_name = :2
ORDER BY column_id`
	rows, err := db.QueryContext(ctx, sqlText, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []columnMeta
	for rows.Next() {
		var c columnMeta
		var nullable string
		var charLength sql.NullInt64
		var charDeclLength sql.NullInt64
		var charUsed sql.NullString
		if err := rows.Scan(&c.Name, &c.DataType, &c.Length, &charLength, &charDeclLength, &charUsed, &c.Precision, &c.Scale, &nullable); err != nil {
			return nil, err
		}
		c.Name = strings.ToUpper(strings.TrimSpace(c.Name))
		c.DataType = strings.ToUpper(strings.TrimSpace(c.DataType))
		if charLength.Valid {
			c.CharLength = charLength.Int64
		}
		if charDeclLength.Valid {
			c.CharDeclLength = charDeclLength.Int64
		}
		if charUsed.Valid {
			c.CharUsed = strings.ToUpper(strings.TrimSpace(charUsed.String))
		}
		c.Nullable = strings.EqualFold(nullable, "Y")
		columns = append(columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func loadPrimaryKeyColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	const sqlText = `
SELECT cols.column_name
FROM all_constraints cons
JOIN all_cons_columns cols
  ON cons.owner = cols.owner
 AND cons.constraint_name = cols.constraint_name
 AND cons.table_name = cols.table_name
WHERE cons.owner = :1
  AND cons.table_name = :2
  AND cons.constraint_type = 'P'
ORDER BY cols.position`
	rows, err := db.QueryContext(ctx, sqlText, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		result = append(result, strings.ToUpper(strings.TrimSpace(col)))
	}
	return result, rows.Err()
}

func buildPrimaryKeyPlans(ctx context.Context, db *sql.DB, schema, table string, columns []columnMeta, totalRows int64, seed int64) (map[string]pkGenerationPlan, error) {
	plans := make(map[string]pkGenerationPlan)
	tableRef := fmt.Sprintf("%s.%s", oracleIdentifier(schema), oracleIdentifier(table))
	r := rand.New(rand.NewSource(seed ^ 0x5A17C3))

	for _, c := range columns {
		if !c.IsPK {
			continue
		}
		dataType := strings.ToUpper(strings.TrimSpace(c.DataType))
		scale := int64(0)
		if c.Scale.Valid {
			scale = c.Scale.Int64
		}

		switch {
		case strings.HasPrefix(dataType, "NUMBER"):
			if scale <= 0 {
				maxVal, err := queryMaxInt64PK(ctx, db, tableRef, c.Name)
				if err != nil {
					return nil, fmt.Errorf("pk column %s max-value query failed: %w", c.Name, err)
				}
				step := int64(1 + r.Intn(17))
				if step%2 == 0 {
					step++
				}
				randomGap := int64(1 + r.Intn(1000))
				base, overflow := safeAddInt64(maxVal, randomGap)
				if overflow {
					return nil, fmt.Errorf("pk column %s base overflow, max existing value=%d", c.Name, maxVal)
				}
				if totalRows > 0 {
					last, ov := safeMulAddInt64(base, step, totalRows-1)
					if ov {
						return nil, fmt.Errorf("pk column %s value range overflows int64 (base=%d step=%d rows=%d)", c.Name, base, step, totalRows)
					}
					_ = last
				}
				plans[c.Name] = pkGenerationPlan{
					Kind:    "number_int",
					BaseInt: base,
					StepInt: step,
				}
				log.Printf("PK plan: column=%s mode=number_int base=%d step=%d", c.Name, base, step)
			} else {
				scaleDigits := int(minInt64(scale, 9))
				stepFloat := 1.0 / math.Pow10(scaleDigits)
				maxVal, err := queryMaxFloat64PK(ctx, db, tableRef, c.Name)
				if err != nil {
					return nil, fmt.Errorf("pk column %s max-value query failed: %w", c.Name, err)
				}
				base := maxVal + stepFloat*float64(1+r.Intn(100))
				plans[c.Name] = pkGenerationPlan{
					Kind:      "number_float",
					BaseFloat: base,
					StepFloat: stepFloat,
				}
				log.Printf("PK plan: column=%s mode=number_float base=%f step=%f", c.Name, base, stepFloat)
			}
		case strings.Contains(dataType, "CHAR"):
			maxLen, _ := stringLengthLimit(c)
			if maxLen <= 0 {
				maxLen = 32
			}
			prefix := fmt.Sprintf("PK%s", strings.ToUpper(strconv.FormatInt(seed^int64(len(c.Name)), 36)))
			plans[c.Name] = pkGenerationPlan{
				Kind:   "string",
				Prefix: prefix,
				MaxLen: maxLen,
			}
			log.Printf("PK plan: column=%s mode=string prefix=%s max_len=%d", c.Name, prefix, maxLen)
		case strings.HasPrefix(dataType, "FLOAT"), dataType == "BINARY_FLOAT", dataType == "BINARY_DOUBLE":
			maxVal, err := queryMaxFloat64PK(ctx, db, tableRef, c.Name)
			if err != nil {
				return nil, fmt.Errorf("pk column %s max-value query failed: %w", c.Name, err)
			}
			step := 0.0001
			base := maxVal + step*float64(1+r.Intn(1000))
			plans[c.Name] = pkGenerationPlan{
				Kind:      "float",
				BaseFloat: base,
				StepFloat: step,
			}
			log.Printf("PK plan: column=%s mode=float base=%f step=%f", c.Name, base, step)
		default:
			log.Printf("WARN pk column=%s has unsupported type for precomputed uniqueness plan (%s), using fallback generator", c.Name, dataType)
		}
	}
	return plans, nil
}

func queryMaxInt64PK(ctx context.Context, db *sql.DB, tableRef, columnName string) (int64, error) {
	query := fmt.Sprintf("SELECT TO_CHAR(MAX(%s)) FROM %s", oracleIdentifier(columnName), tableRef)
	var maxStr sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&maxStr); err != nil {
		return 0, err
	}
	if !maxStr.Valid || strings.TrimSpace(maxStr.String) == "" {
		return 0, nil
	}
	raw := strings.TrimSpace(maxStr.String)
	raw = strings.ReplaceAll(raw, ",", "")
	if dot := strings.IndexByte(raw, '.'); dot >= 0 {
		raw = raw[:dot]
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int64 from %q: %w", maxStr.String, err)
	}
	return v, nil
}

func queryMaxFloat64PK(ctx context.Context, db *sql.DB, tableRef, columnName string) (float64, error) {
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", oracleIdentifier(columnName), tableRef)
	var maxVal sql.NullFloat64
	if err := db.QueryRowContext(ctx, query).Scan(&maxVal); err != nil {
		return 0, err
	}
	if !maxVal.Valid {
		return 0, nil
	}
	return maxVal.Float64, nil
}

func safeAddInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, true
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, true
	}
	return a + b, false
}

func safeMulAddInt64(base, step, n int64) (int64, bool) {
	if n < 0 {
		return base, false
	}
	if step != 0 && n > math.MaxInt64/step {
		return 0, true
	}
	prod := step * n
	return safeAddInt64(base, prod)
}

func filterInsertColumns(cols []columnMeta, pkSet, excludeSet map[string]struct{}) ([]columnMeta, []string, error) {
	insertCols := make([]columnMeta, 0, len(cols))
	skipped := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, ok := pkSet[c.Name]; ok {
			c.IsPK = true
		}
		if _, excluded := excludeSet[c.Name]; excluded {
			skipped = append(skipped, fmt.Sprintf("Column %s excluded by user option", c.Name))
			continue
		}
		if isSupportedDataType(c.DataType) {
			insertCols = append(insertCols, c)
			continue
		}
		if c.Nullable {
			skipped = append(skipped, fmt.Sprintf("Column %s (%s) unsupported and nullable, skipped", c.Name, c.DataType))
			continue
		}
		return nil, skipped, fmt.Errorf("column %s has unsupported type %s and is NOT NULL; use -exclude-columns if DB default can fill it", c.Name, c.DataType)
	}
	return insertCols, skipped, nil
}

func isSupportedDataType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	switch {
	case strings.HasPrefix(t, "NUMBER"),
		strings.HasPrefix(t, "FLOAT"),
		t == "BINARY_FLOAT",
		t == "BINARY_DOUBLE",
		strings.Contains(t, "CHAR"),
		strings.HasPrefix(t, "DATE"),
		strings.HasPrefix(t, "TIMESTAMP"),
		strings.HasSuffix(t, "CLOB"),
		strings.HasSuffix(t, "BLOB"),
		strings.HasPrefix(t, "RAW"),
		strings.HasPrefix(t, "LONG RAW"):
		return true
	default:
		return false
	}
}

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

func generateRow(columns []columnMeta, rowSeq uint64, r *rand.Rand, nullRate float64, timeRangeDays int, pkPlans map[string]pkGenerationPlan) ([]interface{}, error) {
	vals := make([]interface{}, 0, len(columns))
	for _, c := range columns {
		if c.Nullable && !c.IsPK && r.Float64() < nullRate {
			vals = append(vals, nil)
			continue
		}
		v, err := generateValue(c, rowSeq, r, timeRangeDays, pkPlans)
		if err != nil {
			return nil, fmt.Errorf("column=%s type=%s: %w", c.Name, c.DataType, err)
		}
		normalizedValue, changed, changeDetail := normalizeColumnValue(c, v)
		if changed {
			log.Printf("WARN row-seq=%d column=%s normalized value: %s", rowSeq, c.Name, changeDetail)
		}
		vals = append(vals, normalizedValue)
	}
	return vals, nil
}

func generateValue(c columnMeta, rowSeq uint64, r *rand.Rand, timeRangeDays int, pkPlans map[string]pkGenerationPlan) (interface{}, error) {
	t := strings.ToUpper(strings.TrimSpace(c.DataType))
	if c.IsPK {
		plan := pkPlans[c.Name]
		return generatePrimaryKeyValue(c, rowSeq, r, plan), nil
	}

	switch {
	case strings.HasPrefix(t, "NUMBER"):
		return randomNumberValue(c, rowSeq, r), nil
	case strings.HasPrefix(t, "FLOAT"), t == "BINARY_FLOAT", t == "BINARY_DOUBLE":
		return randomFloatValue(c, rowSeq, r), nil
	case strings.Contains(t, "CHAR"):
		return randomStringValue(c, rowSeq, r), nil
	case strings.HasPrefix(t, "DATE"):
		return randomDateTimeValue(r, timeRangeDays), nil
	case strings.HasPrefix(t, "TIMESTAMP"):
		return randomDateTimeValue(r, timeRangeDays), nil
	case strings.HasSuffix(t, "CLOB"):
		return randomParagraph(randomLength(r, 32, 200), r), nil
	case strings.HasSuffix(t, "BLOB"), strings.HasPrefix(t, "RAW"), strings.HasPrefix(t, "LONG RAW"):
		return randomBytesValue(c, r), nil
	default:
		return nil, fmt.Errorf("unsupported type")
	}
}

func generatePrimaryKeyValue(c columnMeta, rowSeq uint64, r *rand.Rand, plan pkGenerationPlan) interface{} {
	switch plan.Kind {
	case "number_int":
		v, overflow := safeMulAddInt64(plan.BaseInt, plan.StepInt, int64(rowSeq-1))
		if overflow {
			// Keep runtime alive even when reaching edge value; fallback to random positive int64.
			return int64(rowSeq + uint64(r.Int63n(1000000)))
		}
		return v
	case "number_float":
		return plan.BaseFloat + float64(rowSeq-1)*plan.StepFloat
	case "float":
		return plan.BaseFloat + float64(rowSeq-1)*plan.StepFloat
	case "string":
		return buildUniqueStringPK(plan.Prefix, rowSeq, plan.MaxLen)
	default:
		// Fallback to existing generator when plan is not available.
		t := strings.ToUpper(strings.TrimSpace(c.DataType))
		switch {
		case strings.HasPrefix(t, "NUMBER"):
			if c.Scale.Valid && c.Scale.Int64 > 0 {
				base := float64(rowSeq)
				frac := float64(r.Intn(99)) / 100.0
				return base + frac
			}
			return int64(rowSeq)
		case strings.Contains(t, "CHAR"):
			maxLen, byteSemantic := stringLengthLimit(c)
			if maxLen <= 0 {
				maxLen = 32
			}
			value := buildUniqueStringPK("PK", rowSeq, maxLen)
			if byteSemantic {
				return truncateStringByBytes(value, maxLen)
			}
			return truncateString(value, maxLen)
		case strings.HasPrefix(t, "FLOAT"), t == "BINARY_FLOAT", t == "BINARY_DOUBLE":
			return float64(rowSeq) + (r.Float64() * 0.0001)
		default:
			return int64(rowSeq)
		}
	}
}

func buildUniqueStringPK(prefix string, rowSeq uint64, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	seq := strings.ToUpper(strconv.FormatUint(rowSeq, 36))
	value := prefix + seq
	if len(value) <= maxLen {
		return value
	}
	// keep the sequence tail to maintain uniqueness under truncation.
	if maxLen <= len(seq) {
		return seq[len(seq)-maxLen:]
	}
	headLen := maxLen - len(seq)
	if headLen < 0 {
		headLen = 0
	}
	if headLen > len(prefix) {
		headLen = len(prefix)
	}
	return prefix[:headLen] + seq
}

func randomNumberValue(c columnMeta, rowSeq uint64, r *rand.Rand) interface{} {
	scale := int64(0)
	if c.Scale.Valid {
		scale = c.Scale.Int64
	}
	precision := int64(0)
	if c.Precision.Valid {
		precision = c.Precision.Int64
	}

	if c.IsPK {
		if scale > 0 {
			base := float64(rowSeq)
			frac := float64(r.Intn(99)) / 100.0
			return base + frac
		}
		return int64(rowSeq)
	}

	if precision <= 0 {
		if scale > 0 {
			return randomScaledFloat(r, scale, 1000000)
		}
		n := r.Int63()
		if r.Intn(10) < 2 {
			n = -n
		}
		return n
	}

	if scale <= 0 {
		maxDigits := minInt64(precision, 18)
		maxV := int64(math.Pow10(int(maxDigits))) - 1
		if maxV <= 0 {
			maxV = 1000000
		}
		n := r.Int63n(maxV + 1)
		if r.Intn(10) < 2 {
			n = -n
		}
		return n
	}

	intDigits := precision - scale
	if intDigits <= 0 {
		intDigits = 1
	}
	maxInt := int64(math.Pow10(int(minInt64(intDigits, 12)))) - 1
	if maxInt <= 0 {
		maxInt = 100000
	}
	intPart := float64(r.Int63n(maxInt + 1))
	fracDigits := int(minInt64(scale, 9))
	fracMax := pow10Int64(fracDigits) - 1
	fracPart := float64(0)
	if fracMax > 0 {
		fracPart = float64(r.Int63n(fracMax+1)) / float64(fracMax+1)
	}
	sign := 1.0
	if r.Intn(10) < 2 {
		sign = -1.0
	}
	value := sign * (intPart + fracPart)
	return roundFloat(value, fracDigits)
}

func randomFloatValue(c columnMeta, rowSeq uint64, r *rand.Rand) interface{} {
	if c.IsPK {
		return float64(rowSeq) + (r.Float64() * 0.0001)
	}
	base := (r.Float64()*2 - 1) * 1000000
	return math.Round(base*1000000) / 1000000
}

func randomStringValue(c columnMeta, rowSeq uint64, r *rand.Rand) interface{} {
	length, byteSemantic := stringLengthLimit(c)
	if length <= 0 {
		length = 32
	}
	trim := func(in string) string {
		if byteSemantic {
			return truncateStringByBytes(in, length)
		}
		return truncateString(in, length)
	}
	if c.IsPK {
		s := fmt.Sprintf("K%020d", rowSeq)
		return trim(s)
	}

	if length < 10 {
		return trim(randomFirstName(r))
	}
	if length < 30 {
		full := randomFirstName(r) + " " + randomLastName(r)
		return trim(full)
	}
	return trim(randomParagraph(randomLength(r, 20, 100), r))
}

func randomDateTimeValue(r *rand.Rand, rangeDays int) time.Time {
	if rangeDays <= 0 {
		rangeDays = defaultTimeRangeDays
	}
	end := time.Now()
	start := end.AddDate(0, 0, -rangeDays)
	delta := end.Unix() - start.Unix()
	randomSec := r.Int63n(delta + 1)
	return time.Unix(start.Unix()+randomSec, 0)
}

func randomBytesValue(c columnMeta, r *rand.Rand) []byte {
	maxLen := int(c.Length)
	if maxLen <= 0 {
		maxLen = 64
	}
	maxLen = minInt(maxLen, 128)
	minLen := 1
	if maxLen >= 8 {
		minLen = 8
	}
	size := randomLength(r, minLen, maxLen)
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

func normalizeColumnValue(c columnMeta, value interface{}) (interface{}, bool, string) {
	if value == nil {
		return nil, false, ""
	}
	dataType := strings.ToUpper(strings.TrimSpace(c.DataType))
	if strings.Contains(dataType, "CHAR") {
		s, ok := value.(string)
		if !ok {
			return value, false, ""
		}
		limit, byteSemantic := stringLengthLimit(c)
		if limit <= 0 {
			return value, false, ""
		}
		normalized := s
		if byteSemantic {
			normalized = truncateStringByBytes(s, limit)
			if len(normalized) != len(s) {
				return normalized, true, fmt.Sprintf("byte-length %d -> %d (limit=%d)", len(s), len(normalized), limit)
			}
			return normalized, false, ""
		}
		normalized = truncateString(s, limit)
		if utf8.RuneCountInString(normalized) != utf8.RuneCountInString(s) {
			return normalized, true, fmt.Sprintf("char-length %d -> %d (limit=%d)", utf8.RuneCountInString(s), utf8.RuneCountInString(normalized), limit)
		}
		return normalized, false, ""
	}

	if strings.HasPrefix(dataType, "RAW") || strings.HasPrefix(dataType, "LONG RAW") {
		raw, ok := value.([]byte)
		if !ok || c.Length <= 0 {
			return value, false, ""
		}
		limit := int(c.Length)
		if len(raw) <= limit {
			return value, false, ""
		}
		normalized := make([]byte, limit)
		copy(normalized, raw[:limit])
		return normalized, true, fmt.Sprintf("raw length %d -> %d (limit=%d)", len(raw), len(normalized), limit)
	}

	return value, false, ""
}

func formatRowForLog(columns []columnMeta, row []interface{}) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i := range row {
		if i > 0 {
			sb.WriteString(", ")
		}
		colName := fmt.Sprintf("col%d", i+1)
		if i < len(columns) {
			colName = columns[i].Name
		}
		sb.WriteString(colName)
		sb.WriteByte('=')
		sb.WriteString(formatValueForLog(row[i]))
	}
	sb.WriteByte('}')
	return sb.String()
}

func formatValueForLog(value interface{}) string {
	if value == nil {
		return "<nil>"
	}
	switch v := value.(type) {
	case string:
		s := v
		if utf8.RuneCountInString(s) > 40 {
			s = truncateString(s, 40) + "..."
		}
		return strconv.Quote(s)
	case []byte:
		if len(v) == 0 {
			return "0x"
		}
		maxLen := minInt(len(v), 16)
		return fmt.Sprintf("0x%X(len=%d)", v[:maxLen], len(v))
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func buildInsertAllSQL(tableRef string, colNames []string, rowCount int) string {
	var sb strings.Builder
	sb.Grow(rowCount * (len(colNames)*8 + 64))
	sb.WriteString("INSERT ALL")
	argPos := 1
	for i := 0; i < rowCount; i++ {
		sb.WriteString("\n  INTO ")
		sb.WriteString(tableRef)
		sb.WriteString(" (")
		sb.WriteString(strings.Join(colNames, ","))
		sb.WriteString(") VALUES (")
		for j := range colNames {
			if j > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(":")
			sb.WriteString(strconv.Itoa(argPos))
			argPos++
		}
		sb.WriteString(")")
	}
	sb.WriteString("\nSELECT 1 FROM DUAL")
	return sb.String()
}

func oracleIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return trimmed
	}
	if strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"") {
		return trimmed
	}
	if simpleOracleIdentifierPattern.MatchString(trimmed) {
		return strings.ToUpper(trimmed)
	}
	return `"` + strings.ReplaceAll(trimmed, `"`, `""`) + `"`
}

func monitorProgress(cfg config, st *stats, total int64, start time.Time, done <-chan struct{}) {
	ticker := time.NewTicker(cfg.ProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			generated := atomic.LoadUint64(&st.GeneratedRows)
			inserted := atomic.LoadUint64(&st.InsertedRows)
			failed := atomic.LoadUint64(&st.FailedRows)
			okBatches := atomic.LoadUint64(&st.OKBatches)
			failBatches := atomic.LoadUint64(&st.FailBatches)
			retries := atomic.LoadUint64(&st.Retries)
			batchRetries := atomic.LoadUint64(&st.BatchRetries)
			rowRetries := atomic.LoadUint64(&st.RowRetries)
			elapsed := time.Since(start).Seconds()
			if elapsed <= 0 {
				elapsed = 1
			}
			processed := inserted + failed
			progress := 100.0 * float64(processed) / float64(total)
			if progress > 100 {
				progress = 100
			}
			rate := float64(inserted) / elapsed
			log.Printf("progress=%.2f%% generated=%d inserted=%d failed=%d ok_batches=%d fail_batches=%d retries=%d(batch=%d,row=%d) rate=%.1f rows/s",
				progress, generated, inserted, failed, okBatches, failBatches, retries, batchRetries, rowRetries, rate)
		}
	}
}

func printSummary(cfg config, st *stats, start time.Time, runErr error) {
	generated := atomic.LoadUint64(&st.GeneratedRows)
	inserted := atomic.LoadUint64(&st.InsertedRows)
	failed := atomic.LoadUint64(&st.FailedRows)
	okBatches := atomic.LoadUint64(&st.OKBatches)
	failBatches := atomic.LoadUint64(&st.FailBatches)
	retries := atomic.LoadUint64(&st.Retries)
	batchRetries := atomic.LoadUint64(&st.BatchRetries)
	rowRetries := atomic.LoadUint64(&st.RowRetries)
	elapsed := time.Since(start)
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1
	}

	log.Printf("========== oracle-random-data-load summary ==========")
	log.Printf("target: %s.%s", cfg.Schema, cfg.Table)
	log.Printf("rows target=%d generated=%d inserted=%d failed=%d", cfg.Rows, generated, inserted, failed)
	log.Printf("batches ok=%d failed=%d retries=%d(batch=%d,row=%d)", okBatches, failBatches, retries, batchRetries, rowRetries)
	log.Printf("elapsed=%s throughput=%.1f rows/s", elapsed.Truncate(time.Millisecond).String(), float64(inserted)/sec)
	if runErr != nil {
		log.Printf("result=FAILED error=%v", runErr)
	} else if failed > 0 {
		log.Printf("result=PARTIAL_SUCCESS continue_on_error=true")
	} else {
		log.Printf("result=SUCCESS")
	}
}

func nonBlockingSendErr(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}

func applyDBPoolSettings(db *sql.DB, cfg config) {
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = maxInt(cfg.Workers*2, 8)
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = maxInt(cfg.Workers, 4)
	}
	if maxIdle > maxOpen {
		log.Printf("WARN db-max-idle-conns (%d) > db-max-open-conns (%d), auto-adjusting idle to %d", maxIdle, maxOpen, maxOpen)
		maxIdle = maxOpen
	}
	connLifetime := cfg.ConnMaxLifetime
	if connLifetime <= 0 {
		connLifetime = time.Duration(defaultConnLifetimeMin) * time.Minute
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connLifetime)
}

func capBatchSizeByBindLimit(batchSize, columnCount int) (int, error) {
	if columnCount <= 0 {
		return batchSize, nil
	}
	if columnCount > oracleMaxBindVariables {
		return 0, fmt.Errorf("column count %d exceeds Oracle bind-variable limit %d", columnCount, oracleMaxBindVariables)
	}
	maxBatchByBind := oracleMaxBindVariables / columnCount
	if maxBatchByBind < 1 {
		maxBatchByBind = 1
	}
	if batchSize > maxBatchByBind {
		return maxBatchByBind, nil
	}
	return batchSize, nil
}

func randomLength(r *rand.Rand, minV, maxV int) int {
	if maxV < minV {
		maxV = minV
	}
	return minV + r.Intn(maxV-minV+1)
}

func truncateString(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen])
}

func truncateStringByBytes(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	idx := 0
	for idx < len(s) {
		_, size := utf8.DecodeRuneInString(s[idx:])
		if size <= 0 {
			break
		}
		if idx+size > maxBytes {
			break
		}
		idx += size
	}
	return s[:idx]
}

func randomParagraph(length int, r *rand.Rand) string {
	words := []string{
		"alpha", "beta", "gamma", "delta", "omega", "oracle", "mysql",
		"random", "loader", "batch", "worker", "schema", "table", "column",
		"checksum", "sample", "vector", "signal", "storage", "engine",
		"thread", "commit", "rollback", "window", "stream", "profile",
		"index", "latency", "throughput", "segment", "planner", "cursor",
		"snapshot", "cluster", "consistency", "durability", "replica", "query",
		"partition", "transaction", "recover", "archive", "metrics", "memory",
		"compression", "channel", "pipeline", "adapter", "runtime", "scheduler",
	}
	var b strings.Builder
	for b.Len() < length {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[r.Intn(len(words))])
	}
	return truncateString(b.String(), length)
}

func randomFirstName(r *rand.Rand) string {
	first := []string{
		"Olivia", "Liam", "Emma", "Noah", "Ava", "Lucas", "Mia", "Ethan", "Ivy", "Mason",
		"Sophia", "Logan", "Amelia", "James", "Harper", "Benjamin", "Ella", "Henry", "Luna", "Jack",
		"Grace", "Owen", "Chloe", "Levi", "Nora", "Elijah", "Zoe", "Daniel", "Aria", "Isaac",
	}
	return first[r.Intn(len(first))]
}

func randomLastName(r *rand.Rand) string {
	last := []string{
		"Smith", "Johnson", "Brown", "Davis", "Wilson", "Taylor", "Anderson", "Thomas", "Clark", "Moore",
		"Martin", "Lee", "Perez", "White", "Harris", "Sanchez", "Allen", "Young", "King", "Wright",
		"Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts", "Carter",
	}
	return last[r.Intn(len(last))]
}

func stringLengthLimit(c columnMeta) (maxLen int, byteSemantic bool) {
	dataType := strings.ToUpper(strings.TrimSpace(c.DataType))
	if strings.HasPrefix(dataType, "NCHAR") || strings.HasPrefix(dataType, "NVARCHAR2") {
		if c.CharDeclLength > 0 {
			return int(c.CharDeclLength), false
		}
		if c.CharLength > 0 {
			return int(c.CharLength), false
		}
		if c.Length > 0 {
			// In Oracle, NCHAR/NVARCHAR2 data_length may be bytes. 11g metadata can
			// occasionally miss char_length; fall back to byte/2 for UTF-16 storage.
			estimatedChars := int(c.Length / 2)
			if estimatedChars > 0 {
				return estimatedChars, false
			}
			return int(c.Length), false
		}
		return 32, false
	}

	if strings.Contains(dataType, "CHAR") {
		if c.CharUsed == "C" {
			if c.CharDeclLength > 0 {
				return int(c.CharDeclLength), false
			}
			if c.CharLength > 0 {
				return int(c.CharLength), false
			}
			if c.Length > 0 {
				return int(c.Length), false
			}
		}
		if c.Length > 0 {
			return int(c.Length), true
		}
		if c.CharLength > 0 {
			return int(c.CharLength), false
		}
		return 32, false
	}

	if c.Length > 0 {
		return int(c.Length), true
	}
	return 32, true
}

func pow10Int64(digits int) int64 {
	if digits <= 0 {
		return 1
	}
	if digits > 18 {
		digits = 18
	}
	result := int64(1)
	for i := 0; i < digits; i++ {
		result *= 10
	}
	return result
}

func roundFloat(value float64, scale int) float64 {
	if scale <= 0 {
		return math.Round(value)
	}
	factor := math.Pow10(scale)
	return math.Round(value*factor) / factor
}

func randomScaledFloat(r *rand.Rand, scale int64, maxAbs float64) float64 {
	effectiveScale := int(scale)
	if effectiveScale < 0 {
		effectiveScale = 0
	}
	if effectiveScale > 9 {
		effectiveScale = 9
	}
	sign := 1.0
	if r.Intn(10) < 2 {
		sign = -1.0
	}
	value := sign * (r.Float64() * maxAbs)
	return roundFloat(value, effectiveScale)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
