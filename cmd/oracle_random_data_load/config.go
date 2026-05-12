package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

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
