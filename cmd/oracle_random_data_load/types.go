package main

import (
	"database/sql"
	"sync"
	"time"
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
