package main

import (
	"sync"
	"time"
)

// Config stores configuration information
type Config struct {
	DstDSN       string
	ParallelThds int
	FixFileDir   string
	LogFile      string
	LogBin       bool   // true = keep sql_log_bin ON (default); false = SET sql_log_bin=0 per connection
	ResultFile   string // custom output path for CSV report, empty = use default
	// SSL fields
	SslCa   string
	SslCert string
	SslKey  string
	SslMode string
}

// FixSQLStatistics stores statistics about fix SQL files
type FixSQLStatistics struct {
	// Basic statistics
	TotalFiles int
	TotalSize  int64

	// Classification statistics
	DeleteFiles  int
	TableFiles   int
	ViewFiles    int
	RoutineFiles int
	TriggerFiles int
	UnknownFiles int

	// Data mode statistics
	TableCount int
	InsertRows int64
	UpdateRows int64
	DeleteRows int64

	// Struct/routine/trigger mode statistics
	DropCount   int
	AlterCount  int
	CreateCount int

	// Binlog estimation
	EstimatedBinlogSize int64
}

// unitExecOutcome captures statement-level execution results for a single execution unit.
type unitExecOutcome struct {
	InsertSuccess int64
	InsertFailure int64
	DeleteSuccess int64
	DeleteFailure int64
	AlterSuccess  int64
	AlterFailure  int64
	CreateSuccess int64
	CreateFailure int64
	DropSuccess   int64
	DropFailure   int64
}

func (o *unitExecOutcome) add(other unitExecOutcome) {
	o.InsertSuccess += other.InsertSuccess
	o.InsertFailure += other.InsertFailure
	o.DeleteSuccess += other.DeleteSuccess
	o.DeleteFailure += other.DeleteFailure
	o.AlterSuccess += other.AlterSuccess
	o.AlterFailure += other.AlterFailure
	o.CreateSuccess += other.CreateSuccess
	o.CreateFailure += other.CreateFailure
	o.DropSuccess += other.DropSuccess
	o.DropFailure += other.DropFailure
}

// FileExecResult contains the execution result of a single SQL file.
type FileExecResult struct {
	FilePath   string
	Schema     string
	ObjectName string
	Stage      string // DELETE, TABLE, VIEW, ROUTINE, TRIGGER, UNKNOWN
	ObjectType string // table, view, procedure, function, trigger, unknown

	InsertSuccess int64
	InsertFailure int64
	DeleteSuccess int64
	DeleteFailure int64
	AlterSuccess  int64
	AlterFailure  int64
	CreateSuccess int64
	CreateFailure int64
	DropSuccess   int64
	DropFailure   int64

	Elapsed     time.Duration
	ErrorReason string // empty = success
}

// mergeFromOutcome aggregates a unitExecOutcome into this file-level result.
func (r *FileExecResult) mergeFromOutcome(o unitExecOutcome) {
	r.InsertSuccess += o.InsertSuccess
	r.InsertFailure += o.InsertFailure
	r.DeleteSuccess += o.DeleteSuccess
	r.DeleteFailure += o.DeleteFailure
	r.AlterSuccess += o.AlterSuccess
	r.AlterFailure += o.AlterFailure
	r.CreateSuccess += o.CreateSuccess
	r.CreateFailure += o.CreateFailure
	r.DropSuccess += o.DropSuccess
	r.DropFailure += o.DropFailure
}

// ExecSummary aggregates execution statistics across all files.
type ExecSummary struct {
	TotalFiles      int
	SuccessFiles    int
	FailureFiles    int
	TotalInsertOk   int64
	TotalInsertFail int64
	TotalDeleteOk   int64
	TotalDeleteFail int64
	TotalAlterOk    int64
	TotalAlterFail  int64
	TotalCreateOk   int64
	TotalCreateFail int64
	TotalDropOk     int64
	TotalDropFail   int64
}

// resultCollector provides thread-safe collection of FileExecResult.
type resultCollector struct {
	mu      sync.Mutex
	results []FileExecResult
}

func (c *resultCollector) append(r FileExecResult) {
	c.mu.Lock()
	c.results = append(c.results, r)
	c.mu.Unlock()
}

func (c *resultCollector) snapshot() []FileExecResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]FileExecResult, len(c.results))
	copy(out, c.results)
	return out
}
