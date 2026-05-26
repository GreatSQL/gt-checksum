package progress

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Status constants for progress files.
const (
	StatusRunning   = "running"
	StatusCompleted = "completed"
)

// TableChunkProgress tracks chunk-level progress for a single table.
// CompletedChunks is a sorted list of beginSeq values that have been fully queried.
type TableChunkProgress struct {
	TotalRows       int64   `json:"total_rows"`
	ChunkSize       int     `json:"chunk_size"`
	CompletedChunks []int64 `json:"completed_chunks"`
	CheckingChunks  []int64 `json:"checking_chunks,omitempty"`
	CompletedFixSQL []int64 `json:"completed_fixsql,omitempty"`
}

// ChecksumTableResult stores the terminal/report row for a completed data table.
// It intentionally mirrors only stable, user-facing result fields so progress files
// remain decoupled from the actions.Pod implementation.
type ChecksumTableResult struct {
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	IndexColumn string `json:"index_column"`
	CheckObject string `json:"check_object"`
	Rows        string `json:"rows"`
	Diffs       string `json:"diffs"`
	Datafix     string `json:"datafix"`
	MappingInfo string `json:"mapping_info,omitempty"`
	ColumnsInfo string `json:"columns_info,omitempty"`
}

// ChecksumProgress tracks which tables have been verified for gt-checksum.
type ChecksumProgress struct {
	mu sync.Mutex

	RunID                 string                         `json:"run_id"`
	StartTime             string                         `json:"start_time"`
	ConfigHash            string                         `json:"config_hash"`
	CompletedTables       []string                       `json:"completed_tables"`
	CompletedTableResults []ChecksumTableResult          `json:"completed_table_results,omitempty"`
	TableProgress         map[string]*TableChunkProgress `json:"table_progress,omitempty"`
	Status                string                         `json:"status"`

	// filePath is the on-disk location (not serialized).
	filePath string `json:"-"`
}

// RepairProgress tracks which SQL files have been executed for repairDB.
type RepairProgress struct {
	mu sync.Mutex

	CreatedAt  string            `json:"created_at"`
	FixFileDir string            `json:"fix_file_dir"`
	FileStates map[string]string `json:"file_states"` // filename -> "success" | "failed"
	Status     string            `json:"status"`

	// filePath is the on-disk location (not serialized).
	filePath string `json:"-"`
}

// NewChecksumProgress creates a new ChecksumProgress with the given metadata.
func NewChecksumProgress(runID, configHash, filePath string) *ChecksumProgress {
	return &ChecksumProgress{
		RunID:                 runID,
		StartTime:             time.Now().Format(time.RFC3339),
		ConfigHash:            configHash,
		CompletedTables:       make([]string, 0),
		CompletedTableResults: make([]ChecksumTableResult, 0),
		TableProgress:         make(map[string]*TableChunkProgress),
		Status:                StatusRunning,
		filePath:              filePath,
	}
}

// NewRepairProgress creates a new RepairProgress with the given metadata.
func NewRepairProgress(fixFileDir, filePath string) *RepairProgress {
	return &RepairProgress{
		CreatedAt:  time.Now().Format(time.RFC3339),
		FixFileDir: fixFileDir,
		FileStates: make(map[string]string),
		Status:     StatusRunning,
		filePath:   filePath,
	}
}

// LoadChecksumProgress loads a ChecksumProgress from the given file path.
// Returns nil if the file does not exist.
func LoadChecksumProgress(path string) (*ChecksumProgress, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read checksum progress file %s: %w", path, err)
	}

	var p ChecksumProgress
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("failed to parse checksum progress file %s: %w", path, err)
	}
	p.filePath = path
	if p.CompletedTables == nil {
		p.CompletedTables = make([]string, 0)
	}
	if p.CompletedTableResults == nil {
		p.CompletedTableResults = make([]ChecksumTableResult, 0)
	}
	if p.TableProgress == nil {
		p.TableProgress = make(map[string]*TableChunkProgress)
	}
	for _, tp := range p.TableProgress {
		if tp.CompletedChunks == nil {
			tp.CompletedChunks = make([]int64, 0)
		}
		if tp.CheckingChunks == nil {
			tp.CheckingChunks = make([]int64, 0)
		}
		if tp.CompletedFixSQL == nil {
			tp.CompletedFixSQL = make([]int64, 0)
		}
	}
	return &p, nil
}

// LoadRepairProgress loads a RepairProgress from the given file path.
// Returns nil if the file does not exist.
func LoadRepairProgress(path string) (*RepairProgress, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read repair progress file %s: %w", path, err)
	}

	var p RepairProgress
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("failed to parse repair progress file %s: %w", path, err)
	}
	p.filePath = path
	if p.FileStates == nil {
		p.FileStates = make(map[string]string)
	}
	return &p, nil
}

// Save writes the progress to disk atomically (tmpfile + rename).
func (p *ChecksumProgress) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saveLocked()
}

func (p *ChecksumProgress) saveLocked() error {
	if p.filePath == "" {
		return fmt.Errorf("checksum progress file path not set")
	}

	dir := filepath.Dir(p.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checksum progress: %w", err)
	}

	tmpPath := p.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, p.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, p.filePath, err)
	}

	return nil
}

// Save writes the repair progress to disk atomically (tmpfile + rename).
func (p *RepairProgress) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saveLocked()
}

func (p *RepairProgress) saveLocked() error {
	if p.filePath == "" {
		return fmt.Errorf("repair progress file path not set")
	}

	dir := filepath.Dir(p.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal repair progress: %w", err)
	}

	tmpPath := p.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, p.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename %s to %s: %w", tmpPath, p.filePath, err)
	}

	return nil
}

// IsCompleted returns true if the given table is in the completed list.
func (p *ChecksumProgress) IsCompleted(schemaTable string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, t := range p.CompletedTables {
		if t == schemaTable {
			return true
		}
	}
	return false
}

// MarkCompleted adds the table to the completed list and persists to disk.
func (p *ChecksumProgress) MarkCompleted(schemaTable string) error {
	return p.MarkCompletedWithResult(schemaTable, nil)
}

// MarkCompletedWithResult adds the table to the completed list, stores its
// report row when provided, and persists to disk.
func (p *ChecksumProgress) MarkCompletedWithResult(schemaTable string, result *ChecksumTableResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	exists := false
	for _, t := range p.CompletedTables {
		if t == schemaTable {
			exists = true
			break
		}
	}
	if !exists {
		p.CompletedTables = append(p.CompletedTables, schemaTable)
	}

	if result != nil {
		p.upsertCompletedTableResultLocked(schemaTable, *result)
	}
	// Remove in-progress chunk tracking now that the table is fully done.
	delete(p.TableProgress, schemaTable)
	return p.saveLocked()
}

func (p *ChecksumProgress) upsertCompletedTableResultLocked(schemaTable string, result ChecksumTableResult) {
	if p.CompletedTableResults == nil {
		p.CompletedTableResults = make([]ChecksumTableResult, 0)
	}
	for i, existing := range p.CompletedTableResults {
		if completedTableResultKey(existing) == schemaTable {
			p.CompletedTableResults[i] = result
			return
		}
	}
	p.CompletedTableResults = append(p.CompletedTableResults, result)
}

// MarkStatus updates the status and persists to disk.
func (p *ChecksumProgress) MarkStatus(status string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Status = status
	return p.saveLocked()
}

// FilePath returns the on-disk path of this progress file.
func (p *ChecksumProgress) FilePath() string {
	return p.filePath
}

// Remove deletes the progress file from disk.
// This method is thread-safe and can be called concurrently with Save/Mark methods.
func (p *ChecksumProgress) Remove() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.filePath == "" {
		return nil
	}
	if err := os.Remove(p.filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove progress file %s: %w", p.filePath, err)
	}
	return nil
}

// IsRunning returns true if the progress status is "running".
func (p *ChecksumProgress) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Status == StatusRunning
}

// CompletedCount returns the number of completed tables.
func (p *ChecksumProgress) CompletedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.CompletedTables)
}

// CompletedTableResultsSnapshot returns a copy of persisted completed-table
// report rows in completion order.
func (p *ChecksumProgress) CompletedTableResultsSnapshot() []ChecksumTableResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]ChecksumTableResult, len(p.CompletedTableResults))
	copy(result, p.CompletedTableResults)
	return result
}

func completedTableResultKey(result ChecksumTableResult) string {
	return strings.TrimSpace(result.Schema) + "." + strings.TrimSpace(result.Table)
}

// IsFileSuccess returns true if the given file has status "success".
func (p *RepairProgress) IsFileSuccess(filename string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, exists := p.FileStates[filename]
	return exists && state == "success"
}

// MarkFile updates the file state and persists to disk.
func (p *RepairProgress) MarkFile(filename, status string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.FileStates[filename] = status
	return p.saveLocked()
}

// MarkStatus updates the status and persists to disk.
func (p *RepairProgress) MarkStatus(status string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Status = status
	return p.saveLocked()
}

// FilePath returns the on-disk path of this progress file.
func (p *RepairProgress) FilePath() string {
	return p.filePath
}

// Remove deletes the progress file from disk.
// This method is thread-safe and can be called concurrently with Save/Mark methods.
func (p *RepairProgress) Remove() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.filePath == "" {
		return nil
	}
	if err := os.Remove(p.filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove progress file %s: %w", p.filePath, err)
	}
	return nil
}

// IsRunning returns true if the progress status is "running".
func (p *RepairProgress) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Status == StatusRunning
}

// SuccessCount returns the number of files with "success" status.
func (p *RepairProgress) SuccessCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := 0
	for _, state := range p.FileStates {
		if state == "success" {
			count++
		}
	}
	return count
}

// FileCount returns the total number of tracked files.
func (p *RepairProgress) FileCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.FileStates)
}

// SortedCompletedTables returns a sorted copy of the completed tables list.
func (p *ChecksumProgress) SortedCompletedTables() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]string, len(p.CompletedTables))
	copy(result, p.CompletedTables)
	sort.Strings(result)
	return result
}

// SetTableTotalRows caches the COUNT(*) result and chunk size for a table.
func (p *ChecksumProgress) SetTableTotalRows(schemaTable string, totalRows int64, chunkSize int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		p.TableProgress = make(map[string]*TableChunkProgress)
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		tp = &TableChunkProgress{
			CompletedChunks: make([]int64, 0),
			CheckingChunks:  make([]int64, 0),
			CompletedFixSQL: make([]int64, 0),
		}
		p.TableProgress[schemaTable] = tp
	}
	tp.TotalRows = totalRows
	tp.ChunkSize = chunkSize
	return p.saveLocked()
}

// GetTableTotalRows returns the cached total row count and chunk size for a table.
// ok is false if no cached value exists.
func (p *ChecksumProgress) GetTableTotalRows(schemaTable string) (totalRows int64, chunkSize int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return 0, 0, false
	}
	tp, exists := p.TableProgress[schemaTable]
	if !exists || tp.TotalRows == 0 {
		return 0, 0, false
	}
	return tp.TotalRows, tp.ChunkSize, true
}

// MarkChunkCompleted records beginSeq as a completed chunk for the table.
// CompletedChunks is kept sorted for efficient consecutive-offset calculation.
func (p *ChecksumProgress) MarkChunkCompleted(schemaTable string, beginSeq int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		p.TableProgress = make(map[string]*TableChunkProgress)
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		tp = &TableChunkProgress{
			CompletedChunks: make([]int64, 0),
			CheckingChunks:  make([]int64, 0),
			CompletedFixSQL: make([]int64, 0),
		}
		p.TableProgress[schemaTable] = tp
	}

	// Binary search: find insertion point.
	idx := sort.Search(len(tp.CompletedChunks), func(i int) bool {
		return tp.CompletedChunks[i] >= beginSeq
	})
	if idx < len(tp.CompletedChunks) && tp.CompletedChunks[idx] == beginSeq {
		return nil // already recorded
	}
	tp.CompletedChunks = append(tp.CompletedChunks, 0)
	copy(tp.CompletedChunks[idx+1:], tp.CompletedChunks[idx:])
	tp.CompletedChunks[idx] = beginSeq
	return p.saveLocked()
}

func insertSortedUnique(values []int64, value int64) []int64 {
	idx := sort.Search(len(values), func(i int) bool { return values[i] >= value })
	if idx < len(values) && values[idx] == value {
		return values
	}
	values = append(values, 0)
	copy(values[idx+1:], values[idx:])
	values[idx] = value
	return values
}

func removeSortedValue(values []int64, value int64) []int64 {
	idx := sort.Search(len(values), func(i int) bool { return values[i] >= value })
	if idx < len(values) && values[idx] == value {
		copy(values[idx:], values[idx+1:])
		values = values[:len(values)-1]
	}
	return values
}

func (p *ChecksumProgress) ensureTableProgressLocked(schemaTable string) *TableChunkProgress {
	if p.TableProgress == nil {
		p.TableProgress = make(map[string]*TableChunkProgress)
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		tp = &TableChunkProgress{}
		p.TableProgress[schemaTable] = tp
	}
	if tp.CompletedChunks == nil {
		tp.CompletedChunks = make([]int64, 0)
	}
	if tp.CheckingChunks == nil {
		tp.CheckingChunks = make([]int64, 0)
	}
	if tp.CompletedFixSQL == nil {
		tp.CompletedFixSQL = make([]int64, 0)
	}
	return tp
}

// MarkChunkChecking records that a chunk has started processing but its fixsql is not safe yet.
func (p *ChecksumProgress) MarkChunkChecking(schemaTable string, beginSeq int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tp := p.ensureTableProgressLocked(schemaTable)
	idx := sort.Search(len(tp.CompletedFixSQL), func(i int) bool { return tp.CompletedFixSQL[i] >= beginSeq })
	if idx < len(tp.CompletedFixSQL) && tp.CompletedFixSQL[idx] == beginSeq {
		return nil
	}
	tp.CheckingChunks = insertSortedUnique(tp.CheckingChunks, beginSeq)
	return p.saveLocked()
}

// MarkChunkFixSQLCompleted records that all fixsql for a chunk has been safely written.
func (p *ChecksumProgress) MarkChunkFixSQLCompleted(schemaTable string, beginSeq int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tp := p.ensureTableProgressLocked(schemaTable)
	tp.CheckingChunks = removeSortedValue(tp.CheckingChunks, beginSeq)
	tp.CompletedFixSQL = insertSortedUnique(tp.CompletedFixSQL, beginSeq)
	tp.CompletedChunks = insertSortedUnique(tp.CompletedChunks, beginSeq)
	return p.saveLocked()
}

func consecutiveResumeOffset(values []int64, chunkSize int64) int64 {
	expected := int64(0)
	for _, offset := range values {
		if offset != expected {
			break
		}
		expected += chunkSize
	}
	return expected
}

// GetSafeFixSQLResumeOffset returns the first chunk offset that still needs fixsql generation.
func (p *ChecksumProgress) GetSafeFixSQLResumeOffset(schemaTable string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return 0
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok || tp.ChunkSize <= 0 {
		return 0
	}
	if len(tp.CompletedFixSQL) == 0 && len(tp.CheckingChunks) == 0 {
		return 0
	}

	return consecutiveResumeOffset(tp.CompletedFixSQL, int64(tp.ChunkSize))
}

// GetFixSQLResumeState returns the consecutive completed_fixsql prefix and whether
// the persisted state is unsafe for partial fixsql resume.
func (p *ChecksumProgress) GetFixSQLResumeState(schemaTable string) (offset int64, hasNewState bool, unsafe bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return 0, false, false
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		return 0, false, false
	}
	hasNewState = len(tp.CompletedFixSQL) > 0 || len(tp.CheckingChunks) > 0
	if !hasNewState {
		return 0, false, false
	}
	if tp.ChunkSize <= 0 {
		return 0, true, true
	}

	offset = consecutiveResumeOffset(tp.CompletedFixSQL, int64(tp.ChunkSize))
	if len(tp.CheckingChunks) > 0 {
		return offset, true, true
	}
	for _, seq := range tp.CompletedFixSQL {
		if seq >= offset {
			return offset, true, true
		}
	}
	return offset, true, false
}

func (p *ChecksumProgress) GetSafeFixSQLCompletedChunks(schemaTable string, allowCompletedChunks bool) map[int64]struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return nil
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		return nil
	}

	hasNewState := len(tp.CompletedFixSQL) > 0 || len(tp.CheckingChunks) > 0
	var chunks []int64
	var resumeOffset int64
	if hasNewState {
		chunks = tp.CompletedFixSQL
		if tp.ChunkSize <= 0 {
			return nil
		}
		resumeOffset = consecutiveResumeOffset(tp.CompletedFixSQL, int64(tp.ChunkSize))
	} else if allowCompletedChunks {
		chunks = tp.CompletedChunks
		if tp.ChunkSize <= 0 {
			return nil
		}
		resumeOffset = consecutiveResumeOffset(tp.CompletedChunks, int64(tp.ChunkSize))
	} else {
		return nil
	}

	if len(chunks) == 0 || resumeOffset <= 0 {
		return nil
	}
	result := make(map[int64]struct{}, int(resumeOffset/int64(tp.ChunkSize)))
	for _, seq := range chunks {
		if seq >= resumeOffset {
			continue
		}
		result[seq] = struct{}{}
	}
	return result
}

// ClearCheckingChunksBefore removes stale in-flight chunk markers that are already safe to skip.
func (p *ChecksumProgress) ClearCheckingChunksBefore(schemaTable string, beforeSeq int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil || beforeSeq <= 0 {
		return nil
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok || len(tp.CheckingChunks) == 0 {
		return nil
	}
	kept := tp.CheckingChunks[:0]
	for _, chunkSeq := range tp.CheckingChunks {
		if chunkSeq >= beforeSeq {
			kept = append(kept, chunkSeq)
		}
	}
	if len(kept) == len(tp.CheckingChunks) {
		return nil
	}
	tp.CheckingChunks = kept
	return p.saveLocked()
}

// ClearCheckingChunks removes in-flight chunk markers after their tail SQL files were cleaned up.
func (p *ChecksumProgress) ClearCheckingChunks(schemaTable string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return nil
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok || len(tp.CheckingChunks) == 0 {
		return nil
	}
	tp.CheckingChunks = tp.CheckingChunks[:0]
	return p.saveLocked()
}

// ResetTableChunkState clears all chunk-level state for a table while preserving
// cached table size metadata. Used when existing fixsql files are unsafe to reuse.
func (p *ChecksumProgress) ResetTableChunkState(schemaTable string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return nil
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		return nil
	}
	tp.CompletedChunks = tp.CompletedChunks[:0]
	tp.CheckingChunks = tp.CheckingChunks[:0]
	tp.CompletedFixSQL = tp.CompletedFixSQL[:0]
	return p.saveLocked()
}

// HasFixSQLProgress returns true when the table has new-format chunk resume state.
func (p *ChecksumProgress) HasFixSQLProgress(schemaTable string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return false
	}
	tp, ok := p.TableProgress[schemaTable]
	return ok && (len(tp.CompletedFixSQL) > 0 || len(tp.CheckingChunks) > 0)
}

// GetSafeResumeOffset returns the first beginSeq that has not been completed consecutively
// from 0.  On resume, start the chunk loop from this offset.
// Returns 0 when no chunks are completed or chunk size is unknown.
func (p *ChecksumProgress) GetSafeResumeOffset(schemaTable string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return 0
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok || len(tp.CompletedChunks) == 0 || tp.ChunkSize <= 0 {
		return 0
	}

	chunkSize := int64(tp.ChunkSize)
	expected := int64(0)
	for _, offset := range tp.CompletedChunks {
		if offset != expected {
			break
		}
		expected += chunkSize
	}
	return expected
}

// RollbackLastChunks removes the last count entries from CompletedChunks for the table
// and persists the change. Used after deleting a partial INSERT fixsql file on resume,
// so that GetSafeResumeOffset returns the correct offset and those chunks are re-processed.
func (p *ChecksumProgress) RollbackLastChunks(schemaTable string, count int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil || count <= 0 {
		return nil
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok || len(tp.CompletedChunks) == 0 {
		return nil
	}

	if count >= len(tp.CompletedChunks) {
		tp.CompletedChunks = tp.CompletedChunks[:0]
	} else {
		tp.CompletedChunks = tp.CompletedChunks[:len(tp.CompletedChunks)-count]
	}
	return p.saveLocked()
}

// HasChunkProgress returns true if any chunks of the table have been recorded.
func (p *ChecksumProgress) HasChunkProgress(schemaTable string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return false
	}
	tp, ok := p.TableProgress[schemaTable]
	return ok && len(tp.CompletedChunks) > 0
}

// GetCompletedChunkCount returns the total number of completed chunks recorded for the table.
func (p *ChecksumProgress) GetCompletedChunkCount(schemaTable string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.TableProgress == nil {
		return 0
	}
	tp, ok := p.TableProgress[schemaTable]
	if !ok {
		return 0
	}
	return len(tp.CompletedChunks)
}

// FindRunningChecksumProgress scans the given directory for a checksum progress file
// with status "running". Returns nil if none found.
func FindRunningChecksumProgress(resultDir string) (*ChecksumProgress, error) {
	pattern := filepath.Join(resultDir, "gt-checksum-progress-*.json")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to glob %s: %w", pattern, err)
	}

	for _, match := range matches {
		p, err := LoadChecksumProgress(match)
		if err != nil {
			// Log warning for corrupted progress files
			fmt.Fprintf(os.Stderr, "[WARN] Skipping corrupted progress file %s: %v\n", match, err)
			continue
		}
		if p != nil && p.IsRunning() {
			return p, nil
		}
	}

	return nil, nil
}

// FindRunningRepairProgress checks the given path for a repair progress file
// with status "running". Returns nil if none found.
func FindRunningRepairProgress(path string) (*RepairProgress, error) {
	p, err := LoadRepairProgress(path)
	if err != nil {
		return nil, err
	}
	if p != nil && p.IsRunning() {
		return p, nil
	}
	return nil, nil
}

// ProgressFilePath returns the standard path for a checksum progress file.
func ProgressFilePath(resultDir, runID string) string {
	return filepath.Join(resultDir, fmt.Sprintf("gt-checksum-progress-%s.json", runID))
}

// RepairProgressFilePath returns the standard path for a repair progress file.
func RepairProgressFilePath(fixFileDir string) string {
	return filepath.Join(fixFileDir, ".repairDB-progress.json")
}

// FormatCompletedTablesSummary returns a human-readable summary of completed tables.
func (p *ChecksumProgress) FormatCompletedTablesSummary() string {
	tables := p.SortedCompletedTables()
	if len(tables) == 0 {
		return "  (none)"
	}

	var sb strings.Builder
	for i, t := range tables {
		if i >= 10 {
			sb.WriteString(fmt.Sprintf("  ... and %d more tables\n", len(tables)-10))
			break
		}
		sb.WriteString(fmt.Sprintf("  %s\n", t))
	}
	return sb.String()
}
