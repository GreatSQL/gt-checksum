package progress

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestChecksumProgress_LoadSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	// Create and save
	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)
	if err := p.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load
	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil")
	}
	if loaded.RunID != "20260520-143022" {
		t.Errorf("RunID mismatch: got %s, want %s", loaded.RunID, "20260520-143022")
	}
	if loaded.ConfigHash != "sha256:abc123" {
		t.Errorf("ConfigHash mismatch: got %s, want %s", loaded.ConfigHash, "sha256:abc123")
	}
	if loaded.Status != StatusRunning {
		t.Errorf("Status mismatch: got %s, want %s", loaded.Status, StatusRunning)
	}
}

func TestChecksumProgress_LoadNonExistent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.json")

	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded != nil {
		t.Error("Load should return nil for non-existent file")
	}
}

func TestChecksumProgress_MarkCompleted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)

	// Mark tables as completed
	if err := p.MarkCompleted("db1.table1"); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}
	if err := p.MarkCompleted("db1.table2"); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}

	// Check completion status
	if !p.IsCompleted("db1.table1") {
		t.Error("db1.table1 should be completed")
	}
	if !p.IsCompleted("db1.table2") {
		t.Error("db1.table2 should be completed")
	}
	if p.IsCompleted("db1.table3") {
		t.Error("db1.table3 should not be completed")
	}

	// Check count
	if p.CompletedCount() != 2 {
		t.Errorf("CompletedCount mismatch: got %d, want 2", p.CompletedCount())
	}

	// Load and verify persistence
	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !loaded.IsCompleted("db1.table1") {
		t.Error("Loaded progress should show db1.table1 as completed")
	}
	if !loaded.IsCompleted("db1.table2") {
		t.Error("Loaded progress should show db1.table2 as completed")
	}
}

func TestChecksumProgress_MarkCompletedDuplicate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)

	// Mark same table twice
	if err := p.MarkCompleted("db1.table1"); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}
	if err := p.MarkCompleted("db1.table1"); err != nil {
		t.Fatalf("MarkCompleted duplicate failed: %v", err)
	}

	if p.CompletedCount() != 1 {
		t.Errorf("CompletedCount should be 1, got %d", p.CompletedCount())
	}
}

func TestChecksumProgress_MarkStatus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)

	if err := p.MarkStatus(StatusCompleted); err != nil {
		t.Fatalf("MarkStatus failed: %v", err)
	}

	if p.IsRunning() {
		t.Error("Should not be running after marking completed")
	}

	// Load and verify
	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.IsRunning() {
		t.Error("Loaded progress should not be running")
	}
}

func TestChecksumProgress_ConcurrentMarkCompleted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			table := "db1.table" + string(rune('0'+i%10))
			_ = p.MarkCompleted(table)
		}(i)
	}
	wg.Wait()

	// Should have at most 10 unique tables
	count := p.CompletedCount()
	if count > 10 {
		t.Errorf("CompletedCount should be <= 10, got %d", count)
	}
}

func TestChecksumProgress_Remove(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)
	if err := p.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if err := p.Remove(); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("File should be removed")
	}
}

func TestRepairProgress_LoadSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewRepairProgress("/data/fixsql", path)
	if err := p.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := LoadRepairProgress(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil")
	}
	if loaded.FixFileDir != "/data/fixsql" {
		t.Errorf("FixFileDir mismatch: got %s, want %s", loaded.FixFileDir, "/data/fixsql")
	}
	if loaded.Status != StatusRunning {
		t.Errorf("Status mismatch: got %s, want %s", loaded.Status, StatusRunning)
	}
}

func TestRepairProgress_MarkFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewRepairProgress("/data/fixsql", path)

	// Mark files
	if err := p.MarkFile("table.db1.orders.sql", "success"); err != nil {
		t.Fatalf("MarkFile failed: %v", err)
	}
	if err := p.MarkFile("table.db1.users.sql", "failed"); err != nil {
		t.Fatalf("MarkFile failed: %v", err)
	}

	// Check status
	if !p.IsFileSuccess("table.db1.orders.sql") {
		t.Error("table.db1.orders.sql should be success")
	}
	if p.IsFileSuccess("table.db1.users.sql") {
		t.Error("table.db1.users.sql should not be success")
	}
	if p.IsFileSuccess("nonexistent.sql") {
		t.Error("nonexistent.sql should not be success")
	}

	// Check counts
	if p.SuccessCount() != 1 {
		t.Errorf("SuccessCount mismatch: got %d, want 1", p.SuccessCount())
	}
	if p.FileCount() != 2 {
		t.Errorf("FileCount mismatch: got %d, want 2", p.FileCount())
	}

	// Load and verify persistence
	loaded, err := LoadRepairProgress(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !loaded.IsFileSuccess("table.db1.orders.sql") {
		t.Error("Loaded progress should show table.db1.orders.sql as success")
	}
}

func TestRepairProgress_MarkStatus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewRepairProgress("/data/fixsql", path)

	if err := p.MarkStatus(StatusCompleted); err != nil {
		t.Fatalf("MarkStatus failed: %v", err)
	}

	if p.IsRunning() {
		t.Error("Should not be running after marking completed")
	}
}

func TestFindRunningChecksumProgress(t *testing.T) {
	dir := t.TempDir()

	// No progress file
	found, err := FindRunningChecksumProgress(dir)
	if err != nil {
		t.Fatalf("FindRunningChecksumProgress failed: %v", err)
	}
	if found != nil {
		t.Error("Should return nil when no progress file exists")
	}

	// Create a running progress file
	path := ProgressFilePath(dir, "20260520-143022")
	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)
	if err := p.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	found, err = FindRunningChecksumProgress(dir)
	if err != nil {
		t.Fatalf("FindRunningChecksumProgress failed: %v", err)
	}
	if found == nil {
		t.Fatal("Should find running progress file")
	}
	if found.RunID != "20260520-143022" {
		t.Errorf("RunID mismatch: got %s, want %s", found.RunID, "20260520-143022")
	}

	// Mark as completed
	if err := p.MarkStatus(StatusCompleted); err != nil {
		t.Fatalf("MarkStatus failed: %v", err)
	}

	found, err = FindRunningChecksumProgress(dir)
	if err != nil {
		t.Fatalf("FindRunningChecksumProgress failed: %v", err)
	}
	if found != nil {
		t.Error("Should not find completed progress file")
	}
}

func TestFindRunningRepairProgress(t *testing.T) {
	dir := t.TempDir()
	path := RepairProgressFilePath(dir)

	// No progress file
	found, err := FindRunningRepairProgress(path)
	if err != nil {
		t.Fatalf("FindRunningRepairProgress failed: %v", err)
	}
	if found != nil {
		t.Error("Should return nil when no progress file exists")
	}

	// Create a running progress file
	p := NewRepairProgress(dir, path)
	if err := p.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	found, err = FindRunningRepairProgress(path)
	if err != nil {
		t.Fatalf("FindRunningRepairProgress failed: %v", err)
	}
	if found == nil {
		t.Fatal("Should find running progress file")
	}

	// Mark as completed
	if err := p.MarkStatus(StatusCompleted); err != nil {
		t.Fatalf("MarkStatus failed: %v", err)
	}

	found, err = FindRunningRepairProgress(path)
	if err != nil {
		t.Fatalf("FindRunningRepairProgress failed: %v", err)
	}
	if found != nil {
		t.Error("Should not find completed progress file")
	}
}

func TestChecksumProgress_SortedCompletedTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)
	_ = p.MarkCompleted("db1.z_table")
	_ = p.MarkCompleted("db1.a_table")
	_ = p.MarkCompleted("db1.m_table")

	sorted := p.SortedCompletedTables()
	if len(sorted) != 3 {
		t.Fatalf("SortedCompletedTables length mismatch: got %d, want 3", len(sorted))
	}
	if sorted[0] != "db1.a_table" {
		t.Errorf("First table should be db1.a_table, got %s", sorted[0])
	}
	if sorted[1] != "db1.m_table" {
		t.Errorf("Second table should be db1.m_table, got %s", sorted[1])
	}
	if sorted[2] != "db1.z_table" {
		t.Errorf("Third table should be db1.z_table, got %s", sorted[2])
	}
}

func TestChecksumProgress_FormatCompletedTablesSummary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")

	p := NewChecksumProgress("20260520-143022", "sha256:abc123", path)

	// Empty list
	summary := p.FormatCompletedTablesSummary()
	if summary != "  (none)" {
		t.Errorf("Empty summary should be '  (none)', got '%s'", summary)
	}

	// With tables
	_ = p.MarkCompleted("db1.table1")
	_ = p.MarkCompleted("db1.table2")
	summary = p.FormatCompletedTablesSummary()
	if summary == "  (none)" {
		t.Error("Summary should not be '(none)' with completed tables")
	}
}

// TestChecksumProgress_InitialCreation 验证首次运行时能正确创建进度文件
// 模拟 break-resume bug 场景：Resume != "OFF" 但进度文件不存在
func TestChecksumProgress_InitialCreation(t *testing.T) {
	dir := t.TempDir()
	runID := "20260520-173315"
	path := ProgressFilePath(dir, runID)

	// 模拟首次运行：尝试加载不存在的进度文件
	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	if loaded != nil {
		t.Fatal("LoadChecksumProgress should return nil for non-existent file")
	}

	// 修复后：创建新的进度对象并保存
	p := NewChecksumProgress(runID, "", path)
	if err := p.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 验证文件已创建
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("Progress file should exist after Save()")
	}

	// 验证文件内容
	loaded, err = LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadChecksumProgress returned nil after Save()")
	}
	if loaded.RunID != runID {
		t.Errorf("RunID mismatch: got %s, want %s", loaded.RunID, runID)
	}
	if loaded.Status != StatusRunning {
		t.Errorf("Status mismatch: got %s, want %s", loaded.Status, StatusRunning)
	}
	if loaded.CompletedCount() != 0 {
		t.Errorf("CompletedCount should be 0 for new progress, got %d", loaded.CompletedCount())
	}
	if len(loaded.CompletedTables) != 0 {
		t.Errorf("CompletedTables should be empty for new progress, got %v", loaded.CompletedTables)
	}
}

func TestProgressFilePath(t *testing.T) {
	path := ProgressFilePath("result", "20260520-143022")
	expected := filepath.Join("result", "gt-checksum-progress-20260520-143022.json")
	if path != expected {
		t.Errorf("ProgressFilePath mismatch: got %s, want %s", path, expected)
	}
}

func TestRepairProgressFilePath(t *testing.T) {
	path := RepairProgressFilePath("/data/fixsql")
	expected := filepath.Join("/data/fixsql", ".repairDB-progress.json")
	if path != expected {
		t.Errorf("RepairProgressFilePath mismatch: got %s, want %s", path, expected)
	}
}

func TestSetGetTableTotalRows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)

	// No entry yet.
	rows, size, ok := p.GetTableTotalRows("db1.t1")
	if ok || rows != 0 || size != 0 {
		t.Error("GetTableTotalRows should return false for unknown table")
	}

	if err := p.SetTableTotalRows("db1.t1", 1000000, 10000); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}

	rows, size, ok = p.GetTableTotalRows("db1.t1")
	if !ok || rows != 1000000 || size != 10000 {
		t.Errorf("GetTableTotalRows mismatch: got (%d, %d, %v), want (1000000, 10000, true)", rows, size, ok)
	}

	// Reload and verify persistence.
	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	rows, size, ok = loaded.GetTableTotalRows("db1.t1")
	if !ok || rows != 1000000 || size != 10000 {
		t.Errorf("After reload, GetTableTotalRows mismatch: got (%d, %d, %v)", rows, size, ok)
	}
}

func TestMarkChunkCompleted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 50000, 10000); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}

	// Mark chunks out of order.
	for _, offset := range []int64{20000, 0, 10000, 40000, 30000} {
		if err := p.MarkChunkCompleted("db1.t1", offset); err != nil {
			t.Fatalf("MarkChunkCompleted(%d) failed: %v", offset, err)
		}
	}

	// CompletedChunks must be sorted.
	p.mu.Lock()
	chunks := p.TableProgress["db1.t1"].CompletedChunks
	p.mu.Unlock()
	expected := []int64{0, 10000, 20000, 30000, 40000}
	if len(chunks) != len(expected) {
		t.Fatalf("CompletedChunks length mismatch: got %d, want %d", len(chunks), len(expected))
	}
	for i, v := range expected {
		if chunks[i] != v {
			t.Errorf("CompletedChunks[%d]: got %d, want %d", i, chunks[i], v)
		}
	}

	// Duplicate mark is a no-op.
	if err := p.MarkChunkCompleted("db1.t1", 10000); err != nil {
		t.Fatalf("MarkChunkCompleted duplicate failed: %v", err)
	}
	p.mu.Lock()
	if len(p.TableProgress["db1.t1"].CompletedChunks) != 5 {
		t.Errorf("duplicate mark should not change length")
	}
	p.mu.Unlock()
}

func TestGetSafeResumeOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 50000, 10000); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}

	// No completed chunks.
	if off := p.GetSafeResumeOffset("db1.t1"); off != 0 {
		t.Errorf("expected 0, got %d", off)
	}

	// Mark 0 and 10000 but not 20000 (gap).
	_ = p.MarkChunkCompleted("db1.t1", 0)
	_ = p.MarkChunkCompleted("db1.t1", 10000)
	_ = p.MarkChunkCompleted("db1.t1", 30000) // gap at 20000
	if off := p.GetSafeResumeOffset("db1.t1"); off != 20000 {
		t.Errorf("expected 20000, got %d", off)
	}

	// Fill the gap.
	_ = p.MarkChunkCompleted("db1.t1", 20000)
	if off := p.GetSafeResumeOffset("db1.t1"); off != 40000 {
		t.Errorf("expected 40000, got %d", off)
	}

	// All done.
	_ = p.MarkChunkCompleted("db1.t1", 40000)
	if off := p.GetSafeResumeOffset("db1.t1"); off != 50000 {
		t.Errorf("expected 50000, got %d", off)
	}
}

func TestHasChunkProgress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)

	if p.HasChunkProgress("db1.t1") {
		t.Error("should not have chunk progress initially")
	}

	_ = p.SetTableTotalRows("db1.t1", 10000, 1000)
	if p.HasChunkProgress("db1.t1") {
		t.Error("SetTableTotalRows alone should not count as chunk progress")
	}

	_ = p.MarkChunkCompleted("db1.t1", 0)
	if !p.HasChunkProgress("db1.t1") {
		t.Error("should have chunk progress after MarkChunkCompleted")
	}
}

func TestChunkProgressPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	_ = p.SetTableTotalRows("db1.big", 1000000, 10000)
	for _, off := range []int64{0, 10000, 20000} {
		_ = p.MarkChunkCompleted("db1.big", off)
	}

	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	if off := loaded.GetSafeResumeOffset("db1.big"); off != 30000 {
		t.Errorf("after reload, expected offset 30000, got %d", off)
	}
	rows, size, ok := loaded.GetTableTotalRows("db1.big")
	if !ok || rows != 1000000 || size != 10000 {
		t.Errorf("after reload, total rows mismatch: got (%d,%d,%v)", rows, size, ok)
	}
}

func TestChecksumProgress_FixSQLResumeBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 5, 1); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}
	for _, seq := range []int64{0, 1, 2} {
		if err := p.MarkChunkFixSQLCompleted("db1.t1", seq); err != nil {
			t.Fatalf("MarkChunkFixSQLCompleted(%d) failed: %v", seq, err)
		}
	}
	if err := p.MarkChunkChecking("db1.t1", 3); err != nil {
		t.Fatalf("MarkChunkChecking failed: %v", err)
	}

	if off := p.GetSafeFixSQLResumeOffset("db1.t1"); off != 3 {
		t.Fatalf("expected safe fixsql offset 3, got %d", off)
	}

	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	if off := loaded.GetSafeFixSQLResumeOffset("db1.t1"); off != 3 {
		t.Fatalf("after reload expected safe fixsql offset 3, got %d", off)
	}
	if !loaded.HasFixSQLProgress("db1.t1") {
		t.Fatal("expected fixsql progress to be detected")
	}
}

func TestChecksumProgress_LegacyCompletedChunksNotSafeFixSQL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 5, 1); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}
	for _, seq := range []int64{0, 1, 2} {
		if err := p.MarkChunkCompleted("db1.t1", seq); err != nil {
			t.Fatalf("MarkChunkCompleted(%d) failed: %v", seq, err)
		}
	}

	if off := p.GetSafeResumeOffset("db1.t1"); off != 3 {
		t.Fatalf("legacy safe offset expected 3, got %d", off)
	}
	if off := p.GetSafeFixSQLResumeOffset("db1.t1"); off != 0 {
		t.Fatalf("legacy chunks must not be treated as safe fixsql, got %d", off)
	}
	if p.HasFixSQLProgress("db1.t1") {
		t.Fatal("legacy completed_chunks should not enable partial fixsql resume")
	}
}

func TestChecksumProgress_ClearCheckingChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 4, 1); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}
	_ = p.MarkChunkFixSQLCompleted("db1.t1", 0)
	_ = p.MarkChunkChecking("db1.t1", 1)
	_ = p.MarkChunkChecking("db1.t1", 2)

	if err := p.ClearCheckingChunks("db1.t1"); err != nil {
		t.Fatalf("ClearCheckingChunks failed: %v", err)
	}
	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	loaded.mu.Lock()
	checking := append([]int64(nil), loaded.TableProgress["db1.t1"].CheckingChunks...)
	loaded.mu.Unlock()
	if len(checking) != 0 {
		t.Fatalf("checking chunks should be cleared, got %v", checking)
	}
	if off := loaded.GetSafeFixSQLResumeOffset("db1.t1"); off != 1 {
		t.Fatalf("safe offset should remain at completed fixsql boundary 1, got %d", off)
	}
}

func TestChecksumProgress_FixSQLResumeIgnoresCompletedChunksWithNewState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 96, 1); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}

	p.mu.Lock()
	tp := p.TableProgress["db1.t1"]
	for seq := int64(0); seq <= 35; seq++ {
		tp.CompletedChunks = append(tp.CompletedChunks, seq)
	}
	for seq := int64(0); seq <= 11; seq++ {
		tp.CompletedFixSQL = append(tp.CompletedFixSQL, seq)
	}
	tp.CheckingChunks = []int64{12, 36, 37, 38, 39, 40}
	if err := p.saveLocked(); err != nil {
		p.mu.Unlock()
		t.Fatalf("saveLocked failed: %v", err)
	}
	p.mu.Unlock()

	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	if off := loaded.GetSafeFixSQLResumeOffset("db1.t1"); off != 12 {
		t.Fatalf("expected completed_fixsql boundary 12, got %d", off)
	}
	completed := loaded.GetSafeFixSQLCompletedChunks("db1.t1", true)
	for _, seq := range []int64{0, 11} {
		if _, ok := completed[seq]; !ok {
			t.Fatalf("expected completed fixsql chunk set to include %d", seq)
		}
	}
	for _, seq := range []int64{12, 35, 36, 40} {
		if _, ok := completed[seq]; ok {
			t.Fatalf("chunk %d must not be treated as completed fixsql", seq)
		}
	}
	if err := loaded.ClearCheckingChunksBefore("db1.t1", 12); err != nil {
		t.Fatalf("ClearCheckingChunksBefore failed: %v", err)
	}

	loaded.mu.Lock()
	checking := append([]int64(nil), loaded.TableProgress["db1.t1"].CheckingChunks...)
	loaded.mu.Unlock()
	expected := []int64{12, 36, 37, 38, 39, 40}
	if len(checking) != len(expected) {
		t.Fatalf("expected checking chunks %v, got %v", expected, checking)
	}
	for i := range expected {
		if checking[i] != expected[i] {
			t.Fatalf("expected checking chunks %v, got %v", expected, checking)
		}
	}
}

func TestChecksumProgress_FixSQLCompletedChunksCanBeSparse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-progress.json")
	p := NewChecksumProgress("r1", "h1", path)
	if err := p.SetTableTotalRows("db1.t1", 10, 1); err != nil {
		t.Fatalf("SetTableTotalRows failed: %v", err)
	}

	for _, seq := range []int64{0, 1, 2, 6, 7} {
		if err := p.MarkChunkFixSQLCompleted("db1.t1", seq); err != nil {
			t.Fatalf("MarkChunkFixSQLCompleted(%d) failed: %v", seq, err)
		}
	}

	loaded, err := LoadChecksumProgress(path)
	if err != nil {
		t.Fatalf("LoadChecksumProgress failed: %v", err)
	}
	if off := loaded.GetSafeFixSQLResumeOffset("db1.t1"); off != 3 {
		t.Fatalf("expected consecutive safe offset 3, got %d", off)
	}
	completed := loaded.GetSafeFixSQLCompletedChunks("db1.t1", true)
	for _, seq := range []int64{0, 1, 2} {
		if _, ok := completed[seq]; !ok {
			t.Fatalf("expected consecutive completed chunk set to include %d", seq)
		}
	}
	for _, seq := range []int64{3, 4, 5, 6, 7, 8} {
		if _, ok := completed[seq]; ok {
			t.Fatalf("chunk %d must not be treated as completed", seq)
		}
	}
}
