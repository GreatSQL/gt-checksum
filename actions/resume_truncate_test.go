package actions

import (
	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
	"gt-checksum/progress"
	"os"
	"path/filepath"
	"testing"
)

// TestTruncateFileToLastCommit_NormalCase 验证截断到最后一个 COMMIT 的正常场景
func TestTruncateFileToLastCommit_NormalCase(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")

	// 写入两个完整事务 + 一个不完整事务
	content := "SET NAMES utf8mb4;\nBEGIN;\nDELETE FROM t2 WHERE id=1;\nCOMMIT;\nBEGIN;\nDELETE FROM t2 WHERE id=2;\nCOMMIT;\nBEGIN;\nDELETE FROM t2 WHERE id=3;\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	kept, err := truncateFileToLastCommit(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kept == 0 {
		t.Fatal("expected non-zero bytes kept")
	}

	data, _ := os.ReadFile(path)
	result := string(data)
	if len(result) == len(content) {
		t.Error("file was not truncated")
	}
	if result[len(result)-1] != '\n' {
		t.Error("truncated file should end with newline after COMMIT;")
	}
	// 确保最后一行是 COMMIT;
	lines := splitLines(result)
	lastNonEmpty := ""
	for i := len(lines) - 1; i >= 0; i-- {
		if lines[i] != "" {
			lastNonEmpty = lines[i]
			break
		}
	}
	if lastNonEmpty != "COMMIT;" {
		t.Errorf("last non-empty line should be COMMIT;, got %q", lastNonEmpty)
	}
	// 不完整事务的内容不应出现
	if containsStr(result, "id=3") {
		t.Error("incomplete transaction should be removed after truncation")
	}
}

// TestTruncateFileToLastCommit_NoCommit 文件中无 COMMIT 时应返回 0
func TestTruncateFileToLastCommit_NoCommit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")

	content := "SET NAMES utf8mb4;\nBEGIN;\nDELETE FROM t2 WHERE id=1;\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	kept, err := truncateFileToLastCommit(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kept != 0 {
		t.Errorf("expected 0 bytes kept (no COMMIT found), got %d", kept)
	}
	// 文件应保持不变
	data, _ := os.ReadFile(path)
	if string(data) != content {
		t.Error("file should not be modified when no COMMIT found")
	}
}

// TestTruncateFileToLastCommit_CompleteFile 完整文件（以 COMMIT 结尾）不应改变大小
func TestTruncateFileToLastCommit_CompleteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")

	content := "SET NAMES utf8mb4;\nBEGIN;\nDELETE FROM t2 WHERE id=1;\nCOMMIT;\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	kept, err := truncateFileToLastCommit(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kept != int64(len(content)) {
		t.Errorf("complete file should keep all %d bytes, kept %d", len(content), kept)
	}
}

// TestKeepAndTruncateLastFixFile_DeleteFile 模拟 DELETE 文件续传场景
// 验证最后一个 DELETE 文件被截断（而非删除），且返回的 seq 供 writer 创建新文件
func TestKeepAndTruncateLastFixFile_DeleteFile(t *testing.T) {
	fixDir := t.TempDir()
	prefix := "table.sbtest.t2-DELETE-"

	// 创建 t2-DELETE-1.sql：包含一个完整事务 + 一个不完整事务
	f1 := filepath.Join(fixDir, prefix+"1.sql")
	content1 := "SET NAMES utf8mb4;\nBEGIN;\nDELETE FROM t2 WHERE id=15494;\nCOMMIT;\nBEGIN;\nDELETE FROM t2 WHERE id=15495;\n"
	if err := os.WriteFile(f1, []byte(content1), 0644); err != nil {
		t.Fatal(err)
	}

	seq := keepAndTruncateLastFixFile(fixDir, prefix)

	// 应返回 1（文件被保留，writer 将从 seq+1=2 开始）
	if seq != 1 {
		t.Errorf("expected seq=1 (file kept), got %d", seq)
	}

	// 文件应存在且被截断
	data, err := os.ReadFile(f1)
	if err != nil {
		t.Fatalf("file should still exist after truncation: %v", err)
	}
	result := string(data)
	if containsStr(result, "id=15495") {
		t.Error("incomplete transaction (id=15495) should be removed after truncation")
	}
	if !containsStr(result, "id=15494") {
		t.Error("complete transaction (id=15494) should be preserved")
	}
}

// TestKeepAndTruncateLastFixFile_AllIncomplete 文件中无 COMMIT 时应删除文件并返回 seq-1=0
func TestKeepAndTruncateLastFixFile_AllIncomplete(t *testing.T) {
	fixDir := t.TempDir()
	prefix := "table.sbtest.t2-DELETE-"

	f1 := filepath.Join(fixDir, prefix+"1.sql")
	content1 := "SET NAMES utf8mb4;\nBEGIN;\nDELETE FROM t2 WHERE id=1;\n"
	if err := os.WriteFile(f1, []byte(content1), 0644); err != nil {
		t.Fatal(err)
	}

	seq := keepAndTruncateLastFixFile(fixDir, prefix)

	// 无 COMMIT，文件应被删除，返回 maxSeq-1=0
	if seq != 0 {
		t.Errorf("expected seq=0 (file deleted, no COMMIT), got %d", seq)
	}
	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Error("entirely incomplete file should be deleted")
	}
}

// TestKeepAndTruncateLastFixFile_NoFiles 目录中无匹配文件时返回 0
func TestKeepAndTruncateLastFixFile_NoFiles(t *testing.T) {
	fixDir := t.TempDir()
	seq := keepAndTruncateLastFixFile(fixDir, "table.sbtest.t2-DELETE-")
	if seq != 0 {
		t.Errorf("expected seq=0 (no files), got %d", seq)
	}
}

// TestCleanupIncompleteFixSQL_LastFilesTruncated 验证 cleanupIncompleteFixSQLForTable 保留最后文件中的完整事务
func TestCleanupIncompleteFixSQL_LastFilesTruncated(t *testing.T) {
	fixDir := t.TempDir()
	rollDir := t.TempDir()

	for i := 1; i <= 2; i++ {
		p := filepath.Join(fixDir, "table.sbtest.t2-"+itoa(i)+".sql")
		if err := os.WriteFile(p, []byte("BEGIN;\nINSERT INTO t2 VALUES ("+itoa(i)+");\nCOMMIT;\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	p3 := filepath.Join(fixDir, "table.sbtest.t2-3.sql")
	if err := os.WriteFile(p3, []byte("BEGIN;\nINSERT INTO t2 VALUES (3);\nCOMMIT;\nBEGIN;\nINSERT INTO t2 VALUES (99);\n"), 0644); err != nil {
		t.Fatal(err)
	}

	delFile := filepath.Join(fixDir, "table.sbtest.t2-DELETE-1.sql")
	if err := os.WriteFile(delFile, []byte("BEGIN;\nDELETE FROM t2 WHERE id=15494;\nCOMMIT;\nBEGIN;\nDELETE FROM t2 WHERE id=99;\n"), 0644); err != nil {
		t.Fatal(err)
	}

	seqs := cleanupIncompleteFixSQLForTable(fixDir, rollDir, "sbtest", "t2")

	if seqs["INSERT"] != 3 {
		t.Errorf("INSERT: expected seq=3 (file kept+truncated), got %d", seqs["INSERT"])
	}
	if seqs["DELETE"] != 1 {
		t.Errorf("DELETE: expected seq=1 (file kept+truncated), got %d", seqs["DELETE"])
	}
	insertData, err := os.ReadFile(p3)
	if err != nil {
		t.Fatalf("INSERT file should be kept after truncation: %v", err)
	}
	if containsStr(string(insertData), "VALUES (99)") {
		t.Error("incomplete INSERT transaction should be removed")
	}
	if !containsStr(string(insertData), "VALUES (3)") {
		t.Error("complete INSERT transaction should be preserved")
	}
	deleteData, err := os.ReadFile(delFile)
	if err != nil {
		t.Fatalf("DELETE file should be kept after truncation: %v", err)
	}
	if containsStr(string(deleteData), "id=99") {
		t.Error("incomplete DELETE transaction should be removed")
	}
	if !containsStr(string(deleteData), "id=15494") {
		t.Error("complete DELETE transaction should be preserved")
	}
}

// TestResume_DeletedFileChunkRollback 验证 bug fix：
// 断点续传删除最后一个不完整 INSERT 文件前，findLastFixFilePath + countCommitsInFile
// 能正确定位文件并统计 chunk 数，确保只回滚被删除文件中的 chunk，不会全量回滚。
// 这是 break-resume-generated-more-fixsql bug 的核心回归测试。
func TestResume_DeletedFileChunkRollback(t *testing.T) {
	fixDir := t.TempDir()
	prefix := "table.sbtest.t2-"

	// 模拟 t2-3.sql：包含 3 个完整 chunk（3 个 COMMIT）+ 1 个不完整尾部
	p3 := filepath.Join(fixDir, prefix+"3.sql")
	content3 := "BEGIN;\nINSERT INTO t2 VALUES (1001);\nCOMMIT;\n" +
		"BEGIN;\nINSERT INTO t2 VALUES (1002);\nCOMMIT;\n" +
		"BEGIN;\nINSERT INTO t2 VALUES (1003);\nCOMMIT;\n" +
		"BEGIN;\nINSERT INTO t2 VALUES (1004);\n" // 不完整
	if err := os.WriteFile(p3, []byte(content3), 0644); err != nil {
		t.Fatal(err)
	}

	// 创建 t2-1.sql 和 t2-2.sql（完整文件，不应被删除）
	for i := 1; i <= 2; i++ {
		p := filepath.Join(fixDir, prefix+itoa(i)+".sql")
		if err := os.WriteFile(p, []byte("BEGIN;\nINSERT INTO t2 VALUES ("+itoa(i)+");\nCOMMIT;\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// 新架构：先用 findLastFixFilePath 定位文件，countCommitsInFile 统计 chunk 数
	lastPath := findLastFixFilePath(fixDir, prefix)
	if lastPath == "" {
		t.Fatal("findLastFixFilePath should find t2-3.sql")
	}
	deletedChunks := countCommitsInFile(lastPath)

	// 然后再删除文件
	keptSeq, fileDeleted := findAndDeleteLastFixFileV2(fixDir, prefix)
	if !fileDeleted {
		t.Error("file should be deleted")
	}

	// 应删除 t2-3.sql，保留 seq=2
	if keptSeq != 2 {
		t.Errorf("keptSeq: expected 2, got %d", keptSeq)
	}
	// 被删除文件中有 3 个完整 COMMIT（精确回滚量）
	if deletedChunks != 3 {
		t.Errorf("deletedChunks: expected 3, got %d", deletedChunks)
	}
	// t2-3.sql 应被删除
	if _, err := os.Stat(p3); !os.IsNotExist(err) {
		t.Error("t2-3.sql should be deleted")
	}
	// t2-1.sql 和 t2-2.sql 应保留
	for i := 1; i <= 2; i++ {
		p := filepath.Join(fixDir, prefix+itoa(i)+".sql")
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("t2-%d.sql should be kept", i)
		}
	}
}

// TestResume_CountCommitsInFile 验证 countCommitsInFile 对各种文件内容的统计准确性
func TestResume_CountCommitsInFile(t *testing.T) {
	cases := []struct {
		name     string
		content  string
		expected int
	}{
		{"empty", "", 0},
		{"no commit", "BEGIN;\nINSERT INTO t VALUES (1);\n", 0},
		{"one commit", "BEGIN;\nINSERT INTO t VALUES (1);\nCOMMIT;\n", 1},
		{"three commits", "BEGIN;\nINSERT INTO t VALUES (1);\nCOMMIT;\nBEGIN;\nINSERT INTO t VALUES (2);\nCOMMIT;\nBEGIN;\nINSERT INTO t VALUES (3);\nCOMMIT;\n", 3},
		{"partial last", "BEGIN;\nINSERT INTO t VALUES (1);\nCOMMIT;\nBEGIN;\nINSERT INTO t VALUES (2);\n", 1},
		{"commit with spaces", "BEGIN;\nINSERT INTO t VALUES (1);\n  COMMIT;  \n", 1}, // TrimSpace 后匹配
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.sql")
			if err := os.WriteFile(path, []byte(tc.content), 0644); err != nil {
				t.Fatal(err)
			}
			got := countCommitsInFile(path)
			if got != tc.expected {
				t.Errorf("countCommitsInFile: expected %d, got %d", tc.expected, got)
			}
		})
	}
}

// TestResume_CleanupPreservesCompleteInsertFile 验证续跑清理不会删除包含完整事务的 INSERT 文件
func TestResume_CleanupPreservesCompleteInsertFile(t *testing.T) {
	fixDir := t.TempDir()
	rollDir := t.TempDir()

	// INSERT 文件：t2-1.sql（1 chunk）、t2-2.sql（2 chunks）、t2-3.sql（3 chunks，最后一个不完整）
	if err := os.WriteFile(filepath.Join(fixDir, "table.sbtest.t2-1.sql"),
		[]byte("BEGIN;\nINSERT INTO t2 VALUES (1);\nCOMMIT;\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixDir, "table.sbtest.t2-2.sql"),
		[]byte("BEGIN;\nINSERT INTO t2 VALUES (2);\nCOMMIT;\nBEGIN;\nINSERT INTO t2 VALUES (3);\nCOMMIT;\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixDir, "table.sbtest.t2-3.sql"),
		[]byte("BEGIN;\nINSERT INTO t2 VALUES (4);\nCOMMIT;\nBEGIN;\nINSERT INTO t2 VALUES (5);\nCOMMIT;\nBEGIN;\nINSERT INTO t2 VALUES (6);\nCOMMIT;\nBEGIN;\nINSERT INTO t2 VALUES (7);\n"), 0644); err != nil {
		t.Fatal(err)
	}

	seqs := cleanupIncompleteFixSQLForTable(fixDir, rollDir, "sbtest", "t2")

	if seqs["INSERT"] != 3 {
		t.Errorf("INSERT seq: expected 3, got %d", seqs["INSERT"])
	}
	data, err := os.ReadFile(filepath.Join(fixDir, "table.sbtest.t2-3.sql"))
	if err != nil {
		t.Fatalf("t2-3.sql should be preserved: %v", err)
	}
	if !containsStr(string(data), "VALUES (6)") {
		t.Error("complete INSERT transactions in t2-3.sql should be preserved")
	}
	if containsStr(string(data), "VALUES (7)") {
		t.Error("incomplete INSERT transaction in t2-3.sql should be truncated")
	}
}

func TestResume_CleanupPreservesRollbackInsertFile(t *testing.T) {
	fixDir := t.TempDir()
	rollDir := t.TempDir()

	rbFile := filepath.Join(rollDir, "table.sbtest.t2.rollback-INSERT-1.sql")
	if err := os.WriteFile(rbFile, []byte("BEGIN;\nINSERT INTO t2 VALUES (1);\nCOMMIT;\nBEGIN;\nINSERT INTO t2 VALUES (99);\n"), 0644); err != nil {
		t.Fatal(err)
	}

	seqs := cleanupIncompleteFixSQLForTable(fixDir, rollDir, "sbtest", "t2")
	if seqs["rollback-INSERT"] != 1 {
		t.Errorf("rollback-INSERT seq: expected 1, got %d", seqs["rollback-INSERT"])
	}
	data, err := os.ReadFile(rbFile)
	if err != nil {
		t.Fatalf("rollback INSERT file should be preserved: %v", err)
	}
	if !containsStr(string(data), "VALUES (1)") {
		t.Error("complete rollback INSERT transaction should be preserved")
	}
	if containsStr(string(data), "VALUES (99)") {
		t.Error("incomplete rollback INSERT transaction should be truncated")
	}
}

func TestResume_PrepareUsesCompletedChunksBoundary(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "resume.log"), "debug")
	defer func() { global.Wlog = origWlog }()

	fixDir := t.TempDir()
	rollDir := t.TempDir()
	progressPath := filepath.Join(t.TempDir(), "progress.json")
	p := progress.NewChecksumProgress("r1", "h1", progressPath)
	if err := p.SetTableTotalRows("sbtest.t4", 96, 1); err != nil {
		t.Fatal(err)
	}
	for seq := int64(0); seq < 36; seq++ {
		if err := p.MarkChunkCompleted("sbtest.t4", seq); err != nil {
			t.Fatal(err)
		}
	}

	for seq := 1; seq <= 4; seq++ {
		path := filepath.Join(fixDir, "table.sbtest.t4-"+itoa(seq)+".sql")
		if err := os.WriteFile(path, []byte("BEGIN;\nINSERT INTO t4 VALUES ("+itoa(seq)+");\nCOMMIT;\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	seqs, offset := prepareResumeFixSQLForTable(p, "sbtest.t4", fixDir, rollDir, "sbtest", "t4", true)
	if offset != 0 {
		t.Fatalf("completed_chunks must not be used as fixsql-safe resume offset, got %d", offset)
	}
	if seqs != nil {
		t.Fatalf("expected nil file seqs after cleaning legacy-only progress, got %v", seqs)
	}
	if _, err := os.Stat(filepath.Join(fixDir, "table.sbtest.t4-4.sql")); !os.IsNotExist(err) {
		t.Fatalf("legacy-only fixsql should be removed before recheck, stat err=%v", err)
	}
}

func TestResume_PrepareDoesNotSkipCheckingChunksWithNewFixSQLState(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "resume.log"), "debug")
	defer func() { global.Wlog = origWlog }()

	fixDir := t.TempDir()
	rollDir := t.TempDir()
	progressPath := filepath.Join(t.TempDir(), "progress.json")
	p := progress.NewChecksumProgress("r1", "h1", progressPath)
	if err := p.SetTableTotalRows("sbtest.t4", 96, 1); err != nil {
		t.Fatal(err)
	}
	for seq := int64(0); seq < 4; seq++ {
		if err := p.MarkChunkFixSQLCompleted("sbtest.t4", seq); err != nil {
			t.Fatal(err)
		}
	}
	for seq := int64(10); seq < 20; seq++ {
		if err := p.MarkChunkCompleted("sbtest.t4", seq); err != nil {
			t.Fatal(err)
		}
	}
	for _, seq := range []int64{4, 10, 11} {
		if err := p.MarkChunkChecking("sbtest.t4", seq); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(fixDir, "table.sbtest.t4-1.sql")
	if err := os.WriteFile(path, []byte("BEGIN;\nINSERT INTO t4 VALUES (1);\nCOMMIT;\n"), 0644); err != nil {
		t.Fatal(err)
	}

	seqs, offset := prepareResumeFixSQLForTable(p, "sbtest.t4", fixDir, rollDir, "sbtest", "t4", true)
	if offset != 0 {
		t.Fatalf("unsafe checking chunks must force full table recheck, got offset %d", offset)
	}
	if seqs != nil {
		t.Fatalf("expected nil file seqs after unsafe cleanup, got %v", seqs)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unsafe partial fixsql should be removed, stat err=%v", err)
	}

	completed := p.GetSafeFixSQLCompletedChunks("sbtest.t4", true)
	if len(completed) != 0 {
		t.Fatalf("completed chunks should be reset after unsafe cleanup, got %v", completed)
	}
	for _, seq := range []int64{4, 10, 11} {
		if shouldSkipResumeChunk(seq, offset, completed) {
			t.Fatalf("checking or legacy-only chunk %d must be regenerated", seq)
		}
	}
}

func TestResume_PrepareCleansWhenNoSafeBoundary(t *testing.T) {
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "resume.log"), "debug")
	defer func() { global.Wlog = origWlog }()

	fixDir := t.TempDir()
	rollDir := t.TempDir()
	progressPath := filepath.Join(t.TempDir(), "progress.json")
	p := progress.NewChecksumProgress("r1", "h1", progressPath)
	if err := p.SetTableTotalRows("sbtest.t4", 96, 1); err != nil {
		t.Fatal(err)
	}
	if err := p.MarkChunkChecking("sbtest.t4", 1); err != nil {
		t.Fatal(err)
	}

	oldFile := filepath.Join(fixDir, "table.sbtest.t4-1.sql")
	if err := os.WriteFile(oldFile, []byte("BEGIN;\nINSERT INTO t4 VALUES (1);\nCOMMIT;\n"), 0644); err != nil {
		t.Fatal(err)
	}

	seqs, offset := prepareResumeFixSQLForTable(p, "sbtest.t4", fixDir, rollDir, "sbtest", "t4", true)
	if offset != 0 {
		t.Fatalf("expected no safe resume offset, got %d", offset)
	}
	if seqs != nil {
		t.Fatalf("expected nil file seqs after full cleanup, got %v", seqs)
	}
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("old fixsql should be removed when resume boundary is unsafe, stat err=%v", err)
	}
}

func TestResume_ShouldSkipSparseCompletedChunks(t *testing.T) {
	completed := map[int64]struct{}{
		0: {},
		1: {},
		2: {},
		6: {},
		7: {},
	}

	for _, seq := range []int64{0, 1, 2} {
		if !shouldSkipResumeChunk(seq, 3, completed) {
			t.Fatalf("expected chunk %d to be skipped", seq)
		}
	}
	for _, seq := range []int64{3, 4, 5, 6, 7, 8} {
		if shouldSkipResumeChunk(seq, 3, completed) {
			t.Fatalf("chunk %d must be regenerated", seq)
		}
	}
}

// 辅助函数
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	pos := 20
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
