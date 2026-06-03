package actions

import (
	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func withRollbackWriterTestLogger(t *testing.T) {
	t.Helper()
	origWlog := global.Wlog
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "rollback-writer.log"), "debug")
	t.Cleanup(func() { global.Wlog = origWlog })
}

func TestRollbackSQLRollingWriterForcesFileMode(t *testing.T) {
	withRollbackWriterTestLogger(t)

	rollDir := t.TempDir()
	sp := &SchedulePlan{
		datafixType: "table",
		schema:      "src",
		table:       "t1",
		destSchema:  "dst",
		destTable:   "t1",
		rollSqlDir:  rollDir,
		ddrive:      "mysql",
		djdbc:       "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4",
		fixTrxNum:   1000,
		fixTrxSize:  4,
	}
	writer := sp.newRollbackSQLRollingWriter("DELETE", 1000, 4*1024*1024, 1)
	if writer.datafixType != "file" {
		t.Fatalf("rollback writer datafixType = %q, want file", writer.datafixType)
	}
	if err := writer.write([]string{"DELETE FROM `dst`.`t1` WHERE `id` = 1;"}); err != nil {
		t.Fatalf("rollback writer should write file instead of executing online: %v", err)
	}
	if err := writer.close(); err != nil {
		t.Fatalf("close rollback writer failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(rollDir, "table.dst.t1.rollback-DELETE-1.sql"))
	if err != nil {
		t.Fatalf("expected rollback DELETE file to be created: %v", err)
	}
	content := string(data)
	for _, want := range []string{"SET ", "BEGIN;", "DELETE FROM `dst`.`t1` WHERE `id` = 1;", "COMMIT;"} {
		if !strings.Contains(content, want) {
			t.Fatalf("rollback DELETE file missing %q, content:\n%s", want, content)
		}
	}
}

func TestRollbackDisposWritesTruncateWithPreamble(t *testing.T) {
	withRollbackWriterTestLogger(t)

	rollDir := t.TempDir()
	sp := &SchedulePlan{
		schema:     "src",
		table:      "t1",
		destSchema: "dst",
		destTable:  "t1",
		rollSqlDir: rollDir,
		ddrive:     "mysql",
		djdbc:      "user:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4",
		fixTrxNum:  1000,
		fixTrxSize: 4,
	}
	rollCC := make(chanString, 1)
	rollCC <- "TRUNCATE TABLE `dst`.`t1`;"
	close(rollCC)

	sp.RollbackDispos(rollCC, 1)

	data, err := os.ReadFile(filepath.Join(rollDir, "table.dst.t1.rollback-TRUNCATE-1.sql"))
	if err != nil {
		t.Fatalf("expected rollback TRUNCATE file to be created: %v", err)
	}
	content := string(data)
	for _, want := range []string{"SET ", "BEGIN;", "TRUNCATE TABLE `dst`.`t1`;", "COMMIT;"} {
		if !strings.Contains(content, want) {
			t.Fatalf("rollback TRUNCATE file missing %q, content:\n%s", want, content)
		}
	}
}
