package actions

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gt-checksum/global"
	golog "gt-checksum/go-log/log"
)

func TestWriteColumnsModeAdvisory_UsesRealTargetReference(t *testing.T) {
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "advisory.log"), "debug")

	dir := t.TempDir()
	sp := &SchedulePlan{
		datafixSql:           dir,
		columnPlanSourceCols: []string{"id", "amount"},
		sourceOnlyAdvisory: &columnsModeSourceOnlyAdvisory{
			schema:     "db1",
			table:      "orders",
			destSchema: "archive",
			destTable:  "orders_archive",
			indexCols:  []string{"id"},
		},
	}

	sp.writeColumnsModeAdvisory(1, 1, 1)

	path := filepath.Join(dir, "columns-advisory.db1.orders.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() failed: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "-- Target table       : `archive`.`orders_archive`") {
		t.Fatalf("target header missing real target ref:\n%s", content)
	}
	if !strings.Contains(content, "LEFT JOIN `archive`.`orders_archive` dst USING (id)") {
		t.Fatalf("source-only query did not use real target ref:\n%s", content)
	}
	if !strings.Contains(content, "SELECT dst.* FROM `archive`.`orders_archive` dst") {
		t.Fatalf("target-only query did not use real target ref:\n%s", content)
	}
}
