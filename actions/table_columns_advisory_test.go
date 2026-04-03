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
	if !strings.Contains(content, "LEFT JOIN `archive`.`orders_archive` dst USING (`id`)") {
		t.Fatalf("source-only query did not use real target ref:\n%s", content)
	}
	if !strings.Contains(content, "SELECT dst.* FROM `archive`.`orders_archive` dst") {
		t.Fatalf("target-only query did not use real target ref:\n%s", content)
	}
}

func TestWriteColumnsModeAdvisory_EscapesQuotedIdentifiers(t *testing.T) {
	global.Wlog = golog.NewWlog(filepath.Join(t.TempDir(), "advisory.log"), "debug")

	dir := t.TempDir()
	sp := &SchedulePlan{
		datafixSql:           dir,
		columnPlanSourceCols: []string{"id`part", "status"},
		sourceOnlyAdvisory: &columnsModeSourceOnlyAdvisory{
			schema:     "db`1",
			table:      "ord`ers",
			destSchema: "arch`ive",
			destTable:  "orders`archive",
			indexCols:  []string{"id`part", "tenant`id"},
		},
	}

	sp.writeColumnsModeAdvisory(1, 1, 1)

	path := filepath.Join(dir, "columns-advisory.db%601.ord%60ers.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() failed: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "-- Source table       : `db``1`.`ord``ers`") {
		t.Fatalf("source header did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "-- Target table       : `arch``ive`.`orders``archive`") {
		t.Fatalf("target header did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "-- Columns checked    : `id``part`, `status`") {
		t.Fatalf("columns header did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "-- Primary/unique key : `id``part`, `tenant``id`") {
		t.Fatalf("pk header did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "LEFT JOIN `arch``ive`.`orders``archive` dst USING (`id``part`, `tenant``id`)") {
		t.Fatalf("source-only join did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "WHERE dst.`id``part` IS NULL;") {
		t.Fatalf("source-only where clause did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "LEFT JOIN `db``1`.`ord``ers` src USING (`id``part`, `tenant``id`)") {
		t.Fatalf("target-only join did not escape identifiers:\n%s", content)
	}
	if !strings.Contains(content, "WHERE src.`id``part` IS NULL;") {
		t.Fatalf("target-only where clause did not escape identifiers:\n%s", content)
	}
}
