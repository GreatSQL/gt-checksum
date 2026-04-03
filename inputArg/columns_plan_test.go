package inputArg

import (
	"strings"
	"testing"
)

func TestParseColumnsParam_DuplicateSimpleColumnsCaseInsensitive(t *testing.T) {
	_, err := parseColumnsParam("id,ID", "db1.t1")
	if err == nil {
		t.Fatal("expected duplicate-column error")
	}
	if !strings.Contains(err.Error(), "duplicate column name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseColumnsParam_DuplicateMappedSourceColumnsCaseInsensitive(t *testing.T) {
	columns := "db1.t1.amount:db2.t2.total,db1.t1.AMOUNT:db2.t2.total2"
	_, err := parseColumnsParam(columns, "db1.t1:db2.t2")
	if err == nil {
		t.Fatal("expected duplicate-source-column error")
	}
	if !strings.Contains(err.Error(), "duplicate source column") {
		t.Fatalf("unexpected error: %v", err)
	}
}
