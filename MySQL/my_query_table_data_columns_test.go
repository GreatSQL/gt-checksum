package mysql

import "testing"

func TestOrderColumnsForCompare_CaseInsensitive(t *testing.T) {
	allCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
		{"columnName": "note", "dataType": "varchar(32)"},
	}

	got := orderColumnsForCompare(allCols, []string{"ID"}, []string{"ID", "AMOUNT"})
	if len(got) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(got))
	}
	if got[0]["columnName"] != "id" {
		t.Fatalf("first column = %q, want id", got[0]["columnName"])
	}
	if got[1]["columnName"] != "amount" {
		t.Fatalf("second column = %q, want amount", got[1]["columnName"])
	}
}
