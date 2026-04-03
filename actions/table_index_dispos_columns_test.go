package actions

import "testing"

func TestColumnsModeSplitPKAndCompare_CaseInsensitive(t *testing.T) {
	filteredCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
		{"columnName": "note", "dataType": "varchar(32)"},
	}

	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredCols, []string{"ID"})
	if len(pkPositions) != 1 || pkPositions[0] != 0 {
		t.Fatalf("pkPositions = %v, want [0]", pkPositions)
	}
	if len(compareColNames) != 2 {
		t.Fatalf("compareColNames len = %d, want 2", len(compareColNames))
	}
	if compareColNames[0] != "amount" || compareColNames[1] != "note" {
		t.Fatalf("compareColNames = %v, want [amount note]", compareColNames)
	}
}

func TestColumnsModeExtractPKKey_CaseInsensitiveSplitKeepsDistinctRows(t *testing.T) {
	filteredCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
	}
	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredCols, []string{"ID"})
	if len(compareColNames) != 1 || compareColNames[0] != "amount" {
		t.Fatalf("compareColNames = %v, want [amount]", compareColNames)
	}

	row1 := "1/*go actions columnData*/10.00"
	row2 := "2/*go actions columnData*/20.00"
	key1 := columnsModeExtractPKKey(row1, pkPositions)
	key2 := columnsModeExtractPKKey(row2, pkPositions)
	if key1 == key2 {
		t.Fatalf("distinct PK rows produced same key: %q", key1)
	}
}
