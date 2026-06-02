package oracle

import (
	"reflect"
	"testing"
)

func TestPrivGrantsPrecheck_OracleGlobalGrantSQL(t *testing.T) {
	grantSQL := oracleGlobalGrantSQL([]string{"SELECT ANY DICTIONARY"}, "CHECKSUM")
	wantGrantSQL := "GRANT SELECT ANY DICTIONARY TO CHECKSUM;"
	if grantSQL != wantGrantSQL {
		t.Fatalf("grant SQL = %q, want %q", grantSQL, wantGrantSQL)
	}

	fallbackGrantSQL := oracleGlobalGrantSQL([]string{"SELECT ANY DICTIONARY"}, "")
	wantFallbackGrantSQL := "GRANT SELECT ANY DICTIONARY TO <USER>;"
	if fallbackGrantSQL != wantFallbackGrantSQL {
		t.Fatalf("fallback grant SQL = %q, want %q", fallbackGrantSQL, wantFallbackGrantSQL)
	}
}

func TestPrivGrantsPrecheck_OracleRequiredTablePrivilegesByCheckObject(t *testing.T) {
	dataPrivileges, dataAnyPrivileges := oracleRequiredTablePrivileges("data", "table", "dest")
	wantDataPrivileges := []string{"DELETE", "INSERT", "SELECT"}
	if got := sortedPrivilegeKeys(dataPrivileges); !reflect.DeepEqual(got, wantDataPrivileges) {
		t.Fatalf("data privileges = %v, want %v", got, wantDataPrivileges)
	}
	wantDataAnyPrivileges := []string{"DELETE ANY TABLE", "INSERT ANY TABLE", "SELECT ANY TABLE"}
	if got := sortedPrivilegeKeys(dataAnyPrivileges); !reflect.DeepEqual(got, wantDataAnyPrivileges) {
		t.Fatalf("data any privileges = %v, want %v", got, wantDataAnyPrivileges)
	}

	structPrivileges, structAnyPrivileges := oracleRequiredTablePrivileges("struct", "table", "dest")
	wantStructPrivileges := []string{"ALTER", "SELECT"}
	if got := sortedPrivilegeKeys(structPrivileges); !reflect.DeepEqual(got, wantStructPrivileges) {
		t.Fatalf("struct privileges = %v, want %v", got, wantStructPrivileges)
	}
	wantStructAnyPrivileges := []string{"ALTER ANY TABLE", "SELECT ANY TABLE"}
	if got := sortedPrivilegeKeys(structAnyPrivileges); !reflect.DeepEqual(got, wantStructAnyPrivileges) {
		t.Fatalf("struct any privileges = %v, want %v", got, wantStructAnyPrivileges)
	}

	routinePrivileges, routineAnyPrivileges := oracleRequiredTablePrivileges("routine", "table", "dest")
	if len(routinePrivileges) != 0 || len(routineAnyPrivileges) != 0 {
		t.Fatalf("routine privileges = %v/%v, want empty", routinePrivileges, routineAnyPrivileges)
	}
}
