package actions

import (
	"reflect"
	"testing"
)

func TestPrivGrantsPrecheck_DestWildcardSchemaCompressesTableList(t *testing.T) {
	st := &schemaTable{
		table:                   "sbtest.*",
		datafix:                 "table",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "no",
	}

	got := st.compressAccessCheckListForWildcardSchemas(
		[]string{"sbtest.t1", "sbtest.t3", "other.t1"},
		st.destWildcardAccessSchemas(),
		true,
	)
	want := []string{"sbtest.*", "other.t1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compressed table list = %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_DestMappedWildcardSchemaCompressesTableList(t *testing.T) {
	st := &schemaTable{
		table:                   "srcdb.*:dstdb.*",
		datafix:                 "table",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "no",
	}

	got := st.compressAccessCheckListForWildcardSchemas(
		[]string{"dstdb.t1", "dstdb.t3"},
		st.destWildcardAccessSchemas(),
		true,
	)
	want := []string{"dstdb.*"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compressed mapped table list = %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_WildcardPrivilegeCoversExpandedTables(t *testing.T) {
	st := &schemaTable{
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "no",
	}

	privilegeMap := map[string]int{"sbtest.*": 1}
	for _, tableName := range []string{"sbtest.t1", "sbtest.t3"} {
		if !st.privilegeMapCoversTable(privilegeMap, tableName, true) {
			t.Fatalf("wildcard privilege should cover %s", tableName)
		}
	}

	singleTablePrivilegeMap := map[string]int{"sbtest.t1": 1}
	if !st.privilegeMapCoversTable(singleTablePrivilegeMap, "sbtest.t1", true) {
		t.Fatalf("single-table privilege should cover its own table")
	}
	if st.privilegeMapCoversTable(singleTablePrivilegeMap, "sbtest.t3", true) {
		t.Fatalf("single-table privilege must not cover other wildcard-expanded tables")
	}
}
