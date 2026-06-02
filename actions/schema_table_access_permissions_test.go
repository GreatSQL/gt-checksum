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

func TestPrivGrantsPrecheck_DestWildcardSchemaCompressesFileTableList(t *testing.T) {
	st := &schemaTable{
		table:                   "hash_partition_table",
		rawTables:               "gt_checksum.*",
		datafix:                 "file",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "no",
	}

	got := st.compressAccessCheckListForWildcardSchemas(
		[]string{"gt_checksum.hash_partition_table", "gt_checksum.normal_table", "other.t1"},
		st.destWildcardAccessSchemas(),
		true,
	)
	want := []string{"gt_checksum.*", "other.t1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compressed dest file table list = %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_SourceWildcardSchemaCompressesTableList(t *testing.T) {
	st := &schemaTable{
		table:                   "sbtest.*",
		datafix:                 "file",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "no",
	}

	got := st.compressAccessCheckListForWildcardSchemas(
		[]string{"sbtest.t1", "sbtest.t3", "other.t1"},
		st.sourceWildcardAccessSchemas(),
		false,
	)
	want := []string{"sbtest.*", "other.t1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compressed source table list = %v, want %v", got, want)
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

func TestPrivGrantsPrecheck_DestMappedWildcardSchemaCompressesFileTableList(t *testing.T) {
	st := &schemaTable{
		table:                   "t1",
		rawTables:               "srcdb.*:dstdb.*",
		datafix:                 "file",
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
		t.Fatalf("compressed mapped dest file table list = %v, want %v", got, want)
	}
}

func TestPrivGrantsPrecheck_SourceMappedWildcardSchemaCompressesTableList(t *testing.T) {
	st := &schemaTable{
		table:                   "srcdb.*:dstdb.*",
		datafix:                 "file",
		sourceDrive:             "mysql",
		destDrive:               "mysql",
		caseSensitiveObjectName: "no",
	}

	got := st.compressAccessCheckListForWildcardSchemas(
		[]string{"srcdb.t1", "srcdb.t3"},
		st.sourceWildcardAccessSchemas(),
		false,
	)
	want := []string{"srcdb.*"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compressed mapped source table list = %v, want %v", got, want)
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
