package actions

import (
	"gt-checksum/inputArg"
	"testing"
)

// mockConfig returns a minimal ConfigParameter for testing.
func mockConfig(runID, terminalResultMode string) *inputArg.ConfigParameter {
	m := &inputArg.ConfigParameter{}
	m.RunID = runID
	m.SecondaryL.RulesV.TerminalResultMode = terminalResultMode
	m.SecondaryL.RulesV.ResultExport = "csv"
	m.SecondaryL.RepairV.Datafix = "file"
	return m
}

// ---------------------------------------------------------------------------
// normalizeSchemaObjectName
// ---------------------------------------------------------------------------

func TestNormalizeSchemaObjectName_dotSeparated(t *testing.T) {
	schema, name := normalizeSchemaObjectName("", "db1.t1")
	if schema != "db1" || name != "t1" {
		t.Errorf("got schema=%q name=%q, want db1/t1", schema, name)
	}
}

func TestNormalizeSchemaObjectName_colonSeparatedEmptySchema(t *testing.T) {
	schema, name := normalizeSchemaObjectName("", "db1:t1")
	if schema != "db1" || name != "t1" {
		t.Errorf("got schema=%q name=%q, want db1/t1", schema, name)
	}
}

func TestNormalizeSchemaObjectName_colonSeparatedWithSchema(t *testing.T) {
	// When schema is already known, colon-prefixed name keeps only the left part.
	schema, name := normalizeSchemaObjectName("db1", "t1:t2")
	if schema != "db1" || name != "t1" {
		t.Errorf("got schema=%q name=%q, want db1/t1", schema, name)
	}
}

func TestNormalizeSchemaObjectName_wildcardMapping(t *testing.T) {
	// "db1.*:db2.*" format: schema becomes "db2.*", name stays as-is.
	schema, name := normalizeSchemaObjectName("", "db1.*:db2.*")
	if schema != "db2.*" {
		t.Errorf("got schema=%q, want db2.*", schema)
	}
	_ = name
}

func TestNormalizeSchemaObjectName_plain(t *testing.T) {
	// No separator: nothing changes.
	schema, name := normalizeSchemaObjectName("mydb", "mytable")
	if schema != "mydb" || name != "mytable" {
		t.Errorf("got schema=%q name=%q, want mydb/mytable", schema, name)
	}
}

// ---------------------------------------------------------------------------
// resolveObjectIdentity
// ---------------------------------------------------------------------------

func TestResolveObjectIdentity_dataMode(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "orders", CheckObject: "data"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" || objectName != "orders" || objectType != "table" {
		t.Errorf("got schema=%q objectName=%q objectType=%q", schema, objectName, objectType)
	}
}

func TestResolveObjectIdentity_structMode(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "users", CheckObject: "struct"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" || objectName != "users" || objectType != "table" {
		t.Errorf("got schema=%q objectName=%q objectType=%q", schema, objectName, objectType)
	}
}

func TestResolveObjectIdentity_procedure(t *testing.T) {
	pod := Pod{Schema: "db1", ProcName: "sp_calc", CheckObject: "procedure"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" || objectName != "sp_calc" || objectType != "procedure" {
		t.Errorf("got schema=%q objectName=%q objectType=%q", schema, objectName, objectType)
	}
}

func TestResolveObjectIdentity_function(t *testing.T) {
	pod := Pod{Schema: "db1", FuncName: "fn_sum", CheckObject: "function"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" || objectName != "fn_sum" || objectType != "function" {
		t.Errorf("got schema=%q objectName=%q objectType=%q", schema, objectName, objectType)
	}
}

func TestResolveObjectIdentity_trigger(t *testing.T) {
	pod := Pod{Schema: "db1", TriggerName: "trg_ins", CheckObject: "trigger"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" || objectName != "trg_ins" || objectType != "trigger" {
		t.Errorf("got schema=%q objectName=%q objectType=%q", schema, objectName, objectType)
	}
}

func TestResolveObjectIdentity_sequence(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "seq_order", CheckObject: "sequence"}
	_, objectName, objectType := resolveObjectIdentity(pod)
	if objectName != "seq_order" || objectType != "sequence" {
		t.Errorf("got objectName=%q objectType=%q", objectName, objectType)
	}
}

// ---------------------------------------------------------------------------
// normalizePodToRecord — field mapping
// ---------------------------------------------------------------------------

func TestNormalizePodToRecord_dataModeBasic(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	pod := Pod{
		Schema:      "db1",
		Table:       "orders",
		CheckObject: "data",
		DIFFS:       "no",
		Rows:        "1000",
		IndexColumn: "id",
		Datafix:     "file",
	}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.RunID != "20260323120000" {
		t.Errorf("RunID = %q", rec.RunID)
	}
	if rec.Schema != "db1" || rec.Table != "orders" || rec.ObjectType != "table" {
		t.Errorf("schema/table/type wrong: %+v", rec)
	}
	if rec.Rows != "1000" {
		t.Errorf("Rows = %q, want 1000", rec.Rows)
	}
	if rec.IndexColumn != "id" {
		t.Errorf("IndexColumn = %q", rec.IndexColumn)
	}
}

func TestNormalizePodToRecord_DDLYesRowsEmpty(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	pod := Pod{Schema: "db1", Table: "t1", CheckObject: "data", DIFFS: "DDL-yes", Rows: "500"}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.Rows != "" {
		t.Errorf("expected empty Rows for DDL-yes, got %q", rec.Rows)
	}
	if rec.Diffs != "DDL-yes" {
		t.Errorf("Diffs = %q, want DDL-yes", rec.Diffs)
	}
}

func TestNormalizePodToRecord_routineNoTableField(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	pod := Pod{Schema: "db1", ProcName: "sp_calc", CheckObject: "procedure", DIFFS: "no"}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.Table != "" {
		t.Errorf("Table should be empty for procedure, got %q", rec.Table)
	}
	if rec.ObjectName != "sp_calc" {
		t.Errorf("ObjectName = %q, want sp_calc", rec.ObjectName)
	}
	if rec.ObjectType != "procedure" {
		t.Errorf("ObjectType = %q, want procedure", rec.ObjectType)
	}
}

func TestNormalizePodToRecord_triggerNoTableField(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	pod := Pod{Schema: "db1", TriggerName: "trg_ins", CheckObject: "trigger", DIFFS: "yes"}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.Table != "" {
		t.Errorf("Table should be empty for trigger, got %q", rec.Table)
	}
	if rec.ObjectType != "trigger" {
		t.Errorf("ObjectType = %q, want trigger", rec.ObjectType)
	}
}

// ---------------------------------------------------------------------------
// ShouldDisplayInTerminal
// ---------------------------------------------------------------------------

func TestShouldDisplayInTerminal_allModeAlwaysTrue(t *testing.T) {
	for _, diffs := range []string{"yes", "no", "DDL-yes", "warn-only", "collation-mapped", ""} {
		rec := ResultRecord{Diffs: diffs}
		if !ShouldDisplayInTerminal(rec, "all") {
			t.Errorf("mode=all should show diffs=%q", diffs)
		}
	}
}

func TestShouldDisplayInTerminal_abnormalModeFilters(t *testing.T) {
	show := []string{"yes", "DDL-yes", "warn-only"}
	hide := []string{"no", "collation-mapped", ""}

	for _, diffs := range show {
		rec := ResultRecord{Diffs: diffs}
		if !ShouldDisplayInTerminal(rec, "abnormal") {
			t.Errorf("mode=abnormal should show diffs=%q", diffs)
		}
	}
	for _, diffs := range hide {
		rec := ResultRecord{Diffs: diffs}
		if ShouldDisplayInTerminal(rec, "abnormal") {
			t.Errorf("mode=abnormal should hide diffs=%q", diffs)
		}
	}
}

func TestShouldDisplayInTerminal_unknownModeFallsThrough(t *testing.T) {
	rec := ResultRecord{Diffs: "no"}
	if !ShouldDisplayInTerminal(rec, "unknown") {
		t.Errorf("unknown mode should default to show-all")
	}
}
