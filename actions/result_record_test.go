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

func TestResolveObjectIdentity_structModeViewOverride(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "v_users", CheckObject: "struct", ObjectKind: "view"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" || objectName != "v_users" || objectType != "view" {
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

func TestStructResultObjectType_defaultsToTable(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "orders", CheckObject: "struct"}
	if got := structResultObjectType(pod); got != "table" {
		t.Fatalf("expected table, got %q", got)
	}
}

func TestStructResultObjectType_viewOverride(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "v_orders", CheckObject: "struct", ObjectKind: "view"}
	if got := structResultObjectType(pod); got != "view" {
		t.Fatalf("expected view, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// normalizeCheckObject
// ---------------------------------------------------------------------------

func TestNormalizeCheckObject_procedureBecomesRoutine(t *testing.T) {
	if got := normalizeCheckObject("Procedure"); got != "routine" {
		t.Errorf("normalizeCheckObject(Procedure) = %q, want routine", got)
	}
}

func TestNormalizeCheckObject_functionBecomesRoutine(t *testing.T) {
	if got := normalizeCheckObject("Function"); got != "routine" {
		t.Errorf("normalizeCheckObject(Function) = %q, want routine", got)
	}
}

func TestNormalizeCheckObject_lowercasePassThrough(t *testing.T) {
	for _, raw := range []string{"data", "struct", "trigger", "sequence"} {
		if got := normalizeCheckObject(raw); got != raw {
			t.Errorf("normalizeCheckObject(%q) = %q, want %q", raw, got, raw)
		}
	}
}

func TestNormalizeCheckObject_uppercaseLowered(t *testing.T) {
	if got := normalizeCheckObject("DATA"); got != "data" {
		t.Errorf("normalizeCheckObject(DATA) = %q, want data", got)
	}
}

// ---------------------------------------------------------------------------
// resolveEffectiveDiffs
// ---------------------------------------------------------------------------

func TestResolveEffectiveDiffs_nonDataModePassThrough(t *testing.T) {
	pod := Pod{CheckObject: "struct", DIFFS: "no"}
	if got := resolveEffectiveDiffs(pod); got != "no" {
		t.Errorf("resolveEffectiveDiffs struct no-diff = %q, want no", got)
	}
}

func TestResolveEffectiveDiffs_dataModePlainDiff(t *testing.T) {
	pod := Pod{CheckObject: "data", Schema: "db1", Table: "t1", DIFFS: "yes"}
	if got := resolveEffectiveDiffs(pod); got != "yes" {
		t.Errorf("resolveEffectiveDiffs data yes = %q, want yes", got)
	}
}

func TestResolveEffectiveDiffs_dataModeNoDiff(t *testing.T) {
	// A data pod with DIFFS="no" should pass through as-is.
	pod := Pod{CheckObject: "data", Schema: "db1", Table: "orders", DIFFS: "no"}
	if got := resolveEffectiveDiffs(pod); got != "no" {
		t.Errorf("resolveEffectiveDiffs data no-diff = %q, want no", got)
	}
}

func TestNormalizePodToRecord_routineCheckObjectIsRoutine(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	// Simulate what schema_tab_struct.go sets for a stored procedure.
	pod := Pod{Schema: "db1", ProcName: "sp_calc", CheckObject: "Procedure", DIFFS: "no"}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.CheckObject != "routine" {
		t.Errorf("CheckObject = %q, want routine", rec.CheckObject)
	}
	if rec.ObjectType != "procedure" {
		t.Errorf("ObjectType = %q, want procedure", rec.ObjectType)
	}
}

func TestNormalizePodToRecord_functionCheckObjectIsRoutine(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	pod := Pod{Schema: "db1", FuncName: "fn_sum", CheckObject: "Function", DIFFS: "no"}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.CheckObject != "routine" {
		t.Errorf("CheckObject = %q, want routine", rec.CheckObject)
	}
	if rec.ObjectType != "function" {
		t.Errorf("ObjectType = %q, want function", rec.ObjectType)
	}
}

// ---------------------------------------------------------------------------
// VIEW ObjectKind → ObjectType routing (Phase 2)
// ---------------------------------------------------------------------------

func TestResolveObjectIdentity_viewObjectKind(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "v_orders", CheckObject: "struct", ObjectKind: "view"}
	schema, objectName, objectType := resolveObjectIdentity(pod)
	if schema != "db1" {
		t.Errorf("schema = %q, want db1", schema)
	}
	if objectName != "v_orders" {
		t.Errorf("objectName = %q, want v_orders", objectName)
	}
	if objectType != "view" {
		t.Errorf("objectType = %q, want view", objectType)
	}
}

func TestResolveObjectIdentity_viewObjectKindCaseInsensitive(t *testing.T) {
	pod := Pod{Schema: "db1", Table: "v_orders", CheckObject: "struct", ObjectKind: "VIEW"}
	_, _, objectType := resolveObjectIdentity(pod)
	if objectType != "view" {
		t.Errorf("objectType = %q, want view (ObjectKind='VIEW' should be case-insensitive)", objectType)
	}
}

func TestResolveObjectIdentity_emptyObjectKindFallsThrough(t *testing.T) {
	// Empty ObjectKind must fall through to CheckObject-based logic (no regressions).
	pod := Pod{Schema: "db1", Table: "orders", CheckObject: "struct", ObjectKind: ""}
	_, _, objectType := resolveObjectIdentity(pod)
	if objectType != "table" {
		t.Errorf("objectType = %q, want table when ObjectKind is empty", objectType)
	}
}

func TestNormalizePodToRecord_viewObjectType(t *testing.T) {
	m := mockConfig("20260323120000", "all")
	pod := Pod{Schema: "db1", Table: "v_orders", CheckObject: "struct", ObjectKind: "view", DIFFS: "yes"}
	rec := normalizePodToRecord(m, pod, "2026-03-23 12:00:00")
	if rec.ObjectType != "view" {
		t.Errorf("ObjectType = %q, want view", rec.ObjectType)
	}
	if rec.ObjectName != "v_orders" {
		t.Errorf("ObjectName = %q, want v_orders", rec.ObjectName)
	}
	// Table field is empty for non-table object types (consistent with routine/trigger).
	if rec.Table != "" {
		t.Errorf("Table = %q, want empty for view records", rec.Table)
	}
	if rec.Diffs != "yes" {
		t.Errorf("Diffs = %q, want yes", rec.Diffs)
	}
	// VIEW pods keep CheckObject=struct in the result record (cc §5.3).
	if rec.CheckObject != "struct" {
		t.Errorf("CheckObject = %q, want struct (VIEW pods must retain CheckObject=struct)", rec.CheckObject)
	}
}
