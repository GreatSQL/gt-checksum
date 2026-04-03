package actions

import (
	"fmt"
	"gt-checksum/inputArg"
	"strings"
	"time"
)

// ResultRecord is the normalized, export-stable representation of a single check result.
// It is derived from Pod and serves as the canonical model for CSV export. Terminal output
// currently still renders directly from Pod; ResultRecord is partially reused there via
// ShouldDisplayInTerminal() and resolveEffectiveDiffs(). Fields are intentionally stable
// across all checkObject modes; unused fields are left empty rather than omitted so that
// CSV column order never changes.
type ResultRecord struct {
	RunID       string
	CheckTime   string
	CheckObject string

	Schema     string
	Table      string
	ObjectName string
	ObjectType string

	IndexColumn string
	Rows        string
	Diffs       string
	Datafix     string

	Mapping string
	Definer string
	Columns string
}

// BuildResultRecords converts the global measuredDataPods slice into a normalized
// []ResultRecord. Must be called after all pods have been collected (i.e., after
// the checksum phase completes).
func BuildResultRecords(m *inputArg.ConfigParameter) []ResultRecord {
	pods := measuredDataPods
	checkTime := time.Now().Format("2006-01-02 15:04:05")
	records := make([]ResultRecord, 0, len(pods))
	for _, pod := range pods {
		records = append(records, normalizePodToRecord(m, pod, checkTime))
	}
	return records
}

// normalizeCheckObject maps the internal pod CheckObject value (which may be
// "Procedure" or "Function" in routine mode) to the canonical user-facing mode
// name as configured by the checkObject parameter.
func normalizeCheckObject(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "procedure", "function":
		return "routine"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

// resolveEffectiveDiffs returns the effective Diffs value for a pod.
// Pod.DIFFS is always set to the correct value before the pod is appended to
// measuredDataPods (both index and no-index data paths set it synchronously),
// so this function is a direct passthrough. It is retained as the single call
// site for Diffs resolution so that terminal pre-filtering and CSV normalization
// remain consistent without duplicating logic.
func resolveEffectiveDiffs(pod Pod) string {
	return pod.DIFFS
}

// normalizePodToRecord converts a single Pod into a stable ResultRecord.
func normalizePodToRecord(m *inputArg.ConfigParameter, pod Pod, checkTime string) ResultRecord {
	schema, objectName, objectType := resolveObjectIdentity(pod)

	// Resolve effective Diffs through the shared helper so terminal and CSV stay aligned.
	diffs := resolveEffectiveDiffs(pod)

	// DDL-yes rows are always empty (consistent with dataResultRows helper).
	rows := pod.Rows
	if pod.DIFFS == "DDL-yes" {
		rows = ""
	}

	// Table field is only meaningful for data / struct / sequence objects.
	table := ""
	if objectType == "table" || objectType == "sequence" {
		table = objectName
	}

	return ResultRecord{
		RunID:       m.RunID,
		CheckTime:   checkTime,
		CheckObject: normalizeCheckObject(pod.CheckObject),
		Schema:      schema,
		Table:       table,
		ObjectName:  objectName,
		ObjectType:  objectType,
		IndexColumn: pod.IndexColumn,
		Rows:        rows,
		Diffs:       diffs,
		Datafix:     pod.Datafix,
		Mapping:     resolveMappingForRecord(schema, objectName, pod),
		Definer:     pod.Definer,
		Columns:     pod.ColumnsInfo,
	}
}

// resolveObjectIdentity extracts the canonical (schema, objectName, objectType) from a Pod,
// normalizing any colon- or dot-encoded schema prefixes that the internal Pod fields carry.
//
// When pod.ObjectKind is set to a recognised value ("view"), it takes priority over the
// CheckObject-based derivation so that VIEW objects in struct mode are correctly labelled
// as ObjectType="view" rather than the default "table".  All other ObjectKind values (or
// the empty string) fall through to the existing CheckObject switch, preserving all
// pre-Phase-1 behaviour with zero risk to existing code paths.
func resolveObjectIdentity(pod Pod) (schema, objectName, objectType string) {
	schema = pod.Schema

	// ObjectKind override — only "view" is recognised in Phase 1.
	if strings.ToLower(strings.TrimSpace(pod.ObjectKind)) == "view" {
		objectName = pod.Table
		objectType = "view"
		schema, objectName = normalizeSchemaObjectName(schema, objectName)
		return schema, objectName, objectType
	}

	lc := strings.ToLower(strings.TrimSpace(pod.CheckObject))

	switch lc {
	case "procedure":
		objectName = pod.ProcName
		if objectName == "" {
			objectName = pod.FuncName
		}
		objectType = "procedure"
	case "function":
		objectName = pod.FuncName
		if objectName == "" {
			objectName = pod.ProcName
		}
		objectType = "function"
	case "trigger":
		objectName = pod.TriggerName
		objectType = "trigger"
	case "sequence":
		objectName = pod.Table
		objectType = "sequence"
	default: // data, struct, index, partitions, foreign, etc.
		objectName = pod.Table
		objectType = "table"
	}

	schema, objectName = normalizeSchemaObjectName(schema, objectName)
	return schema, objectName, objectType
}

// normalizeSchemaObjectName handles the three encoded name formats that Pod fields may contain:
//   - "db1.*:db2.*"  → schema = "db2", name unchanged
//   - "schema:name"  → split on first ":", schema = parts[0], name = parts[1]
//   - "schema.name"  → split on "." when schema is still empty
func normalizeSchemaObjectName(schema, name string) (string, string) {
	if strings.Contains(name, ".*:") {
		parts := strings.SplitN(name, ".*:", 2)
		if len(parts) == 2 {
			schema = parts[1]
		}
		return schema, name
	}
	if strings.Contains(name, ":") {
		parts := strings.SplitN(name, ":", 2)
		if schema == "" {
			schema = parts[0]
			name = parts[1]
		} else {
			name = parts[0]
		}
	}
	if schema == "" && strings.Contains(name, ".") {
		parts := strings.SplitN(name, ".", 2)
		schema = parts[0]
		name = parts[1]
	}
	return schema, name
}

// resolveMappingForRecord returns the mapping description string for a record.
// Prefers pod.MappingInfo if already populated; falls back to the schema mapping table.
func resolveMappingForRecord(schema, objectName string, pod Pod) string {
	if !hasMappingRelations() {
		return ""
	}
	if pod.MappingInfo != "" {
		return pod.MappingInfo
	}
	schemaMap := getSchemaMappings()
	if destSchema, exists := schemaMap[schema]; exists {
		return fmt.Sprintf("Schema: %s:%s", schema, destSchema)
	}
	return ""
}

// ShouldDisplayInTerminal reports whether a ResultRecord should be shown on the terminal
// given the configured terminalResultMode.
//
//   - "all"      → show everything (default)
//   - "abnormal" → show only Diffs values that indicate a problem
func ShouldDisplayInTerminal(record ResultRecord, mode string) bool {
	if mode == "abnormal" {
		return record.Diffs == "yes" ||
			record.Diffs == "DDL-yes" ||
			record.Diffs == "warn-only"
	}
	return true // "all" or any unrecognized value
}
