package actions

import (
	"fmt"
	"gt-checksum/global"
	"os"
	"path/filepath"
	"strings"
)

func (sp *SchedulePlan) writeColumnsModeAdvisory(sourceOnlyCount, targetOnlyCount int, logThreadSeq int64) {
	adv := sp.sourceOnlyAdvisory
	if adv == nil || (sourceOnlyCount == 0 && targetOnlyCount == 0) {
		return
	}
	const advisoryDrive = "mysql"

	// sp.datafixSql 仅在 datafix=file 时才会被初始化；datafix=table/yes 时为空字符串。
	// advisory 文件使用独立的目录：优先复用 datafixSql，否则回退到默认目录 "fixsql"。
	advisoryDir := sp.datafixSql
	if advisoryDir == "" {
		advisoryDir = "fixsql"
		// 记录回退时的实际绝对路径，避免因进程工作目录不同导致文件落盘位置不明确。
		if absDir, err := filepath.Abs(advisoryDir); err == nil {
			global.Wlog.Info(fmt.Sprintf("(%d) [columns-advisory] datafix dir not set, using default advisory dir: %s", logThreadSeq, absDir))
		}
	}
	if err := os.MkdirAll(advisoryDir, 0755); err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) [columns-advisory] Failed to create advisory directory %q: %v", logThreadSeq, advisoryDir, err))
		return
	}

	filePath := fmt.Sprintf("%s/columns-advisory.%s.%s.sql",
		advisoryDir, fixFileNameEncode(adv.schema), fixFileNameEncode(adv.table))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) [columns-advisory] Failed to create advisory file %q: %v", logThreadSeq, filePath, err))
		return
	}
	defer f.Close()

	quotedSourceCols := make([]string, len(sp.columnPlanSourceCols))
	for i, c := range sp.columnPlanSourceCols {
		quotedSourceCols[i] = quoteIdentifierByDrive(c, advisoryDrive)
	}
	cols := strings.Join(quotedSourceCols, ", ")

	quotedIndexCols := make([]string, len(adv.indexCols))
	for i, c := range adv.indexCols {
		quotedIndexCols[i] = quoteIdentifierByDrive(c, advisoryDrive)
	}
	pkCols := strings.Join(quotedIndexCols, ", ")
	firstPKCol := adv.indexCols[0]
	firstPKRef := quoteIdentifierByDrive(firstPKCol, advisoryDrive)

	// 表映射场景下目标表名可能与源表名不同
	destTableName := adv.destTable
	if destTableName == "" {
		destTableName = adv.table
	}
	destSchemaName := adv.destSchema
	if destSchemaName == "" {
		destSchemaName = adv.schema
	}
	sourceRef := qualifiedTableByDrive(adv.schema, adv.table, advisoryDrive)
	targetRef := qualifiedTableByDrive(destSchemaName, destTableName, advisoryDrive)

	var sb strings.Builder
	sb.WriteString("-- ===========================================================================\n")
	sb.WriteString("-- [MANUAL ACTION REQUIRED] gt-checksum columns-mode advisory\n")
	sb.WriteString("-- ===========================================================================\n")
	sb.WriteString("--\n")
	sb.WriteString(fmt.Sprintf("-- Source table       : %s\n", sourceRef))
	sb.WriteString(fmt.Sprintf("-- Target table       : %s\n", targetRef))
	sb.WriteString(fmt.Sprintf("-- Columns checked    : %s\n", cols))
	sb.WriteString(fmt.Sprintf("-- Primary/unique key : %s\n", pkCols))
	sb.WriteString("--\n")

	if sourceOnlyCount > 0 {
		sb.WriteString(fmt.Sprintf("-- Source-only rows (exist in source, absent in target) : %d\n", sourceOnlyCount))
		sb.WriteString("--   In columns mode, INSERT statements are NOT auto-generated because\n")
		sb.WriteString("--   only partial columns were compared and the full row content is unknown.\n")
		sb.WriteString("--   Query to locate these rows:\n")
		sb.WriteString(fmt.Sprintf("--     SELECT src.* FROM %s src\n", sourceRef))
		sb.WriteString(fmt.Sprintf("--     LEFT JOIN %s dst USING (%s)\n", targetRef, pkCols))
		sb.WriteString(fmt.Sprintf("--     WHERE dst.%s IS NULL;\n", firstPKRef))
		sb.WriteString("--\n")
	}

	if targetOnlyCount > 0 {
		sb.WriteString(fmt.Sprintf("-- Target-only rows (exist in target, absent in source) : %d\n", targetOnlyCount))
		sb.WriteString("--   extraRowsSyncToSource=OFF, DELETE statements are NOT auto-generated.\n")
		sb.WriteString("--   Set extraRowsSyncToSource=ON to auto-generate DELETE fix SQL, or\n")
		sb.WriteString("--   manually review and delete these rows if appropriate.\n")
		sb.WriteString("--   Query to locate these rows:\n")
		sb.WriteString(fmt.Sprintf("--     SELECT dst.* FROM %s dst\n", targetRef))
		sb.WriteString(fmt.Sprintf("--     LEFT JOIN %s src USING (%s)\n", sourceRef, pkCols))
		sb.WriteString(fmt.Sprintf("--     WHERE src.%s IS NULL;\n", firstPKRef))
		sb.WriteString("--\n")
	}

	sb.WriteString("-- RECOMMENDATION:\n")
	sb.WriteString("--   Review the rows above, then manually INSERT / DELETE as needed.\n")
	sb.WriteString("--   After remediation, re-run gt-checksum to verify consistency.\n")
	sb.WriteString("--\n")
	sb.WriteString("-- ===========================================================================\n")

	if _, err := f.WriteString(sb.String()); err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) [columns-advisory] Failed to write advisory file %q: %v", logThreadSeq, filePath, err))
		return
	}
	global.Wlog.Info(fmt.Sprintf("(%d) [columns-advisory] Written advisory file: %s (source-only=%d, target-only=%d)",
		logThreadSeq, filePath, sourceOnlyCount, targetOnlyCount))
}

func columnsModeFilteredCols(allCols []map[string]string, compareCols []string, pkCols []string) []map[string]string {
	// Case-insensitive lookup to match MySQL/MariaDB column name semantics.
	// TrimSpace is applied in addition to ToLower so that metadata with incidental
	// surrounding whitespace (rare but possible) is handled consistently with
	// normalizeColumnLookupKey in my_query_table_data.go.
	colByName := make(map[string]map[string]string, len(allCols))
	for _, col := range allCols {
		if name, ok := col["columnName"]; ok {
			colByName[strings.ToLower(strings.TrimSpace(name))] = col
		}
	}
	pkSet := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		pkSet[strings.ToLower(strings.TrimSpace(c))] = true
	}
	// PK columns first, in pkCols order
	var result []map[string]string
	for _, pkCol := range pkCols {
		if c, ok := colByName[strings.ToLower(strings.TrimSpace(pkCol))]; ok {
			result = append(result, c)
		}
	}
	// Compare columns in compareCols order (skip any that are also PK)
	for _, cmpCol := range compareCols {
		if pkSet[strings.ToLower(strings.TrimSpace(cmpCol))] {
			continue
		}
		if c, ok := colByName[strings.ToLower(strings.TrimSpace(cmpCol))]; ok {
			result = append(result, c)
		}
	}
	return result
}

func columnsModeSplitPKAndCompare(filteredCols []map[string]string, pkCols []string) ([]int, []string) {
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, c := range pkCols {
		pkSet[strings.ToLower(strings.TrimSpace(c))] = struct{}{}
	}

	pkPositions := make([]int, 0, len(pkCols))
	compareColNames := make([]string, 0, len(filteredCols))
	for i, col := range filteredCols {
		name := strings.ToLower(strings.TrimSpace(col["columnName"]))
		if _, ok := pkSet[name]; ok {
			pkPositions = append(pkPositions, i)
			continue
		}
		compareColNames = append(compareColNames, col["columnName"])
	}
	return pkPositions, compareColNames
}

func columnsModeExtractPKKey(rowData string, pkPositions []int) string {
	parts := strings.Split(rowData, "/*go actions columnData*/")
	vals := make([]string, len(pkPositions))
	for i, pos := range pkPositions {
		if pos < len(parts) {
			vals[i] = parts[pos]
		} else {
			vals[i] = pkKeyMissingMarker
			if global.Wlog != nil {
				global.Wlog.Warnf("columnsModeExtractPKKey: pos %d out of bounds (parts=%d), row data may be corrupted", pos, len(parts))
			}
		}
	}
	return strings.Join(vals, "\x00")
}
