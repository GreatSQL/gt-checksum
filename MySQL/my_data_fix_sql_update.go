package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strings"
)

// FixUpdateSqlExec generates UPDATE SET <compareColNames> WHERE <pkCols> for columns-mode two-sided rows.
// srcRowData is the source-side row data (values separated by "/*go actions columnData*/") in ColData column order.
// compareColNames are the non-PK source column names used to look up values in ColData.
// srcToDstCol maps source column names to destination column names for the SET clause; nil means names are identical.
// The WHERE clause is built from my.IndexColumn (PK/unique key columns).
func (my *MysqlDataAbnormalFixStruct) FixUpdateSqlExec(db *sql.DB, srcRowData string, compareColNames []string, srcToDstCol map[string]string, logThreadSeq int64) (string, error) {
	if len(compareColNames) == 0 || len(my.ColData) == 0 {
		return "", nil
	}

	// Build col name → position map from ColData.
	// Keys are lowercased so lookups are case-insensitive across DB metadata and user config.
	colPosMap := make(map[string]int, len(my.ColData))
	colTypeMap := make(map[string]string, len(my.ColData))
	for i, col := range my.ColData {
		if name, ok := col["columnName"]; ok {
			colPosMap[strings.ToLower(name)] = i
			colTypeMap[strings.ToLower(name)] = col["dataType"]
		}
	}

	rowParts := strings.Split(srcRowData, "/*go actions columnData*/")

	// Helper: get formatted literal for a named column (case-insensitive lookup)
	colLiteral := func(colName string) (string, bool) {
		key := strings.ToLower(colName)
		pos, ok := colPosMap[key]
		if !ok || pos >= len(rowParts) {
			return "", false
		}
		return formatMySQLInsertLiteral(rowParts[pos], colTypeMap[key]), true
	}

	// Build SET clause from compareColNames (non-PK columns to update).
	// Use srcToDstCol to translate source column names to destination column names for the SET key.
	// srcToDstCol keys are lowercased at construction time; look up with ToLower for case-insensitive mapping.
	//
	// Design note on error strategy: a missing SET column is treated as a degraded but recoverable
	// situation — the UPDATE is still emitted with the remaining columns, and a Warn is logged so the
	// operator can investigate. This is intentional: skipping one non-PK column causes a partial
	// update, which is visible and correctable on the next checksum cycle.
	// By contrast, a missing PK column (see WHERE clause below) is fatal: without a complete WHERE
	// predicate the UPDATE would match unintended rows, so we return an error immediately.
	var setClauses []string
	for _, srcColName := range compareColNames {
		lit, ok := colLiteral(srcColName)
		if !ok {
			global.Wlog.Warn(fmt.Sprintf("(%d) FixUpdateSqlExec: column %q not found in ColData for %s.%s; skipping SET clause",
				logThreadSeq, srcColName, my.Schema, my.Table))
			continue
		}
		dstColName := srcColName
		if srcToDstCol != nil {
			if mapped, exists := srcToDstCol[strings.ToLower(srcColName)]; exists {
				dstColName = mapped
			}
		}
		setClauses = append(setClauses, fmt.Sprintf("%s=%s", mysqlQuoteIdent(dstColName), lit))
	}
	if len(setClauses) == 0 {
		return "", nil
	}

	// Build WHERE clause from IndexColumn (PK columns).
	// colLiteral looks up values case-insensitively (ToLower), so the case of pkCol only
	// affects the quoted identifier emitted into the SQL string — not the value lookup.
	// MySQL/MariaDB evaluate quoted identifiers case-insensitively, so this is safe for
	// the current target set. If support for case-sensitive databases (e.g. PostgreSQL) is
	// added in the future, IndexColumn entries must be normalised to match the target's
	// actual column definition before being passed here.
	var whereClauses []string
	for _, pkCol := range my.IndexColumn {
		lit, ok := colLiteral(pkCol)
		if !ok {
			return "", fmt.Errorf("(%d) FixUpdateSqlExec: PK column %q not found in row data for %s.%s",
				logThreadSeq, pkCol, my.Schema, my.Table)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s=%s", mysqlQuoteIdent(pkCol), lit))
	}
	if len(whereClauses) == 0 {
		return "", fmt.Errorf("(%d) FixUpdateSqlExec: no PK columns (IndexColumn) configured for %s.%s", logThreadSeq, my.Schema, my.Table)
	}

	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;",
		mysqlQuoteIdent(my.Schema), mysqlQuoteIdent(my.Table),
		strings.Join(setClauses, ","),
		strings.Join(whereClauses, " AND ")), nil
}
