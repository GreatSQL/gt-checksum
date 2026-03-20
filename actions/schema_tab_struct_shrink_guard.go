package actions

import (
	"database/sql"
	"fmt"
	"strings"

	"gt-checksum/schemacompat"
)

func quoteMySQLIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
}

func buildColumnShrinkSafetyCheckSQL(schema, table, column string, guard schemacompat.ColumnShrinkGuard) string {
	// Defensive whitelist: only allow known MySQL measurement functions to be
	// spliced into the generated SQL, preventing injection if ColumnShrinkMetric
	// is ever extended with externally-sourced values.
	metric := guard.Metric
	if !metric.IsValid() {
		metric = schemacompat.ColumnShrinkMetricCharLength
	}
	// 使用 SELECT 1 ... LIMIT 1 短路查询代替 SELECT COUNT(*)，
	// 避免在亿级大表上执行全表扫描。业务语义只需判断"是否存在超宽行"。
	return fmt.Sprintf(
		"SELECT 1 FROM %s.%s WHERE %s IS NOT NULL AND %s(%s) > %d LIMIT 1;",
		quoteMySQLIdentifier(schema),
		quoteMySQLIdentifier(table),
		quoteMySQLIdentifier(column),
		metric,
		quoteMySQLIdentifier(column),
		guard.Limit,
	)
}

func queryColumnShrinkViolationExists(db *sql.DB, schema, table, column string, guard schemacompat.ColumnShrinkGuard) (bool, string, error) {
	checkSQL := buildColumnShrinkSafetyCheckSQL(schema, table, column, guard)
	if db == nil {
		return false, checkSQL, fmt.Errorf("target database handle is nil")
	}

	query := strings.TrimSuffix(checkSQL, ";")
	var dummy int
	err := db.QueryRow(query).Scan(&dummy)
	if err == sql.ErrNoRows {
		return false, checkSQL, nil
	}
	if err != nil {
		return false, checkSQL, err
	}
	return true, checkSQL, nil
}

func buildColumnShrinkBlockedSQL(schema, table, modifySQL string) string {
	trimmed := strings.TrimSpace(modifySQL)
	if trimmed == "" {
		return ""
	}
	return fmt.Sprintf(
		"ALTER TABLE %s.%s %s;",
		quoteMySQLIdentifier(schema),
		quoteMySQLIdentifier(table),
		trimmed,
	)
}

func buildColumnShrinkSafetySuggestion(column string, guard schemacompat.ColumnShrinkGuard, hasViolation bool, checkSQL, blockedSQL string, err error) (schemacompat.ConstraintRepairSuggestion, bool) {
	if err == nil && !hasViolation {
		return schemacompat.ConstraintRepairSuggestion{}, false
	}

	reason := ""
	switch {
	case err != nil:
		reason = fmt.Sprintf(
			"unable to verify target data before shrinking column from %s to %s with %s <= %d: %v",
			guard.TargetType,
			guard.SourceType,
			guard.Metric,
			guard.Limit,
			err,
		)
	default:
		reason = fmt.Sprintf(
			"target contains row(s) that exceed the new width check %s <= %d before shrinking column from %s to %s",
			guard.Metric,
			guard.Limit,
			guard.TargetType,
			guard.SourceType,
		)
	}

	statements := []string{checkSQL}
	if blockedSQL != "" {
		statements = append(statements, blockedSQL)
	}

	return schemacompat.ConstraintRepairSuggestion{
		ConstraintName: column,
		Kind:           "COLUMN WIDTH SHRINK",
		Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
		Reason:         reason,
		Statements:     statements,
	}, true
}

func (stcls *schemaTable) buildColumnShrinkAdvisory(destSchema, destTable, column string, sourceCanonical, destCanonical schemacompat.CanonicalColumn, modifySQL string) (schemacompat.ConstraintRepairSuggestion, bool) {
	guard, ok := schemacompat.BuildColumnShrinkGuard(sourceCanonical, destCanonical)
	if !ok {
		return schemacompat.ConstraintRepairSuggestion{}, false
	}

	hasViolation, checkSQL, err := queryColumnShrinkViolationExists(stcls.destDB, destSchema, destTable, column, guard)
	blockedSQL := buildColumnShrinkBlockedSQL(destSchema, destTable, modifySQL)
	return buildColumnShrinkSafetySuggestion(column, guard, hasViolation, checkSQL, blockedSQL, err)
}
