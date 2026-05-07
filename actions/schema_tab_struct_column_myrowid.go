package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"strings"
)

// checkAndGenerateMyRowIDRepositionSQL 检查是否需要调整 my_row_id 隐式主键的位置
// 当 requirePK=ON 且目标端存在 my_row_id 隐式主键时，如果需要调整其他列到 my_row_id 前面，
// 需要生成两步 SQL：1) 先设置 VISIBLE 2) 调整位置并设置回 INVISIBLE
//
// 返回值：
// - []string: 生成的独立 ALTER TABLE 语句（两条），如果不需要调整则返回空切片
// - error: 查询错误
func (stcls *schemaTable) checkAndGenerateMyRowIDRepositionSQL(
	sms *structModeState,
	cm *columnMetaState,
	destSchema string,
	logThreadSeq int64,
	event string,
) ([]string, error) {
	var vlog string

	// 1. 检查目标端是否存在 my_row_id 列
	query := `
		SELECT c.COLUMN_NAME, c.DATA_TYPE, c.EXTRA, c.ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS c
		WHERE c.TABLE_SCHEMA = ?
		  AND c.TABLE_NAME = ?
		  AND c.COLUMN_NAME = 'my_row_id'
	`
	var colName, dataType, extra string
	var ordinalPosition int
	err := stcls.destDB.QueryRow(query, destSchema, stcls.table).Scan(&colName, &dataType, &extra, &ordinalPosition)
	if err != nil {
		if err == sql.ErrNoRows {
			// 目标端不存在 my_row_id 列，无需调整
			return nil, nil
		}
		return nil, err
	}

	// 2. 检查 my_row_id 是否为 INVISIBLE 主键
	extra = strings.ToUpper(strings.TrimSpace(extra))
	if !strings.Contains(extra, "INVISIBLE") {
		// 不是 INVISIBLE 列，无需特殊处理
		vlog = fmt.Sprintf("(%d) %s Column my_row_id in %s.%s is not INVISIBLE, no reposition needed", logThreadSeq, event, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		return nil, nil
	}

	// 3. 检查 my_row_id 是否为主键
	isPrimaryKey, err := stcls.checkIfMyRowIDIsPrimaryKey(destSchema, logThreadSeq)
	if err != nil {
		return nil, err
	}
	if !isPrimaryKey {
		// 不是主键，无需特殊处理
		vlog = fmt.Sprintf("(%d) %s Column my_row_id in %s.%s is not PRIMARY KEY, no reposition needed", logThreadSeq, event, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		return nil, nil
	}

	// 4. 获取目标表的总列数
	totalColumns, err := stcls.getDestTableTotalColumns(destSchema, logThreadSeq)
	if err != nil {
		return nil, err
	}

	// 5. 检查 my_row_id 当前位置（ordinalPosition 从 1 开始）
	// 如果 my_row_id 已经在最后一列，无需调整
	if ordinalPosition == totalColumns {
		vlog = fmt.Sprintf("(%d) %s Column my_row_id in %s.%s is already at last position (%d/%d), no reposition needed", logThreadSeq, event, destSchema, stcls.table, ordinalPosition, totalColumns)
		global.Wlog.Debug(vlog)
		return nil, nil
	}

	// 6. 检查是否将要添加显式主键列
	// 如果 sms.alterSlice 中包含 ADD COLUMN ... PRIMARY KEY 语句，
	// 说明将要添加显式主键，此时应该删除 my_row_id 而不是调整它
	for _, alterSQL := range sms.alterSlice {
		upperSQL := strings.ToUpper(alterSQL)
		if strings.Contains(upperSQL, "ADD COLUMN") && strings.Contains(upperSQL, "PRIMARY KEY") {
			vlog = fmt.Sprintf("(%d) %s Detected explicit PRIMARY KEY column addition in %s.%s, my_row_id should be dropped instead of repositioned", logThreadSeq, event, destSchema, stcls.table)
			global.Wlog.Info(vlog)
			return nil, nil
		}
	}

	// 7. 检查是否有其他列需要调整到 my_row_id 前面
	// 如果 sms.alterSlice 中包含 MODIFY COLUMN ... FIRST 或 MODIFY COLUMN ... AFTER 语句，
	// 且这些语句会导致列位置在 my_row_id 之前，则需要调整 my_row_id 位置
	hasColumnPositionChange := false
	for _, alterSQL := range sms.alterSlice {
		upperSQL := strings.ToUpper(alterSQL)
		if strings.Contains(upperSQL, "MODIFY COLUMN") && (strings.Contains(upperSQL, "FIRST") || strings.Contains(upperSQL, "AFTER")) {
			hasColumnPositionChange = true
			break
		}
	}

	if !hasColumnPositionChange {
		// 没有列位置调整，无需处理 my_row_id
		vlog = fmt.Sprintf("(%d) %s No column position changes detected for %s.%s, no my_row_id reposition needed", logThreadSeq, event, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		return nil, nil
	}

	// 7. 生成 my_row_id 位置调整 SQL
	// 获取目标表的最后一列名（排除 my_row_id 本身）
	lastColumnName, err := stcls.getDestTableLastColumnExcludingMyRowID(destSchema, logThreadSeq)
	if err != nil {
		return nil, err
	}
	if lastColumnName == "" {
		// 表中只有 my_row_id 一列，无需调整
		vlog = fmt.Sprintf("(%d) %s Table %s.%s has only my_row_id column, no reposition needed", logThreadSeq, event, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		return nil, nil
	}

	// 8. 生成两条独立的 ALTER TABLE 语句
	// 第一步：设置 my_row_id 为 VISIBLE
	step1SQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `my_row_id` %s unsigned NOT NULL AUTO_INCREMENT /*!80023 VISIBLE */;", destSchema, stcls.table, dataType)
	vlog = fmt.Sprintf("(%d) %s Step 1: Set my_row_id to VISIBLE for %s.%s: %s", logThreadSeq, event, destSchema, stcls.table, step1SQL)
	global.Wlog.Info(vlog)

	// 第二步：调整 my_row_id 位置到最后一列，并设置回 INVISIBLE
	step2SQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `my_row_id` %s unsigned NOT NULL AUTO_INCREMENT /*!80023 INVISIBLE */ AFTER `%s`;", destSchema, stcls.table, dataType, lastColumnName)
	vlog = fmt.Sprintf("(%d) %s Step 2: Reposition my_row_id to last and set INVISIBLE for %s.%s: %s", logThreadSeq, event, destSchema, stcls.table, step2SQL)
	global.Wlog.Info(vlog)

	return []string{step1SQL, step2SQL}, nil
}

// checkIfMyRowIDIsPrimaryKey 检查 my_row_id 是否为主键
func (stcls *schemaTable) checkIfMyRowIDIsPrimaryKey(destSchema string, logThreadSeq int64) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME = 'my_row_id'
		  AND CONSTRAINT_NAME = 'PRIMARY'
	`
	var count int
	err := stcls.destDB.QueryRow(query, destSchema, stcls.table).Scan(&count)
	if err != nil {
		vlog := fmt.Sprintf("(%d) Error checking if my_row_id is PRIMARY KEY for %s.%s: %v", logThreadSeq, destSchema, stcls.table, err)
		global.Wlog.Error(vlog)
		return false, err
	}
	return count > 0, nil
}

// getDestTableTotalColumns 获取目标表的总列数
func (stcls *schemaTable) getDestTableTotalColumns(destSchema string, logThreadSeq int64) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME = ?
	`
	var count int
	err := stcls.destDB.QueryRow(query, destSchema, stcls.table).Scan(&count)
	if err != nil {
		vlog := fmt.Sprintf("(%d) Error getting total columns for %s.%s: %v", logThreadSeq, destSchema, stcls.table, err)
		global.Wlog.Error(vlog)
		return 0, err
	}
	return count, nil
}

// getDestTableLastColumnExcludingMyRowID 获取目标表的最后一列名（排除 my_row_id）
func (stcls *schemaTable) getDestTableLastColumnExcludingMyRowID(destSchema string, logThreadSeq int64) (string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME = ?
		  AND COLUMN_NAME != 'my_row_id'
		ORDER BY ORDINAL_POSITION DESC
		LIMIT 1
	`
	var columnName string
	err := stcls.destDB.QueryRow(query, destSchema, stcls.table).Scan(&columnName)
	if err != nil {
		if err == sql.ErrNoRows {
			// 表中只有 my_row_id 一列
			return "", nil
		}
		vlog := fmt.Sprintf("(%d) Error getting last column for %s.%s: %v", logThreadSeq, destSchema, stcls.table, err)
		global.Wlog.Error(vlog)
		return "", err
	}
	return columnName, nil
}
