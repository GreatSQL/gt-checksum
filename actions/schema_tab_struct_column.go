package actions

import (
	"fmt"
	"gt-checksum/global"
	"sort"
	"strings"
)


func (stcls *schemaTable) rememberColumnRepairOperations(tableKey string, sqls []string) {
	if stcls == nil || len(sqls) == 0 {
		return
	}
	if stcls.columnRepairMap == nil {
		stcls.columnRepairMap = make(map[string][]string)
	}
	stcls.columnRepairMap[tableKey] = cloneSQLStatements(sqls)
}

func (stcls *schemaTable) pendingColumnRepairOperations(tableKey string) []string {
	if stcls == nil || stcls.columnRepairMap == nil {
		return nil
	}
	return cloneSQLStatements(stcls.columnRepairMap[tableKey])
}

func (stcls *schemaTable) forgetColumnRepairOperations(tableKey string) {
	if stcls == nil || stcls.columnRepairMap == nil {
		return
	}
	delete(stcls.columnRepairMap, tableKey)
}

func hasAutoIncrementColumnAttribute(columnDefinition []string) bool {
	for _, attr := range columnDefinition {
		if strings.Contains(strings.ToUpper(attr), "AUTO_INCREMENT") {
			return true
		}
	}
	return false
}

func adjustDestColumnSeqAfterDrops(destColumnSeq map[string]int, dropped []string) {
	droppedPositions := make([]int, 0, len(dropped))
	for _, col := range dropped {
		droppedPositions = append(droppedPositions, destColumnSeq[col])
		delete(destColumnSeq, col)
	}
	sort.Ints(droppedPositions)
	for col, seq := range destColumnSeq {
		adj := 0
		for _, dp := range droppedPositions {
			if dp < seq {
				adj++
			}
		}
		destColumnSeq[col] -= adj
	}
}


func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		event                                string
	)
	stcls.prepareStructCheck(checkTableList, logThreadSeq, logThreadSeq2, event)
	for _, v := range checkTableList {
		sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, ok := stcls.resolveTableMapping(v, logThreadSeq, event)
		if !ok {
			continue
		}

		sourceTableExists, destTableExists, skip, err := stcls.checkTableExistence(sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event, logThreadSeq)
		if err != nil {
			return nil, nil, err
		}
		if skip {
			abnormalTableList = append(abnormalTableList, mappedTableKey)
			continue
		}

		oracleToMySQLDataMode := stcls.sourceDrive == "godror" && stcls.destDrive == "mysql" && stcls.checkRules.CheckObject != "struct"

		if sourceTableExists && !destTableExists {
			abnormalKey, err := stcls.handleTargetMissingTable(sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event, logThreadSeq)
			if err != nil {
				return nil, nil, err
			}
			if abnormalKey != "" {
				abnormalTableList = append(abnormalTableList, abnormalKey)
			}
			continue
		}

		// 处理特殊情况：源表不存在但目标表存在
		if !sourceTableExists && destTableExists {
			abnormalKey, err := stcls.handleSourceMissingTable(destSchema, destTableName, logThreadSeq, event)
			if err != nil {
				return nil, nil, err
			}
			abnormalTableList = append(abnormalTableList, abnormalKey)
			continue
		}

		cm, err := stcls.loadAndNormalizeColumns(sourceSchema, sourceTableName, destSchema, destTableName, event, oracleToMySQLDataMode, aa, logThreadSeq, logThreadSeq2)
		if err != nil {
			return nil, nil, err
		}
		if stcls.checkRules.CheckObject != "struct" {
			newKey, abnormalKey := stcls.evaluateNonStructColumnDiff(cm, sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event, logThreadSeq)
			if newKey != "" {
				newCheckTableList = append(newCheckTableList, newKey)
			}
			if abnormalKey != "" {
				abnormalTableList = append(abnormalTableList, abnormalKey)
			}
			continue
		}

		// 8a: struct 上下文准备（SHOW CREATE TABLE / definitions）
		sms := stcls.prepareStructModeState(sourceSchema, destSchema, cm.alterSlice, logThreadSeq, event)

		// 兜底检查：即使 checkTableExistence 返回 destTableExists=true（information_schema 显示表存在），
		// SHOW CREATE TABLE 仍可能失败（权限问题、表损坏等），此时 destColumnDefinitions 为空。
		// 这种情况下应该生成 CREATE TABLE 而不是继续执行 ALTER TABLE 逻辑。
		if len(sms.destColumnDefinitions) == 0 && len(sms.sourceColumnDefinitions) > 0 {
			// 目标端表不存在，应该生成 CREATE TABLE 语句而不是 ALTER TABLE 语句
			global.Wlog.Info(fmt.Sprintf("(%d) %s Target table %s.%s does not exist (SHOW CREATE TABLE failed), generating CREATE TABLE instead of ALTER TABLE", logThreadSeq, event, destSchema, stcls.destTable))
			abnormalKey, err := stcls.handleTargetMissingTable(sourceSchema, sourceTableName, destSchema, stcls.destTable, mappedTableKey, event, logThreadSeq)
			if err != nil {
				return nil, nil, err
			}
			if abnormalKey != "" {
				abnormalTableList = append(abnormalTableList, abnormalKey)
			}
			continue
		}

		vlog = fmt.Sprintf("(%d) %s Columns to remove from target %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, cm.delColumn)
		global.Wlog.Debug(vlog)
		// 8b: 删除目标端多余列（AUTO_INCREMENT 守护）
		stcls.dropExcessColumns(sms, cm, logThreadSeq, event, destSchema)
		// 8c+8d: 列差异调和（新增列 + 列修改）
		myRowIDRepositionSQLs := stcls.reconcileColumnDiffs(sms, cm, sourceSchema, destSchema, logThreadSeq, event)

		// 8e: 生成列级别的修复SQL
		fixer := cm.dbf.DataAbnormalFix()
		sqlS := fixer.FixAlterColumnSqlGenerate(sms.alterSlice, logThreadSeq)

		// 8e-1: 将 my_row_id 位置调整的独立 SQL 语句追加到 sqlS 中
		// 这些语句必须在其他列修复操作之后执行，且不能与其他操作合并
		if len(myRowIDRepositionSQLs) > 0 {
			sqlS = append(sqlS, myRowIDRepositionSQLs...)
			vlog = fmt.Sprintf("(%d) %s Appended %d independent my_row_id reposition SQL statements for %s.%s", logThreadSeq, event, len(myRowIDRepositionSQLs), destSchema, stcls.table)
			global.Wlog.Debug(vlog)
		}

		// 8f: MySQL→MySQL 字符集/排序规则/表级属性 advisory 检查
		result := stcls.buildCharsetAdvisory(sms, cm, fixer, sourceSchema, destSchema, logThreadSeq, event)
		sqlS = append(sqlS, result.sqlS...)

		// 8f-1: 将独立的 AUTO_INCREMENT 修复 SQL 追加到 sqlS 末尾
		// 这条 SQL 必须在主 ALTER TABLE 之后单独执行，不能被合并
		if result.autoIncrementSQL != "" {
			sqlS = append(sqlS, result.autoIncrementSQL)
			vlog = fmt.Sprintf("(%d) %s Appended independent AUTO_INCREMENT fix SQL for %s.%s", logThreadSeq, event, destSchema, stcls.table)
			global.Wlog.Debug(vlog)
		}

		// 8g: 风险评估与 fix SQL 写入
		eval := stcls.evaluateStructRiskAndWriteFixSQL(sms, result, sourceSchema, sourceTableName, destSchema, len(myRowIDRepositionSQLs), logThreadSeq, event)
		if eval.abnormalKey != "" {
			abnormalTableList = append(abnormalTableList, eval.abnormalKey)
		}
		if eval.newKey != "" {
			newCheckTableList = append(newCheckTableList, eval.newKey)
		}

		// 8h: Pod 记录登记与收尾
		if err = stcls.finalizeStructPod(sqlS, result.constraintAdvisorySQLs, sourceSchema, sourceTableName, destSchema, logThreadSeq, event); err != nil {
			return nil, nil, err
		}
	}
	vlog = fmt.Sprintf("(%d) %s Table structure validation completed", logThreadSeq, event)
	global.Wlog.Info(vlog)

	return newCheckTableList, abnormalTableList, nil
}
