package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strings"
)

/*
MySQL 获取对应的库表信息，排除'information_Schema','performance_Schema','sys','mysql'
*/
func (my *QueryTable) DatabaseNameList(db *sql.DB, logThreadSeq int64) (map[string]int, error) {
	var (
		A      = make(map[string]int)
		Event  = "Q_Schema_Table_List"
		query  string
		logMsg string
		err    error
	)
	excludeSchema := fmt.Sprintf("'information_Schema','performance_Schema','sys','mysql'")
	query = fmt.Sprintf("SELECT TABLE_SCHEMA AS databaseName, TABLE_NAME AS tableName FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN (%s);", excludeSchema)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the metadata of the %s database and obtain library and table information. SQL: {%s}", logThreadSeq, Event, DBType, query)
	global.Wlog.Debug(logMsg)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for i := range tableData {
		var ga string
		gd, gt := fmt.Sprintf("%v", tableData[i]["databaseName"]), fmt.Sprintf("%v", tableData[i]["tableName"])
		if strings.ToLower(my.CaseSensitiveObjectName) == "no" {
			gd = strings.ToLower(gd)
			gt = strings.ToLower(gt)
		}
		ga = fmt.Sprintf("%v/*schema&table*/%v", gd, gt)
		A[ga]++
	}
	logMsg = fmt.Sprintf("(%d) [%s] Complete the library and table information query of the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	defer dispos.SqlRows.Close()
	return A, nil
}

// ObjectTypeMap returns a map from the canonical "schema/*schema&table*/table" key
// (same format as DatabaseNameList) to the object's TABLE_TYPE value ("BASE TABLE"
// or "VIEW"). This lets callers distinguish ordinary tables from views without an
// additional per-object round-trip query.
func (my *QueryTable) ObjectTypeMap(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		A      = make(map[string]string)
		Event  = "Q_Object_Type_Map"
		logMsg string
		err    error
	)
	var query string
	if len(my.CandidateSchemas) > 0 {
		// Restrict to the candidate schemas supplied by the caller so that large
		// instances do not pay the cost of a full INFORMATION_SCHEMA scan.
		quoted := make([]string, len(my.CandidateSchemas))
		for i, s := range my.CandidateSchemas {
			quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
		}
		query = fmt.Sprintf(
			"SELECT TABLE_SCHEMA AS databaseName, TABLE_NAME AS tableName, TABLE_TYPE AS tableType FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN (%s);",
			strings.Join(quoted, ","),
		)
	} else {
		excludeSchema := "'information_Schema','performance_Schema','sys','mysql'"
		query = fmt.Sprintf(
			"SELECT TABLE_SCHEMA AS databaseName, TABLE_NAME AS tableName, TABLE_TYPE AS tableType FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN (%s);",
			excludeSchema,
		)
	}
	logMsg = fmt.Sprintf("(%d) [%s] Querying TABLE_TYPE metadata. SQL: {%s}", logThreadSeq, Event, query)
	global.Wlog.Debug(logMsg)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	defer dispos.SqlRows.Close()
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for i := range tableData {
		gd := fmt.Sprintf("%v", tableData[i]["databaseName"])
		gt := fmt.Sprintf("%v", tableData[i]["tableName"])
		tp := fmt.Sprintf("%v", tableData[i]["tableType"])
		if strings.ToLower(my.CaseSensitiveObjectName) == "no" {
			gd = strings.ToLower(gd)
			gt = strings.ToLower(gt)
		}
		A[fmt.Sprintf("%v/*schema&table*/%v", gd, gt)] = tp
	}
	logMsg = fmt.Sprintf("(%d) [%s] TABLE_TYPE metadata query complete (entries=%d).", logThreadSeq, Event, len(A))
	global.Wlog.Debug(logMsg)
	return A, nil
}

/*
MySQL 通过查询表的元数据信息获取列名
*/

func (my *QueryTable) Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb   = make(map[string]string)
		Event  = "Q_Foreign"
		query  string
		logMsg string
		err    error
	)
	logMsg = fmt.Sprintf("(%d) [%s] Start to query the Foreign information under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)

	// 使用INFORMATION_SCHEMA获取完整的外键约束信息
	// 这个查询会获取外键名称、列名、引用的表和列信息
	query = fmt.Sprintf(`
			SELECT 
				kcu.CONSTRAINT_NAME,
				kcu.COLUMN_NAME,
				kcu.REFERENCED_TABLE_SCHEMA,
				kcu.REFERENCED_TABLE_NAME,
				kcu.REFERENCED_COLUMN_NAME,
				rc.DELETE_RULE,
				rc.UPDATE_RULE
		FROM 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
		JOIN 
			INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
				ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
				AND rc.TABLE_NAME = kcu.TABLE_NAME
				AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		WHERE 
			kcu.TABLE_SCHEMA = '%s' 
			AND kcu.TABLE_NAME = '%s'
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY 
			kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
		`, my.Schema, my.Table)

	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		logMsg = fmt.Sprintf("(%d) [%s] Error executing foreign key query: %v", logThreadSeq, Event, err)
		global.Wlog.Error(logMsg)
		return nil, err
	}

	foreignKeys, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		logMsg = fmt.Sprintf("(%d) [%s] Error processing foreign key results: %v", logThreadSeq, Event, err)
		global.Wlog.Error(logMsg)
		return nil, err
	}
	defer dispos.SqlRows.Close()

	// 按约束名称分组外键信息
	fkMap := make(map[string][]map[string]interface{})
	for _, fk := range foreignKeys {
		constraintName := fmt.Sprintf("%s", fk["CONSTRAINT_NAME"])
		if _, exists := fkMap[constraintName]; !exists {
			fkMap[constraintName] = []map[string]interface{}{}
		}
		fkMap[constraintName] = append(fkMap[constraintName], fk)
	}

	// 构建完整的外键DDL定义
	for constraintName, fkInfos := range fkMap {
		if len(fkInfos) == 0 {
			continue
		}

		// 获取第一个外键信息作为基础
		firstFk := fkInfos[0]
		referencedSchema := fmt.Sprintf("%s", firstFk["REFERENCED_TABLE_SCHEMA"])
		referencedTable := fmt.Sprintf("%s", firstFk["REFERENCED_TABLE_NAME"])
		deleteRule := fmt.Sprintf("%s", firstFk["DELETE_RULE"])
		updateRule := fmt.Sprintf("%s", firstFk["UPDATE_RULE"])

		// 收集列信息
		var sourceColumns []string
		var referencedColumns []string
		for _, fkInfo := range fkInfos {
			sourceColumns = append(sourceColumns, fmt.Sprintf("!%s!", fkInfo["COLUMN_NAME"]))
			referencedColumns = append(referencedColumns, fmt.Sprintf("!%s!", fkInfo["REFERENCED_COLUMN_NAME"]))
		}

		// 构建外键DDL
		sourceColumnsStr := strings.Join(sourceColumns, ", ")
		referencedColumnsStr := strings.Join(referencedColumns, ", ")
		ddl := fmt.Sprintf("CONSTRAINT !%s! FOREIGN KEY (%s) REFERENCES !%s!.!%s! (%s)",
			constraintName, sourceColumnsStr, referencedSchema, referencedTable, referencedColumnsStr)

		// 添加删除和更新规则
		if deleteRule != "NO ACTION" && deleteRule != "RESTRICT" {
			ddl += " ON DELETE " + deleteRule
		}
		if updateRule != "NO ACTION" && updateRule != "RESTRICT" {
			ddl += " ON UPDATE " + updateRule
		}

		// Use an upper-cased key for stable lookups, but keep the original DDL text
		// so downstream metadata probes can still use the real schema/table casing.
		tmpb[strings.ToUpper(constraintName)] = ddl

		logMsg = fmt.Sprintf("(%d) [%s] Found foreign key: %s", logThreadSeq, Event, ddl)
		global.Wlog.Debug(logMsg)
	}

	logMsg = fmt.Sprintf("(%d) [%s] Complete the Foreign information query under the %s database.", logThreadSeq, Event, DBType)
	global.Wlog.Debug(logMsg)
	return tmpb, nil
}

/*
分区表校验
*/
func (my *QueryTable) Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error) {
	var (
		tmpb   = make(map[string]string)
		Event  = "Q_Partitions"
		err    error
		logMsg string
		query  string
	)

	// 正确提取表名，避免表名中包含schema信息
	actualTableName := my.Table
	if strings.Contains(actualTableName, ":") {
		parts := strings.Split(actualTableName, ":")
		if len(parts) > 0 {
			actualTableName = parts[0]
		}
	}

	logMsg = fmt.Sprintf("(%d) [%s] Start to query the Partitions information for table %s.%s under the %s database.", logThreadSeq, Event, my.Schema, actualTableName, DBType)
	global.Wlog.Debug(logMsg)

	// 直接查询表的分区信息，包括分区名称和详细定义
	query = fmt.Sprintf("SELECT PARTITION_NAME, PARTITION_ORDINAL_POSITION, PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_DESCRIPTION, TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND PARTITION_NAME<>'' ORDER BY PARTITION_ORDINAL_POSITION;", my.Schema, actualTableName)
	logMsg = fmt.Sprintf("(%d) [%s] Executing query on INFORMATION_SCHEMA.PARTITIONS: %s", logThreadSeq, Event, query)
	global.Wlog.Debug(logMsg)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
		return nil, err
	}
	partitionsInfo, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	// 如果有分区，获取表的创建语句以提取完整的分区定义
	if len(partitionsInfo) > 0 {
		query = fmt.Sprintf("SHOW CREATE TABLE %s.%s;", my.Schema, actualTableName)
		if dispos.SqlRows, err = dispos.DBSQLforExec(query); err != nil {
			return nil, err
		}
		createTableInfo, err1 := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
		if err1 != nil {
			return nil, err1
		}

		if len(createTableInfo) > 0 {
			createTableSQL := fmt.Sprintf("%s", createTableInfo[0]["Create Table"])
			z := strings.Split(createTableSQL, "\n")

			// 提取分区定义信息 - 改进版本，确保捕获完整的分区定义
			var partitionDefs []string
			inPartitionSection := false

			for _, bi := range z {
				trimmedLine := strings.TrimSpace(bi)
				upperLine := strings.ToUpper(trimmedLine)

				// 检测分区定义开始 - 支持PARTITION BY和SUBPARTITION BY
				if strings.Contains(upperLine, "PARTITION BY") || strings.Contains(upperLine, "SUBPARTITION BY") {
					inPartitionSection = true
					partitionDefs = append(partitionDefs, upperLine)
				} else if inPartitionSection {
					// 收集分区定义部分的所有行，直到遇到结束括号或引擎定义
					if trimmedLine != "" && !strings.HasPrefix(upperLine, "ENGINE=") && !strings.HasPrefix(upperLine, "DEFAULT CHARSET") {
						partitionDefs = append(partitionDefs, upperLine)
					}
					// 分区定义结束 - 确保我们不会提前退出
					if strings.Contains(upperLine, ");") {
						inPartitionSection = false
						break
					}
				}
			}

			// 将所有分区定义合并为一个字符串作为表的分区定义
			// 保留 SHOW CREATE TABLE 的原始标识符引用形式，后续在 compare
			// 层统一做归一化，避免这里引入额外的符号噪音。
			fullPartitionDef := strings.Join(partitionDefs, " ")
			fullPartitionDef = strings.Join(strings.Fields(fullPartitionDef), " ")

			// 增加日志，记录完整的分区定义用于调试
			logMsg = fmt.Sprintf("(%d) [%s] Extracted full partition definition for %s.%s: %s", logThreadSeq, Event, my.Schema, actualTableName, fullPartitionDef)
			global.Wlog.Debug(logMsg)

			// 使用表名作为键，存储完整的分区定义
			tableKey := fmt.Sprintf("%s.%s", my.Schema, my.Table)
			tmpb[tableKey] = fullPartitionDef

			// 同时为每个分区单独创建条目，便于比较
			for _, p := range partitionsInfo {
				partitionName := fmt.Sprintf("%s", p["PARTITION_NAME"])
				partitionKey := fmt.Sprintf("%s.%s.%s", my.Schema, my.Table, partitionName)
				// 存储分区的详细信息，包括所有分区属性
				partitionDetails := fmt.Sprintf("NAME=%s,ORDINAL=%s,METHOD=%s,EXPRESSION=%s,DESCRIPTION=%s,ROWS=%s",
					partitionName,
					p["PARTITION_ORDINAL_POSITION"],
					p["PARTITION_METHOD"],
					p["PARTITION_EXPRESSION"],
					p["PARTITION_DESCRIPTION"],
					p["TABLE_ROWS"])
				tmpb[partitionKey] = partitionDetails
				logMsg = fmt.Sprintf("(%d) [%s] Stored partition %s details: %s", logThreadSeq, Event, partitionKey, partitionDetails)
				global.Wlog.Debug(logMsg)
			}
		}
	}

	defer dispos.SqlRows.Close()
	logMsg = fmt.Sprintf("(%d) [%s] Complete the Partitions information query for table %s.%s under the %s database. Found %d partitions.", logThreadSeq, Event, my.Schema, actualTableName, DBType, len(partitionsInfo))
	global.Wlog.Debug(logMsg)
	return tmpb, nil
}

func (my *QueryTable) Struct(db *sql.DB) (map[string]string, error) {
	return nil, nil
}
