package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"strings"
)

func queryOraclePrimaryKeyColumns(db *sql.DB, schema, table string) ([]string, error) {
	// Use ALL_* views (not DBA_*) so that non-DBA application accounts with only
	// SELECT privileges on the target schema can still retrieve PK columns. Using
	// DBA_* requires SELECT_CATALOG_ROLE which production accounts rarely have,
	// and failing back to a PK-less CREATE TABLE breaks sql_require_primary_key=ON.
	query := `SELECT cc.column_name
FROM all_constraints c
JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
WHERE c.constraint_type = 'P' AND c.owner = :1 AND c.table_name = :2
ORDER BY cc.position`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if scanErr := rows.Scan(&col); scanErr != nil {
			return nil, scanErr
		}
		cols = append(cols, col)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func (stcls *schemaTable) resolveTableMapping(item string, logThreadSeq int64, event string) (sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey string, ok bool) {
	sourceTable := item
	destTable := item
	if strings.Contains(item, ":") {
		parts := strings.Split(item, ":")
		sourceTable = parts[0]
		destTable = parts[1]
	}

	sourceParts := strings.Split(sourceTable, ".")
	if len(sourceParts) < 2 {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, sourceTable))
		return "", "", "", "", "", false
	}
	sourceSchema = sourceParts[0]
	sourceTableName = sourceParts[1]

	destParts := strings.Split(destTable, ".")
	if len(destParts) < 2 {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Invalid table format: %s, expected schema.table", logThreadSeq, event, destTable))
		return "", "", "", "", "", false
	}
	destSchema = destParts[0]
	destTableName = destParts[1]

	stcls.schema = sourceSchema
	stcls.table = sourceTableName
	stcls.destTable = destTableName

	if sourceTable == destTable && sourceSchema == destSchema {
		if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
			destSchema = mappedSchema
		}
	}

	global.Wlog.Debug(fmt.Sprintf("Table mapping options - source: %s, target: %s, mappings: %v", sourceSchema, destSchema, stcls.tableMappings))
	mappedTableKey = fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)
	if sourceSchema != destSchema || sourceTableName != destTableName {
		mappedTableKey = fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTableName, destSchema, destTableName)
	}

	global.Wlog.Debug(fmt.Sprintf("(%d %s Validating table structure %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table))
	return sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, true
}

func (stcls *schemaTable) checkTableExistence(
	sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event string,
	logThreadSeq int64,
) (sourceTableExists, destTableExists, skip bool, err error) {
	sourceTableExists, err = stcls.tableExistsByDrive(stcls.sourceDB, stcls.sourceDrive, sourceSchema, sourceTableName, "table")
	if err != nil {
		vlog := fmt.Sprintf("(%d) %s Error checking source table existence %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, err)
		global.Wlog.Error(vlog)
		return false, false, false, err
	}
	destTableExists, err = stcls.tableExistsByDrive(stcls.destDB, stcls.destDrive, destSchema, destTableName, "table")
	if err != nil {
		vlog := fmt.Sprintf("(%d) %s Error checking target table existence %s.%s: %v", logThreadSeq, event, destSchema, destTableName, err)
		global.Wlog.Error(vlog)
		return false, false, false, err
	}

	if sourceTableExists && destTableExists {
		return true, true, false, nil
	}

	oracleToMySQLDataMode := stcls.sourceDrive == "godror" && stcls.destDrive == "mysql" && stcls.checkRules.CheckObject != "struct"

	if oracleToMySQLDataMode {
		diffReason := "table missing on one side"
		if !sourceTableExists && !destTableExists {
			diffReason = "table missing on both source and target"
		} else if !sourceTableExists {
			diffReason = "table missing on source"
		} else if !destTableExists {
			diffReason = "table missing on target"
		}
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: "data",
			DIFFS:       "DDL-yes",
			Datafix:     stcls.datafix,
			Rows:        diffReason,
		})
		global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", diffReason, global.SkipDiffsDDLYes)
		vlog := fmt.Sprintf("(%d) %s Skip data check for %s.%s due to DDL mismatch: %s", logThreadSeq, event, sourceSchema, sourceTableName, diffReason)
		global.Wlog.Warn(vlog)
		_ = mappedTableKey
		return sourceTableExists, destTableExists, true, nil
	}

	if !sourceTableExists && !destTableExists {
		vlog := fmt.Sprintf("(%d) %s Source/target table both missing: %s.%s -> %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName)
		global.Wlog.Warn(vlog)
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: stcls.checkRules.CheckObject,
			DIFFS:       "yes",
			Datafix:     stcls.datafix,
		})
		return false, false, true, nil
	}

	if !sourceTableExists {
		vlog := fmt.Sprintf("(%d) %s Source table %s.%s does not exist", logThreadSeq, event, sourceSchema, sourceTableName)
		global.Wlog.Warn(vlog)
		global.AddSkippedTableWithDiffs(sourceSchema, sourceTableName, "data", "table does not exist", global.SkipDiffsDDLYes)
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: stcls.checkRules.CheckObject,
			DIFFS:       "yes",
			Datafix:     stcls.datafix,
		})
		// 将表添加到skipIndexCheckTables，跳过后续的索引检查
		tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
		stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
		return false, true, true, nil
	}

	// sourceTableExists && !destTableExists — 由 P3 分支（handleTargetMissingTable）处理。
	return sourceTableExists, destTableExists, false, nil
}

func (stcls *schemaTable) applyDestFixSQL(destSchema, destTableName string, sqls []string, logThreadSeq int64) error {
	originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
	stcls.schema = destSchema
	stcls.table = destTableName
	stcls.destTable = destTableName
	defer func() {
		stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
	}()
	return stcls.writeFixSql(sqls, logThreadSeq)
}

func (stcls *schemaTable) applyDestAdvisorySQL(destSchema, destTableName string, lines []string, logThreadSeq int64) error {
	originalSchema, originalTable, originalDestTable := stcls.schema, stcls.table, stcls.destTable
	stcls.schema = destSchema
	stcls.table = destTableName
	stcls.destTable = destTableName
	defer func() {
		stcls.schema, stcls.table, stcls.destTable = originalSchema, originalTable, originalDestTable
	}()
	return stcls.writeAdvisoryFixSql(lines, logThreadSeq)
}

func (stcls *schemaTable) handleTargetMissingTable(
	sourceSchema, sourceTableName, destSchema, destTableName, mappedTableKey, event string,
	logThreadSeq int64,
) (abnormalKey string, err error) {
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Processing table creation with mapping - source: %s.%s -> dest: %s.%s", logThreadSeq, event, sourceSchema, sourceTableName, destSchema, destTableName))

	// Oracle→MySQL: generate CREATE TABLE from Oracle metadata
	if stcls.isOracleToMySQL() {
		tc := dbExec.TableColumnNameStruct{Drive: stcls.sourceDrive, Schema: sourceSchema, Table: sourceTableName}
		oracleColumns, oraErr := tc.Query().TableColumnName(stcls.sourceDB, logThreadSeq)
		if oraErr != nil {
			global.Wlog.Error(fmt.Sprintf("(%d) %s Error querying Oracle columns for CREATE TABLE %s.%s: %v", logThreadSeq, event, sourceSchema, sourceTableName, oraErr))
			return "", oraErr
		}
		// Query Oracle primary key columns so the generated CREATE TABLE
		// includes a PRIMARY KEY clause; this is required when the target
		// MySQL has sql_require_primary_key=ON (default on MySQL 8.0+).
		oracleIndexData := make(map[string][]string)
		if pkCols, pkErr := queryOraclePrimaryKeyColumns(stcls.sourceDB, sourceSchema, sourceTableName); pkErr != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) %s Warning: failed to query Oracle primary key for %s.%s: %v (proceeding without PK)", logThreadSeq, event, sourceSchema, sourceTableName, pkErr))
		} else if len(pkCols) > 0 {
			oracleIndexData["PRIMARY"] = pkCols
		}
		createTableSql := schemacompat.GenerateOracleToMySQLCreateTableSQL(destSchema, destTableName, oracleColumns, oracleIndexData, stcls.destVersionInfo())
		if createTableSql == "" {
			return "", nil
		}
		global.Wlog.Info(fmt.Sprintf("(%d) %s Generated Oracle→MySQL CREATE TABLE for %s.%s", logThreadSeq, event, destSchema, destTableName))
		if err = stcls.applyDestFixSQL(destSchema, destTableName, []string{createTableSql}, logThreadSeq); err != nil {
			return "", err
		}
		stcls.appendPod(Pod{
			Schema:      sourceSchema,
			Table:       sourceTableName,
			CheckObject: stcls.checkRules.CheckObject,
			DIFFS:       "yes",
			Datafix:     stcls.datafix,
		})
		return mappedTableKey, nil
	}

	sourceMeta, sourceMetaErr := queryMySQLTableLevelMetadata(stcls.sourceDB, sourceSchema, sourceTableName)
	if sourceMetaErr != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) %s Failed to query source table metadata for %s.%s before CREATE TABLE generation: %v", logThreadSeq, event, sourceSchema, sourceTableName, sourceMetaErr))
	} else {
		jsonDowngradeColumns := schemacompat.DetectMariaDBJSONDowngradeColumns(
			sourceMeta.CreateTableSQL,
			stcls.sourceVersionInfo(),
			stcls.destVersionInfo(),
			stcls.checkRules.MariaDBJSONTargetType,
		)
		if len(jsonDowngradeColumns) > 0 {
			advisoryLines := buildConstraintAdvisoryLines(
				fmt.Sprintf("%s.%s MariaDB JSON downgrade", destSchema, destTableName),
				schemacompat.BuildMariaDBJSONDowngradeSuggestions(destSchema, destTableName, jsonDowngradeColumns, stcls.checkRules.MariaDBJSONTargetType),
			)
			if err = stcls.applyDestAdvisorySQL(destSchema, destTableName, advisoryLines, logThreadSeq); err != nil {
				return "", err
			}
		}

		// MariaDB-only temporal and sequence constructs must stay on the
		// advisory path because there is no safe automatic MySQL rewrite.
		unsupportedFeatures := schemacompat.DetectMariaDBUnsupportedTableFeatures(sourceMeta.CreateTableSQL, stcls.sourceVersionInfo(), stcls.destVersionInfo())
		if len(unsupportedFeatures) > 0 {
			global.Wlog.Warn(fmt.Sprintf("(%d) %s Skip automatic CREATE TABLE for %s.%s because unsupported MariaDB features were detected: %+v", logThreadSeq, event, sourceSchema, sourceTableName, unsupportedFeatures))

			advisoryLines := buildConstraintAdvisoryLines(
				fmt.Sprintf("%s.%s MariaDB unsupported features", destSchema, destTableName),
				schemacompat.BuildMariaDBUnsupportedFeatureSuggestions(destSchema, destTableName, unsupportedFeatures),
			)
			if err = stcls.applyDestAdvisorySQL(destSchema, destTableName, advisoryLines, logThreadSeq); err != nil {
				return "", err
			}

			stcls.appendPod(Pod{
				Schema:      sourceSchema,
				Table:       sourceTableName,
				CheckObject: stcls.checkRules.CheckObject,
				DIFFS:       global.SkipDiffsWarnOnly,
				Datafix:     stcls.datafix,
			})
			tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
			stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
			stcls.structWarnOnlyDiffsMap[fmt.Sprintf("%s.%s", sourceSchema, sourceTableName)] = true
			// 该分支原先 append "sourceSchema.sourceTableName" 风格键；保留原 mappedTableKey 结构不必追加
			// 因为原实现此处也不 append abnormalTableList（只 continue）。
			return "", nil
		}
	}

	createTableSql, err := generateCreateTableSql(stcls.sourceDB, sourceSchema, destSchema, sourceTableName, destTableName, stcls.sourceVersionInfo(), stcls.destVersionInfo(), stcls.checkRules.MariaDBJSONTargetType, logThreadSeq)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Error generating CREATE TABLE statement for %s.%s: %v", logThreadSeq, event, destSchema, stcls.table, err))
		return "", err
	}

	// 如果需要，注入 my_row_id 列定义
	createTableSql, err = injectMyRowIDIntoCreateTable(createTableSql, stcls.destDB, destSchema, destTableName, stcls.checkRules.RequirePK, logThreadSeq)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Error injecting my_row_id into CREATE TABLE for %s.%s: %v", logThreadSeq, event, destSchema, destTableName, err))
		return "", err
	}

	if !strings.Contains(createTableSql, fmt.Sprintf("`%s`", destSchema)) {
		global.Wlog.Warn(fmt.Sprintf("(%d) %s Warning: Generated CREATE TABLE statement may be missing target schema '%s': %s", logThreadSeq, event, destSchema, createTableSql))
	}

	global.Wlog.Debug(fmt.Sprintf("(%d) %s Generated CREATE TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, createTableSql))
	global.Wlog.Debug(fmt.Sprintf("(%d) %s Applying CREATE TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName))
	if err = stcls.applyDestFixSQL(destSchema, destTableName, []string{createTableSql}, logThreadSeq); err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) %s Error applying CREATE TABLE statement: %v", logThreadSeq, event, err))
		return "", err
	}

	stcls.appendPod(Pod{
		Schema:      destSchema,
		Table:       destTableName,
		CheckObject: stcls.checkRules.CheckObject,
		DIFFS:       "yes",
		Datafix:     stcls.datafix,
	})

	tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
	stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
	return tableKey, nil
}

func (stcls *schemaTable) handleSourceMissingTable(destSchema, destTableName string, logThreadSeq int64, event string) (string, error) {
	dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", destSchema, destTableName)

	vlog := fmt.Sprintf("(%d) %s Generated DROP TABLE statement for %s.%s: %s", logThreadSeq, event, destSchema, destTableName, dropTableSql)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) %s Applying DROP TABLE statement to %s.%s", logThreadSeq, event, destSchema, destTableName)
	global.Wlog.Debug(vlog)
	if err := stcls.writeFixSql([]string{dropTableSql}, logThreadSeq); err != nil {
		return "", err
	}

	tableKey := fmt.Sprintf("%s.%s", destSchema, destTableName)
	stcls.skipIndexCheckTables = append(stcls.skipIndexCheckTables, tableKey)
	return tableKey, nil
}
