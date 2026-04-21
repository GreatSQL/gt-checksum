package actions

import (
	"database/sql"
	"fmt"
	"strings"

	mysql "gt-checksum/MySQL"
	"gt-checksum/dbExec"
	"gt-checksum/global"
)

// buildTriggerCharsetSetStatements 生成 trigger fix SQL 需要的 charset session 变量 SET 语句
func buildTriggerCharsetSetStatements(result triggerCreateResult, isMariaDBToMySQL bool) []string {
	return buildRoutineCharsetSetStatements(result.CharacterSetClient, result.CollationConnection, result.DatabaseCollation, isMariaDBToMySQL)
}

// Trigger metadata compare currently relies on INFORMATION_SCHEMA fields that
// are stable for MySQL-family sources and MySQL targets in the first-stage
// support matrix. When version info is unavailable, fall back to the driver
// pair so the existing MySQL -> MySQL behavior does not regress.
func (stcls *schemaTable) shouldCompareTriggerMetadata() bool {
	src := stcls.sourceVersionInfo()
	dst := stcls.destVersionInfo()

	if strings.TrimSpace(src.Raw) == "" || strings.TrimSpace(dst.Raw) == "" {
		return stcls.isMySQLToMySQL()
	}

	if dst.Flavor == global.DatabaseFlavorMariaDB {
		return src.Flavor == global.DatabaseFlavorMariaDB
	}

	if dst.Flavor != global.DatabaseFlavorMySQL {
		return false
	}

	switch src.Flavor {
	case global.DatabaseFlavorMySQL:
		return dst.Flavor == global.DatabaseFlavorMySQL
	case global.DatabaseFlavorMariaDB:
		return dst.Series == "8.0" || dst.Series == "8.4"
	default:
		return false
	}
}

func showCreateTriggerSQL(db *sql.DB, schema, triggerName string) (string, error) {
	result, err := showCreateTriggerSQLWithCharset(db, schema, triggerName)
	if err != nil {
		return "", err
	}
	return result.CreateSQL, nil
}

func showCreateTriggerSQLWithCharset(db *sql.DB, schema, triggerName string) (triggerCreateResult, error) {
	row := db.QueryRow(
		`SELECT DEFINER, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT,
		        CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION
		   FROM INFORMATION_SCHEMA.TRIGGERS
		  WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = ?`,
		schema,
		triggerName,
	)

	var definer, actionTiming, eventManipulation, eventObjectTable, actionStatement string
	var csClient, colConnection, dbCollation sql.NullString
	if err := row.Scan(&definer, &actionTiming, &eventManipulation, &eventObjectTable, &actionStatement,
		&csClient, &colConnection, &dbCollation); err != nil {
		if err == sql.ErrNoRows {
			return triggerCreateResult{}, fmt.Errorf("no trigger metadata found for %s.%s", schema, triggerName)
		}
		return triggerCreateResult{}, err
	}

	createSQL := mysql.BuildTriggerCreateSQL(schema, triggerName, definer, actionTiming, eventManipulation, eventObjectTable, actionStatement)
	return triggerCreateResult{
		CreateSQL:           createSQL,
		CharacterSetClient:  strings.TrimSpace(csClient.String),
		CollationConnection: strings.TrimSpace(colConnection.String),
		DatabaseCollation:   strings.TrimSpace(dbCollation.String),
	}, nil
}

func loadMySQLTriggerMetadata(db *sql.DB, schema string, logThreadSeq int64) (map[string]string, map[string]string) {
	comments := make(map[string]string)
	definers := make(map[string]string)

	rows, err := db.Query(`
SELECT TRIGGER_NAME, DEFINER, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT
FROM INFORMATION_SCHEMA.TRIGGERS
WHERE TRIGGER_SCHEMA = ?
`, schema)
	if err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLTriggerMetadata] failed to query trigger metadata for %s: %v", logThreadSeq, schema, err))
		return comments, definers
	}
	defer rows.Close()

	for rows.Next() {
		var triggerName string
		var definer sql.NullString
		var actionTiming string
		var eventManipulation string
		var eventObjectTable string
		var actionStatement string

		if err := rows.Scan(&triggerName, &definer, &actionTiming, &eventManipulation, &eventObjectTable, &actionStatement); err != nil {
			global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLTriggerMetadata] failed to scan trigger metadata for %s: %v", logThreadSeq, schema, err))
			continue
		}

		key := strings.ToUpper(fmt.Sprintf("\"%s\".\"%s\"", schema, triggerName))
		definers[key] = strings.TrimSpace(definer.String)

		// Build the trigger definition from INFORMATION_SCHEMA once so comment
		// extraction does not trigger an extra SHOW CREATE round-trip per row.
		createSQL := mysql.BuildTriggerCreateSQL(
			schema,
			triggerName,
			definer.String,
			actionTiming,
			eventManipulation,
			eventObjectTable,
			actionStatement,
		)
		comments[key] = extractMySQLObjectCommentFromCreate(createSQL)
	}

	if err := rows.Err(); err != nil {
		global.Wlog.Warn(fmt.Sprintf("(%d) [loadMySQLTriggerMetadata] row iteration failed for %s: %v", logThreadSeq, schema, err))
	}

	return comments, definers
}

/*
校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog       string
		tmpM       = make(map[string]int)
		schemaMap  = make(map[string]int)
		triggerMap = make(map[string]string) // 存储具体的触发器名称
		c, d       []string
		pods       = Pod{
			Datafix:     stcls.datafix,
			CheckObject: "trigger",
		}
		sourceTrigger, destTrigger map[string]string
		err                        error
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)

	// 从dtabS中提取schema信息和触发器名称
	for _, i := range dtabS {
		// 处理映射格式 schema.trigger:schema.trigger
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) >= 1 {
					schema := sourceParts[0]

					// schema的名字要区分大小写
					if stcls.caseSensitiveObjectName == "yes" {
						// 当区分大小写时，保持原始大小写
					} else {
						// 当不区分大小写时，也保持原始大小写
					}
					schemaMap[schema] = 1

					// 如果指定了具体的触发器名称
					if len(sourceParts) >= 2 && sourceParts[1] != "*" {
						// 保持trigger名称的原始大小写
						triggerName := sourceParts[1]
						triggerMap[schema+"."+triggerName] = triggerName
					}
				}
			}
		} else {
			// 处理普通格式 schema.trigger 或 schema.*
			parts := strings.Split(i, ".")
			if len(parts) >= 1 {
				schema := parts[0]

				if stcls.caseSensitiveObjectName == "yes" {
					// 当区分大小写时，保持原始大小写
				} else {
					// 当不区分大小写时，也保持原始大小写
				}
				schemaMap[schema] = 1

				// 如果指定了具体的触发器名称
				if len(parts) >= 2 && parts[1] != "*" {
					triggerName := parts[1]
					triggerMap[schema+"."+triggerName] = triggerName
				}
			}
		}
	}

	// 添加调试日志，显示提取的schema和触发器信息
	vlog = fmt.Sprintf("(%d) Extracted schema map: %v, trigger map: %v", logThreadSeq, schemaMap, triggerMap)
	global.Wlog.Debug(vlog)

	// 如果schemaMap为空，但stcls.schema不为空，则使用stcls.schema
	if len(schemaMap) == 0 && stcls.schema != "" {
		schema := stcls.schema
		if stcls.caseSensitiveObjectName == "yes" {
			// 当区分大小写时，保持原始大小写
		} else {
			// 当不区分大小写时，也保持原始大小写
		}
		schemaMap[schema] = 1
		vlog = fmt.Sprintf("(%d) No schema found in dtabS, using default schema: %s", logThreadSeq, schema)
		global.Wlog.Debug(vlog)
	}
	//校验触发器
	for schema, _ := range schemaMap {
		pods.Schema = schema
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{
			Schema:                  schema,
			Drive:                   stcls.sourceDrive,
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}

		// 获取源数据库的触发器
		if sourceTrigger, err = tc.Query().Trigger(stcls.sourceDB, logThreadSeq2); err != nil {
			vlog = fmt.Sprintf("(%d) Error querying source triggers: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		// 如果有指定具体的触发器，则过滤结果
		if len(triggerMap) > 0 {
			filteredSourceTrigger := make(map[string]string)
			for k, v := range sourceTrigger {
				// 提取触发器名称时需要更加小心
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					// 移除可能存在的引号
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					// 如果没有点号，使用整个键
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName

				// 添加调试日志
				vlog = fmt.Sprintf("(%d) Checking trigger: %s, key: %s", logThreadSeq, k, triggerKey)
				global.Wlog.Debug(vlog)

				// 检查是否在过滤映射中
				if _, exists := triggerMap[triggerKey]; exists {
					filteredSourceTrigger[k] = v
					vlog = fmt.Sprintf("(%d) Keeping trigger: %s", logThreadSeq, k)
					global.Wlog.Debug(vlog)
				}
			}
			sourceTrigger = filteredSourceTrigger
		} else {
			// 如果triggerMap为空（表示使用通配符），则不进行过滤，保留所有触发器
			vlog = fmt.Sprintf("(%d) No specific triggers specified, keeping all %d source triggers", logThreadSeq, len(sourceTrigger))
			global.Wlog.Debug(vlog)

			// 当使用通配符时，将所有触发器名称添加到triggerMap中，以便后续比较
			for k, _ := range sourceTrigger {
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName
				triggerMap[triggerKey] = triggerName
				vlog = fmt.Sprintf("(%d) Added trigger to map: %s", logThreadSeq, triggerKey)
				global.Wlog.Debug(vlog)
			}
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, schema, sourceTrigger)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, schema)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive

		// 获取目标数据库的触发器
		if destTrigger, err = tc.Query().Trigger(stcls.destDB, logThreadSeq2); err != nil {
			vlog = fmt.Sprintf("(%d) Error querying destination triggers: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}

		// 如果有指定具体的触发器，则过滤结果
		if len(triggerMap) > 0 {
			filteredDestTrigger := make(map[string]string)
			for k, v := range destTrigger {
				// 提取触发器名称时需要更加小心
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					// 移除可能存在的引号
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					// 如果没有点号，使用整个键
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName

				// 添加调试日志
				vlog = fmt.Sprintf("(%d) Checking dest trigger: %s, key: %s", logThreadSeq, k, triggerKey)
				global.Wlog.Debug(vlog)

				// 检查是否在过滤映射中
				if _, exists := triggerMap[triggerKey]; exists {
					filteredDestTrigger[k] = v
					vlog = fmt.Sprintf("(%d) Keeping dest trigger: %s", logThreadSeq, k)
					global.Wlog.Debug(vlog)
				}
			}
			destTrigger = filteredDestTrigger
		} else {
			// 如果triggerMap为空（表示使用通配符），则不进行过滤，保留所有触发器
			vlog = fmt.Sprintf("(%d) No specific triggers specified, keeping all %d destination triggers", logThreadSeq, len(destTrigger))
			global.Wlog.Debug(vlog)

			// 当使用通配符时，将所有目标端触发器名称也添加到triggerMap中
			for k, _ := range destTrigger {
				parts := strings.Split(k, ".")
				var triggerName string
				if len(parts) > 1 {
					triggerName = strings.ReplaceAll(parts[1], "\"", "")
				} else {
					triggerName = strings.ReplaceAll(k, "\"", "")
				}

				// 保持trigger名称的原始大小写，不做转换

				triggerKey := schema + "." + triggerName
				triggerMap[triggerKey] = triggerName
				vlog = fmt.Sprintf("(%d) Added dest trigger to map: %s", logThreadSeq, triggerKey)
				global.Wlog.Debug(vlog)
			}
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.destDrive, schema, destTrigger)
		global.Wlog.Debug(vlog)

		sourceTriggerComments := make(map[string]string)
		destTriggerComments := make(map[string]string)
		sourceTriggerDefiners := make(map[string]string)
		destTriggerDefiners := make(map[string]string)
		if stcls.shouldCompareTriggerMetadata() {
			sourceTriggerComments, sourceTriggerDefiners = loadMySQLTriggerMetadata(stcls.sourceDB, schema, logThreadSeq)
			destTriggerComments, destTriggerDefiners = loadMySQLTriggerMetadata(stcls.destDB, schema, logThreadSeq)
		}

		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, schema)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceTrigger {
			tmpM[k]++
		}
		for k, _ := range destTrigger {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Trigger is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			pods.TriggerName = strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
			definitionDiff := sourceTrigger[k] != destTrigger[k]
			collationMappedOnly := false
			if definitionDiff && stcls.isMariaDBToMySQL() {
				mappedSource := mapMariaDBCollationInRoutineSQL(sourceTrigger[k])
				if mappedSource == destTrigger[k] {
					definitionDiff = false
					collationMappedOnly = true
					global.Wlog.Debug(fmt.Sprintf("(%d) Trigger %s definition matches after MariaDB collation mapping", logThreadSeq, k))
				}
			}
			commentDiff := false
			definerDiff := false
			if stcls.shouldCompareTriggerMetadata() {
				sourceComment := normalizeMetadataComment(sourceTriggerComments[k])
				destComment := normalizeMetadataComment(destTriggerComments[k])
				if sourceComment != destComment {
					commentDiff = true
					vlog = fmt.Sprintf("(%d) Trigger comment mismatch %s: source=%q, dest=%q", logThreadSeq, k, sourceComment, destComment)
					global.Wlog.Warn(vlog)
				}

				sourceDefiner := strings.TrimSpace(sourceTriggerDefiners[k])
				destDefiner := strings.TrimSpace(destTriggerDefiners[k])
				if sourceDefiner != destDefiner {
					definerDiff = true
					vlog = fmt.Sprintf("(%d) Trigger definer mismatch %s: source=%q, dest=%q", logThreadSeq, k, sourceDefiner, destDefiner)
					global.Wlog.Warn(vlog)
				}
			}

			// MariaDB→MySQL：当 body 和其他属性均一致时，检查 charset 会话元数据的 collation 差异
			metadataCollationDiff := false
			if !definitionDiff && !commentDiff && !definerDiff && !collationMappedOnly && stcls.isMariaDBToMySQL() {
				trName := strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
				srcResult, srcErr := showCreateTriggerSQLWithCharset(stcls.sourceDB, schema, trName)
				dstResult, dstErr := showCreateTriggerSQLWithCharset(stcls.destDB, schema, trName)
				if srcErr == nil && dstErr == nil {
					if isCharsetMetadataCollationMapped(srcResult.CharacterSetClient, srcResult.CollationConnection, srcResult.DatabaseCollation,
						dstResult.CharacterSetClient, dstResult.CollationConnection, dstResult.DatabaseCollation) {
						// uca1400→0900 映射（仅 MariaDB 11.5+ 触发）
						collationMappedOnly = true
						global.Wlog.Debug(fmt.Sprintf("(%d) Trigger %s charset metadata collation-mapped: uca1400→0900 drift (src=%s/%s dst=%s/%s)", logThreadSeq, k, srcResult.CollationConnection, srcResult.DatabaseCollation, dstResult.CollationConnection, dstResult.DatabaseCollation))
					} else if hasCharsetMetadataCollationDiff(srcResult.CharacterSetClient, srcResult.CollationConnection, srcResult.DatabaseCollation,
						dstResult.CharacterSetClient, dstResult.CollationConnection, dstResult.DatabaseCollation) {
						// 非可映射的 collation 差异（如 general_ci ↔ 0900_ai_ci），需生成 fix SQL
						metadataCollationDiff = true
						global.Wlog.Warn(fmt.Sprintf("(%d) Trigger %s charset metadata collation mismatch requiring fix SQL (src=%s/%s dst=%s/%s)", logThreadSeq, k, srcResult.CollationConnection, srcResult.DatabaseCollation, dstResult.CollationConnection, dstResult.DatabaseCollation))
					}
				}
			}

			if definitionDiff || commentDiff || definerDiff || metadataCollationDiff {
				pods.DIFFS = "yes"
				d = append(d, k)

				// Rebuild full trigger DDL from INFORMATION_SCHEMA instead of relying
				// on the body-only statement column returned by SHOW CREATE TRIGGER.
				trName := strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
				trResult, showCreateErr := showCreateTriggerSQLWithCharset(stcls.sourceDB, schema, trName)
				trSourceDef := trResult.CreateSQL
				if showCreateErr != nil {
					global.Wlog.Warn(fmt.Sprintf("(%d) Failed to rebuild source trigger DDL for %s.%s: %v", logThreadSeq, schema, trName, showCreateErr))
					trSourceDef = sourceTrigger[k]
				}
				// 确定目标schema
				destSchema := schema
				if mappedSchema, exists := stcls.tableMappings[schema]; exists {
					destSchema = mappedSchema
				}
				// MariaDB→MySQL：映射源端定义中的 MariaDB 特有 collation
				if stcls.isMariaDBToMySQL() {
					trSourceDef = mapMariaDBCollationInRoutineSQL(trSourceDef)
				}
				tsqls := mysql.GenerateTriggerFixSQL(schema, destSchema, trName, trSourceDef)
				// 在 DROP/CREATE 语句前插入 charset session 变量设置
				if showCreateErr == nil && trResult.CharacterSetClient != "" {
					charsetSetStmts := buildTriggerCharsetSetStatements(trResult, stcls.isMariaDBToMySQL())
					if len(charsetSetStmts) > 0 {
						enriched := make([]string, 0, len(charsetSetStmts)+len(tsqls))
						enriched = append(enriched, charsetSetStmts...)
						enriched = append(enriched, tsqls...)
						tsqls = enriched
					}
				}
				// 每个 trigger 写入独立文件（trigger.schema.triggername.sql）
				out := make([]string, 0, len(tsqls)+2)
				out = append(out, "DELIMITER $$")
				for _, stmt := range tsqls {
					out = append(out, stmt+"\n$$")
				}
				out = append(out, "DELIMITER ;")
				origSchema, origTable, origObjType := stcls.schema, stcls.table, stcls.fixFileObjectType
				stcls.schema = schema
				stcls.table = trName
				stcls.fixFileObjectType = "trigger"
				if werr := stcls.writeFixSql(out, logThreadSeq); werr != nil {
					global.Wlog.Error(fmt.Sprintf("(%d) failed to write trigger fix SQL for %s.%s: %v", logThreadSeq, schema, trName, werr))
				}
				stcls.schema, stcls.table, stcls.fixFileObjectType = origSchema, origTable, origObjType
			} else if collationMappedOnly {
				pods.DIFFS = global.SkipDiffsCollationMapped
				c = append(c, k)
				global.Wlog.Debug(fmt.Sprintf("(%d) Trigger %s collation-mapped: only uca1400→0900 collation difference, no fix SQL generated", logThreadSeq, k))
			} else {
				pods.DIFFS = "no"
				c = append(c, k)
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed", logThreadSeq, schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}
