package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
	"strings"
)

func (stcls *schemaTable) Foreign(dtabS []string, logThreadSeq, logThreadSeq2 int64, isCalledFromStruct ...bool) {
	var (
		vlog                       string
		sourceForeign, destForeign map[string]string
		err                        error
		pods                       = Pod{
			Datafix:     "no",
			CheckObject: "foreign",
		}
	)

	// Preload FK metadata in one batch query when the source is Oracle, so the
	// per-table loop below does an in-memory lookup instead of 21× round-trips
	// against all_constraints/all_cons_columns.
	var sourceFKCache map[string]map[string]map[string]string
	if isOracleDrive(stcls.sourceDrive) {
		schemasSet := make(map[string]struct{}, len(dtabS))
		for _, i := range dtabS {
			srcSchema, _, _, _ := parseSourceAndDestTablePair(i, stcls.tableMappings)
			if srcSchema != "" {
				schemasSet[strings.ToUpper(srcSchema)] = struct{}{}
			}
		}
		schemas := make([]string, 0, len(schemasSet))
		for s := range schemasSet {
			schemas = append(schemas, s)
		}
		sourceFKCache = preloadOracleForeignKeys(stcls.sourceDB, schemas, logThreadSeq2)
	}

	// 如果是从 Struct 函数调用的，则将 CheckObject 设置为 "struct"
	if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
		pods.CheckObject = "struct"
	}

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		sourceSchema, sourceTable, destSchema, destTable := parseSourceAndDestTablePair(i, stcls.tableMappings)
		stcls.schema = sourceSchema
		stcls.table = sourceTable
		stcls.destTable = destTable
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable)
		global.Wlog.Debug(vlog)
		pods.Schema = sourceSchema
		pods.Table = sourceTable
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTable, Drive: stcls.sourceDrive}
		if cached, ok := lookupForeignKeyCache(sourceFKCache, sourceSchema, sourceTable); ok {
			sourceForeign = cached
		} else if sourceForeign, err = tc.Query().Foreign(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, sourceSchema, sourceTable, sourceForeign)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, destSchema, destTable)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		tc.Schema = destSchema
		tc.Table = destTable
		if destForeign, err = tc.Query().Foreign(stcls.destDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, destSchema, destTable, destForeign)
		global.Wlog.Debug(vlog)
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			continue
		}

		// Oracle identifiers are always upper-case; MySQL identifier case depends on
		// lower_case_table_names. For Oracle→MySQL comparisons we must normalize
		// case, but for MySQL→MySQL we must preserve it to honor case-sensitive
		// deployments (lower_case_table_names=0 on Linux).
		fkCaseInsensitive := stcls.isOracleToMySQL()
		sourceCanonicalFKs := schemacompat.CanonicalizeForeignKeyDefinitionsWithOptions(sourceForeign, fkCaseInsensitive)
		destCanonicalFKs := schemacompat.CanonicalizeForeignKeyDefinitionsWithOptions(destForeign, fkCaseInsensitive)
		sourceByName := make(map[string]schemacompat.CanonicalConstraint)
		destByName := make(map[string]schemacompat.CanonicalConstraint)
		unionNames := make(map[string]struct{})
		for _, fk := range sourceCanonicalFKs {
			sourceByName[fk.Name] = fk
			unionNames[fk.Name] = struct{}{}
		}
		for _, fk := range destCanonicalFKs {
			destByName[fk.Name] = fk
			unionNames[fk.Name] = struct{}{}
		}

		vlog = fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		// 初始化为"no"，如果发现任何不一致，则设置为"yes"
		pods.DIFFS = "no"
		advisorySuggestions := make([]schemacompat.ConstraintRepairSuggestion, 0)

		for fkName := range unionNames {
			sourceFK, sourceExists := sourceByName[fkName]
			destFK, destExists := destByName[fkName]
			if !sourceExists || !destExists {
				pods.DIFFS = "yes"
				d = append(d, fkName)
				vlog = fmt.Sprintf("(%d) Foreign key %s existence mismatch on table %s.%s", logThreadSeq, fkName, sourceSchema, sourceTable)
				global.Wlog.Warn(vlog)
				continue
			}

			decision := schemacompat.DecideForeignKeyCompatibility(sourceFK, destFK)
			if decision.IsMismatch() {
				pods.DIFFS = "yes"
				d = append(d, fkName)
				vlog = fmt.Sprintf("(%d) Foreign key %s definition mismatch on table %s.%s: %s", logThreadSeq, fkName, sourceSchema, sourceTable, decision.Reason)
				global.Wlog.Warn(vlog)
			} else {
				c = append(c, fkName)
			}
		}
		advisorySuggestions = append(advisorySuggestions, schemacompat.BuildForeignKeyRepairSuggestions(destSchema, destTable, sourceCanonicalFKs, destCanonicalFKs, stcls.tableMappings)...)

		if strings.EqualFold(stcls.destDrive, "mysql") && stcls.destVersionInfo().Series == "8.4" {
			strictIssues, strictErr := detectStrictForeignKeyIssues(stcls.sourceDB, sourceCanonicalFKs)
			if strictErr != nil {
				vlog = fmt.Sprintf("(%d) Failed to validate strict foreign key requirements for table %s.%s: %v", logThreadSeq, sourceSchema, sourceTable, strictErr)
				global.Wlog.Warn(vlog)
			} else if len(strictIssues) > 0 {
				pods.DIFFS = "yes"
				for _, issue := range strictIssues {
					d = append(d, issue.Name)
					vlog = fmt.Sprintf(
						"(%d) MySQL 8.4 strict foreign key precheck warning for table %s.%s: foreign key %s references %s.%s(%s) without an exact UNIQUE/PRIMARY KEY match",
						logThreadSeq,
						sourceSchema,
						sourceTable,
						issue.Name,
						issue.ReferencedSchema,
						issue.ReferencedTable,
						strings.Join(issue.ReferencedColumns, ", "),
					)
					global.Wlog.Warn(vlog)
				}
				advisorySuggestions = append(advisorySuggestions, schemacompat.BuildStrictForeignKeyRepairSuggestions(strictIssues, stcls.tableMappings)...)
			}
		}

		if len(advisorySuggestions) > 0 {
			advisoryLines := buildConstraintAdvisoryLines(fmt.Sprintf("%s.%s FOREIGN KEY constraints", destSchema, destTable), advisorySuggestions)
			if err := stcls.writeAdvisoryFixSql(advisoryLines, logThreadSeq); err != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) Failed to write foreign key advisory SQL for %s.%s: %v", logThreadSeq, destSchema, destTable, err))
				return
			}
		}

		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, sourceSchema, sourceTable, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed", logThreadSeq, sourceSchema, sourceTable)
		global.Wlog.Debug(vlog)
		// 如果是从 Struct 函数调用的，则将结果存储在全局变量中
		if len(isCalledFromStruct) > 0 && isCalledFromStruct[0] {
			// 使用完整的schema.table作为键
			tableKey := fmt.Sprintf("%s.%s", pods.Schema, pods.Table)

			// Keep foreign-key diff state on the schemaTable instance so each run
			// owns its own lifecycle.
			if stcls.foreignKeyDiffsMap == nil {
				stcls.foreignKeyDiffsMap = make(map[string]bool)
			}
			stcls.foreignKeyDiffsMap[tableKey] = pods.DIFFS == "yes"

			vlog = fmt.Sprintf("(%d) Storing foreign key check result for table %s: %v",
				logThreadSeq, tableKey, stcls.foreignKeyDiffsMap[tableKey])
			global.Wlog.Debug(vlog)
		} else {
			// 不是从 Struct 函数调用时，添加到 measuredDataPods
			stcls.appendPod(pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}
