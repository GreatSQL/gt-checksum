package actions

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/schemacompat"
)

var mysqlColumnCharsetOrCollationClausePattern = regexp.MustCompile(`(?i)\bCHARACTER\s+SET\b|\bCOLLATE\b`)
var mysqlCharacterColumnDefinitionPattern = regexp.MustCompile(`(?i)^(?:varchar|char|tinytext|text|mediumtext|longtext|enum|set)\b`)

type mysqlTableLevelMetadata struct {
	TableCollation string
	TableCharset   string
	AutoIncrement  sql.NullInt64
	RowFormat      string
	CreateOptions  string
	TableComment   string
	CreateTableSQL string
}

type columnCollationRepairCandidate struct {
	ColumnName       string
	ColumnSeq        int
	LastColumn       string
	SourceAttrs      []string
	SourceDefinition string
	SourceCharset    string
	SourceCollation  string
	DestCharset      string
	DestCollation    string
	Reason           string
}

func hasExplicitColumnCharsetOrCollation(definition string) bool {
	return mysqlColumnCharsetOrCollationClausePattern.MatchString(strings.TrimSpace(definition))
}

func isCharacterColumnDefinition(definition string) bool {
	return mysqlCharacterColumnDefinitionPattern.MatchString(strings.TrimSpace(definition))
}

func canUseTableCharsetConvertForColumnCollationDrift(sourceMeta, destMeta mysqlTableLevelMetadata, sourceColumnDefinitions map[string]string, candidates []columnCollationRepairCandidate) bool {
	if len(candidates) == 0 {
		return false
	}
	// 当 LEFT JOIN COLLATIONS 失败导致 charset 为空时，从 collation 名推断
	sourceCharset := strings.TrimSpace(sourceMeta.TableCharset)
	if sourceCharset == "" {
		sourceCharset = schemacompat.InferCharsetFromCollation(sourceMeta.TableCollation)
	}
	if sourceCharset == "" {
		return false
	}
	if !strings.EqualFold(sourceCharset, strings.TrimSpace(destMeta.TableCharset)) {
		return false
	}

	// 优化判断逻辑：当所有字段的 COLLATION 差异都是由表级 COLLATION 差异引起时
	// （即字段都继承各自表级的 COLLATION），只需修改表级定义即可
	sourceTableCollation := strings.TrimSpace(sourceMeta.TableCollation)
	destTableCollation := strings.TrimSpace(destMeta.TableCollation)

	// 关键优化：MySQL 的 ALTER TABLE ... CONVERT TO CHARACTER SET ... COLLATE ... 会正确处理
	// 所有字符类型字段，包括显式定义了 CHARSET/COLLATE 的字段。
	// 只要所有字段的 CHARSET/COLLATE 都与表级定义一致（无论是隐式继承还是显式定义），
	// 使用表级 CONVERT TO 都能正确修复。
	//
	// 判断逻辑：
	// 1. 所有 candidates 中的字段，其 SourceCharset 必须与表级 TableCharset 一致
	// 2. 所有 candidates 中的字段，其 SourceCollation 必须与表级 TableCollation 一致（或表级为空）
	// 3. 所有 candidates 中的字段，其 DestCollation 必须与目标端表级 TableCollation 一致
	//
	// 这样可以确保：
	// - 字段隐式继承表级定义的场景：可以使用表级修复
	// - 字段显式定义但与表级一致的场景：可以使用表级修复
	// - 字段显式定义且与表级不一致的场景：不能使用表级修复（会在后续检查中被拒绝）

	// 检查所有 candidates 是否都满足表级修复的条件
	for i, candidate := range candidates {
		// 字段 CHARSET 必须与表级 CHARSET 一致
		if !strings.EqualFold(strings.TrimSpace(candidate.SourceCharset), sourceCharset) {
			if global.Wlog != nil {
				global.Wlog.Debug(fmt.Sprintf("canUseTableCharsetConvertForColumnCollationDrift: candidate[%d] %s SourceCharset=%s != tableCharset=%s, rejecting table-level repair",
					i, candidate.ColumnName, candidate.SourceCharset, sourceCharset))
			}
			return false
		}
		// 检查目标端字段 COLLATION 是否与目标端表级 COLLATION 一致
		// 如果一致，说明字段是继承表级 COLLATION，可以通过修改表级定义来修复
		if !strings.EqualFold(strings.TrimSpace(candidate.DestCollation), destTableCollation) {
			if global.Wlog != nil {
				global.Wlog.Debug(fmt.Sprintf("canUseTableCharsetConvertForColumnCollationDrift: candidate[%d] %s DestCollation=%s != destTableCollation=%s, rejecting table-level repair",
					i, candidate.ColumnName, candidate.DestCollation, destTableCollation))
			}
			return false
		}
		// 如果源端表级 COLLATION 已显式定义，则要求源端字段 COLLATION 也与之一致
		// 如果源端表级 COLLATION 未显式定义（为空），则跳过此检查（字段继承隐式默认值）
		if sourceTableCollation != "" && !strings.EqualFold(strings.TrimSpace(candidate.SourceCollation), sourceTableCollation) {
			if global.Wlog != nil {
				global.Wlog.Debug(fmt.Sprintf("canUseTableCharsetConvertForColumnCollationDrift: candidate[%d] %s SourceCollation=%s != sourceTableCollation=%s, rejecting table-level repair",
					i, candidate.ColumnName, candidate.SourceCollation, sourceTableCollation))
			}
			return false
		}
	}

	if global.Wlog != nil {
		global.Wlog.Debug(fmt.Sprintf("canUseTableCharsetConvertForColumnCollationDrift: all %d candidates match table-level charset/collation, using table-level CONVERT TO", len(candidates)))
	}
	return true
}

func buildColumnCollationAdvisorySuggestions(candidates []columnCollationRepairCandidate) []schemacompat.ConstraintRepairSuggestion {
	suggestions := make([]schemacompat.ConstraintRepairSuggestion, 0, len(candidates))
	for _, candidate := range candidates {
		suggestions = append(suggestions, schemacompat.ConstraintRepairSuggestion{
			ConstraintName: candidate.ColumnName,
			Kind:           "COLUMN COLLATION",
			Level:          schemacompat.ConstraintRepairLevelAdvisoryOnly,
			Reason:         candidate.Reason,
		})
	}
	return suggestions
}

func (stcls *schemaTable) buildColumnCollationRepairSQL(
	fixer dbExec.DataAbnormalFixInterface,
	sourceMeta, destMeta mysqlTableLevelMetadata,
	sourceColumnDefinitions map[string]string,
	candidates []columnCollationRepairCandidate,
	logThreadSeq int64,
) ([]string, bool) {
	if len(candidates) == 0 {
		return nil, false
	}

	if canUseTableCharsetConvertForColumnCollationDrift(sourceMeta, destMeta, sourceColumnDefinitions, candidates) {
		// 检查是否为 MariaDB→MySQL 的等价映射场景（如 utf8mb4_uca1400_ai_ci → utf8mb4_0900_ai_ci）
		// 如果是，则不生成修复 SQL，让上层逻辑标记为 collation-mapped
		sourceCollation := strings.TrimSpace(sourceMeta.TableCollation)
		destCollation := strings.TrimSpace(destMeta.TableCollation)
		if sourceCollation != "" && destCollation != "" {
			if mappedCollation, ok := schemacompat.MapMariaDBCollationToMySQL(sourceCollation); ok {
				if strings.EqualFold(mappedCollation, destCollation) {
					// 源端 collation 映射后与目标端一致，属于等价映射，不生成修复 SQL
					global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: source collation %s maps to target %s, skipping repair SQL generation (collation-mapped)",
						logThreadSeq, sourceCollation, destCollation))
					return nil, false
				}
			}
		}

		// 检查是否存在 dTypeMapping 覆盖的列类型差异
		// 如果存在，需要生成列级 MODIFY COLUMN SQL（同时包含 collation 和 dTypeMapping 映射的类型）
		// 而不是表级 CONVERT TO SQL
		hasDTypeMappingOverride := false
		if schemacompat.GlobalDTypeMappingRules != nil {
			var dtRules []schemacompat.TypeMappingRule
			isMariaDBToMySQL := stcls.isMariaDBToMySQL()
			if isMariaDBToMySQL {
				dtRules = schemacompat.GlobalDTypeMappingRules.DTypeMapping.MariaDBToMySQL
			} else {
				dtRules = schemacompat.GlobalDTypeMappingRules.DTypeMapping.MySQLUpgrade
			}
			global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: dTypeMapping check: isMariaDBToMySQL=%v, dtRulesLen=%d, candidatesLen=%d",
				logThreadSeq, isMariaDBToMySQL, len(dtRules), len(candidates)))
			for _, candidate := range candidates {
				if len(dtRules) == 0 {
					global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: no dTypeMapping rules available, skipping candidate %s",
						logThreadSeq, candidate.ColumnName))
					continue
				}
				// 从 SourceAttrs 中获取源端类型
				sourceType := ""
				if len(candidate.SourceAttrs) > 0 {
					sourceType = candidate.SourceAttrs[0]
				}
				// 构建 MappingContext 检查是否存在匹配的 dTypeMapping 规则
				sourceNullable := false
				if len(candidate.SourceAttrs) > 3 {
					sourceNullable = strings.EqualFold(strings.TrimSpace(candidate.SourceAttrs[3]), "YES")
				}
				sourceAutoInc := false
				if strings.Contains(strings.ToUpper(sourceType), "AUTO_INCREMENT") {
					sourceAutoInc = true
				}
				ctx := schemacompat.BuildMappingContext(sourceType, sourceNullable, candidate.ColumnName, sourceAutoInc, stcls.schema, stcls.table)
				global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: checking candidate %s: sourceType=%q, sourceNullable=%v, sourceAutoInc=%v, schema=%s, table=%s, ctx.SourceType=%q",
					logThreadSeq, candidate.ColumnName, sourceType, sourceNullable, sourceAutoInc, stcls.schema, stcls.table, ctx.SourceType))
				if _, _, matched := schemacompat.MatchUserRule(dtRules, ctx); matched {
					hasDTypeMappingOverride = true
					global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: column %s has dTypeMapping override, switching to column-level MODIFY",
						logThreadSeq, candidate.ColumnName))
					break
				} else {
					global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: column %s has NO dTypeMapping match",
						logThreadSeq, candidate.ColumnName))
				}
			}
		} else {
			global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: GlobalDTypeMappingRules is nil",
				logThreadSeq))
		}

		// 如果存在 dTypeMapping 覆盖，生成列级 MODIFY COLUMN SQL
		if hasDTypeMappingOverride {
			sort.SliceStable(candidates, func(i, j int) bool {
				return candidates[i].ColumnSeq < candidates[j].ColumnSeq
			})

			alterOps := make([]string, 0, len(candidates))
			for _, candidate := range candidates {
				repairAttrs := append([]string(nil), candidate.SourceAttrs...)
				if len(repairAttrs) < 6 {
					for len(repairAttrs) < 6 {
						repairAttrs = append(repairAttrs, "null")
					}
				}

				// 使用 BuildTargetColumnRepairPlan 获取基础修复计划
				repairPlan := schemacompat.BuildTargetColumnRepairPlan(
					candidate.ColumnName,
					repairAttrs,
					stcls.sourceVersionInfo(),
					stcls.destVersionInfo(),
					candidate.SourceDefinition,
					stcls.checkRules.MariaDBJSONTargetType,
					stcls.schema, stcls.table,
				)
				if strings.TrimSpace(repairPlan.Type) != "" {
					repairAttrs[0] = repairPlan.Type
				}
				if strings.TrimSpace(repairPlan.Charset) != "" {
					repairAttrs[1] = repairPlan.Charset
				}
				if strings.TrimSpace(repairPlan.Collation) != "" {
					repairAttrs[2] = repairPlan.Collation
				}
				if repairPlan.UseDirectDefinition {
					if len(repairAttrs) < 7 {
						repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
					} else {
						repairAttrs[6] = repairPlan.DirectDefinition
					}
				}

				// 应用 dTypeMapping 覆盖属性（nullable/default/unsigned）
				applyDTypeMappingOverrides(repairAttrs, candidate.ColumnName, stcls.isOracleToMySQL(), stcls.isMariaDBToMySQL(), candidate.SourceAttrs[0], stcls.schema, stcls.table)

				alterOps = append(alterOps, fixer.FixAlterColumnSqlDispos("modify", repairAttrs, candidate.ColumnSeq, candidate.LastColumn, candidate.ColumnName, logThreadSeq))
			}

			if len(alterOps) == 0 {
				return nil, false
			}
			global.Wlog.Debug(fmt.Sprintf("(%d) buildColumnCollationRepairSQL: generated %d column-level MODIFY statements with dTypeMapping overrides for %s.%s",
				logThreadSeq, len(alterOps), stcls.schema, stcls.table))
			return fixer.FixAlterColumnSqlGenerate(alterOps, logThreadSeq), true
		}

		// 使用 CONVERT TO CHARACTER SET 修复列级 collation 漂移时，必须始终显式指定
		// source collation，否则在跨版本场景（如 MySQL 5.6 → 8.0）下，目标端会使用
		// 其自身默认 collation（utf8mb4_0900_ai_ci），而非源端期望的 collation。
		collation := sourceCollation
		// MariaDB UCA 14.0.0 collation 在 MySQL 上不存在，映射为 UCA 9.0.0 等价物
		if collation != "" {
			if mapped, ok := schemacompat.MapMariaDBCollationToMySQL(collation); ok {
				collation = mapped
			}
		}
		sqls := fixer.FixTableCharsetSqlGenerate(sourceMeta.TableCharset, collation, logThreadSeq)
		return sqls, len(sqls) > 0
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].ColumnSeq < candidates[j].ColumnSeq
	})

	alterOps := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		repairAttrs := append([]string(nil), candidate.SourceAttrs...)
		if len(repairAttrs) < 6 {
			for len(repairAttrs) < 6 {
				repairAttrs = append(repairAttrs, "null")
			}
		}
		repairPlan := schemacompat.BuildTargetColumnRepairPlan(
			candidate.ColumnName,
			repairAttrs,
			stcls.sourceVersionInfo(),
			stcls.destVersionInfo(),
			candidate.SourceDefinition,
			stcls.checkRules.MariaDBJSONTargetType,
			stcls.schema, stcls.table,
		)
		if strings.TrimSpace(repairPlan.Type) != "" {
			repairAttrs[0] = repairPlan.Type
		}
		if strings.TrimSpace(repairPlan.Charset) != "" {
			repairAttrs[1] = repairPlan.Charset
		}
		if strings.TrimSpace(repairPlan.Collation) != "" {
			repairAttrs[2] = repairPlan.Collation
		}
		if repairPlan.UseDirectDefinition {
			if len(repairAttrs) < 7 {
				repairAttrs = append(repairAttrs, repairPlan.DirectDefinition)
			} else {
				repairAttrs[6] = repairPlan.DirectDefinition
			}
		}
		alterOps = append(alterOps, fixer.FixAlterColumnSqlDispos("modify", repairAttrs, candidate.ColumnSeq, candidate.LastColumn, candidate.ColumnName, logThreadSeq))
	}

	if len(alterOps) == 0 {
		return nil, false
	}
	return fixer.FixAlterColumnSqlGenerate(alterOps, logThreadSeq), true
}

func queryMySQLTableLevelMetadata(db *sql.DB, schema, table string) (mysqlTableLevelMetadata, error) {
	var (
		collation sql.NullString
		charset   sql.NullString
		comment   sql.NullString
		rowFormat sql.NullString
		createOpt sql.NullString
	)

	query := `
SELECT t.TABLE_COLLATION, c.CHARACTER_SET_NAME, t.AUTO_INCREMENT, t.ROW_FORMAT, t.CREATE_OPTIONS, t.TABLE_COMMENT
FROM information_schema.TABLES t
LEFT JOIN information_schema.COLLATIONS c ON t.TABLE_COLLATION = c.COLLATION_NAME
WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
`

	var runtimeNextAutoInc sql.NullInt64
	if err := db.QueryRow(query, schema, table).Scan(&collation, &charset, &runtimeNextAutoInc, &rowFormat, &createOpt, &comment); err != nil {
		return mysqlTableLevelMetadata{}, err
	}

	createStmt, err := queryMySQLCreateTableStatement(db, schema, table)
	if err != nil {
		return mysqlTableLevelMetadata{}, err
	}

	return mysqlTableLevelMetadata{
		TableCollation: collation.String,
		TableCharset:   charset.String,
		AutoIncrement:  extractExplicitMySQLTableAutoIncrementValue(createStmt),
		RowFormat:      rowFormat.String,
		CreateOptions:  createOpt.String,
		TableComment:   comment.String,
		CreateTableSQL: createStmt,
	}, nil
}
