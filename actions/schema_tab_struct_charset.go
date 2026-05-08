package actions

import (
	"database/sql"
	"regexp"
	"sort"
	"strings"

	"gt-checksum/dbExec"
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

	for _, definition := range sourceColumnDefinitions {
		if !isCharacterColumnDefinition(definition) {
			continue
		}
		if hasExplicitColumnCharsetOrCollation(definition) {
			return false
		}
	}

	// 优化判断逻辑：当所有字段的 COLLATION 差异都是由表级 COLLATION 差异引起时
	// （即字段都继承各自表级的 COLLATION），只需修改表级定义即可
	sourceTableCollation := strings.TrimSpace(sourceMeta.TableCollation)
	destTableCollation := strings.TrimSpace(destMeta.TableCollation)

	for _, candidate := range candidates {
		if !strings.EqualFold(strings.TrimSpace(candidate.SourceCharset), strings.TrimSpace(sourceMeta.TableCharset)) {
			return false
		}
		// 检查目标端字段 COLLATION 是否与目标端表级 COLLATION 一致
		// 如果一致，说明字段是继承表级 COLLATION，可以通过修改表级定义来修复
		if !strings.EqualFold(strings.TrimSpace(candidate.DestCollation), destTableCollation) {
			return false
		}
		// 如果源端表级 COLLATION 已显式定义，则要求源端字段 COLLATION 也与之一致
		// 如果源端表级 COLLATION 未显式定义（为空），则跳过此检查（字段继承隐式默认值）
		if sourceTableCollation != "" && !strings.EqualFold(strings.TrimSpace(candidate.SourceCollation), sourceTableCollation) {
			return false
		}
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
		// 使用 CONVERT TO CHARACTER SET 修复列级 collation 漂移时，必须始终显式指定
		// source collation，否则在跨版本场景（如 MySQL 5.6 → 8.0）下，目标端会使用
		// 其自身默认 collation（utf8mb4_0900_ai_ci），而非源端期望的 collation。
		collation := strings.TrimSpace(sourceMeta.TableCollation)
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
