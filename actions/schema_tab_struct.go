package actions

import (
	"database/sql"
	"errors"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"sort"
	"strconv"
	"strings"
)

type schemaTable struct {
	schema                  string
	table                   string
	ignoreSchema            string
	ignoreTable             string
	sourceDrive             string
	destDrive               string
	sourceDB                *sql.DB
	destDB                  *sql.DB
	caseSensitiveObjectName string
	datefix                 string
	sfile                   *os.File
	djdbc                   string
	checkMode               string //列的校验模式，分为宽松模式和严格模式
	structRul               inputArg.StructS
	checkRules              inputArg.RulesS
	// 添加表映射规则
	tableMappings map[string]string
}

// getDisplayTableName 返回表的显示名称，包含映射关系信息
// 如果存在映射关系，返回格式为 "sourceSchema.table:destSchema.table"
// 如果不存在映射关系，返回格式为 "schema.table"
func (stcls *schemaTable) getDisplayTableName(schema, table string) string {
	// 检查是否存在映射关系
	if mappedSchema, exists := stcls.tableMappings[schema]; exists && mappedSchema != schema {
		// 存在映射关系，返回包含映射信息的名称
		return fmt.Sprintf("%s.%s:%s.%s", schema, table, mappedSchema, table)
	}

	// 不存在映射关系，返回普通名称
	return fmt.Sprintf("%s.%s", schema, table)
}

// getSourceTableName 返回源表的名称
func (stcls *schemaTable) getSourceTableName(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}

// getDestTableName 返回目标表的名称
func (stcls *schemaTable) getDestTableName(schema, table string) string {
	destSchema := schema
	if mappedSchema, exists := stcls.tableMappings[schema]; exists {
		destSchema = mappedSchema
	}
	return fmt.Sprintf("%s.%s", destSchema, table)
}

/*
查询待校验表的列名
*/
func (stcls *schemaTable) tableColumnName(db *sql.DB, tc dbExec.TableColumnNameStruct, logThreadSeq, logThreadSeq2 int64) ([]map[string][]string, error) {
	var (
		col       []map[string][]string
		vlog      string
		CS        []string
		queryData []map[string]interface{}
		err       error
		Event     = "Q_table_columns"
		A         = make(map[string][]string)
		C         = func(c string) string {
			switch c {
			case "<nil>":
				return "null"
			case "<entry>":
				return "empty"
			default:
				return c
			}
		}
	)
	if queryData, err = tc.Query().TableColumnName(db, logThreadSeq2); err != nil {
		return col, err
	}
	vlog = fmt.Sprintf("(%d) [%s] start checking columns", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		if fmt.Sprintf("%v", v["columnName"]) != "" {
			A[fmt.Sprintf("%v", v["columnName"])] = []string{C(fmt.Sprintf("%v", v["columnType"])), C(fmt.Sprintf("%v", v["charset"])), C(fmt.Sprintf("%v", v["collationName"])), C(fmt.Sprintf("%v", v["isNull"])), C(fmt.Sprintf("%v", v["columnDefault"])), C(fmt.Sprintf("%v", v["columnComment"]))}
			CS = append(CS, fmt.Sprintf("%v", v["columnName"]))
		}
	}
	for _, v := range CS {
		col = append(col, map[string][]string{v: A[v]})
	}
	vlog = fmt.Sprintf("(%d) [%s] columns checksum completed", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	return col, nil
}

/*
校验表的列名是否正确
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		err                                  error
		tableAbnormalBool                    = false
		event                                string
	)
	vlog = fmt.Sprintf("(%d) %s Start checking the differences between the table structure and columns of srcDSN and dstDSN", logThreadSeq, event)
	global.Wlog.Debug(vlog)
	for _, v := range checkTableList {
		// 从表列表中提取源端schema和表名
		sourceSchema := strings.Split(v, ".")[0]
		stcls.table = strings.Split(v, ".")[1]

		// 根据映射规则确定目标端schema
		destSchema := sourceSchema
		if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
			destSchema = mappedSchema
		}

		vlog = fmt.Sprintf("Table mapping check - source: %s, mapped: %s, mappings: %v", sourceSchema, destSchema, stcls.tableMappings)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d %s Start checking structure of table %s.%s:[%s.%s]", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		var sColumn, dColumn []map[string][]string

		dbf := dbExec.DataAbnormalFixStruct{Schema: sourceSchema, Table: stcls.table, DestDevice: stcls.destDrive, DatafixType: stcls.datefix}
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: stcls.table, Drive: stcls.sourceDrive}
		sColumn, err = stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Obtain metadata of table %s.%s in srcDSN {%s} failed: {%s}", logThreadSeq, event, sourceSchema, stcls.table, stcls.sourceDrive, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s srcDSN {%s} table: [%s.%s] [%d] columns: {%v}", logThreadSeq, event, stcls.sourceDrive, sourceSchema, stcls.table, len(sColumn), sColumn)
		global.Wlog.Debug(vlog)

		// 使用目标端schema
		tc.Schema = destSchema
		tc.Drive = stcls.destDrive
		dColumn, err = stcls.tableColumnName(stcls.destDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Obtain metadata of table %s.%s in dstDSN {%s} failed: {%s}", logThreadSeq, event, destSchema, stcls.table, stcls.destDrive, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s dstDSN {%s} table: [%s.%s] [%d] columns: {%v}", logThreadSeq, event, stcls.destDrive, destSchema, stcls.table, len(dColumn), dColumn)
		global.Wlog.Debug(vlog)

		alterSlice := []string{}
		var sourceColumnSlice, destColumnSlice []string
		var sourceColumnMap, destColumnMap = make(map[string][]string), make(map[string][]string)
		var sourceColumnSeq, destColumnSeq = make(map[string]int), make(map[string]int)
		for k1, v1 := range sColumn {
			v1k := ""
			v2 := []string{}
			for k, v22 := range v1 {
				v1k = k
				if stcls.caseSensitiveObjectName == "no" {
					v1k = strings.ToUpper(k)
				}
				v2 = v22
			}
			sourceColumnMap[v1k] = v2
			sourceColumnSeq[v1k] = k1
			sourceColumnSlice = append(sourceColumnSlice, v1k)
		}
		for k1, v1 := range dColumn {
			v1k := ""
			v2 := []string{}
			for k, v22 := range v1 {
				v1k = k
				if stcls.caseSensitiveObjectName == "no" {
					v1k = strings.ToUpper(k)
				}
				v2 = v22
			}
			destColumnMap[v1k] = v2
			destColumnSeq[v1k] = k1
			destColumnSlice = append(destColumnSlice, v1k)
		}
		addColumn, delColumn := aa.Arrcmp(sourceColumnSlice, destColumnSlice)
		if stcls.checkRules.CheckObject == "data" {
			if len(addColumn) == 0 && len(delColumn) == 0 {
				// 使用目标端schema
				newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
			} else {
				// 使用正确的源和目标数据库名
				vlog = fmt.Sprintf("(%d) %s The [%s.%s]:[%s.%s] table structure of srcDB and dstDB are different, the extra columns: {%v}, the missing columns: {%v}", logThreadSeq, event, sourceSchema, stcls.table, destSchema, stcls.table, addColumn, delColumn)
				global.Wlog.Error(vlog)
				abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
			}
			// yejr存疑：不要加continue，否则可能导致当检查到有个表中列定义不一致时，这里会被跳过忽略检查
			//continue
		}

		vlog = fmt.Sprintf("(%d) %s Some columns that should be deleted from dstDSN {%s}, table {%s.%s}, columns {%v}", logThreadSeq, event, stcls.destDrive, destSchema, stcls.table, delColumn)
		global.Wlog.Debug(vlog)
		// 先删除缺失的
		if len(delColumn) > 0 {
			// 收集所有需要删除的列名
			var colsToDelete []string
			for _, v1 := range delColumn {
				dropSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("drop", destColumnMap[v1], 1, "", v1, logThreadSeq)
				alterSlice = append(alterSlice, dropSql)
				colsToDelete = append(colsToDelete, v1)
			}
			// 在循环外删除所有标记的列
			for _, col := range colsToDelete {
				delete(destColumnMap, col)
			}
		}
		vlog = fmt.Sprintf("(%d) %s The DROP SQL on Table {%s.%s} on dstDSN {%s} should be \"%v\"", logThreadSeq, event, destSchema, stcls.table, stcls.destDrive, alterSlice)
		global.Wlog.Debug(vlog)
		for k1, v1 := range sourceColumnSlice {
			lastcolumn := ""
			var alterColumnData []string
			if k1 == 0 {
				lastcolumn = sourceColumnSlice[k1]
			} else {
				lastcolumn = sourceColumnSlice[k1-1]
			}
			switch stcls.structRul.ScheckFixRule {
			case "src":
				alterColumnData = sourceColumnMap[v1]
			case "dst":
				alterColumnData = destColumnMap[v1]
			default:
				err = errors.New(fmt.Sprintf("unknown options"))
				vlog = fmt.Sprintf("(%d) %s The option \"checkObject\" is set incorrectly, error: {%v}", logThreadSeq, event, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}
			if _, ok := destColumnMap[v1]; ok {
				switch stcls.structRul.ScheckMod {
				case "loose":
					switch stcls.structRul.ScheckOrder {
					case "yes":
						if sourceColumnSeq[v1] != destColumnSeq[v1] {
							tableAbnormalBool = true
						} else {
							tableAbnormalBool = false
						}
					case "no":
						tableAbnormalBool = false
					default:
						err = errors.New(fmt.Sprintf("unknown options"))
						vlog = fmt.Sprintf("(%d) %s The option \"checkObject\" is set incorrectly, error: {%v}", logThreadSeq, event, err)
						global.Wlog.Error(vlog)
						return nil, nil, err
					}
					if tableAbnormalBool {
						modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", alterColumnData, k1, lastcolumn, v1, logThreadSeq)
						vlog = fmt.Sprintf("(%d) %s The column definition of table {%s.%s} is different, and ALTER SQL is \"%s\"", logThreadSeq, v1, destSchema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						alterSlice = append(alterSlice, modifySql)
					} else {
						vlog = fmt.Sprintf("(%d) %s The column definition of table {%s.%s} is consistent, no ALTER SQL needed", logThreadSeq, v1, destSchema, stcls.table)
						global.Wlog.Debug(vlog)
					}

				case "strict":
					switch stcls.structRul.ScheckOrder {
					case "yes":
						if CheckSum().CheckMd5(strings.Join(sourceColumnMap[v1], "")) != CheckSum().CheckMd5(strings.Join(destColumnMap[v1], "")) || sourceColumnSeq[v1] != destColumnSeq[v1] {
							tableAbnormalBool = true
						} else {
							tableAbnormalBool = false
						}
					case "no":
						if CheckSum().CheckMd5(strings.Join(sourceColumnMap[v1], "")) != CheckSum().CheckMd5(strings.Join(destColumnMap[v1], "")) {
							tableAbnormalBool = true
						} else {
							tableAbnormalBool = false
						}
					default:
						err = errors.New(fmt.Sprintf("unknown parameters"))
						vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
						global.Wlog.Error(vlog)
						return nil, nil, err
					}
					if tableAbnormalBool {
						modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", alterColumnData, k1, lastcolumn, v1, logThreadSeq)
						vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s:[%s.%s] is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, v1, stcls.schema, stcls.table, destSchema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						alterSlice = append(alterSlice, modifySql)
					}
				default:
					err = errors.New(fmt.Sprintf("unknown parameters"))
					vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
					global.Wlog.Error(vlog)
					return nil, nil, err
				}
				delete(destColumnMap, v1)
			} else {
				switch stcls.structRul.ScheckOrder {
				case "yes":
					lastcolumn = lastcolumn
				case "no":
					lastcolumn = "alterNoAfter"
				default:
					err = errors.New(fmt.Sprintf("unknown parameters"))
					vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
					global.Wlog.Error(vlog)
					return nil, nil, err
				}
				var position int
				if stcls.structRul.ScheckOrder == "yes" {
					// Use the source column's actual position
					position = k1
				} else {
					// In loose mode, append at the end
					position = len(destColumnMap)
				}
				addSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", sourceColumnMap[v1], position, lastcolumn, v1, logThreadSeq)
				vlog = fmt.Sprintf("(%d) %s The column %s is missing in the dstDSN {%s} table %s.%s on the target side, and the add statement is generated, and the add statement is {%v}", logThreadSeq, event, v1, stcls.destDrive, destSchema, stcls.table, addSql)
				global.Wlog.Warn(vlog)
				alterSlice = append(alterSlice, addSql)
				delete(destColumnMap, v1)
			}
		}
		if len(alterSlice) > 0 {
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		} else {
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", destSchema, stcls.table))
		}
		sqlS := dbf.DataAbnormalFix().FixAlterColumnSqlGenerate(alterSlice, logThreadSeq)
		vlog = fmt.Sprintf("(%d) %s The table structure consistency check of table %s.%s:[%s.%s] is completed.", logThreadSeq, event, stcls.schema, stcls.table, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		if len(sqlS) > 0 {
			vlog = fmt.Sprintf("(%d) %s Start to repair the statement in dstDSN {%s} table %s.%s on the target side according to the specified repair method. The repair statement is {%v}.", logThreadSeq, event, stcls.destDrive, destSchema, stcls.table, sqlS)
			global.Wlog.Debug(vlog)
			if err = ApplyDataFix(sqlS, stcls.datefix, stcls.sfile, stcls.destDrive, stcls.djdbc, logThreadSeq); err != nil {
				return nil, nil, err
			}
			vlog = fmt.Sprintf("(%d) %s dstDSN {%s} table %s.%s repair statement application is completed.", logThreadSeq, event, stcls.destDrive, destSchema, stcls.table)
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) %s The table structure checksum of srcDSN and dstDSN completed", logThreadSeq)
	global.Wlog.Info(vlog)

	return newCheckTableList, abnormalTableList, nil
}

/*
该函数用于获取MySQL的表的索引信息,判断表是否存在索引，加入存在，获取索引的类型，以主键索引、唯一索引、普通索引及无索引，主键索引或唯一索引以自增id为优先

	缺少索引列为空或null的处理
*/
func (stcls *schemaTable) tableIndexAlgorithm(indexType map[string][]string) (string, []string) {
	if len(indexType) > 0 {
		// 优先选择主键索引
		if len(indexType["pri_single"]) > 0 {
			return "pri_single", indexType["pri_single"]
		}
		if len(indexType["pri_multi"]) > 0 {
			return "pri_multi", indexType["pri_multi"]
		}

		// 其次选择唯一索引
		if len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}
		if len(indexType["uni_multi"]) > 0 {
			return "uni_multi", indexType["uni_multi"]
		}

		// 最后选择普通索引
		if len(indexType["mul_single"]) > 0 {
			return "mul_single", indexType["mul_single"]
		}
		if len(indexType["mul_multi"]) > 0 {
			return "mul_multi", indexType["mul_multi"]
		}
	}
	return "", []string{}
}

// 处理模糊匹配，支持数据库映射规则
func (stcls *schemaTable) FuzzyMatchingDispos(dbCheckNameList map[string]int, Ftable string, logThreadSeq int64) map[string]int {
	var (
		schema string
		vlog   string
	)
	b := make(map[string]int)
	f := make(map[string]int)

	// 添加调试日志，显示当前的映射规则
	vlog = fmt.Sprintf("Current table mappings in FuzzyMatchingDispos: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	//处理库的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		// 解析映射关系
		srcPattern := i
		dstPattern := ""
		hasMappingRule := false

		if strings.Contains(i, ":") {
			parts := strings.SplitN(i, ":", 2)
			if len(parts) == 2 {
				srcPattern = parts[0]
				dstPattern = parts[1]
				hasMappingRule = true
			}
		}

		vlog = fmt.Sprintf("Processing pattern: src=%s, dst=%s, hasMapping=%v", srcPattern, dstPattern, hasMappingRule)
		global.Wlog.Debug(vlog)

		if !strings.Contains(srcPattern, ".") {
			continue
		}

		schema = strings.ReplaceAll(srcPattern[:strings.Index(srcPattern, ".")], "%", "")

		// 处理通配符模式
		if schema == "*" { //处理*库
			for k, _ := range dbCheckNameList {
				b[k]++
				vlog = fmt.Sprintf("Added wildcard schema: %s", k)
				global.Wlog.Debug(vlog)
			}
		} else if strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理%schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range dbCheckNameList {
				if strings.Contains(k, tmpschema) {
					b[k]++
					vlog = fmt.Sprintf("Added %schema% match: %s", k)
					global.Wlog.Debug(vlog)
				}
			}
		} else if strings.HasPrefix(schema, "%") && !strings.HasSuffix(schema, "%") { //处理%schema
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range dbCheckNameList {
				if strings.HasSuffix(k, tmpschema) {
					b[k]++
					vlog = fmt.Sprintf("Added %schema match: %s", k)
					global.Wlog.Debug(vlog)
				}
			}
		} else if !strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理schema%
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range dbCheckNameList {
				if strings.HasPrefix(k, tmpschema) {
					b[k]++
					vlog = fmt.Sprintf("Added schema% match: %s", k)
					global.Wlog.Debug(vlog)
				}
			}
		} else { //处理schema
			// 检查是否在映射规则中存在
			if _, exists := stcls.tableMappings[schema]; exists {
				// schema是源端schema，直接添加
				b[schema]++
				vlog = fmt.Sprintf("Added source schema from mapping: %s", schema)
				global.Wlog.Debug(vlog)
			} else if hasMappingRule {
				// 如果有明确的映射规则，尝试使用它
				dstSchema := ""
				if strings.Contains(dstPattern, ".") {
					dstSchema = dstPattern[:strings.Index(dstPattern, ".")]
				} else {
					dstSchema = dstPattern
				}

				// 检查源schema是否存在于数据库列表中
				if _, exists := dbCheckNameList[schema]; exists {
					b[schema]++
					vlog = fmt.Sprintf("Added explicit mapping source schema: %s -> %s", schema, dstSchema)
					global.Wlog.Debug(vlog)
				}
			} else {
				// 检查是否是目标端schema
				found := false
				for src, dst := range stcls.tableMappings {
					if dst == schema {
						// 找到对应源端schema
						b[src]++
						found = true
						vlog = fmt.Sprintf("Added reverse mapping source schema: %s -> %s", src, dst)
						global.Wlog.Debug(vlog)
						break
					}
				}
				// 如果没有映射关系，则按常规处理
				if !found {
					// 检查schema是否存在于数据库列表中
					if _, exists := dbCheckNameList[schema]; exists {
						b[schema]++
						vlog = fmt.Sprintf("Added direct schema (no mapping): %s", schema)
						global.Wlog.Debug(vlog)
					}
				}
			}
		}
	}

	vlog = fmt.Sprintf("After schema processing, b map: %v", b)
	global.Wlog.Debug(vlog)

	//处理表的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		// 解析映射关系
		srcPattern := i
		dstPattern := ""
		hasMappingRule := false

		if strings.Contains(i, ":") {
			parts := strings.SplitN(i, ":", 2)
			if len(parts) == 2 {
				srcPattern = parts[0]
				dstPattern = parts[1]
				hasMappingRule = true
			}
		}

		vlog = fmt.Sprintf("Processing table pattern: src=%s, dst=%s, hasMapping=%v", srcPattern, dstPattern, hasMappingRule)
		global.Wlog.Debug(vlog)

		if !strings.Contains(srcPattern, ".") {
			continue
		}

		schema = strings.ReplaceAll(srcPattern[:strings.Index(srcPattern, ".")], "%", "")
		table := srcPattern[strings.Index(srcPattern, ".")+1:]

		vlog = fmt.Sprintf("Extracted schema=%s, table=%s", schema, table)
		global.Wlog.Debug(vlog)

		// 处理表名通配符
		for dbSchema, _ := range b {
			// 检查是否有映射关系
			mappedSchema := dbSchema
			if mapped, exists := stcls.tableMappings[dbSchema]; exists {
				mappedSchema = mapped
				vlog = fmt.Sprintf("Found mapping for schema %s -> %s", dbSchema, mappedSchema)
				global.Wlog.Debug(vlog)
			}

			// 检查schema是否匹配
			if dbSchema == schema || schema == "*" {
				// 构建表名查询
				for dbName, _ := range dbCheckNameList {
					dbParts := strings.Split(dbName, "/*schema&table*/")
					if len(dbParts) < 2 {
						continue
					}

					dbSchemaName := dbParts[0]
					dbTableName := dbParts[1]

					// 检查schema是否匹配
					if dbSchemaName != dbSchema {
						continue
					}

					// 处理表名通配符
					if table == "*" { // 处理schema.*
						f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
						vlog = fmt.Sprintf("Added wildcard table: %s.%s", dbSchema, dbTableName)
						global.Wlog.Debug(vlog)
					} else if strings.HasPrefix(table, "%") && !strings.HasSuffix(table, "%") { // 处理schema.%table
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.HasSuffix(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added %table match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if !strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.HasPrefix(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added table% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else if strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { // 处理schema.%table%
						tmptable := strings.ReplaceAll(table, "%", "")
						if strings.Contains(dbTableName, tmptable) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added %table% match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					} else { // 处理schema.table
						if strings.EqualFold(dbTableName, table) {
							f[fmt.Sprintf("%s.%s", dbSchema, dbTableName)]++
							vlog = fmt.Sprintf("Added exact table match: %s.%s", dbSchema, dbTableName)
							global.Wlog.Debug(vlog)
						}
					}
				}
			}
		}
	}

	vlog = fmt.Sprintf("Final result map: %v", f)
	global.Wlog.Debug(vlog)

	return f
}

/*
处理需要校验的库表
将忽略的库表从校验列表中去除，如果校验列表为空则退出
*/
// 定义一个新的结构体来存储表映射信息
type TableMapping struct {
	SourceSchema string // 源端schema
	SourceTable  string // 源端表名
	DestSchema   string // 目标端schema
	DestTable    string // 目标端表名
}

func (stcls *schemaTable) SchemaTableFilter(logThreadSeq1, logThreadSeq2 int64) ([]string, error) {
	var (
		vlog            string
		f               []string
		dbCheckNameList map[string]int
		err             error
	)
	fmt.Println("gt-checksum is opening check tables")
	vlog = fmt.Sprintf("(%d) Obtain schema.table info", logThreadSeq1)
	global.Wlog.Info(vlog)

	// 解析表映射规则
	stcls.parseTableMappings(stcls.table)

	// 添加调试日志，显示解析后的映射规则
	vlog = fmt.Sprintf("Table mappings after parsing: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	// 获取源数据库信息列表
	tc := dbExec.TableColumnNameStruct{
		Table:                   stcls.table,
		Drive:                   stcls.sourceDrive,
		Db:                      stcls.sourceDB,
		IgnoreTable:             stcls.ignoreTable,
		CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
	}
	vlog = fmt.Sprintf("(%d) Obtain source databases list", logThreadSeq1)
	global.Wlog.Debug(vlog)
	if dbCheckNameList, err = tc.Query().DatabaseNameList(stcls.sourceDB, logThreadSeq2); err != nil {
		return f, err
	}
	vlog = fmt.Sprintf("(%d) Source databases list: %v", logThreadSeq1, dbCheckNameList)
	global.Wlog.Debug(vlog)

	// 判断源库是否为空
	if len(dbCheckNameList) == 0 {
		vlog = fmt.Sprintf("(%d) Databases of srcDSN {%s} is empty, please check if the \"tables\" option is correct", logThreadSeq1, stcls.sourceDrive)
		global.Wlog.Error(vlog)
		return f, nil
	}

	// 处理映射关系中的目标库
	// 如果有映射关系，也需要获取目标库的信息
	destDbCheckNameList := make(map[string]int)

	// 检查是否有映射关系
	hasMapping := false
	for _, pattern := range strings.Split(stcls.table, ",") {
		if strings.Contains(pattern, ":") {
			hasMapping = true
			break
		}
	}

	// 如果有映射关系，获取目标库信息
	if hasMapping {
		vlog = fmt.Sprintf("(%d) Mapping relationship detected, obtaining destination databases list", logThreadSeq1)
		global.Wlog.Debug(vlog)

		tcDest := dbExec.TableColumnNameStruct{
			Table:                   stcls.table,
			Drive:                   stcls.destDrive,
			Db:                      stcls.destDB,
			IgnoreTable:             stcls.ignoreTable,
			CaseSensitiveObjectName: stcls.caseSensitiveObjectName,
		}

		destDbList, err := tcDest.Query().DatabaseNameList(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error getting destination databases list: %v", logThreadSeq1, err)
			global.Wlog.Error(vlog)
		} else {
			destDbCheckNameList = destDbList
			vlog = fmt.Sprintf("(%d) Destination databases list: %v", logThreadSeq1, destDbCheckNameList)
			global.Wlog.Debug(vlog)
		}
	}

	// 创建表映射列表
	tableMappings := make([]TableMapping, 0)

	// 处理 db1.*:db2.* 格式的映射
	for _, pattern := range strings.Split(stcls.table, ",") {
		if strings.Contains(pattern, ":") {
			mapping := strings.SplitN(pattern, ":", 2)
			if len(mapping) == 2 {
				srcPattern := mapping[0]
				dstPattern := mapping[1]

				// 处理 db1.*:db2.* 格式
				if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
					srcDB := strings.TrimSuffix(srcPattern, ".*")
					dstDB := strings.TrimSuffix(dstPattern, ".*")

					vlog = fmt.Sprintf("Processing wildcard mapping: %s.* -> %s.*", srcDB, dstDB)
					global.Wlog.Debug(vlog)

					// 获取源库中的所有表
					for dbName, _ := range dbCheckNameList {
						if strings.HasPrefix(dbName, srcDB+"/*schema&table*/") {
							tableName := strings.TrimPrefix(dbName, srcDB+"/*schema&table*/")

							// 创建表映射
							mapping := TableMapping{
								SourceSchema: srcDB,
								SourceTable:  tableName,
								DestSchema:   dstDB,
								DestTable:    tableName,
							}
							tableMappings = append(tableMappings, mapping)

							vlog = fmt.Sprintf("Added mapping: %s.%s -> %s.%s", srcDB, tableName, dstDB, tableName)
							global.Wlog.Debug(vlog)
						}
					}

					// 检查目标库中是否有源库中不存在的表
					for dbName, _ := range destDbCheckNameList {
						if strings.HasPrefix(dbName, dstDB+"/*schema&table*/") {
							tableName := strings.TrimPrefix(dbName, dstDB+"/*schema&table*/")

							// 检查这个表是否已经在映射列表中
							found := false
							for _, m := range tableMappings {
								if m.DestSchema == dstDB && m.DestTable == tableName {
									found = true
									break
								}
							}

							// 如果没有找到，添加新的映射
							if !found {
								mapping := TableMapping{
									SourceSchema: srcDB,
									SourceTable:  tableName,
									DestSchema:   dstDB,
									DestTable:    tableName,
								}
								tableMappings = append(tableMappings, mapping)

								vlog = fmt.Sprintf("Added mapping from dest table: %s.%s -> %s.%s", srcDB, tableName, dstDB, tableName)
								global.Wlog.Debug(vlog)
							}
						}
					}
				} else if strings.Contains(srcPattern, ".") && strings.Contains(dstPattern, ".") {
					// 处理 db1.t1:db2.t2 格式
					srcParts := strings.Split(srcPattern, ".")
					dstParts := strings.Split(dstPattern, ".")

					if len(srcParts) == 2 && len(dstParts) == 2 {
						srcDB := srcParts[0]
						srcTable := srcParts[1]
						dstDB := dstParts[0]
						dstTable := dstParts[1]

						// 创建表映射
						mapping := TableMapping{
							SourceSchema: srcDB,
							SourceTable:  srcTable,
							DestSchema:   dstDB,
							DestTable:    dstTable,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added direct mapping: %s.%s -> %s.%s", srcDB, srcTable, dstDB, dstTable)
						global.Wlog.Debug(vlog)
					}
				}
			}
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")

				// 获取该库中的所有表
				for dbName, _ := range dbCheckNameList {
					if strings.HasPrefix(dbName, srcDB+"/*schema&table*/") {
						tableName := strings.TrimPrefix(dbName, srcDB+"/*schema&table*/")

						// 创建表映射（源端和目标端相同）
						mapping := TableMapping{
							SourceSchema: srcDB,
							SourceTable:  tableName,
							DestSchema:   srcDB,
							DestTable:    tableName,
						}
						tableMappings = append(tableMappings, mapping)

						vlog = fmt.Sprintf("Added non-mapping entry: %s.%s", srcDB, tableName)
						global.Wlog.Debug(vlog)
					}
				}
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				parts := strings.Split(pattern, ".")
				if len(parts) == 2 {
					srcDB := parts[0]
					srcTable := parts[1]

					// 创建表映射（源端和目标端相同）
					mapping := TableMapping{
						SourceSchema: srcDB,
						SourceTable:  srcTable,
						DestSchema:   srcDB,
						DestTable:    srcTable,
					}
					tableMappings = append(tableMappings, mapping)

					vlog = fmt.Sprintf("Added direct non-mapping entry: %s.%s", srcDB, srcTable)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

	// 如果没有找到任何映射，尝试使用默认方式处理
	if len(tableMappings) == 0 {
		vlog = fmt.Sprintf("No mappings found, using default processing")
		global.Wlog.Debug(vlog)

		// 使用模糊匹配处理表名
		schema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.table, logThreadSeq1)

		// 处理忽略表
		ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)
		for k, _ := range ignoreSchema {
			if _, ok := schema[k]; ok {
				delete(schema, k)
			}
		}

		// 构建返回列表
		for k, _ := range schema {
			parts := strings.Split(k, ".")
			if len(parts) == 2 {
				schemaName := parts[0]
				tableName := parts[1]

				// 查找源端schema名
				sourceSchema := schemaName
				destSchema := schemaName

				// 检查是否存在映射关系
				if mappedSchema, exists := stcls.tableMappings[schemaName]; exists {
					destSchema = mappedSchema
				}

				// 创建表映射
				mapping := TableMapping{
					SourceSchema: sourceSchema,
					SourceTable:  tableName,
					DestSchema:   destSchema,
					DestTable:    tableName,
				}
				tableMappings = append(tableMappings, mapping)

				vlog = fmt.Sprintf("Added default mapping: %s.%s -> %s.%s", sourceSchema, tableName, destSchema, tableName)
				global.Wlog.Debug(vlog)
			}
		}
	}

	// 将表映射信息转换为字符串列表，格式为 "sourceSchema.sourceTable:destSchema.destTable"
	for _, mapping := range tableMappings {
		// 构建包含映射信息的表名
		mappedTableName := fmt.Sprintf("%s.%s:%s.%s", mapping.SourceSchema, mapping.SourceTable, mapping.DestSchema, mapping.DestTable)
		f = append(f, mappedTableName)
		vlog = fmt.Sprintf("Final mapped table: %s", mappedTableName)
		global.Wlog.Debug(vlog)
	}

	vlog = fmt.Sprintf("(%d) Obtain schema.table %s success, num [%d].", logThreadSeq1, f, len(f))
	global.Wlog.Info(vlog)
	return f, nil
}

/*
库表的所有列信息
*/
func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var (
		a, b           []map[string]interface{}
		err            error
		vlog           string
		tableCol       = make(map[string]global.TableAllColumnInfoS)
		interfToString = func(colData []map[string]interface{}) []map[string]string {
			kel := make([]map[string]string, 0)
			for i := range colData {
				ke := make(map[string]string)
				for ii, iv := range colData[i] {
					ke[ii] = fmt.Sprintf("%v", iv)
				}
				kel = append(kel, ke)
			}
			return kel
		}
	)

	vlog = fmt.Sprintf("(%d) Start to obtain the metadata information of the source-target verification table ...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range tableList {
		// 添加调试日志，查看当前处理的表项
		vlog = fmt.Sprintf("(%d) Processing table entry: %s", logThreadSeq, i)
		global.Wlog.Debug(vlog)

		var sourceSchema, tableName, destSchema string

		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				destParts := strings.Split(parts[1], ".")

				if len(sourceParts) == 2 && len(destParts) == 2 {
					sourceSchema = sourceParts[0]
					tableName = sourceParts[1]
					destSchema = destParts[0]

					vlog = fmt.Sprintf("(%d) Parsed mapping: sourceSchema=%s, tableName=%s, destSchema=%s", logThreadSeq, sourceSchema, tableName, destSchema)
					global.Wlog.Debug(vlog)
				} else {
					vlog = fmt.Sprintf("(%d) Invalid table mapping format: %s", logThreadSeq, i)
					global.Wlog.Error(vlog)
					continue
				}
			} else {
				vlog = fmt.Sprintf("(%d) Invalid table mapping format: %s", logThreadSeq, i)
				global.Wlog.Error(vlog)
				continue
			}
		} else {
			// 传统格式：schema.table
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				tableName = parts[1]

				// 根据映射规则确定目标端schema
				destSchema = sourceSchema
				if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
					destSchema = mappedSchema
				}

				vlog = fmt.Sprintf("(%d) Traditional format: sourceSchema=%s, tableName=%s, destSchema=%s", logThreadSeq, sourceSchema, tableName, destSchema)
				global.Wlog.Debug(vlog)
			} else {
				vlog = fmt.Sprintf("(%d) Invalid table format: %s", logThreadSeq, i)
				global.Wlog.Error(vlog)
				continue
			}
		}

		vlog = fmt.Sprintf("(%d) Start to query all column information of srcDSN {%s} table %s.%s", logThreadSeq, stcls.sourceDrive, sourceSchema, tableName)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: tableName, Drive: stcls.sourceDrive}
		a, err = tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		vlog = fmt.Sprintf("(%d) All column information query of srcDSN {%s} table %s.%s is completed", logThreadSeq, stcls.sourceDrive, sourceSchema, tableName)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) Start to query all column information of dstDSN {%s} table %s.%s", logThreadSeq, stcls.destDrive, destSchema, tableName)
		global.Wlog.Debug(vlog)
		tc.Schema = destSchema
		tc.Drive = stcls.destDrive
		b, err = tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		vlog = fmt.Sprintf("(%d) All column information query of dstDSN {%s} table %s.%s is completed", logThreadSeq, stcls.destDrive, destSchema, tableName)
		global.Wlog.Debug(vlog)
		tableCol[fmt.Sprintf("%s_gtchecksum_%s", destSchema, tableName)] = global.TableAllColumnInfoS{
			SColumnInfo: interfToString(a),
			DColumnInfo: interfToString(b),
		}
		vlog = fmt.Sprintf("(%d) all column information query of source table %s.%s and target table %s.%s is completed. table column message is {source: %s, dest: %s}", logThreadSeq, sourceSchema, tableName, destSchema, tableName, interfToString(a), interfToString(b))
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableCol
}

/*
获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var (
		queryData           []map[string]interface{}
		err                 error
		vlog                string
		tableIndexColumnMap = make(map[string][]string)
	)
	vlog = fmt.Sprintf("(%d) Start to query the table index listing information and select the appropriate index ...", logThreadSeq)
	global.Wlog.Info(vlog)

	// 添加调试日志，查看传入的表列表和映射规则
	vlog = fmt.Sprintf("TableIndexColumn received dtabS: %v", dtabS)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("Current table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)

	for _, i := range dtabS {
		vlog = fmt.Sprintf("Processing table entry: %s", i)
		global.Wlog.Debug(vlog)

		// 解析表映射信息
		var sourceSchema, sourceTable, destSchema, destTable string

		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(i, ":") {
			parts := strings.Split(i, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				destParts := strings.Split(parts[1], ".")

				if len(sourceParts) == 2 && len(destParts) == 2 {
					sourceSchema = sourceParts[0]
					sourceTable = sourceParts[1]
					destSchema = destParts[0]
					destTable = destParts[1]
				}
			}
		} else {
			// 没有映射关系，源端和目标端相同
			parts := strings.Split(i, ".")
			if len(parts) == 2 {
				sourceSchema = parts[0]
				sourceTable = parts[1]
				destSchema = sourceSchema
				destTable = sourceTable
			}
		}

		// 设置当前表名
		stcls.table = sourceTable

		vlog = fmt.Sprintf("Parsed mapping: sourceSchema=%s, sourceTable=%s, destSchema=%s, destTable=%s",
			sourceSchema, sourceTable, destSchema, destTable)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start querying the index list information of source table %s.%s and target table %s.%s.",
			logThreadSeq, sourceSchema, sourceTable, destSchema, destTable)
		global.Wlog.Debug(vlog)

		// 查询源端索引信息
		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: sourceTable, Drivce: stcls.sourceDrive}
		queryData, err = idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error querying source table index: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			continue
		}
		tc := dbExec.TableColumnNameStruct{Schema: sourceSchema, Table: sourceTable, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
		indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) Source table %s.%s index list information query completed. index list message is {%v}",
			logThreadSeq, sourceSchema, sourceTable, indexType)
		global.Wlog.Debug(vlog)

		// 查询目标端索引信息
		idxcDest := dbExec.IndexColumnStruct{Schema: destSchema, Table: destTable, Drivce: stcls.destDrive}
		queryDataDest, err := idxcDest.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Error querying destination table index: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			continue
		}

		tcDest := dbExec.TableColumnNameStruct{Schema: destSchema, Table: destTable, Drive: stcls.destDrive, Db: stcls.destDB}
		indexTypeDest := tcDest.Query().TableIndexChoice(queryDataDest, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) Target table %s.%s index list information query completed. index list message is {%v}",
			logThreadSeq, destSchema, destTable, indexTypeDest)
		global.Wlog.Debug(vlog)

		// 使用源端schema和表名作为key，因为后续处理中会根据源端表进行数据校验
		// 同时在key中保存目标端schema和表名，以便后续处理
		if len(indexType) == 0 { //针对于表没有索引的，进行处理
			key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*mapping*/%s/*mappingTable*/%s",
				sourceSchema, sourceTable, destSchema, destTable)
			tableIndexColumnMap[key] = []string{}

			// 构建显示名称，包含映射关系
			displayTableName := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)

			vlog = fmt.Sprintf("(%d) The source table %s has no index.", logThreadSeq, displayTableName)
			global.Wlog.Warn(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Start to perform index selection on source table %s.%s according to the algorithm",
				logThreadSeq, sourceSchema, sourceTable)
			global.Wlog.Debug(vlog)
			ab, aa := stcls.tableIndexAlgorithm(indexType)
			key := fmt.Sprintf("%s/*gtchecksumSchemaTable*/%s/*indexColumnType*/%s/*mapping*/%s/*mappingTable*/%s",
				sourceSchema, sourceTable, ab, destSchema, destTable)
			tableIndexColumnMap[key] = aa

			// 构建显示名称，包含映射关系
			displayTableName := fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)

			vlog = fmt.Sprintf("(%d) The index selection of source table %s is completed, and the selected index information is { keyName:%s keyColumn: %s}",
				logThreadSeq, displayTableName, ab, aa)
			global.Wlog.Debug(vlog)
		}
	}
	zlog := fmt.Sprintf("(%d) Table index listing information and appropriate index completion", logThreadSeq)
	global.Wlog.Info(zlog)
	return tableIndexColumnMap
}

// 解析表映射规则
func (stcls *schemaTable) parseTableMappings(Ftable string) {
	stcls.tableMappings = make(map[string]string)

	vlog := fmt.Sprintf("Parsing table mappings for pattern: %s", Ftable)
	global.Wlog.Debug(vlog)

	// 解析映射规则，如 db1.*:db2.*
	for _, pattern := range strings.Split(Ftable, ",") {
		vlog = fmt.Sprintf("Processing pattern: %s", pattern)
		global.Wlog.Debug(vlog)

		if strings.Contains(pattern, ":") {
			mapping := strings.SplitN(pattern, ":", 2)
			if len(mapping) == 2 {
				srcPattern := mapping[0]
				dstPattern := mapping[1]

				vlog = fmt.Sprintf("Found mapping: %s -> %s", srcPattern, dstPattern)
				global.Wlog.Debug(vlog)

				// 处理 db1.*:db2.* 格式
				if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
					srcDB := strings.TrimSuffix(srcPattern, ".*")
					dstDB := strings.TrimSuffix(dstPattern, ".*")
					stcls.tableMappings[srcDB] = dstDB
					vlog = fmt.Sprintf("Mapped (.* format): %s -> %s", srcDB, dstDB)
					global.Wlog.Debug(vlog)
				} else if strings.HasSuffix(srcPattern, "*") && strings.HasSuffix(dstPattern, "*") {
					// 处理 db1*:db2* 格式 (针对用户输入的"db1.*:db2.*"但实际被解析为"db1*:db2*"的情况)
					srcDB := strings.TrimSuffix(srcPattern, "*")
					dstDB := strings.TrimSuffix(dstPattern, "*")
					stcls.tableMappings[srcDB] = dstDB
					vlog = fmt.Sprintf("Mapped (* format): %s -> %s", srcDB, dstDB)
					global.Wlog.Debug(vlog)
				} else {
					// 处理其他格式的映射，如 db1.t1:db2.t2
					srcParts := strings.Split(srcPattern, ".")
					dstParts := strings.Split(dstPattern, ".")

					if len(srcParts) > 0 && len(dstParts) > 0 {
						srcDB := srcParts[0]
						dstDB := dstParts[0]
						stcls.tableMappings[srcDB] = dstDB
						vlog = fmt.Sprintf("Mapped (direct format): %s -> %s", srcDB, dstDB)
						global.Wlog.Debug(vlog)
					}
				}
			}
		} else {
			// 处理非映射模式，如 db1.*
			if strings.HasSuffix(pattern, ".*") {
				srcDB := strings.TrimSuffix(pattern, ".*")
				stcls.tableMappings[srcDB] = srcDB // 没有映射时，源和目标相同
				vlog = fmt.Sprintf("Non-mapping pattern (.* format): %s", srcDB)
				global.Wlog.Debug(vlog)
			} else if strings.HasSuffix(pattern, "*") {
				srcDB := strings.TrimSuffix(pattern, "*")
				stcls.tableMappings[srcDB] = srcDB // 没有映射时，源和目标相同
				vlog = fmt.Sprintf("Non-mapping pattern (* format): %s", srcDB)
				global.Wlog.Debug(vlog)
			} else if strings.Contains(pattern, ".") {
				// 处理 db1.t1 格式
				srcParts := strings.Split(pattern, ".")
				if len(srcParts) > 0 {
					srcDB := srcParts[0]
					stcls.tableMappings[srcDB] = srcDB
					vlog = fmt.Sprintf("Non-mapping pattern (direct format): %s", srcDB)
					global.Wlog.Debug(vlog)
				}
			}
		}
	}

	vlog = fmt.Sprintf("Final table mappings: %v", stcls.tableMappings)
	global.Wlog.Debug(vlog)
}

/*
校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog string
		tmpM = make(map[string]int)
		z    = make(map[string]int)
		c, d []string
		pods = Pod{
			Datafix:     "no",
			CheckObject: "trigger",
		}
		sourceTrigger, destTrigger map[string]string
		err                        error
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		i = strings.Split(i, ".")[0]
		if stcls.caseSensitiveObjectName == "yes" {
			i = strings.ToUpper(i)
			z[i]++
		}
		// yejr存疑，这段代码是否多余，或者和上一段代码是否冲突？
		if stcls.caseSensitiveObjectName == "no" {
			z[i]++
		}
	}
	//校验触发器
	for i, _ := range z {
		pods.Schema = stcls.schema
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} data databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: i, Drive: stcls.sourceDrive}
		if sourceTrigger, err = tc.Query().Trigger(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) dstDSN {%s} data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceTrigger)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destTrigger, err = tc.Query().Trigger(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) dstDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destTrigger)
		global.Wlog.Debug(vlog)
		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, stcls.schema)
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
			if sourceTrigger[k] != destTrigger[k] {
				pods.DIFFS = "yes"
				d = append(d, k)
			} else {
				pods.DIFFS = "no"
				c = append(c, k)
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
校验存储过程
*/
func (stcls *schemaTable) Proc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog      string
		c, d      []string
		schemaMap = make(map[string]int)
		pods      = Pod{
			Datafix:     "no",
			CheckObject: "proc",
		}
		sourceProc, destProc map[string]string
		err                  error
		tmpM                 = make(map[string]int)
	)
	vlog = fmt.Sprintf("(%d) Start init check source and target DB Stored Procedure. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}

	for schema, _ := range schemaMap {
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		if sourceProc, err = tc.Query().Proc(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) srcDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceProc)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s Stored Procedure data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destProc, err = tc.Query().Proc(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) dstDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destProc)
		global.Wlog.Debug(vlog)
		if len(sourceProc) == 0 && len(destProc) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Warn(vlog)
			continue
		}

		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k]++
		}
		for k, _ := range destProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Stored Procedure is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		pods.Schema = schema
		for k, v := range tmpM {
			if stcls.sourceDrive != stcls.destDrive {
				if v == 2 {
					pods.ProcName = k
					pods.DIFFS = "no"
					c = append(c, k)
				} else {
					pods.ProcName = k
					pods.DIFFS = "yes"
					d = append(d, k)
				}
			} else {
				if sourceProc[k] != destProc[k] {
					pods.ProcName = k
					pods.DIFFS = "yes"
					d = append(d, k)
				} else {
					pods.ProcName = k
					pods.DIFFS = "no"
					c = append(c, k)
				}
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Procedure. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Stored Procedure data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Procedure data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
校验函数
*/
func (stcls *schemaTable) Func(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                 string
		sourceFunc, destFunc map[string]string
		tmpM                 = make(map[string]int)
		schemaMap            = make(map[string]int)
		pods                 = Pod{
			Datafix:     "no",
			CheckObject: "func",
		}
		err  error
		c, d []string
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Stored Function. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}

	for schema, _ := range schemaMap {
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} databases %s Stored Function. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		if sourceFunc, err = tc.Query().Func(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) srcDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceFunc)
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s Stored Function data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destFunc, err = tc.Query().Func(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) dstDSN {%s} databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destFunc)
		global.Wlog.Debug(vlog)

		if len(sourceFunc) == 0 && len(destFunc) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Function. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceFunc {
			tmpM[k]++
		}
		for k, _ := range destFunc {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Stored Function is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		pods.Schema = schema
		for k, v := range tmpM {
			var sv, dv string
			if stcls.sourceDrive != stcls.destDrive { //异构,只校验函数名
				if v == 2 {
					pods.FuncName = k
					pods.DIFFS = "no"
					c = append(c, k)
				} else {
					pods.FuncName = k
					pods.DIFFS = "yes"
					d = append(d, k)
				}
			} else { //相同架构，校验函数结构体
				sv, dv = sourceFunc[k], destFunc[k]
				if sv != dv {
					pods.FuncName = k
					pods.DIFFS = "yes"
					d = append(d, k)
				} else {
					pods.FuncName = k
					pods.DIFFS = "no"
					c = append(c, k)
				}
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Function. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Stored Function data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Function data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

func (stcls *schemaTable) Foreign(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                       string
		sourceForeign, destForeign map[string]string
		tmpM                       = make(map[string]int)
		err                        error
		pods                       = Pod{
			Datafix:     "no",
			CheckObject: "Foreign",
		}
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourceForeign, err = tc.Query().Foreign(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourceForeign)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destForeign, err = tc.Query().Foreign(stcls.destDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) dstDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destForeign)
		global.Wlog.Debug(vlog)
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Foreign Name. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceForeign {
			tmpM[k]++
		}
		for k, _ := range destForeign {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			if sourceForeign[k] != destForeign[k] {
				pods.DIFFS = "yes"
				d = append(d, k)
			} else {
				pods.DIFFS = "no"
				c = append(c, k)
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

// 校验分区
func (stcls *schemaTable) Partitions(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                             string
		err                              error
		c, d                             []string
		sourcePartitions, destPartitions map[string]string
		pods                             = Pod{
			Datafix:     "no",
			CheckObject: "Partitions",
		}
		tmpM = make(map[string]int)
	)
	vlog = fmt.Sprintf("(%d) Start init check source and target DB partition table. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing srcDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourcePartitions, err = tc.Query().Partitions(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) srcDSN {%s} table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourcePartitions)
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dstDSN {%s} table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destPartitions, err = tc.Query().Partitions(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destPartitions)
		global.Wlog.Debug(vlog)

		pods.Schema = stcls.schema
		pods.Table = stcls.table
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = make(map[string]int)
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Partitions Column. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourcePartitions {
			tmpM[k]++
		}
		for k, _ := range destPartitions {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the partitions table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			if strings.Join(strings.Fields(sourcePartitions[k]), "") != strings.Join(strings.Fields(destPartitions[k]), "") {
				pods.DIFFS = "yes"
				d = append(d, k)
			} else {
				c = append(c, k)
				pods.DIFFS = "no"
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s partitions data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

func (stcls *schemaTable) Index(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	var (
		vlog  string
		sqlS  []string
		aa    = &CheckSumTypeStruct{}
		event string
		// 辅助函数：提取列名和序号
		extractColumnInfo = func(columnStr string) (string, int) {
			// 从格式 "columnName/*seq*/1/*type*/columnType" 中提取信息
			parts := strings.Split(columnStr, "/*seq*/")
			colName := strings.TrimSpace(parts[0])
			seqStr := strings.Split(parts[1], "/*type*/")[0]
			seq, _ := strconv.Atoi(seqStr)
			return colName, seq
		}

		// 辅助函数：按序号排序列并返回纯列名
		sortColumns = func(columns []string) []string {
			type ColumnInfo struct {
				name string
				seq  int
			}
			var columnInfos []ColumnInfo

			// 提取列信息
			for _, col := range columns {
				name, seq := extractColumnInfo(col)
				columnInfos = append(columnInfos, ColumnInfo{name: name, seq: seq})
			}

			// 按序号排序
			sort.Slice(columnInfos, func(i, j int) bool {
				return columnInfos[i].seq < columnInfos[j].seq
			})

			// 返回排序后的纯列名
			var result []string
			for _, col := range columnInfos {
				result = append(result, fmt.Sprintf("%s", col.name))
			}
			return result
		}

		indexGenerate = func(smu, dmu map[string][]string, a *CheckSumTypeStruct, indexType string) []string {
			var cc, c, d []string
			dbf := dbExec.DataAbnormalFixStruct{
				Schema:       stcls.schema,
				Table:        stcls.table,
				SourceDevice: stcls.sourceDrive,
				DestDevice:   stcls.destDrive,
				IndexType:    indexType,
				DatafixType:  stcls.datefix,
			}

			// 首先比较索引名称
			for k := range smu {
				c = append(c, k)
			}
			for k := range dmu {
				d = append(d, k)
			}

			// 如果索引名称不同，生成修复SQL
			if a.CheckMd5(strings.Join(c, ",")) != a.CheckMd5(strings.Join(d, ",")) {
				e, f := a.Arrcmp(c, d)
				// 对于新增的索引，需要处理列顺序
				newIndexMap := make(map[string][]string)
				for _, idx := range e {
					if cols, ok := smu[idx]; ok {
						// 对列进行排序并去除序号信息
						newIndexMap[idx] = sortColumns(cols)
					}
				}
				cc = dbf.DataAbnormalFix().FixAlterIndexSqlExec(e, f, newIndexMap, stcls.sourceDrive, logThreadSeq)
			} else {
				// 即使索引名称相同，也要比较索引的具体内容
				for k, sColumns := range smu {
					if dColumns, exists := dmu[k]; exists {
						// 比较同名索引的列及其顺序（包含序号信息的比较）
						if a.CheckMd5(strings.Join(sColumns, ",")) != a.CheckMd5(strings.Join(dColumns, ",")) {
							// 1. 先生成删除旧索引的SQL
							if indexType == "pri" {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;", stcls.schema, stcls.table))
							} else {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`;", stcls.schema, stcls.table, k))
							}

							// 2. 获取排序后的纯列名
							sortedColumns := sortColumns(sColumns)

							// 3. 生成创建索引的SQL
							if indexType == "pri" {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY(%s);",
									stcls.schema, stcls.table, strings.Join(sortedColumns, ",")))
							} else if indexType == "uni" {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX `%s`(%s);",
									stcls.schema, stcls.table, k, strings.Join(sortedColumns, ",")))
							} else {
								cc = append(cc, fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX `%s`(%s);",
									stcls.schema, stcls.table, k, strings.Join(sortedColumns, ",")))
							}
						}
					}
				}
			}
			return cc
		}
	)

	fmt.Println("gt-checksum is opening indexes")
	event = fmt.Sprintf("[%s]", "check_table_index")
	//校验索引
	vlog = fmt.Sprintf("(%d) %s start init check source and target DB index Column. to check it...", logThreadSeq, event)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		// 从表列表中提取源端schema和表名
		sourceSchema := strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]

		// 根据映射规则确定目标端schema
		destSchema := sourceSchema
		if mappedSchema, exists := stcls.tableMappings[sourceSchema]; exists {
			destSchema = mappedSchema
		}

		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: stcls.table, Drivce: stcls.sourceDrive}
		vlog = fmt.Sprintf("(%d) %s Start processing srcDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.sourceDrive, sourceSchema, stcls.table)
		global.Wlog.Debug(vlog)
		squeryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of srcDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.sourceDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		spri, suni, smul := idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) %s The index column data of the source %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.sourceDrive,
			sourceSchema,
			stcls.table,
			spri,
			suni,
			smul)
		global.Wlog.Debug(vlog)

		idxc.Schema = destSchema
		idxc.Drivce = stcls.destDrive
		vlog = fmt.Sprintf("(%d) %s Start processing dstDSN {%s} table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.destDrive, destSchema, stcls.table)
		global.Wlog.Debug(vlog)
		dqueryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of dstDSN {%s} database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.destDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		dpri, duni, dmul := idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) %s The index column data of the dest %s database table %s.%s is {primary:%v,unique key:%v,index key:%v}",
			logThreadSeq,
			event,
			stcls.destDrive,
			destSchema,
			stcls.table,
			dpri,
			duni,
			dmul)
		global.Wlog.Debug(vlog)
		global.Wlog.Debug(vlog)

		var pods = Pod{
			Datafix:     stcls.datefix,
			CheckObject: "Index",

			DIFFS:  "no",
			Schema: stcls.schema,
			Table:  stcls.table,
		}
		//先比较主键索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the primary key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(spri, dpri, aa, "pri")...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the primary key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		//再比较唯一索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(suni, duni, aa, "uni")...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Info(vlog)
		//后比较普通索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the no-unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(smul, dmul, aa, "mul")...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the no-unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		// 应用并清空 sqlS
		if len(sqlS) > 0 {
			pods.DIFFS = "yes"

			err := ApplyDataFix(sqlS, stcls.datefix, stcls.sfile, stcls.destDrive, stcls.djdbc, logThreadSeq)
			if err != nil {
				return err
			}
			sqlS = []string{} // 清空 sqlS 以便下一个表使用
		}

		measuredDataPods = append(measuredDataPods, pods)
		vlog = fmt.Sprintf("(%d) %s The source target segment table %s.%s index column data verification is completed", logThreadSeq, event, stcls.schema, stcls.table)
		global.Wlog.Info(vlog)
	}
	fmt.Println("gt-checksum report: indexes verification completed")
	return nil
}

/*
校验表结构是否正确
*/
func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	//校验列名
	var (
		vlog  string
		event string
	)
	event = fmt.Sprintf("[check_table_columns]")
	fmt.Println("gt-checksum is checking table structure")
	vlog = fmt.Sprintf("(%d) %s checking table structure of %v(num[%d]) from srcDSN and dstDSN", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)
	normal, abnormal, err := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) %s Table structure and column checksum of srcDB and dstDB completed. The consistent result is {%s}(num [%d]), and the inconsistent result is {%s}(num [%d])", logThreadSeq, event, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Debug(vlog)
	//输出校验结果信息
	var pods = Pod{
		Datafix:     stcls.datefix,
		CheckObject: "Struct",
	}
	for _, i := range normal {
		aa := strings.Split(i, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.DIFFS = "no"
		measuredDataPods = append(measuredDataPods, pods)
	}
	for _, i := range abnormal {
		aa := strings.Split(i, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.DIFFS = "yes"
		measuredDataPods = append(measuredDataPods, pods)
	}
	fmt.Println("gt-checksum report: Table structure checksum completed")
	vlog = fmt.Sprintf("(%d) %s check source and target DB table struct complete", logThreadSeq, event)
	global.Wlog.Info(vlog)
	return nil
}

/*
用于测试db链接串是否正确，是否可以连接
*/
func dbOpenTest(drive, jdbc string) *sql.DB {
	p := dbExec.DBexec()
	p.JDBC = jdbc
	p.DBDevice = drive
	db, err := p.OpenDB()
	if err != nil {
		fmt.Println("")
		os.Exit(1)
	}
	err1 := db.Ping()
	if err1 != nil {
		os.Exit(1)
	}
	return db
}

/*
库表的初始化
*/
func SchemaTableInit(m *inputArg.ConfigParameter) *schemaTable {
	sdb := dbOpenTest(m.SecondaryL.DsnsV.SrcDrive, m.SecondaryL.DsnsV.SrcJdbc)
	ddb := dbOpenTest(m.SecondaryL.DsnsV.DestDrive, m.SecondaryL.DsnsV.DestJdbc)
	return &schemaTable{
		ignoreTable:             m.SecondaryL.SchemaV.IgnoreTables,
		table:                   m.SecondaryL.SchemaV.Tables,
		sourceDrive:             m.SecondaryL.DsnsV.SrcDrive,
		destDrive:               m.SecondaryL.DsnsV.DestDrive,
		sourceDB:                sdb,
		destDB:                  ddb,
		caseSensitiveObjectName: m.SecondaryL.SchemaV.CaseSensitiveObjectName,
		datefix:                 m.SecondaryL.RepairV.Datafix,
		sfile:                   m.SecondaryL.RepairV.FixFileFINE,
		djdbc:                   m.SecondaryL.DsnsV.DestJdbc,
		structRul:               m.SecondaryL.StructV,
		checkRules:              m.SecondaryL.RulesV,
	}
}

/*
获取源数据库连接
*/
func (stcls *schemaTable) GetSourceDB() *sql.DB {
	return stcls.sourceDB
}

/*
获取目标数据库连接
*/
func (stcls *schemaTable) GetDestDB() *sql.DB {
	return stcls.destDB
}
