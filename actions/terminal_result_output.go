package actions

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"
)

// 进度条
type Bar struct {
	percent         int64  //百分比
	cur             int64  //当前进度位置
	total           int64  //总进度
	rate            string //进度条
	graph           string //显示符号
	taskUnit        string //task单位
	lastUpdate      int64  //上次更新时间戳（毫秒）
	lastForceUpdate int64  //上次强制更新时间戳（毫秒）
	updateInterval  int64  //更新间隔（毫秒）
	startTime       int64  //开始时间戳（毫秒）
}

type Pod struct {
	Schema, Table, IndexColumn, Rows, DIFFS, CheckObject, Datafix, FuncName, Definer, ProcName, TriggerName, MappingInfo string
}

func isStructOutputPod(pod Pod) bool {
	switch strings.ToLower(strings.TrimSpace(pod.CheckObject)) {
	case "struct", "sequence":
		return true
	default:
		return false
	}
}

func dataResultRows(pod Pod) string {
	if pod.DIFFS == "DDL-yes" {
		return ""
	}
	return pod.Rows
}

func skippedTableDiffs(skipped global.SkippedTable) string {
	diffs := strings.TrimSpace(skipped.Diffs)
	if diffs != "" {
		return diffs
	}
	return global.SkipDiffsYes
}

// 获取表的映射信息
func getTableMappingInfo(schema, table string) string {
	// 遍历全局映射关系列表
	for _, mapping := range TableMappingRelations {
		parts := strings.Split(mapping, ":")
		if len(parts) == 2 {
			sourceParts := strings.Split(parts[0], ".")
			destParts := strings.Split(parts[1], ".")

			if len(sourceParts) == 2 && len(destParts) == 2 {
				sourceSchema := sourceParts[0]
				sourceTable := sourceParts[1]
				destSchema := destParts[0]
				destTable := destParts[1]

				// 如果当前表是源表或目标表，返回映射信息
				if (schema == sourceSchema && table == sourceTable) ||
					(schema == destSchema && table == destTable) {
					// 返回格式为 "Schema: sourceSchema:destSchema"
					return fmt.Sprintf("Schema: %s:%s", sourceSchema, destSchema)
				}
			}
		}
	}
	return ""
}

// 检查是否存在映射关系
func hasMappingRelations() bool {
	return len(TableMappingRelations) > 0
}

// 获取schema级别的映射关系
func getSchemaMappings() map[string]string {
	schemaMap := make(map[string]string)

	for _, mapping := range TableMappingRelations {
		parts := strings.Split(mapping, ":")
		if len(parts) == 2 {
			sourceParts := strings.Split(parts[0], ".")
			destParts := strings.Split(parts[1], ".")

			if len(sourceParts) >= 1 && len(destParts) >= 1 {
				sourceSchema := sourceParts[0]
				destSchema := destParts[0]

				// 添加到映射表中
				schemaMap[sourceSchema] = destSchema
			}
		}
	}

	// 添加默认的映射关系，处理db1.*:db2.*格式
	// 从配置文件中解析映射关系
	globalConfig := inputArg.GetGlobalConfig()
	if globalConfig != nil && globalConfig.SecondaryL.SchemaV.Tables != "" {
		for _, pattern := range strings.Split(globalConfig.SecondaryL.SchemaV.Tables, ",") {
			if strings.Contains(pattern, ":") {
				mapping := strings.SplitN(pattern, ":", 2)
				if len(mapping) == 2 {
					srcPattern := mapping[0]
					dstPattern := mapping[1]

					// 处理包含通配符的格式，如 db1.*:db2.* 或 db1.tt%:db2.tt%
					if strings.Contains(srcPattern, ".") && strings.Contains(dstPattern, ".") {
						srcParts := strings.Split(srcPattern, ".")
						dstParts := strings.Split(dstPattern, ".")

						if len(srcParts) >= 1 && len(dstParts) >= 1 {
							srcDB := srcParts[0]
							dstDB := dstParts[0]

							// 添加到映射表中
							schemaMap[srcDB] = dstDB
						}
					}
				}
			}
		}
	}

	return schemaMap
}

var measuredDataPods []Pod
var differencesSchemaTable = make(map[string]string)

func CheckResultOut(m *inputArg.ConfigParameter) {
	// 在函数开始时，将跳过的表添加到measuredDataPods中
	skippedTables := global.GetSkippedTables()
	// 创建一个映射来跟踪已处理的表，避免重复
	processedTables := make(map[string]bool)

	// 先处理已有的measuredDataPods，记录已存在的表
	for _, pod := range measuredDataPods {
		processedTables[pod.Schema+"."+pod.Table] = true
	}

	// 创建一个映射来跟踪跳过的表，避免重复添加
	skippedTableMap := make(map[string]bool)
	for _, skipped := range skippedTables {
		skippedTableMap[skipped.Schema+"."+skipped.Table] = true
	}

	// 创建一个映射来跟踪要添加的跳过表，避免重复添加
	toAddSkippedTables := make([]global.SkippedTable, 0)

	// 过滤跳过的表，只添加那些不是映射关系中的目标表的表
	for _, skipped := range skippedTables {
		if skipped.CheckObject == "data" || skipped.CheckObject == "struct" {
			// 检查是否是映射关系中的目标表
			isTargetTable := false

			// 检查是否存在映射关系
			if m.SecondaryL.SchemaV.Tables != "" {
				for _, pattern := range strings.Split(m.SecondaryL.SchemaV.Tables, ",") {
					if strings.Contains(pattern, ":") {
						mapping := strings.SplitN(pattern, ":", 2)
						if len(mapping) == 2 {
							srcPattern := mapping[0]
							dstPattern := mapping[1]

							// 检查是否是映射关系
							// 提取源和目标的schema
							var srcSchema, dstSchema string
							if strings.Contains(srcPattern, ".") {
								srcParts := strings.Split(srcPattern, ".")
								if len(srcParts) > 0 {
									srcSchema = srcParts[0]
								}
							}
							if strings.Contains(dstPattern, ".") {
								dstParts := strings.Split(dstPattern, ".")
								if len(dstParts) > 0 {
									dstSchema = dstParts[0]
								}
							}

							// 检查是否是目标表
							if skipped.Schema == dstSchema {
								// 检查是否存在对应的源表
								if sourceTableKey := srcSchema + "." + skipped.Table; processedTables[sourceTableKey] || skippedTableMap[sourceTableKey] {
									// 已经处理过对应的源表，跳过目标表
									isTargetTable = true
									break
								}
							}
						}
					}
				}
			}

			// 只有当该表不是映射关系中的目标表时，才添加到结果中
			if !isTargetTable {
				toAddSkippedTables = append(toAddSkippedTables, skipped)
			}
		}
	}

	// 添加过滤后的跳过表到measuredDataPods中
	for _, skipped := range toAddSkippedTables {
		// 构建表的唯一标识
		tableKey := skipped.Schema + "." + skipped.Table

		// 检查是否已经处理过该表
		if !processedTables[tableKey] {
			pod := Pod{
				Schema:      skipped.Schema,
				Table:       skipped.Table,
				CheckObject: skipped.CheckObject,
				DIFFS:       skippedTableDiffs(skipped),
				Datafix:     m.SecondaryL.RepairV.Datafix,
			}
			measuredDataPods = append(measuredDataPods, pod)
			processedTables[tableKey] = true
		}
	}

	// Build a filtered pod slice for terminal display based on terminalResultMode.
	// CSV export always receives the full result set via BuildResultRecords (see gt-checksum.go).
	terminalPods := measuredDataPods
	if m.SecondaryL.RulesV.TerminalResultMode == "abnormal" {
		terminalPods = make([]Pod, 0, len(measuredDataPods))
		for _, p := range measuredDataPods {
			diffs := p.DIFFS
			// For data-mode pods, differencesSchemaTable may promote DIFFS to "yes".
			if strings.ToLower(p.CheckObject) == "data" {
				for k := range differencesSchemaTable {
					if k == "" {
						continue
					}
					parts := strings.SplitN(k, "gtchecksum_gtchecksum", 2)
					if len(parts) == 2 && p.Schema == parts[0] && p.Table == parts[1] {
						diffs = "yes"
						break
					}
				}
			}
			if diffs == "yes" || diffs == "DDL-yes" || diffs == "warn-only" {
				terminalPods = append(terminalPods, p)
			}
		}
	}

	table := uitable.New()
	table.MaxColWidth = 200
	table.RightAlign(20)

	// 检查是否有映射关系
	hasMappings := hasMappingRelations()

	switch m.SecondaryL.RulesV.CheckObject {
	case "routine":
		if hasMappings {
			table.AddRow("Schema", "RoutineName", "CheckObject", "DIFFS", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "RoutineName", "CheckObject", "DIFFS", "Datafix")
		}

		for _, pod := range terminalPods {
			// 仅输出存储过程/存储函数的记录
			lc := strings.ToLower(pod.CheckObject)
			if lc != "procedure" && lc != "function" {
				continue
			}

			schemaName := pod.Schema
			// 统一读取名称：Procedure 用 ProcName，Function 用 FuncName；若为空则互相回退
			routineName := pod.ProcName
			if lc == "function" {
				routineName = pod.FuncName
			}
			if routineName == "" {
				if lc == "function" {
					routineName = pod.ProcName
				} else {
					routineName = pod.FuncName
				}
			}

			// 特殊处理映射格式 "db1.*:db2.*"
			if strings.Contains(routineName, ".*:") {
				parts := strings.Split(routineName, ".*:")
				if len(parts) == 2 {
					// 这是映射规则，db2 作为 schema
					schemaName = parts[1]
				}
			} else if strings.Contains(routineName, ":") {
				// 普通冒号分隔处理
				parts := strings.Split(routineName, ":")
				if len(parts) == 2 {
					if schemaName == "" {
						schemaName = parts[0]
						routineName = parts[1]
					} else {
						routineName = parts[0]
					}
				}
			}

			// 处理 "schema.name" 点号分隔
			if schemaName == "" && strings.Contains(routineName, ".") {
				parts := strings.Split(routineName, ".")
				if len(parts) == 2 {
					schemaName = parts[0]
					routineName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			// CheckObject 列按对象类型显示：Procedure / Function
			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(routineName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(routineName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)

	case "struct":
		if hasMappings {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix")
		}

		for _, pod := range terminalPods {
			// Keep structure-level rows and schema-object warnings in the same output.
			if !isStructOutputPod(pod) {
				continue
			}
			// 检查Table字段是否包含schema.table或schema:table格式
			tableName := pod.Table
			schemaName := pod.Schema

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(tableName, ".*:") {
				parts := strings.Split(tableName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，t1作为table
					// 假设pod.Schema存储的是表名(如t1)，pod.Table存储的是"db1.*:db2.*"
					schemaName = parts[1] // db2
					// tableName保持不变，因为它可能是t1
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(tableName, ":") {
				parts := strings.Split(tableName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						tableName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个表名映射，只保留冒号前的部分作为表名
						tableName = parts[0] // 只保留冒号前的部分作为表名
					}
				}
			}

			// 处理点号分隔的情况 (schema.table)
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				if len(parts) == 2 && schemaName == "" {
					schemaName = parts[0]
					tableName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "index":
		if hasMappings {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix")
		}

		for _, pod := range terminalPods {
			// 检查Table字段是否包含schema.table或schema:table格式
			tableName := pod.Table
			schemaName := pod.Schema

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(tableName, ".*:") {
				parts := strings.Split(tableName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，t1作为table
					// 假设pod.Schema存储的是表名(如t1)，pod.Table存储的是"db1.*:db2.*"
					schemaName = parts[1] // db2
					// tableName保持不变，因为它可能是t1
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(tableName, ":") {
				parts := strings.Split(tableName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						tableName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个表名映射，只保留冒号前的部分作为表名
						tableName = parts[0] // 只保留冒号前的部分作为表名
					}
				}
			}

			// 处理点号分隔的情况 (schema.table)
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				if len(parts) == 2 && schemaName == "" {
					schemaName = parts[0]
					tableName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "partitions":
		if hasMappings {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix")
		}

		for _, pod := range terminalPods {
			// 检查Table字段是否包含schema.table或schema:table格式
			tableName := pod.Table
			schemaName := pod.Schema

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(tableName, ".*:") {
				parts := strings.Split(tableName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，t1作为table
					// 假设pod.Schema存储的是表名(如t1)，pod.Table存储的是"db1.*:db2.*"
					schemaName = parts[1] // db2
					// tableName保持不变，因为它可能是t1
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(tableName, ":") {
				parts := strings.Split(tableName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						tableName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个表名映射，只保留冒号前的部分作为表名
						tableName = parts[0] // 只保留冒号前的部分作为表名
					}
				}
			}

			// 处理点号分隔的情况 (schema.table)
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				if len(parts) == 2 && schemaName == "" {
					schemaName = parts[0]
					tableName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "foreign":
		if hasMappings {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix")
		}

		for _, pod := range terminalPods {
			// 检查Table字段是否包含schema.table或schema:table格式
			tableName := pod.Table
			schemaName := pod.Schema

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(tableName, ".*:") {
				parts := strings.Split(tableName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，t1作为table
					// 假设pod.Schema存储的是表名(如t1)，pod.Table存储的是"db1.*:db2.*"
					schemaName = parts[1] // db2
					// tableName保持不变，因为它可能是t1
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(tableName, ":") {
				parts := strings.Split(tableName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						tableName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个表名映射，只保留冒号前的部分作为表名
						tableName = parts[0] // 只保留冒号前的部分作为表名
					}
				}
			}

			// 处理点号分隔的情况 (schema.table)
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				if len(parts) == 2 && schemaName == "" {
					schemaName = parts[0]
					tableName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(tableName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "func":
		if hasMappings {
			table.AddRow("Schema", "funcName", "CheckObject", "DIFFS", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "funcName", "CheckObject", "DIFFS", "Datafix")
		}

		for _, pod := range terminalPods {
			// 检查Schema和FuncName字段
			schemaName := pod.Schema
			funcName := pod.FuncName

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(funcName, ".*:") {
				parts := strings.Split(funcName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，func作为函数名
					schemaName = parts[1] // db2
					// funcName保持不变，因为它可能是函数名
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(funcName, ":") {
				parts := strings.Split(funcName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						funcName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个函数名映射，只保留冒号前的部分作为函数名
						funcName = parts[0] // 只保留冒号前的部分作为函数名
					}
				}
			}

			// 处理点号分隔的情况 (schema.func)
			if schemaName == "" && strings.Contains(funcName, ".") {
				parts := strings.Split(funcName, ".")
				if len(parts) == 2 {
					schemaName = parts[0]
					funcName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(funcName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(funcName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "proc":
		if hasMappings {
			table.AddRow("Schema", "ProcName", "CheckObject", "DIFFS", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "ProcName", "CheckObject", "DIFFS", "Datafix")
		}

		for _, pod := range terminalPods {
			// 检查Schema和ProcName字段
			schemaName := pod.Schema
			procName := pod.ProcName

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(procName, ".*:") {
				parts := strings.Split(procName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，proc作为存储过程名
					schemaName = parts[1] // db2
					// procName保持不变，因为它可能是存储过程名
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(procName, ":") {
				parts := strings.Split(procName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						procName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个存储过程名映射，只保留冒号前的部分作为存储过程名
						procName = parts[0] // 只保留冒号前的部分作为存储过程名
					}
				}
			}

			// 处理点号分隔的情况 (schema.proc)
			if schemaName == "" && strings.Contains(procName, ".") {
				parts := strings.Split(procName, ".")
				if len(parts) == 2 {
					schemaName = parts[0]
					procName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.WhiteString(procName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.WhiteString(procName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "trigger":
		if hasMappings {
			table.AddRow("Schema", "TriggerName", "CheckObject", "Diffs", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "TriggerName", "CheckObject", "Diffs", "Datafix")
		}

		for _, pod := range terminalPods {
			// 检查Schema和TriggerName字段
			schemaName := pod.Schema
			triggerName := pod.TriggerName

			// 特殊处理"db1.*:db2.*"这种映射格式
			if strings.Contains(triggerName, ".*:") {
				parts := strings.Split(triggerName, ".*:")
				if len(parts) == 2 {
					// 这里是映射规则，需要将db2作为schema，trigger作为触发器名
					schemaName = parts[1] // db2
					// triggerName保持不变，因为它可能是触发器名
				}
				// 如果不包含".*:"但包含":"，则按照普通的冒号分隔处理
			} else if strings.Contains(triggerName, ":") {
				parts := strings.Split(triggerName, ":")
				if len(parts) == 2 {
					// 如果Schema为空，则第一部分是schema
					if schemaName == "" {
						schemaName = parts[0]
						triggerName = parts[1]
					} else {
						// 如果Schema已有值，则这是一个触发器名映射，只保留冒号前的部分作为触发器名
						triggerName = parts[0] // 只保留冒号前的部分作为触发器名
					}
				}
			}

			// 处理点号分隔的情况 (schema.trigger)
			if schemaName == "" && strings.Contains(triggerName, ".") {
				parts := strings.Split(triggerName, ".")
				if len(parts) == 2 {
					schemaName = parts[0]
					triggerName = parts[1]
				}
			}

			// 获取映射信息
			mappingInfo := "-"
			if hasMappings {
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[schemaName]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", schemaName, destSchema)
				}
			}

			if hasMappings {
				table.AddRow(color.RedString(schemaName), color.GreenString(triggerName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			} else {
				table.AddRow(color.RedString(schemaName), color.GreenString(triggerName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	case "data":
		// 直接使用rows模式的代码，不再使用switch
		if hasMappings {
			table.AddRow("Schema", "Table", "IndexColumn", "CheckObject", "Rows", "Diffs", "Datafix", "Mapping")
			for _, pod := range terminalPods {
				var differences = pod.DIFFS
				for k, _ := range differencesSchemaTable {
					if k != "" {
						KI := strings.Split(k, "gtchecksum_gtchecksum")
						if pod.Schema == KI[0] && pod.Table == KI[1] {
							differences = "yes"
						}
					}
				}

				// 获取映射信息
				mappingInfo := "-"
				// 获取schema级别的映射
				schemaMap := getSchemaMappings()
				if destSchema, exists := schemaMap[pod.Schema]; exists {
					mappingInfo = fmt.Sprintf("Schema: %s:%s", pod.Schema, destSchema)
				}

				table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(dataResultRows(pod)), color.GreenString(differences), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
			}
		} else {
			table.AddRow("Schema", "Table", "IndexColumn", "CheckObject", "Rows", "Diffs", "Datafix")
			for _, pod := range terminalPods {
				var differences = pod.DIFFS
				for k, _ := range differencesSchemaTable {
					if k != "" {
						KI := strings.Split(k, "gtchecksum_gtchecksum")
						if pod.Schema == KI[0] && pod.Table == KI[1] {
							differences = "yes"
						}
					}
				}
				table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(dataResultRows(pod)), color.GreenString(differences), color.YellowString(pod.Datafix))
			}
		}
		fmt.Println(table)
	}
}

func (bar *Bar) NewOption(start, total int64, taskUnit string) {
	bar.cur = start
	bar.total = total
	bar.taskUnit = taskUnit
	bar.updateInterval = 100               // 调整为100毫秒更新一次，但强制每秒刷新
	bar.startTime = time.Now().UnixMilli() // 记录开始时间
	if bar.graph == "" {
		bar.graph = "█"
	}

	// 如果总数为0，设置为未初始化状态，避免显示异常
	if bar.total <= 0 {
		bar.percent = 0
		bar.rate = ""
		bar.total = 0 // 保持0，表示未初始化
	} else {
		bar.percent = bar.getPercent()
		// 计算进度条长度：每个█字符代表2%的进度（100% / 50个字符）
		progressBars := int(float64(bar.percent) * 50 / 100)
		bar.rate = strings.Repeat(bar.graph, progressBars) //初始化进度条位置
	}

	bar.lastUpdate = 0      // 初始化最后更新时间
	bar.lastForceUpdate = 0 // 初始化强制更新时间
}

func (bar *Bar) getPercent() int64 {
	if bar.total <= 0 {
		return 0
	}
	percent := int64(float64(bar.cur) / float64(bar.total) * 100)
	// 确保百分比不超过100%
	if percent > 100 {
		return 100
	}
	return percent
}
func (bar *Bar) NewOptionWithGraph(start, total int64, graph, taskUnit string) {
	bar.graph = graph
	bar.NewOption(start, total, taskUnit)
}

// Reinitialize 重新初始化进度条，用于获得实际任务数时
func (bar *Bar) Reinitialize(total int64) {
	if total <= 0 {
		return
	}
	bar.total = total
	bar.cur = 0
	bar.percent = 0
	bar.rate = ""
	bar.lastUpdate = 0
	bar.lastForceUpdate = 0
	bar.startTime = time.Now().UnixMilli()
}

// 显示进度条需要放在循环中执行，循环中展示每轮循环当前的进度状态，fmt.Pringf打印的那句话通过\r控制打印效果，在构建rate进度条时
// 需要保存上一次完成的百分比，只有当百分比发生了变化，且步长变化了2，才能改变进度条长度，也可以设置进度条为100个字符，这样就不需要空值进度条的步长为2了
// 每增长1%，进度条前进1格
func (bar *Bar) Play(cur int64) {
	// 如果进度条未初始化（total=0），不显示进度
	if bar.total <= 0 {
		bar.cur = cur
		return
	}

	bar.cur = cur
	bar.percent = bar.getPercent()

	currentTime := time.Now().UnixMilli()

	// 强制在进度完成时更新进度条
	if bar.percent == 100 || bar.cur == bar.total {
		// 补全进度条到100% (50个█字符，更精细的显示)
		for len(bar.rate) < 50 {
			bar.rate += bar.graph
		}
		bar.percent = 100
		// 计算实时耗时（秒）
		elapsedMilliseconds := time.Now().UnixMilli() - bar.startTime
		if elapsedMilliseconds < 0 {
			elapsedMilliseconds = 0
		}
		fmt.Printf("\r\033[K[%-50s]%3d%%  %s:     %d/%d    Elapsed: %.2fs", bar.rate, bar.percent, bar.taskUnit, bar.cur, bar.total, float64(elapsedMilliseconds)/1000)
		// 强制刷新输出缓冲区，确保实时显示
		os.Stdout.Sync()
	} else {
		// 每次调用Play都更新显示，确保实时性
		// 计算当前应该显示的进度条长度（每个█字符代表2%的进度）
		progressBars := int(float64(bar.percent) * 50 / 100)
		// 确保进度条长度不超过50个字符
		if progressBars > 50 {
			progressBars = 50
		}
		bar.rate = strings.Repeat(bar.graph, progressBars)

		// 计算实时耗时（秒）
		elapsedMilliseconds := currentTime - bar.startTime
		if elapsedMilliseconds < 0 {
			elapsedMilliseconds = 0
		}
		fmt.Printf("\r\033[K[%-50s]%3d%%  %s:     %d/%d    Elapsed: %.2fs", bar.rate, bar.percent, bar.taskUnit, bar.cur, bar.total, float64(elapsedMilliseconds)/1000)
		// 强制刷新输出缓冲区，确保实时显示
		os.Stdout.Sync()
	}
}

// NewTableProgress 开始新表的进度显示，不再输出额外表名
func (bar *Bar) NewTableProgress(tableName string) {
	// 不再输出表名，避免重复显示
}

// 由于上面的打印没有打印换行符，因此，在进度全部结束之后（也就是跳出循环之外时），需要打印一个换行符，因此，封装了一个Finish函数，该函数纯粹的打印一个换行，表示进度条已经完成。
func (bar *Bar) Finish() {
	// 如果进度条未初始化（total=0），不显示进度
	if bar.total <= 0 {
		return
	}

	// 强制设置进度为100%并补全进度条
	bar.cur = bar.total
	bar.percent = 100
	bar.rate = strings.Repeat(bar.graph, 50) // 强制补全进度条到50个字符

	// 计算耗时（秒）
	endTime := time.Now().UnixMilli()
	elapsedSeconds := float64(endTime-bar.startTime) / 1000.0

	fmt.Printf("\r\033[K[%-50s]%3d%%  %s:     %d/%d    Elapsed: %.2fs\n", bar.rate, bar.percent, bar.taskUnit, bar.cur, bar.total, elapsedSeconds)
}
