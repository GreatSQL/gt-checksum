package actions

import (
	"fmt"
	"gt-checksum/inputArg"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"
)

// 进度条
type Bar struct {
	percent        int64  //百分比
	cur            int64  //当前进度位置
	total          int64  //总进度
	rate           string //进度条
	graph          string //显示符号
	taskUnit       string //task单位
	lastUpdate     int64  //上次更新时间戳（毫秒）
	updateInterval int64  //更新间隔（毫秒）
	startTime      int64  //开始时间戳（毫秒）
}

type Pod struct {
	Schema, Table, IndexColumn, CheckMode, Rows, DIFFS, CheckObject, Datafix, FuncName, Definer, ProcName, Sample, TriggerName, MappingInfo string
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

	return schemaMap
}

var measuredDataPods []Pod
var differencesSchemaTable = make(map[string]string)

func CheckResultOut(m *inputArg.ConfigParameter) {
	table := uitable.New()
	table.MaxColWidth = 200
	table.RightAlign(20)

	// 检查是否有映射关系
	hasMappings := hasMappingRelations()

	switch m.SecondaryL.RulesV.CheckObject {
	case "struct":
		if hasMappings {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix", "Mapping")
		} else {
			table.AddRow("Schema", "Table", "CheckObject", "Diffs", "Datafix")
		}

		for _, pod := range measuredDataPods {
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

		for _, pod := range measuredDataPods {
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

		for _, pod := range measuredDataPods {
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

		for _, pod := range measuredDataPods {
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

		for _, pod := range measuredDataPods {
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

		for _, pod := range measuredDataPods {
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

		for _, pod := range measuredDataPods {
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
		switch m.SecondaryL.RulesV.CheckMode {
		case "count":
			if hasMappings {
				table.AddRow("Schema", "Table", "CheckObject", "CheckMode", "Rows", "Diffs", "Mapping")
				for _, pod := range measuredDataPods {
					// 获取映射信息
					mappingInfo := "-"
					// 获取schema级别的映射
					schemaMap := getSchemaMappings()
					if destSchema, exists := schemaMap[pod.Schema]; exists {
						mappingInfo = fmt.Sprintf("Schema: %s:%s", pod.Schema, destSchema)
					}

					table.AddRow(color.RedString(pod.Schema), color.GreenString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.CheckMode), color.RedString(pod.Rows), color.YellowString(pod.DIFFS), color.CyanString(mappingInfo))
				}
			} else {
				table.AddRow("Schema", "Table", "CheckObject", "CheckMode", "Rows", "Diffs")
				for _, pod := range measuredDataPods {
					table.AddRow(color.RedString(pod.Schema), color.GreenString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.CheckMode), color.RedString(pod.Rows), color.YellowString(pod.DIFFS))
				}
			}
			fmt.Println(table)
		case "sample":
			if hasMappings {
				for _, pod := range measuredDataPods {
					// 获取映射信息
					mappingInfo := "-"
					// 获取schema级别的映射
					schemaMap := getSchemaMappings()
					if destSchema, exists := schemaMap[pod.Schema]; exists {
						mappingInfo = fmt.Sprintf("Schema: %s:%s", pod.Schema, destSchema)
					}

					if pod.Sample == "" {
						table.AddRow("Schema", "Table", "IndexColumn", "CheckObject", "CheckMode", "Rows", "Diffs", "Mapping")
						table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.GreenString(pod.DIFFS), color.CyanString(mappingInfo))
					} else {
						table.AddRow("Schema", "Table", "IndexColumn", "CheckObject", "CheckMode", "Rows", "Samp", "Diffs", "Mapping")
						table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.RedString(pod.Sample), color.GreenString(pod.DIFFS), color.CyanString(mappingInfo))
					}
				}
			} else {
				for _, pod := range measuredDataPods {
					if pod.Sample == "" {
						table.AddRow("Schema", "Table", "IndexColumn", "CheckObject", "CheckMode", "Rows", "Diffs")
						table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.GreenString(pod.DIFFS))
					} else {
						table.AddRow("Schema", "Table", "IndexColumn", "CheckObject", "CheckMode", "Rows", "Samp", "Diffs")
						table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.RedString(pod.Sample), color.GreenString(pod.DIFFS))
					}
				}
			}
			fmt.Println(table)
		case "rows":
			if hasMappings {
				table.AddRow("Schema", "Table", "IndexColumn", "CheckMode", "Rows", "Diffs", "Datafix", "Mapping")
				for _, pod := range measuredDataPods {
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

					table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.GreenString(differences), color.YellowString(pod.Datafix), color.CyanString(mappingInfo))
				}
			} else {
				table.AddRow("Schema", "Table", "IndexColumn", "CheckMode", "Rows", "Diffs", "Datafix")
				for _, pod := range measuredDataPods {
					var differences = pod.DIFFS
					for k, _ := range differencesSchemaTable {
						if k != "" {
							KI := strings.Split(k, "gtchecksum_gtchecksum")
							if pod.Schema == KI[0] && pod.Table == KI[1] {
								differences = "yes"
							}
						}
					}
					table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.GreenString(differences), color.YellowString(pod.Datafix))
				}
			}
			fmt.Println(table)
		}
	}
}

func (bar *Bar) NewOption(start, total int64, taskUnit string) {
	bar.cur = start
	bar.total = total
	bar.taskUnit = taskUnit
	bar.updateInterval = 100               // 调整为100毫秒更新一次，使进度条更流畅
	bar.startTime = time.Now().UnixMilli() // 记录开始时间
	if bar.graph == "" {
		bar.graph = "█"
	}
	bar.percent = bar.getPercent()
	// 计算进度条长度：每个█字符代表5%的进度（100% / 20个字符）
	progressBars := int(float64(bar.percent) * 20 / 100)
	bar.rate = strings.Repeat(bar.graph, progressBars) //初始化进度条位置
}

func (bar *Bar) getPercent() int64 {
	if bar.total == 0 {
		return 0
	}
	percent := int64(float32(bar.cur) / float32(bar.total) * 100)
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

// 显示进度条需要放在循环中执行，循环中展示每轮循环当前的进度状态，fmt.Pringf打印的那句话通过\r控制打印效果，在构建rate进度条时
// 需要保存上一次完成的百分比，只有当百分比发生了变化，且步长变化了2，才能改变进度条长度，也可以设置进度条为100个字符，这样就不需要空值进度条的步长为2了
// 每增长1%，进度条前进1格
func (bar *Bar) Play(cur int64) {
	bar.cur = cur
	last := bar.percent
	bar.percent = bar.getPercent()

	currentTime := time.Now().UnixMilli()

	// 强制在进度完成时更新进度条
	if bar.percent == 100 || bar.cur == bar.total {
		// 补全进度条到100% (20个█字符)
		for len(bar.rate) < 20 {
			bar.rate += bar.graph
		}
		bar.percent = 100
		// 计算实时耗时（秒）
		elapsedMilliseconds := time.Now().UnixMilli() - bar.startTime
		fmt.Printf("\r\033[K[%-20s]%3d%%  %s%5d/100  Elapsed: %.2fs", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent, float64(elapsedMilliseconds)/1000)
	} else if (bar.percent != last || bar.cur == bar.total) && (currentTime-bar.lastUpdate) >= bar.updateInterval {
		// 只在百分比变化且达到更新时间间隔时才更新进度条
		// 计算当前应该显示的进度条长度（每个█字符代表5%的进度）
		progressBars := int(float64(bar.percent) * 20 / 100)
		// 确保进度条长度不超过20个字符
		if progressBars > 20 {
			progressBars = 20
		}
		bar.rate = strings.Repeat(bar.graph, progressBars)
		bar.lastUpdate = currentTime
		// 使用回车符覆盖当前行，避免刷屏
		// 计算实时耗时（秒）
		elapsedMilliseconds := currentTime - bar.startTime
		fmt.Printf("\r\033[K[%-20s]%3d%%  %s%5d/100     Elapsed time: %.2fs", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent, float64(elapsedMilliseconds)/1000)
	}
}

// NewTableProgress 开始新表的进度显示，先输出换行再开始进度条
func (bar *Bar) NewTableProgress(tableName string) {
	// 先输出换行确保新表进度在新行开始
	fmt.Printf("\n%-40s", fmt.Sprintf("Table: %s", tableName))
}

// 由于上面的打印没有打印换行符，因此，在进度全部结束之后（也就是跳出循环之外时），需要打印一个换行符，因此，封装了一个Finish函数，该函数纯粹的打印一个换行，表示进度条已经完成。
func (bar *Bar) Finish() {
	// 强制设置进度为100%并补全进度条
	bar.cur = bar.total
	bar.percent = 100
	bar.rate = strings.Repeat(bar.graph, 20) // 强制补全进度条到20个字符

	// 计算耗时（秒）
	endTime := time.Now().UnixMilli()
	elapsedSeconds := float64(endTime-bar.startTime) / 1000.0

	fmt.Printf("\r\033[K[%-20s]%3d%%  %s%5d/100  Elapsed: %.2fs", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent, elapsedSeconds)
	fmt.Println()
}
