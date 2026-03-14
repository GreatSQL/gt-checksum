package main

import (
	"fmt"
	"gt-checksum/MySQL"
	"gt-checksum/actions"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"gt-checksum/utils"
	"os"
	"strings"
	"time"
)

var err error

// extractSchemasFromTables 从tables参数中提取schema信息
// 例如：从"db1.table1,db2.table2,db3.*"中提取出["db1", "db2", "db3"]
func extractSchemasFromTables(tables string) []string {
	schemas := make(map[string]bool)

	// 按逗号分割表列表
	tableList := strings.Split(tables, ",")

	for _, table := range tableList {
		// 处理映射格式 schema1.table1:schema2.table2
		if strings.Contains(table, ":") {
			parts := strings.Split(table, ":")
			if len(parts) == 2 {
				sourceParts := strings.Split(parts[0], ".")
				if len(sourceParts) >= 1 {
					schemas[sourceParts[0]] = true
				}
			}
		} else {
			// 处理普通格式 schema.table 或 schema.*
			parts := strings.Split(table, ".")
			if len(parts) >= 1 {
				schemas[parts[0]] = true
			}
		}
	}

	// 将map转换为slice
	result := make([]string, 0, len(schemas))
	for schema := range schemas {
		result = append(result, schema)
	}

	return result
}

func main() {
	global.ResetRuntimeState()

	//获取当前时间
	beginTime := time.Now()

	//获取配置文件
	m := inputArg.ConfigInit(0)

	// 初始化性能指标变量
	initStartTime := time.Now()
	var metadataCollectionTime, connSetupTime, checksumTime, extraOpsTime, totalElapsedTime, miscellaneousTime time.Duration

	//启动内存监控
	utils.MemoryMonitor(fmt.Sprintf("%dMB", m.SecondaryL.RulesV.MemoryLimit), m)
	actions.ResetMemoryPeakStats()

	if m.SecondaryL.RulesV.CheckObject == "data" {
		if !actions.SchemaTableInit(m).GlobalAccessPriCheck(1, 2) {
			fmt.Println(fmt.Sprintf("gt-checksum: Missing required global privileges. Check %s for details or set logLevel=debug", m.SecondaryL.LogV.LogFile))
			os.Exit(1)
		}
	} else {
		global.Wlog.Info(fmt.Sprintf("Skip global privilege precheck for checkObject=%s", m.SecondaryL.RulesV.CheckObject))
	}
	//获取待校验表信息
	var tableList, tableListColCheck, tableListPriCheck []string
	schemaTableInstance := actions.SchemaTableInit(m)

	// 根据checkObject类型决定如何处理表列表
	if m.SecondaryL.RulesV.CheckObject == "trigger" || m.SecondaryL.RulesV.CheckObject == "routine" {
		// 对于触发器、存储过程和函数，我们只需要schema信息
		// 从tables参数中提取schema信息
		schemas := extractSchemasFromTables(m.SecondaryL.SchemaV.Tables)
		if len(schemas) == 0 {
			fmt.Println(fmt.Sprintf("gt-checksum: No valid schemas found in tables parameter. Check %s for details or set logLevel=debug", m.SecondaryL.LogV.LogFile))
			os.Exit(1)
		}

		// 构建tableList，格式为"schema.*"
		for _, schema := range schemas {
			tableList = append(tableList, schema+".*")
		}

		global.Wlog.Info(fmt.Sprintf("Using schemas for %s check: %v", m.SecondaryL.RulesV.CheckObject, schemas))
	} else {
		// 对于其他类型的检查，使用正常的表过滤逻辑
		if tableList, err = schemaTableInstance.SchemaTableFilter(3, 4); err != nil {
			fmt.Println(fmt.Sprintf("gt-checksum: No tables to check. Check %s for details or set logLevel=debug", m.SecondaryL.LogV.LogFile))
			os.Exit(1)
		}
		if len(tableList) == 0 {
			if ignoredSummary := schemaTableInstance.IgnoredMatchedTablesSummary(); ignoredSummary != "" {
				fmt.Println(fmt.Sprintf("gt-checksum: No tables to check. Matched tables were filtered by ignoreTables: %s. Check %s for details or adjust ignoreTables", ignoredSummary, m.SecondaryL.LogV.LogFile))
			} else {
				fmt.Println(fmt.Sprintf("gt-checksum: No tables to check. Check %s for details or set logLevel=debug", m.SecondaryL.LogV.LogFile))
			}
			os.Exit(1)
		}
	}

	switch m.SecondaryL.RulesV.CheckObject {
	case "struct":
		// 当checkObject=struct时，执行所有结构相关的检查（包括表结构、索引、分区和外键）
		if err = schemaTableInstance.Struct(tableList, 5, 6); err != nil {
			fmt.Println(fmt.Sprintf("gt-checksum: Table structure verification failed. Check %s for details or set logLevel=debug", m.SecondaryL.LogV.LogFile))
			os.Exit(1)
		}
	case "trigger":
		schemaTableInstance.Trigger(tableList, 11, 12)
	case "routine":
		// 当checkObject=routine时，统一入口调用 Routine，同时检查存储过程和函数
		fmt.Println("gt-checksum: Checking stored procedures and functions (routine mode)")
		schemaTableInstance.Routine(tableList, 13, 14, "")
	// 注意：proc和func选项已在参数处理阶段被强制改为data，所以这里不再需要单独的case
	case "data":
		initStartTime = time.Now()
		var abnormalTableList []string

		//校验表结构
		tableListColCheck, abnormalTableList, err = schemaTableInstance.TableColumnNameCheck(tableList, 9, 10)
		if err != nil {
			fmt.Println("gt-checksum: Table structure verification failed. Check log file or set logLevel=debug for details")
			os.Exit(1)
		}

		preflightDecision := actions.EvaluateDataCheckPreflight(len(tableListColCheck), len(abnormalTableList), global.HasInvisibleColumnMismatch)
		if preflightDecision.Fatal {
			fmt.Println(preflightDecision.Message)
			fmt.Println("-----------------------------------------------------")
			os.Exit(1)
		}
		if preflightDecision.SkipChecksum {
			fmt.Println(preflightDecision.Message)
			fmt.Println("-----------------------------------------------------")
			metadataCollectionTime = time.Since(initStartTime)
			totalElapsedTime = time.Since(beginTime)
			miscellaneousTime = totalElapsedTime - initStartTime.Sub(beginTime) - metadataCollectionTime
			break
		}

		if err != nil {
			fmt.Println("gt-checksum report: Table structure verification failed. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		} else if len(tableListColCheck) == 0 {
			fmt.Println("gt-checksum report: table checklist is empty. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		}
		//19、20
		if tableListPriCheck, _, err = schemaTableInstance.TableAccessPriCheck(tableListColCheck, 19, 20); err != nil {
			fmt.Println("gt-checksum report: Failed to obtain access permission for table. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		} else if len(tableListPriCheck) == 0 {
			fmt.Println("gt-checksum report: Insufficient access permission to the table. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		}

		metadataCollectionTime = time.Since(initStartTime)

		//根据要校验的表，获取该表的全部列信息
		fmt.Println("gt-checksum: Collecting table column information")
		tableAllCol := schemaTableInstance.SchemaTableAllCol(tableListColCheck, 21, 22)
		//根据要校验的表，筛选查询数据时使用到的索引列信息
		fmt.Println("gt-checksum: Collecting table index information")
		tableIndexColumnMap := schemaTableInstance.TableIndexColumn(tableListColCheck, 23, 24)

		//初始化数据库连接池
		fmt.Println("gt-checksum: Establishing database connections")
		connSetupStart := time.Now()
		sdc, _ := dbExec.GCN().GcnObject(m.ConnPoolV.PoolMin, m.SecondaryL.DsnsV.SrcJdbc, m.SecondaryL.DsnsV.SrcDrive).NewConnPool(27)
		ddc, _ := dbExec.GCN().GcnObject(m.ConnPoolV.PoolMin, m.SecondaryL.DsnsV.DestJdbc, m.SecondaryL.DsnsV.DestDrive).NewConnPool(28)
		connSetupTime = time.Since(connSetupStart)

		//针对待校验表生成查询条件计划清单
		fmt.Println("gt-checksum: Generating data checksum plan")
		checksumStart := time.Now()
		actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).Schedulingtasks()
		checksumTime = time.Since(checksumStart)

		// 记录额外操作时间
		extraOpsStart := time.Now()

		//关闭连接池连接
		sdc.Close(27)
		ddc.Close(28)

		extraOpsTime = time.Since(extraOpsStart)

		// 计算杂项时间（主要是初始化时间）
		totalElapsedTime = time.Since(beginTime)
		miscellaneousTime = totalElapsedTime - (initStartTime.Sub(beginTime)) - metadataCollectionTime - connSetupTime - checksumTime - extraOpsTime

	default:
		fmt.Println("gt-checksum: Invalid checkObject option value. Check log file or set logLevel=debug for details")
		os.Exit(1)
	}
	global.Wlog.Info("gt-checksum check object {", m.SecondaryL.RulesV.CheckObject, "} complete !!!")
	//输出结果信息
	fmt.Println("")
	fmt.Println("** gt-checksum Overview of results **")
	actions.CheckResultOut(m)
	actions.LogMemoryPeakSummary()

	// 输出性能指标
	fmt.Println()
	fmt.Println("Performance Metrics:")
	fmt.Printf("  Initialization: %.2fs\n", initStartTime.Sub(beginTime).Seconds())
	fmt.Printf("  Metadata collection: %.2fs\n", metadataCollectionTime.Seconds())
	fmt.Printf("  Connection setup: %.2fs\n", connSetupTime.Seconds())
	fmt.Printf("  Data checksum: %.2fs\n", checksumTime.Seconds())
	fmt.Printf("  Additional operations: %.2fs\n", extraOpsTime.Seconds())
	fmt.Printf("  Miscellaneous: %.2fs\n", miscellaneousTime.Seconds())
	fmt.Printf("Total execution time: %.2fs\n", totalElapsedTime.Seconds())

	// 检查是否有修复SQL被写入，如果没有则删除空的datafix.sql文件
	if m.SecondaryL.RepairV.Datafix == "file" && m.SecondaryL.RepairV.FixFileDir != "" {
		// 先关闭文件句柄，以便能够删除文件
		if m.SecondaryL.RepairV.FixFileFINE != nil {
			m.SecondaryL.RepairV.FixFileFINE.Close()
		}
		if err := mysql.CheckAndCleanupEmptyFixFile(m.SecondaryL.RepairV.FixFileDir); err != nil {
			fmt.Printf("Warning: Failed to clean up empty fix SQL file: %v\n", err)
		}
	}
}
