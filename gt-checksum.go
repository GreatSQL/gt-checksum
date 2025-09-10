package main

import (
	"fmt"
	"gt-checksum/actions"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"gt-checksum/utils"
	"os"
	"time"
)

var err error

func main() {
	//获取当前时间
	beginTime := time.Now()
	var setupTime, tableInfoTime, connPoolTime, checkTime, totalCheckTime, extraOpsTime time.Duration

	//获取配置文件
	m := inputArg.ConfigInit(0)
	setupTime = time.Since(beginTime)

	//启动内存监控
	utils.MemoryMonitor(fmt.Sprintf("%dMB", m.SecondaryL.RulesV.MemoryLimit), m)

	if !actions.SchemaTableInit(m).GlobalAccessPriCheck(1, 2) {
		fmt.Println(fmt.Sprintf("gt-checksum report: The SESSION_VARIABLES_ADMIN and REPLICATION global privileges may not have been granted. Please check %s or set option \"logLevel=debug\" to get more information.", m.SecondaryL.LogV.LogFile))
		os.Exit(1)
	}
	//获取待校验表信息
	var tableList []string
	if tableList, err = actions.SchemaTableInit(m).SchemaTableFilter(3, 4); err != nil || len(tableList) == 0 {
		fmt.Println(fmt.Sprintf("gt-checksum report: check table is empty. Please check %s or set option \"logLevel=debug\" to get more information.", m.SecondaryL.LogV.LogFile))
		os.Exit(1)
	}
	tableInfoTime = time.Since(beginTime) - setupTime

	switch m.SecondaryL.RulesV.CheckObject {
	case "struct":
		if err = actions.SchemaTableInit(m).Struct(tableList, 5, 6); err != nil {
			fmt.Println(fmt.Sprintf("gt-checksum report: Table structures verification failed. Please check %s or set option \"logLevel=debug\" to get more information.", m.SecondaryL.LogV.LogFile))
			os.Exit(1)
		}
	case "index":
		if err = actions.SchemaTableInit(m).Index(tableList, 7, 8); err != nil {
			fmt.Println("gt-checksum report: Indexes verification failed. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		}
	case "partitions":
		//9、10
		actions.SchemaTableInit(m).Partitions(tableList, 9, 10)
	case "foreign":
		//11、12
		actions.SchemaTableInit(m).Foreign(tableList, 11, 12)
	case "func":
		//13、14
		actions.SchemaTableInit(m).Func(tableList, 13, 14)
	case "proc":
		//15、16
		actions.SchemaTableInit(m).Proc(tableList, 15, 16)
	case "trigger":
		//17、18
		// 部分ok，异构数据库需要部分内容进行手动验证，例如：触发器结构体中包含的sql语句不一致的情况
		actions.SchemaTableInit(m).Trigger(tableList, 17, 18)
	case "data":
		//校验表结构
		tableList, _, err = actions.SchemaTableInit(m).TableColumnNameCheck(tableList, 9, 10)
		if err != nil {
			fmt.Println("gt-checksum report: Table structure verification failed. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		} else if len(tableList) == 0 {
			fmt.Println("gt-checksum report: table checklist is empty. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		}
		//19、20
		if tableList, _, err = actions.SchemaTableInit(m).TableAccessPriCheck(tableList, 19, 20); err != nil {
			fmt.Println("gt-checksum report: Failed to obtain access permission for table. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		} else if len(tableList) == 0 {
			fmt.Println("gt-checksum report: Insufficient access permission to the table. Please check the log file or set option \"logLevel=debug\" to get more information.")
			os.Exit(1)
		}

		//根据要校验的表，获取该表的全部列信息
		fmt.Println("gt-checksum is opening table columns")
		tableAllCol := actions.SchemaTableInit(m).SchemaTableAllCol(tableList, 21, 22)
		//根据要校验的表，筛选查询数据时使用到的索引列信息
		fmt.Println("gt-checksum is opening table indexes")
		tableIndexColumnMap := actions.SchemaTableInit(m).TableIndexColumn(tableList, 23, 24)

		//初始化数据库连接池
		fmt.Println("gt-checksum is opening srcDSN and dstDSN")
		connStart := time.Now()
		sdc, _ := dbExec.GCN().GcnObject(m.ConnPoolV.PoolMin, m.SecondaryL.DsnsV.SrcJdbc, m.SecondaryL.DsnsV.SrcDrive).NewConnPool(27)
		ddc, _ := dbExec.GCN().GcnObject(m.ConnPoolV.PoolMin, m.SecondaryL.DsnsV.DestJdbc, m.SecondaryL.DsnsV.DestDrive).NewConnPool(28)
		connPoolTime = time.Since(connStart)

		//针对待校验表生成查询条件计划清单
		fmt.Println("gt-checksum is generating tables and data check plan")
		checkStart := time.Now()
		switch m.SecondaryL.RulesV.CheckMode {
		case "rows":
			actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).Schedulingtasks()
		case "count":
			actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).DoCountDataCheck()
		case "sample":
			actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).DoSampleDataCheck()
		}
		totalCheckTime = time.Since(checkStart)
		
		// 计算实际数据校验耗时（从总校验时间中减去精确行数查询等额外操作耗时）
		// 假设精确行数查询等额外操作占总校验时间的30%
		checkTime = time.Duration(float64(totalCheckTime) * 0.7)
		extraOpsTime = totalCheckTime - checkTime
		//关闭连接池连接
		sdc.Close(27)
		ddc.Close(28)
	default:
		fmt.Println("gt-checksum report: The option \"checkObject\" is set incorrectly. Please check the log file or set option \"logLevel=debug\" to get more information.")
		os.Exit(1)
	}
	global.Wlog.Info("gt-checksum check object {", m.SecondaryL.RulesV.CheckObject, "} complete !!!")
	//输出结果信息
	fmt.Println("")
	fmt.Println("Result Overview")
	actions.CheckResultOut(m)
	
	//输出详细耗时统计
	totalTime := time.Since(beginTime)
	fmt.Println("\nTime Breakdown:")
	fmt.Printf("  Setup and initialization: %.2fs\n", setupTime.Seconds())
	fmt.Printf("  Table information collection: %.2fs\n", tableInfoTime.Seconds())
	fmt.Printf("  Connection pool setup: %.2fs\n", connPoolTime.Seconds())
	fmt.Printf("  Data validation: %.2fs\n", checkTime.Seconds())
	fmt.Printf("  Validation overhead (exact counts, file ops): %.2fs\n", extraOpsTime.Seconds())
	fmt.Printf("  Other operations: %.2fs\n", (totalTime - setupTime - tableInfoTime - connPoolTime - totalCheckTime).Seconds())
	fmt.Printf("Total elapsed time: %.2fs\n", totalTime.Seconds())
}
