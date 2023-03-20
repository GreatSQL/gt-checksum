package main

import (
	"fmt"
	"gt-checksum/actions"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"time"
)

var err error

func main() {
	//获取当前时间
	beginTime := time.Now()

	//获取配置文件
	m := inputArg.ConfigInit(0)
	if !actions.SchemaTableInit(m).GlobalAccessPriCheck(1, 2) {
		fmt.Println("gt-checksum report: Missing global permissions, please check the log for details.")
		os.Exit(1)
	}
	//获取待校验表信息
	var tableList []string
	if tableList, err = actions.SchemaTableInit(m).SchemaTableFilter(3, 4); err != nil || len(tableList) == 0 {
		fmt.Println("gt-checksum report: check table is empty,please check the log for details!")
		os.Exit(1)
	}

	switch m.SecondaryL.RulesV.CheckObject {
	case "struct":
		if err = actions.SchemaTableInit(m).Struct(tableList, 5, 6); err != nil {
			fmt.Println("-- gt-checksum report: The table Struct verification failed, please refer to the log file for details, enable debug to get more information -- ")
			os.Exit(1)
		}
	case "index":
		if err = actions.SchemaTableInit(m).Index(tableList, 7, 8); err != nil {
			fmt.Println("-- gt-checksum report: The table Index verification failed, please refer to the log file for details, enable debug to get more information -- ")
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
			fmt.Println("-- gt-checksum report: The table structure verification failed, please refer to the log file for details, enable debug to get more information -- ")
			os.Exit(1)
		} else if len(tableList) == 0 {
			fmt.Println("gt-checksum report: No checklist, please check the log for details.")
			os.Exit(1)
		}
		//19、20
		if tableList, _, err = actions.SchemaTableInit(m).TableAccessPriCheck(tableList, 19, 20); err != nil {
			fmt.Println("-- gt-checksum report: The table access permissions query failed, please refer to the log file for details, enable debug to get more information -- ")
			os.Exit(1)
		} else if len(tableList) == 0 {
			fmt.Println("gt-checksum report: Insufficient permissions for the verification table, please check the log for details.")
			os.Exit(1)
		}

		//根据要校验的表，获取该表的全部列信息
		fmt.Println("-- gt-checksum init check table column --")
		tableAllCol := actions.SchemaTableInit(m).SchemaTableAllCol(tableList, 21, 22)
		//根据要校验的表，筛选查询数据时使用到的索引列信息
		fmt.Println("-- gt-checksum init check table index column --")
		tableIndexColumnMap := actions.SchemaTableInit(m).TableIndexColumn(tableList, 23, 24)
		//获取全局一致 x性位点
		//fmt.Println("-- GreatdbCheck Obtain global consensus sites --")
		//sglobalSites, err := dbExec.GCN().GcnObject(m.PoolMin, m.PoolMax, m.SourceJdbc, m.SourceDrive).GlobalCN(25)
		//if err != nil {
		//	os.Exit(1)
		//}
		//dglobalSites, err := dbExec.GCN().GcnObject(m.PoolMin, m.PoolMax, m.DestJdbc, m.DestDrive).GlobalCN(26)
		//if err != nil {
		//	os.Exit(1)
		//}
		//fmt.Println(sglobalSites, dglobalSites)

		//var SourceItemAbnormalDataChan = make(chan actions.SourceItemAbnormalDataStruct, 100)
		//var addChan, delChan = make(chan string, 100), make(chan string, 100)

		// 开启差异数据修复的线程
		//go actions.DifferencesDataDispos(SourceItemAbnormalDataChan, addChan, delChan)
		//go actions.DataFixSql(addChan, delChan)

		//开始进行增量校验
		//if m.IncCheckSwitch == "yesno" {
		//	fmt.Println("-- GreatdbCheck begin cehck table incerment date --")
		//	actions.IncDataDisops(m.SourceDrive, m.DestDrive, m.SourceJdbc, m.DestJdbc, sglobalSites, dglobalSites, tableList).Aa(fullDataCompletionStatus, SourceItemAbnormalDataChan)
		//}

		//初始化数据库连接池
		fmt.Println("-- gt-checksum init source and dest transaction snapshoot conn pool --")
		sdc, _ := dbExec.GCN().GcnObject(m.ConnPoolV.PoolMin, m.SecondaryL.DsnsV.SrcJdbc, m.SecondaryL.DsnsV.SrcDrive).NewConnPool(27)
		ddc, _ := dbExec.GCN().GcnObject(m.ConnPoolV.PoolMin, m.SecondaryL.DsnsV.DestJdbc, m.SecondaryL.DsnsV.DestDrive).NewConnPool(28)

		//针对待校验表生成查询条件计划清单
		fmt.Println("-- gt-checksum init cehck table query plan and check data --")
		switch m.SecondaryL.RulesV.CheckMode {
		case "rows":
			actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).Schedulingtasks()
		case "count":
			actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).DoCountDataCheck()
		case "sample":
			actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).DoSampleDataCheck()
		}
		//关闭连接池连接
		sdc.Close(27)
		ddc.Close(28)
	default:
		fmt.Println("-- gt-checksum report: checkObject parameter selection error, please refer to the log file for details, enable debug to get more information -- ")
		os.Exit(1)
	}
	global.Wlog.Info("gt-checksum check object {", m.SecondaryL.RulesV.CheckObject, "} complete !!!")
	//输出结果信息
	fmt.Println("")
	fmt.Println("** gt-checksum Overview of results **")
	fmt.Println("Check time: ", fmt.Sprintf("%.2fs", time.Since(beginTime).Seconds()), "(Seconds)")
	actions.CheckResultOut(m)
}
