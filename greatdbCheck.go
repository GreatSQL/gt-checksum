package main

import (
	"fmt"
	"greatdbCheck/actions"
	"greatdbCheck/dbExec"
	"greatdbCheck/global"
	"greatdbCheck/go-log/log"
	"greatdbCheck/inputArg"
	"time"
)

var fullDataCompletionStatus = make(chan struct{}, 1)

func main() {
	//获取当前时间
	beginTime := time.Now()

	//获取配置文件
	fmt.Println("-- GreatdbCheck init configuration files -- ")
	m := inputArg.NewConfigInit()

	//初始化日志文件
	fmt.Println("-- GreatdbCheck init log files -- ")
	wlog := log.NewWlog(m.LogPath, m.LogFile)
	global.Wlog = wlog

	//获取待校验表信息
	fmt.Println("-- GreatdbCheck init check table -- ")
	tableList := actions.SchemaTableInit(m).SchemaTableFilter()
	if m.CheckObject != "data" {
		switch m.CheckObject {
		case "struct":
			actions.SchemaTableInit(m).Struct(tableList)
		case "index":
			actions.SchemaTableInit(m).Index(tableList)
		case "partitions":
			actions.SchemaTableInit(m).Partitions(tableList)
		case "foreign":
			actions.SchemaTableInit(m).Foreign(tableList)
		case "func":
			actions.SchemaTableInit(m).Func(tableList)
		case "proc":
			actions.SchemaTableInit(m).Proc(tableList)
		case "trigger":
			actions.SchemaTableInit(m).Trigger(tableList)
		}
	} else {
		//校验表结构
		tableList, _ = actions.SchemaTableInit(m).TableColumnNameCheck(tableList)
		if len(tableList) > 0 {
			//根据要校验的表，获取该表的全部列信息
			fmt.Println("-- GreatdbCheck init check table column --")
			tableAllCol := actions.SchemaTableInit(m).SchemaTableAllCol(tableList)

			//根据要校验的表，筛选查询数据时使用到的索引列信息
			fmt.Println("-- GreatdbCheck init check table index column --")
			tableIndexColumnMap := actions.SchemaTableInit(m).TableIndexColumn(tableList)

			////获取全局一致性位点
			//fmt.Println("-- GreatdbCheck Obtain global consensus sites --")
			//sglobalSites, err := dbExec.GCN().GcnObject(m.PoolMin, m.PoolMax, m.SourceJdbc, m.SourceDrive).GlobalCN()
			//if err != nil {
			//	os.Exit(1)
			//}
			//dglobalSites, err := dbExec.GCN().GcnObject(m.PoolMin, m.PoolMax, m.DestJdbc, m.DestDrive).GlobalCN()
			//if err != nil {
			//	os.Exit(1)
			//}

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
			fmt.Println("-- GreatdbCheck init source and dest transaction snapshoot conn pool --")
			sdc, _ := dbExec.GCN().GcnObject(m.PoolMin, m.PoolMax, m.SourceJdbc, m.SourceDrive).NewConnPool()
			ddc, _ := dbExec.GCN().GcnObject(m.PoolMin, m.PoolMax, m.DestJdbc, m.DestDrive).NewConnPool()

			//针对待校验表生成查询条件计划清单
			fmt.Println("-- GreatdbCheck init cehck table query plan and check data --")
			switch m.CheckMode {
			case "rows":
				actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).Schedulingtasks()
			case "count":
				actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).DoCountDataCheck()
			case "sample":
				actions.CheckTableQuerySchedule(sdc, ddc, tableIndexColumnMap, tableAllCol, *m).Schedulingtasks()
			}
			//关闭连接池连接
			sdc.Close()
			ddc.Close()
		}
	}
	//输出结果信息
	fmt.Println("")
	fmt.Println("** GreatdbCheck Overview of verification results **")
	fmt.Println("Check time: ", fmt.Sprintf("%.2fs", time.Since(beginTime).Seconds()), "(Seconds)")
	actions.CheckResultOut(m)
}
