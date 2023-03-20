package inputArg

import (
	"errors"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"os"
	"regexp"
	"runtime"
	"strings"
)

//type ConfigParameter struct {
//	config                                       string   //配置文件信息
//	SourceJdbc, DestJdbc, SourceDrive, DestDrive string   //源端的连接信息
//	PoolMin                                      int      //数据库连接池最小值
//	Table, Igtable                               string   //待校验的表和忽略的表
//	CheckNoIndexTable                            string   //是否校验无索引表
//	LowerCaseTableNames                          string   //是否忽略校验表的大小写
//	LogFile, LogLevel                            string   //关于日志输出信息配置
//	Concurrency                                  int      //查询并发度
//	SingleIndexChanRowCount                      int      //单索引列校验数据块长度
//	JointIndexChanRowCount                       int      //多列索引校验数据块长度
//	QueueDepth                                   int      //数据块长度
//	Datafix, FixFileName                         string   //差异数据修复的方式及配置
//	IncCheckSwitch                               string   //增量数据校验
//	CheckMode                                    string   //校验的方式，可以为count(*)或者是校验row数据
//	CheckObject                                  string   //校验的对象，可以是struct或者是data
//	Ratio                                        int      //配置数据抽检时配置的比例
//	Sfile                                        *os.File //修复文件的文件句柄
//	FixTrxNum                                    int      //单并发修复语句的事务数量
//}

var illegalParameterStatus = false

//判断库表配置参数是否存在非法参数
func (rc *ConfigParameter) rexPat(rex *regexp.Regexp, rexStr string, illegalParameterStatus bool) {
	if strings.Contains(rexStr, ",") {
		ab := strings.Split(rexStr, ",")
		for _, i := range ab {
			if i != "" {
				if !rex.MatchString(i) {
					illegalParameterStatus = true
				} else {
					illegalParameterStatus = false
				}
			}
		}
	} else {
		if rexStr != "NIL" && rexStr != "nil" {
			if !rex.MatchString(rexStr) {
				illegalParameterStatus = true
			} else {
				illegalParameterStatus = false
			}
		}
	}
	if illegalParameterStatus { //不法参数
		rc.getErr("table/ignoreTable Parameter setting error.", errors.New("parameter error"))
	}
}

func (rc *ConfigParameter) fileExsit(logFile string) {
	var err error
	if _, err = os.Stat(logFile); err != nil {
		if _, err = os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
			rc.getErr("Failed to create a log file. Procedure.", err)
		}
	}
}

var schemaTableFilter = func(igschema, igtable string) string {
	var tmps, tmpt = make(map[string]string), make(map[string]string)
	tmpc := strings.Split(igschema, ",")
	tmpd := strings.Split(igtable, ",")
	var tmpl []string
	for _, i := range tmpc {
		tmps[i] = ""
	}
	for _, i := range tmpd {
		var ii string
		if strings.Contains(i, ".") {
			a := strings.Split(i, ".")
			ii = a[0]
		}
		tmpt[ii] = ""
	}
	for k, _ := range tmpt {
		if _, ok := tmps[k]; ok {
			delete(tmps, k)
		}
	}
	for k, _ := range tmps {
		tmpl = append(tmpl, k)
	}
	return strings.Join(tmpl, ",")
}

func (rc *ConfigParameter) getErr(msg string, err error) {
	if err != nil {
		fmt.Println(err, ":", msg)
		os.Exit(0)
	}
}

/*
	校验输入参数是否合规
*/
func (rc *ConfigParameter) checkPar() {
	var (
		vlog  string
		Event = "C_check_Parameter"
		err   error
	)

	if rc.SecondaryL.DsnsV.SrcDrive == "oracle" {
		rc.SecondaryL.DsnsV.SrcDrive = "godror"
	} else if rc.SecondaryL.DsnsV.DestDrive == "oracle" {
		rc.SecondaryL.DsnsV.DestDrive = "godror"
	}

	tmpDbc := dbExec.DBConnStruct{DBDevice: rc.SecondaryL.DsnsV.SrcDrive, JDBC: rc.SecondaryL.DsnsV.SrcJdbc}
	vlog = fmt.Sprintf("(%d) [%s] Start to verify the legality of configuration parameters...", rc.LogThreadSeq, Event)
	global.Wlog.Info(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  source DB node connection message {%s}, start to check it...", rc.LogThreadSeq, Event, rc.SecondaryL.DsnsV.SrcJdbc)
	global.Wlog.Debug(vlog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		fmt.Println("GreatSQL report: source DB connection fail, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  source DB connection message error! error message is {%s}", rc.LogThreadSeq, Event, err)
		global.Wlog.Error(vlog)
		os.Exit(0)
	}
	vlog = fmt.Sprintf("(%d) [%s]  source DB node connection message oK!", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)

	tmpDbc.DBDevice = rc.SecondaryL.DsnsV.DestDrive
	tmpDbc.JDBC = rc.SecondaryL.DsnsV.DestJdbc
	vlog = fmt.Sprintf("(%d) [%s]  dest DB node connection message {%s}, start to check it...", rc.LogThreadSeq, Event, rc.SecondaryL.DsnsV.DestJdbc)
	global.Wlog.Debug(vlog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		fmt.Println("GreatSQL report: dest DB connection fail, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  dest DB connection message error!. error message is {%s}", rc.LogThreadSeq, Event, err)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  dest DB node connection message oK!", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)

	//表级别的正则匹配
	vlog = fmt.Sprintf("(%d) [%s]  start check table Name and ignore table Name Legitimacy.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if rc.SecondaryL.SchemaV.Tables == "" {
		fmt.Println("GreatSQL report: table Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  table cannot all be set to nil! ", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	tabr, _ := regexp.Compile(`[0-9a-zA-Z!@_{}*%-]\.[0-9a-zA-Z!@_{}%*-]`)
	rc.rexPat(tabr, rc.SecondaryL.SchemaV.Tables, illegalParameterStatus)
	rc.rexPat(tabr, rc.SecondaryL.SchemaV.Tables, illegalParameterStatus)
	if rc.SecondaryL.SchemaV.Tables == rc.SecondaryL.SchemaV.IgnoreTables {
		fmt.Println("GreatSQL report: table or ignoretable Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  The test form and the skip form cannot be consistent! ", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	//判断*.*之外是否还包含其他的值
	if strings.Contains(rc.SecondaryL.SchemaV.Tables, "*.*") {
		table := strings.Replace(rc.SecondaryL.SchemaV.Tables, "*.*", "", 1)
		for _, i := range strings.Split(table, ",") {
			ii := strings.TrimSpace(i)
			if ii != "" {
				fmt.Println("GreatSQL report: table Parameter setting error, please check the log for details.")
				vlog = fmt.Sprintf("(%d) [%s]  The table parameter configures *.* and contains other values! ", rc.LogThreadSeq, Event)
				global.Wlog.Error(vlog)
				os.Exit(1)
			}
		}
	}
	var cc []string
	for _, i := range strings.Split(rc.SecondaryL.SchemaV.Tables, ",") {
		cc = append(cc, strings.TrimSpace(i))
	}
	rc.SecondaryL.SchemaV.Tables = strings.Join(cc, ",")
	if strings.HasSuffix(rc.SecondaryL.SchemaV.Tables, ",") {
		rc.SecondaryL.SchemaV.Tables = rc.SecondaryL.SchemaV.Tables[:len(rc.SecondaryL.SchemaV.Tables)-1]
	}
	if rc.SecondaryL.SchemaV.LowerCaseTableNames == "no" {
		rc.SecondaryL.SchemaV.Tables = strings.ToUpper(strings.TrimSpace(rc.SecondaryL.SchemaV.Tables))
		rc.SecondaryL.SchemaV.IgnoreTables = strings.ToUpper(strings.TrimSpace(rc.SecondaryL.SchemaV.IgnoreTables))
	}
	if rc.SecondaryL.SchemaV.Tables == "" {
		fmt.Println("GreatSQL report: table Parameter setting error, please check the log for details.")
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  check table parameter message is {table: %s ignore table: %s}", rc.LogThreadSeq, Event, rc.SecondaryL.SchemaV.Tables, rc.SecondaryL.SchemaV.IgnoreTables)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init check object values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.RulesV.CheckObject = strings.ToLower(rc.SecondaryL.RulesV.CheckObject)
	vlog = fmt.Sprintf("(%d) [%s]  check object parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RulesV.CheckObject)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init check mode values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.RulesV.CheckMode = strings.ToLower(rc.SecondaryL.RulesV.CheckMode)
	vlog = fmt.Sprintf("(%d) [%s]  check mode parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RulesV.CheckMode)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init no index table values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.SchemaV.CheckNoIndexTable = strings.ToLower(rc.SecondaryL.SchemaV.CheckNoIndexTable)
	vlog = fmt.Sprintf("(%d) [%s] check no index table parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.SchemaV.CheckNoIndexTable)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init lower case table name values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.SchemaV.LowerCaseTableNames = strings.ToLower(rc.SecondaryL.SchemaV.LowerCaseTableNames)
	vlog = fmt.Sprintf("(%d) [%s]  check lower case table name parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.SchemaV.LowerCaseTableNames)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init log out values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	//判断日志输入参数
	rc.fileExsit(rc.SecondaryL.LogV.LogFile)
	vlog = fmt.Sprintf("(%d) [%s]  check log out parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.LogV.LogFile)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init data fix file values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if rc.SecondaryL.RepairV.Datafix == "file" {
		vlog = fmt.Sprintf("(%d) [%s]  Open repair file {%s} handle.", rc.LogThreadSeq, Event, rc.SecondaryL.RepairV.FixFileName)
		global.Wlog.Debug(vlog)
		if _, err = os.Stat(rc.SecondaryL.RepairV.FixFileName); err == nil {
			os.Remove(rc.SecondaryL.RepairV.FixFileName)
		}
		rc.fileExsit(rc.SecondaryL.RepairV.FixFileName)
		rc.SecondaryL.RepairV.FixFileFINE, err = os.OpenFile(rc.SecondaryL.RepairV.FixFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("GreatSQL report: fix file open fail, please check the log for details.")
			vlog = fmt.Sprintf("(%d) [%s]  Repair the file {%s} handle opening failure, the failure information is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RepairV.FixFileName, err)
			global.Wlog.Error(vlog)
			os.Exit(1)
		}
		vlog = fmt.Sprintf("(%d) [%s]  check data fix file parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RepairV.FixFileName)
		global.Wlog.Debug(vlog)
	}
	for _, v := range []int{rc.SecondaryL.RulesV.ChanRowCount, rc.SecondaryL.RulesV.QueueSize, rc.SecondaryL.RulesV.Ratio, rc.SecondaryL.RulesV.ParallelThds} {
		if v < 1 {
			fmt.Println("GreatSQL report: chanRowCount || queue-size || ratio || parallel-Thds Parameter setting error, please check the log for details.")
			vlog = fmt.Sprintf("(%d) [%s]  chanRowCount || queue-size || ratio || parallel-Thds parameter must be greater than 0.", rc.LogThreadSeq, Event)
			global.Wlog.Error(vlog)
			os.Exit(1)
		}
	}
	if rc.SecondaryL.RulesV.Ratio > 100 {
		fmt.Println("GreatSQL report: Ratio Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  Ratio value must be between 1 and 100.", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}

	vlog = fmt.Sprintf("(%d) [%s] start init check mode values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if rc.SecondaryL.RulesV.CheckMode == "count" {
		rc.SecondaryL.RepairV.Datafix = "no"
	}
	vlog = fmt.Sprintf("(%d) [%s]  check check mode parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RulesV.CheckMode)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init trx conn pool values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.ConnPoolV.PoolMin = rc.SecondaryL.RulesV.ParallelThds*3 + 10
	vlog = fmt.Sprintf("(%d) [%s]  check trx conn pool message is {%d}.", rc.LogThreadSeq, Event, rc.ConnPoolV.PoolMin)
	global.Wlog.Debug(vlog)

	rc.NoIndexTableTmpFile = "tmp_file"
	if rc.SecondaryL.RulesV.CheckObject == "data" {
		rc.SecondaryL.StructV.ScheckMod = "loose"

	}
	vlog = fmt.Sprintf("(%d) [%s]  Validity verification of configuration parameters completed !!!", rc.LogThreadSeq, Event)
	global.Wlog.Info(vlog)

}

func (rc *ConfigParameter) readConfigFile(config string) {
	//初始化传参
	if rc.Config != "" {
		if !strings.Contains(config, "/") {
			sysType := runtime.GOOS
			if sysType == "linux" {
				config = fmt.Sprintf("./%s", config)
			} else if sysType == "windows" {
				config = fmt.Sprintf(".\\%s", config)
			}
		}
	}
}
