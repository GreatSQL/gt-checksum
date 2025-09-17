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
	"time"
)

//type ConfigParameter struct {
//	config                                       string   //配置文件信息
//	SourceJdbc, DestJdbc, SourceDrive, DestDrive string   //源端的连接信息
//	PoolMin                                      int      //数据库连接池最小值
//	Table, Igtable                               string   //待校验的表和忽略的表
//	CheckNoIndexTable                            string   //是否校验无索引表
//	CaseSensitiveObjectName                          string   //是否忽略校验表的大小写
//	LogFile, LogLevel                            string   //关于日志输出信息配置
//	Concurrency                                  int      //查询并发度
//	ChunkSize                      				 int      //校验数据块长度
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

// 判断库表配置参数是否存在非法参数
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
		rc.getErr("tables/ignoreTables option incorrect", errors.New("option error"))
	}
}

func (rc *ConfigParameter) fileExsit(logFile string) {
	var err error
	// 支持日期时间格式，例如："./gt-checksum-%Y%m%d%H%M%S.log"
	if strings.Contains(logFile, "%") {
		currentTime := time.Now()
		// 替换常见的日期时间格式符
		logFile = strings.ReplaceAll(logFile, "%Y", currentTime.Format("2006"))
		logFile = strings.ReplaceAll(logFile, "%m", currentTime.Format("01"))
		logFile = strings.ReplaceAll(logFile, "%d", currentTime.Format("02"))
		logFile = strings.ReplaceAll(logFile, "%H", currentTime.Format("15"))
		logFile = strings.ReplaceAll(logFile, "%M", currentTime.Format("04"))
		logFile = strings.ReplaceAll(logFile, "%S", currentTime.Format("05"))
		logFile = strings.ReplaceAll(logFile, "%s", fmt.Sprintf("%d", currentTime.Unix()))
		logFile = strings.ReplaceAll(logFile, "%F", currentTime.Format("2006-01-02"))
		logFile = strings.ReplaceAll(logFile, "%T", currentTime.Format("15:04:05"))
	}

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
		Event = "C_check_Options"
		err   error
	)

	if rc.SecondaryL.DsnsV.SrcDrive == "oracle" {
		rc.SecondaryL.DsnsV.SrcDrive = "godror"
	} else if rc.SecondaryL.DsnsV.DestDrive == "oracle" {
		rc.SecondaryL.DsnsV.DestDrive = "godror"
	}

	tmpDbc := dbExec.DBConnStruct{DBDevice: rc.SecondaryL.DsnsV.SrcDrive, JDBC: rc.SecondaryL.DsnsV.SrcJdbc}
	vlog = fmt.Sprintf("(%d) [%s] read and check if the options are correct", rc.LogThreadSeq, Event)
	global.Wlog.Info(vlog)

	vlog = fmt.Sprintf("(%d) [%s] srcDSN is: {%s}", rc.LogThreadSeq, Event, rc.SecondaryL.DsnsV.SrcJdbc)
	global.Wlog.Debug(vlog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		fmt.Println(fmt.Sprintf("gt-checksum report: Failed to connect to srcDSN. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
		vlog = fmt.Sprintf("(%d) [%s] srcDSN connect failed: {%s}", rc.LogThreadSeq, Event, err)
		global.Wlog.Error(vlog)
		os.Exit(0)
	}
	vlog = fmt.Sprintf("(%d) [%s] dstDSN connected", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)

	tmpDbc.DBDevice = rc.SecondaryL.DsnsV.DestDrive
	tmpDbc.JDBC = rc.SecondaryL.DsnsV.DestJdbc
	vlog = fmt.Sprintf("(%d) [%s] dstDSN is: {%s}", rc.LogThreadSeq, Event, rc.SecondaryL.DsnsV.DestJdbc)
	global.Wlog.Debug(vlog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		fmt.Println(fmt.Sprintf("gt-checksum report: Failed to connect to dstDSN. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
		vlog = fmt.Sprintf("(%d) [%s] dstDSN connect failed: {%s}", rc.LogThreadSeq, Event, err)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s] dstDSN connected", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)

	//表级别的正则匹配
	vlog = fmt.Sprintf("(%d) [%s] Check whether the options v1 and v2 are set correctly", rc.LogThreadSeq, Event)

	global.Wlog.Debug(vlog)
	if rc.SecondaryL.SchemaV.Tables == "" {
		fmt.Println(fmt.Sprintf("gt-checksum report: The option \"tables\" is set incorrectly. Please check %s.", rc.SecondaryL.LogV.LogFile))
		vlog = fmt.Sprintf("(%d) [%s] the option \"tables\" cannot be empty", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	tabr, _ := regexp.Compile(`[0-9a-zA-Z!@_{}*%-]\.[0-9a-zA-Z!@_{}%*-]`)
	rc.rexPat(tabr, rc.SecondaryL.SchemaV.Tables, illegalParameterStatus)
	rc.rexPat(tabr, rc.SecondaryL.SchemaV.Tables, illegalParameterStatus)
	if rc.SecondaryL.SchemaV.Tables == rc.SecondaryL.SchemaV.IgnoreTables {
		fmt.Println(fmt.Sprintf("gt-checksum report: The option \"tables\" or \"ignoreTables\" is set incorrectly. Please check %s.", rc.SecondaryL.LogV.LogFile))
		vlog = fmt.Sprintf("(%d) [%s] The option \"table\" and \"ignoreTables\" cannot be the same", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	//判断*.*之外是否还包含其他的值
	if strings.Contains(rc.SecondaryL.SchemaV.Tables, "*.*") {
		table := strings.Replace(rc.SecondaryL.SchemaV.Tables, "*.*", "", 1)
		for _, i := range strings.Split(table, ",") {
			ii := strings.TrimSpace(i)
			if ii != "" {
				fmt.Println(fmt.Sprintf("gt-checksum report: The option \"tables\" is set incorrectly. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
				vlog = fmt.Sprintf("(%d) [%s] The table parameter configures *.* and contains other values! ", rc.LogThreadSeq, Event)
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
	if rc.SecondaryL.SchemaV.CaseSensitiveObjectName == "no" {
		rc.SecondaryL.SchemaV.Tables = strings.ToUpper(strings.TrimSpace(rc.SecondaryL.SchemaV.Tables))
		rc.SecondaryL.SchemaV.IgnoreTables = strings.ToUpper(strings.TrimSpace(rc.SecondaryL.SchemaV.IgnoreTables))
	}
	if rc.SecondaryL.SchemaV.Tables == "" {
		fmt.Println(fmt.Sprintf("gt-checksum report: The option \"tables\" is set incorrectly. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s] check table parameter message is {table: %s ignore table: %s}", rc.LogThreadSeq, Event, rc.SecondaryL.SchemaV.Tables, rc.SecondaryL.SchemaV.IgnoreTables)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init check object values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.RulesV.CheckObject = strings.ToLower(rc.SecondaryL.RulesV.CheckObject)
	vlog = fmt.Sprintf("(%d) [%s] check object parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RulesV.CheckObject)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init check mode values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.RulesV.CheckMode = strings.ToLower(rc.SecondaryL.RulesV.CheckMode)
	vlog = fmt.Sprintf("(%d) [%s] check mode parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RulesV.CheckMode)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init no index table values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.SchemaV.CheckNoIndexTable = strings.ToLower(rc.SecondaryL.SchemaV.CheckNoIndexTable)
	vlog = fmt.Sprintf("(%d) [%s] check no index table parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.SchemaV.CheckNoIndexTable)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init lower case table name values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.SecondaryL.SchemaV.CaseSensitiveObjectName = strings.ToLower(rc.SecondaryL.SchemaV.CaseSensitiveObjectName)
	vlog = fmt.Sprintf("(%d) [%s] check case sensitive object name parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.SchemaV.CaseSensitiveObjectName)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init log out values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	//判断日志输入参数
	rc.fileExsit(rc.SecondaryL.LogV.LogFile)
	vlog = fmt.Sprintf("(%d) [%s] check log out parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.LogV.LogFile)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init data fix file values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if rc.SecondaryL.RepairV.Datafix == "file" {
		vlog = fmt.Sprintf("(%d) [%s] Open repair file {%s} handle.", rc.LogThreadSeq, Event, rc.SecondaryL.RepairV.FixFileName)
		global.Wlog.Debug(vlog)
		if _, err = os.Stat(rc.SecondaryL.RepairV.FixFileName); err == nil {
			os.Remove(rc.SecondaryL.RepairV.FixFileName)
		}
		rc.fileExsit(rc.SecondaryL.RepairV.FixFileName)
		rc.SecondaryL.RepairV.FixFileFINE, err = os.OpenFile(rc.SecondaryL.RepairV.FixFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println(fmt.Sprintf("gt-checksum report: Failed to open the \"fixFileName\". Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
			vlog = fmt.Sprintf("(%d) [%s] Repair the file {%s} handle opening failure, the failure information is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RepairV.FixFileName, err)
			global.Wlog.Error(vlog)
			os.Exit(1)
		}
		vlog = fmt.Sprintf("(%d) [%s] check data fix file parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RepairV.FixFileName)
		global.Wlog.Debug(vlog)
	}
	for _, v := range []int{rc.SecondaryL.RulesV.ChanRowCount, rc.SecondaryL.RulesV.QueueSize, rc.SecondaryL.RulesV.Ratio, rc.SecondaryL.RulesV.ParallelThds} {
		if v < 1 {
			fmt.Println(fmt.Sprintf("gt-checksum report: The options \"chunkSize || queueSize || ratio || parallelThds\" set incorrectly. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
			vlog = fmt.Sprintf("(%d) [%s] chunkSize || queueSize || ratio || parallelThds parameter must be greater than 0.", rc.LogThreadSeq, Event)
			global.Wlog.Error(vlog)
			os.Exit(1)
		}
	}
	if rc.SecondaryL.RulesV.MemoryLimit < 100 || rc.SecondaryL.RulesV.MemoryLimit > 65536 {
		fmt.Println(fmt.Sprintf("gt-checksum report: The option \"memoryLimit\" must be between 100 and 65536. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
		vlog = fmt.Sprintf("(%d) [%s] option \"memoryLimit\" must be between 100 and 65536.", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	if rc.SecondaryL.RulesV.Ratio < 1 || rc.SecondaryL.RulesV.Ratio > 100 {
		fmt.Println(fmt.Sprintf("gt-checksum report: The option \"Ratio\" must be between 1 and 100. Please check %s or set option \"logLevel=debug\" to get more information.", rc.SecondaryL.LogV.LogFile))
		vlog = fmt.Sprintf("(%d) [%s] option \"Ratio\" must be between 1 and 100.", rc.LogThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}

	vlog = fmt.Sprintf("(%d) [%s] start init check mode values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if rc.SecondaryL.RulesV.CheckMode == "count" {
		rc.SecondaryL.RepairV.Datafix = "no"
	}
	vlog = fmt.Sprintf("(%d) [%s] check check mode parameter message is {%s}.", rc.LogThreadSeq, Event, rc.SecondaryL.RulesV.CheckMode)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init trx conn pool values.", rc.LogThreadSeq, Event)
	global.Wlog.Debug(vlog)
	rc.ConnPoolV.PoolMin = rc.SecondaryL.RulesV.ParallelThds*3 + 10
	vlog = fmt.Sprintf("(%d) [%s] check trx conn pool message is {%d}.", rc.LogThreadSeq, Event, rc.ConnPoolV.PoolMin)
	global.Wlog.Debug(vlog)

	rc.NoIndexTableTmpFile = "tmp_file"
	if rc.SecondaryL.RulesV.CheckObject == "data" {
		rc.SecondaryL.StructV.ScheckMod = "loose"

	}
	vlog = fmt.Sprintf("(%d) [%s] All options check have passed", rc.LogThreadSeq, Event)
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
