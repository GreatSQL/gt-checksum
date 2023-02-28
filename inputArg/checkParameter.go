package inputArg

import (
	"errors"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/go-log/log"
	"os"
	"regexp"
	"runtime"
	"strings"
)

type ConfigParameter struct {
	config                                       string   //配置文件信息
	SourceJdbc, DestJdbc, SourceDrive, DestDrive string   //源端的连接信息
	PoolMin                                      int      //数据库连接池最小值
	Table, Igtable                               string   //待校验的表和忽略的表
	CheckNoIndexTable                            string   //是否校验无索引表
	LowerCaseTableNames                          string   //是否忽略校验表的大小写
	LogFile, LogLevel                            string   //关于日志输出信息配置
	Concurrency                                  int      //查询并发度
	SingleIndexChanRowCount                      int      //单索引列校验数据块长度
	JointIndexChanRowCount                       int      //多列索引校验数据块长度
	QueueDepth                                   int      //数据块长度
	Datafix, FixFileName                         string   //差异数据修复的方式及配置
	IncCheckSwitch                               string   //增量数据校验
	CheckMode                                    string   //校验的方式，可以为count(*)或者是校验row数据
	CheckObject                                  string   //校验的对象，可以是struct或者是data
	Ratio                                        int      //配置数据抽检时配置的比例
	Sfile                                        *os.File //修复文件的文件句柄
	FixTrxNum                                    int      //单并发修复语句的事务数量
}

var illegalParameterStatus = false

//判断库表配置参数是否存在非法参数
var rexPat = func(rl *readConf, rex *regexp.Regexp, rexStr string, illegalParameterStatus bool) {
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
		rl.getErr("table/ignoreTable Parameter setting error.", errors.New("parameter error"))
	}
}
var fileExsit = func(rl *readConf, logFile string) {
	if _, err4 := os.Stat(logFile); err4 != nil {
		if _, err5 := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err5 != nil {
			rl.getErr("Failed to create a log file. Procedure.", err5)
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

func (rc *readConf) getErr(msg string, err error) {
	if err != nil {
		fmt.Println(err, ":", msg)
		os.Exit(0)
	}
}

/*
	校验输入参数是否合规
*/
func (rc *readConf) checkPar(cp *ConfigParameter, logThreadSeq int64) {
	var (
		vlog  string
		Event = "C_check_Parameter"
	)

	if cp.SourceDrive == "oracle" {
		cp.SourceDrive = "godror"
	}
	if cp.DestDrive == "oracle" {
		cp.DestDrive = "godror"
	}
	tmpDbc := dbExec.DBConnStruct{}
	tmpDbc.DBDevice = cp.SourceDrive
	tmpDbc.JDBC = cp.SourceJdbc
	vlog = fmt.Sprintf("(%d) [%s] Start to verify the legality of configuration parameters...", logThreadSeq, Event)
	global.Wlog.Info(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  source DB node connection message {%s}, start to check it...", logThreadSeq, Event, cp.SourceJdbc)
	global.Wlog.Debug(vlog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		fmt.Println("GreatSQL report: source DB connection fail, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  source DB connection message error! error message is {%s}", logThreadSeq, Event, err)
		global.Wlog.Error(vlog)
		os.Exit(0)
	}
	vlog = fmt.Sprintf("(%d) [%s]  source DB node connection message oK!", logThreadSeq, Event)
	global.Wlog.Debug(vlog)

	tmpDbc.DBDevice = cp.DestDrive
	tmpDbc.JDBC = cp.DestJdbc
	vlog = fmt.Sprintf("(%d) [%s]  dest DB node connection message {%s}, start to check it...", logThreadSeq, Event, cp.DestJdbc)
	global.Wlog.Debug(vlog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		fmt.Println("GreatSQL report: dest DB connection fail, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  dest DB connection message error!. error message is {%s}", logThreadSeq, Event, err)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  dest DB node connection message oK!", logThreadSeq, Event)
	global.Wlog.Debug(vlog)

	//表级别的正则匹配
	vlog = fmt.Sprintf("(%d) [%s]  start check table Name and ignore table Name Legitimacy.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.Table == "" {
		fmt.Println("GreatSQL report: table Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  table cannot all be set to nil! ", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	tabr, _ := regexp.Compile(`[0-9a-zA-Z!@_{}*%-]\.[0-9a-zA-Z!@_{}%*-]`)
	rexPat(rc, tabr, cp.Table, illegalParameterStatus)
	rexPat(rc, tabr, cp.Igtable, illegalParameterStatus)
	if cp.Table == cp.Igtable {
		fmt.Println("GreatSQL report: table or ignoretable Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  The test form and the skip form cannot be consistent! ", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	//判断*.*之外是否还包含其他的值
	if strings.Contains(cp.Table, "*.*") {
		table := strings.Replace(cp.Table, "*.*", "", 1)
		for _, i := range strings.Split(table, ",") {
			ii := strings.TrimSpace(i)
			if ii != "" {
				fmt.Println("GreatSQL report: table Parameter setting error, please check the log for details.")
				vlog = fmt.Sprintf("(%d) [%s]  The table parameter configures *.* and contains other values! ", logThreadSeq, Event)
				global.Wlog.Error(vlog)
				os.Exit(1)
			}
		}
	}
	var cc []string
	for _, i := range strings.Split(cp.Table, ",") {
		cc = append(cc, strings.TrimSpace(i))
	}
	cp.Table = strings.Join(cc, ",")
	if strings.HasSuffix(cp.Table, ",") {
		cp.Table = cp.Table[:len(cp.Table)-1]
	}
	if cp.LowerCaseTableNames == "no" {
		cp.Table = strings.ToUpper(strings.TrimSpace(cp.Table))
		cp.Igtable = strings.ToUpper(strings.TrimSpace(cp.Igtable))
	}
	vlog = fmt.Sprintf("(%d) [%s]  check table parameter message is {table: %s ignore table: %s}", logThreadSeq, Event, cp.Table, cp.Igtable)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init check object values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	cp.CheckObject = strings.ToLower(cp.CheckObject)
	vlog = fmt.Sprintf("(%d) [%s]  check object parameter message is {%s}.", logThreadSeq, Event, cp.CheckObject)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init check mode values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	cp.CheckMode = strings.ToLower(cp.CheckMode)
	vlog = fmt.Sprintf("(%d) [%s]  check mode parameter message is {%s}.", logThreadSeq, Event, cp.CheckMode)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init no index table values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	cp.CheckNoIndexTable = strings.ToLower(cp.CheckNoIndexTable)
	vlog = fmt.Sprintf("(%d) [%s] check no index table parameter message is {%s}.", logThreadSeq, Event, cp.CheckNoIndexTable)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init lower case table name values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	cp.LowerCaseTableNames = strings.ToLower(cp.LowerCaseTableNames)
	vlog = fmt.Sprintf("(%d) [%s]  check lower case table name parameter message is {%s}.", logThreadSeq, Event, cp.LowerCaseTableNames)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init log out values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	//判断日志输入参数
	if cp.LogFile == "" {
		cp.LogFile = "./gt-checksum.log"
	} else {
		fileExsit(rc, cp.LogFile)
	}
	fileExsit(rc, cp.LogFile)
	vlog = fmt.Sprintf("(%d) [%s]  check log out parameter message is {%s}.", logThreadSeq, Event, cp.LogFile)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init data fix file values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.Datafix == "file" {
		if cp.FixFileName == "" {
			cp.FixFileName = "./gt-checksum-DataFix.sql"
		} else {
			fileExsit(rc, cp.LogFile)
		}
	}

	if strings.EqualFold(cp.Datafix, "file") {
		vlog = fmt.Sprintf("(%d) [%s]  Open repair file {%s} handle.", logThreadSeq, Event, cp.FixFileName)
		global.Wlog.Debug(vlog)
		if _, err := os.Stat(cp.FixFileName); err == nil {
			os.Remove(cp.FixFileName)
		}
		sfile, err := os.OpenFile(cp.FixFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("GreatSQL report: fix file open fail, please check the log for details.")
			vlog = fmt.Sprintf("(%d) [%s]  Repair the file {%s} handle opening failure, the failure information is {%s}.", logThreadSeq, Event, cp.FixFileName, err)
			global.Wlog.Error(vlog)
			os.Exit(1)
		}
		cp.Sfile = sfile
		fileExsit(rc, cp.FixFileName)
		vlog = fmt.Sprintf("(%d) [%s]  check data fix file parameter message is {%s}.", logThreadSeq, Event, cp.FixFileName)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) [%s]  start init parallel-thds values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.Concurrency < int(1) {
		fmt.Println("GreatSQL report: table Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  parallel-thds parameter must be greater than 0.", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  check parallel-thds parameter message is {%d}.", logThreadSeq, Event, cp.Concurrency)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init SingleIndexChanRowCount values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.SingleIndexChanRowCount < int(1) {
		fmt.Println("GreatSQL report: singleIndexChanRowCount Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  singleIndexChanRowCount parameter must be greater than 0.", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  check singleIndexChanRowCount parameter message is {%d}.", logThreadSeq, Event, cp.SingleIndexChanRowCount)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init JointIndexChanRowCount values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.JointIndexChanRowCount < int(1) {
		fmt.Println("GreatSQL report: jointIndexChanRowCount Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  jointIndexChanRowCount parameter must be greater than 0.", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  check JointIndexChanRowCount parameter message is {%d}.", logThreadSeq, Event, cp.JointIndexChanRowCount)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]   start init queue-size values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.QueueDepth < int(1) {
		fmt.Println("GreatSQL report: queue-size Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  queue-size parameter must be greater than 0.", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  check queue-size parameter message is {%d}.", logThreadSeq, Event, cp.QueueDepth)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init Ratio values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.Ratio < 1 && cp.Ratio > 100 {
		fmt.Println("GreatSQL report: Ratio Parameter setting error, please check the log for details.")
		vlog = fmt.Sprintf("(%d) [%s]  Ratio value must be between 1 and 100.", logThreadSeq, Event)
		global.Wlog.Error(vlog)
		os.Exit(1)
	}
	vlog = fmt.Sprintf("(%d) [%s]  check Ratio parameter message is {%d}.", logThreadSeq, Event, cp.Ratio)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s] start init check mode values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	if cp.CheckMode == "count" {
		cp.Datafix = "no"
	}
	vlog = fmt.Sprintf("(%d) [%s]  check check mode parameter message is {%s}.", logThreadSeq, Event, cp.CheckMode)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  start init trx conn pool values.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	cp.PoolMin = cp.Concurrency*3 + 10
	vlog = fmt.Sprintf("(%d) [%s]  check trx conn pool message is {%d}.", logThreadSeq, Event, cp.PoolMin)
	global.Wlog.Debug(vlog)

	vlog = fmt.Sprintf("(%d) [%s]  Validity verification of configuration parameters completed !!!", logThreadSeq, Event)
	global.Wlog.Info(vlog)

}

func (rc *readConf) readConfigFile(config string, cp *ConfigParameter) {
	//初始化传参
	if cp.config != "" {
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

func NewConfigInit(logThreadSeq int64) *ConfigParameter {
	var (
		rc = readConf{}
		cp = &ConfigParameter{}
	)
	cliHelp(cp)
	fmt.Println("-- gt-checksum init configuration files -- ")
	if cp.config != "" {
		if !strings.Contains(cp.config, "/") {
			sysType := runtime.GOOS
			if sysType == "linux" {
				cp.config = fmt.Sprintf("./%s", cp.config)
			} else if sysType == "windows" {
				cp.config = fmt.Sprintf(".\\%s", cp.config)
			}
		}
		rc.getConfig(cp.config, cp)
	}
	//初始化日志文件
	fmt.Println("-- gt-checksum init log files -- ")
	global.Wlog = log.NewWlog(cp.LogFile, cp.LogLevel)

	fmt.Println("-- gt-checksum init check parameter --")
	rc.checkPar(cp, logThreadSeq)
	return cp
}
