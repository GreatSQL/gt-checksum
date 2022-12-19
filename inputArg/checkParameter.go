package inputArg

import (
	"errors"
	"fmt"
	"greatdbCheck/dbExec"
	"greatdbCheck/global"
	"greatdbCheck/go-log/log"
	"os"
	"regexp"
	"runtime"
	"strings"
)

type ConfigParameter struct {
	config                                       string //配置文件信息
	SourceJdbc, DestJdbc, SourceDrive, DestDrive string //源端的连接信息
	PoolMin                                      int    //数据库连接池最小值
	//TableM, IgtableM                             string   //待校验的库和忽略的库
	Table, Igtable          string   //待校验的表和忽略的表
	CheckNoIndexTable       string   //是否校验无索引表
	LowerCaseTableNames     string   //是否忽略校验表的大小写
	LogFile, LogLevel       string   //关于日志输出信息配置
	Concurrency             int      //查询并发度
	SingleIndexChanRowCount int      //单索引列校验数据块长度
	JointIndexChanRowCount  int      //多列索引校验数据块长度
	QueueDepth              int      //数据块长度
	Datafix, FixFileName    string   //差异数据修复的方式及配置
	IncCheckSwitch          string   //增量数据校验
	CheckMode               string   //校验的方式，可以为count(*)或者是校验row数据
	CheckObject             string   //校验的对象，可以是struct或者是data
	Ratio                   int      //配置数据抽检时配置的比例
	Sfile                   *os.File //修复文件的文件句柄
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
	//sysType := runtime.GOOS
	//var logFile string
	//if sysType == "linux" {
	//	logFile = fmt.Sprintf("%s/%s", logpath, logfile)
	//} else if sysType == "windows" {
	//	logFile = fmt.Sprintf("%s\\%s", logpath, logfile)
	//}
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

/*
   判断文件夹是否存在，不存在则创建文件夹
*/
func (rc *readConf) pathExists(path string) (bool, error) {
	var err error
	if _, err = os.Stat(path); err != nil {
		return false, err
	}
	if os.IsNotExist(err) {
		err = os.Mkdir(path, os.ModePerm)
		if err != nil {
			return false, err
		}
	}
	return true, nil
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
func (rc *readConf) checkPar(cp *ConfigParameter) {
	var logThreadSeq int64
	if cp.SourceDrive == "oracle" {
		cp.SourceDrive = "godror"
	}
	if cp.DestDrive == "oracle" {
		cp.DestDrive = "godror"
	}
	tmpDbc := dbExec.DBConnStruct{}
	tmpDbc.DBDevice = cp.SourceDrive
	tmpDbc.JDBC = cp.SourceJdbc
	alog := fmt.Sprintf("(%d) source DB node connection message {%s}, start to check it...", logThreadSeq, cp.SourceJdbc)
	global.Wlog.Info(alog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		alog = fmt.Sprintf("(%d) source DB connection message error! ", logThreadSeq)
		global.Wlog.Error(alog)
		os.Exit(0)
	}
	blog := fmt.Sprintf("(%d) source DB node connection message oK!", logThreadSeq)
	global.Wlog.Info(blog)
	tmpDbc.DBDevice = cp.DestDrive
	tmpDbc.JDBC = cp.DestJdbc
	clog := fmt.Sprintf("(%d) dest DB node connection message {%s}, start to check it...", logThreadSeq, cp.DestJdbc)
	global.Wlog.Info(clog)
	if _, err := tmpDbc.OpenDB(); err != nil {
		clog = fmt.Sprintf("(%d) dest DB connection message error! ", logThreadSeq)
		global.Wlog.Error(clog)
		os.Exit(0)
	}
	dlog := fmt.Sprintf("(%d) dest DB node connection message oK!", logThreadSeq)
	global.Wlog.Info(dlog)
	elog := fmt.Sprintf("(%d) Init table message {%s}, start to check it...", logThreadSeq, cp.Table)
	global.Wlog.Info(elog)

	//表级别的正则匹配
	glog := fmt.Sprintf("(%d) start check table Name and ignore table Name Legitimacy.", logThreadSeq)
	global.Wlog.Info(glog)
	if cp.Table == "" {
		rc.getErr("table cannot all be set to nil ", errors.New("parameter error"))
	}
	tabr, _ := regexp.Compile(`[0-9a-zA-Z!@_{}*%-]\.[0-9a-zA-Z!@_{}%*-]`)
	rexPat(rc, tabr, cp.Table, illegalParameterStatus)
	rexPat(rc, tabr, cp.Igtable, illegalParameterStatus)
	if cp.Table == cp.Igtable {
		rc.getErr("The test form and the skip form cannot be consistent.", errors.New("parameter error"))
	}
	//判断*.*之外是否还包含其他的值
	if strings.Contains(cp.Table, "*.*") {
		table := strings.Replace(cp.Table, "*.*", "", 1)
		for _, i := range strings.Split(table, ",") {
			ii := strings.TrimSpace(i)
			if ii != "" {
				rc.getErr("table Parameter setting error.", errors.New("parameter error"))
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
	hlog := fmt.Sprintf("(%d) check table parameter message is {table: %s ignore table: %s}", logThreadSeq, cp.Table, cp.Igtable)
	global.Wlog.Info(hlog)

	ilog := fmt.Sprintf("(%d) start init check object values.", logThreadSeq)
	global.Wlog.Info(ilog)
	cp.CheckObject = strings.ToLower(cp.CheckObject)
	jlog := fmt.Sprintf("(%d) check object parameter message is {%s}.", logThreadSeq, cp.CheckObject)
	global.Wlog.Info(jlog)

	klog := fmt.Sprintf("(%d) start init check mode values.", logThreadSeq)
	global.Wlog.Info(klog)
	cp.CheckMode = strings.ToLower(cp.CheckMode)
	llog := fmt.Sprintf("(%d) check mode parameter message is {%s}.", logThreadSeq, cp.CheckMode)
	global.Wlog.Info(llog)

	mlog := fmt.Sprintf("(%d) start init no index table values.", logThreadSeq)
	global.Wlog.Info(mlog)
	cp.CheckNoIndexTable = strings.ToLower(cp.CheckNoIndexTable)
	nlog := fmt.Sprintf("(%d) check no index table parameter message is {%s}.", logThreadSeq, cp.CheckNoIndexTable)
	global.Wlog.Info(nlog)

	olog := fmt.Sprintf("(%d) start init lower case table name values.", logThreadSeq)
	global.Wlog.Info(olog)
	cp.LowerCaseTableNames = strings.ToLower(cp.LowerCaseTableNames)
	plog := fmt.Sprintf("(%d) check lower case table name parameter message is {%s}.", logThreadSeq, cp.LowerCaseTableNames)
	global.Wlog.Info(plog)

	qlog := fmt.Sprintf("(%d) start init log out values.", logThreadSeq)
	global.Wlog.Info(qlog)
	//判断日志输入参数
	if cp.LogFile == "" {
		cp.LogFile = "./gt-checksum.log"
	} else {
		if exit, err2 := rc.pathExists(cp.LogFile); !exit {
			rc.getErr("The log Path parameters error.", err2)
		}
	}
	fileExsit(rc, cp.LogFile)
	rlog := fmt.Sprintf("(%d) check log out parameter message is {%s}.", logThreadSeq, cp.LogFile)
	global.Wlog.Info(rlog)

	slog := fmt.Sprintf("(%d) start init data fix file values.", logThreadSeq)
	global.Wlog.Info(slog)
	if cp.FixFileName == "" {
		cp.FixFileName = "./greatdbCheckDataFix.sql"
	} else {
		if exit, err2 := rc.pathExists(cp.FixFileName); !exit {
			rc.getErr("The fix Path parameters error.", err2)
		}
	}

	tlog := fmt.Sprintf("(%d) Open repair file {%s} handle.", logThreadSeq, cp.FixFileName)
	global.Wlog.Info(tlog)
	if _, err := os.Stat(cp.FixFileName); err == nil {
		os.Remove(cp.FixFileName)
	}
	sfile, err := os.OpenFile(cp.FixFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		ulog := fmt.Sprintf("(%d) Repair the file {%s} handle opening failure, the failure information is {%s}.", logThreadSeq, cp.FixFileName, err)
		global.Wlog.Error(ulog)
		os.Exit(1)
	}
	cp.Sfile = sfile

	fileExsit(rc, cp.FixFileName)
	ulog := fmt.Sprintf("(%d) check data fix file parameter message is {%s}.", logThreadSeq, cp.FixFileName)
	global.Wlog.Info(ulog)

	vlog := fmt.Sprintf("(%d) start init parallel-thds values.", logThreadSeq)
	global.Wlog.Info(vlog)
	if cp.Concurrency < int(1) {
		rc.getErr("parallel-thds parameter must be greater than 0", errors.New("parameter error"))
	}
	wlog := fmt.Sprintf("(%d) check parallel-thds parameter message is {%d}.", logThreadSeq, cp.Concurrency)
	global.Wlog.Info(wlog)

	xlog := fmt.Sprintf("(%d) start init SingleIndexChanRowCount values.", logThreadSeq)
	global.Wlog.Info(xlog)
	if cp.SingleIndexChanRowCount < int(1) {
		rc.getErr("singleIndexChanRowCount parameter must be greater than 0", errors.New("parameter error"))
	}
	//oracle的where 条件中使用in 关联条件最大不能超过1000个，所以需要在此处进行限制单列索引的in数量，报错信息： dpiStmt_execute: ORA-01795: maximum number of expressions in a list is 1000
	if cp.SingleIndexChanRowCount > 1000 && (cp.DestDrive == "godror" || cp.SourceDrive == "godror") {
		cp.SingleIndexChanRowCount = 1000
	}
	ylog := fmt.Sprintf("(%d) check singleIndexChanRowCount parameter message is {%d}.", logThreadSeq, cp.SingleIndexChanRowCount)
	global.Wlog.Info(ylog)

	zlog := fmt.Sprintf("(%d) start init JointIndexChanRowCount values.", logThreadSeq)
	global.Wlog.Info(zlog)
	if cp.JointIndexChanRowCount < int(1) {
		rc.getErr("jointIndexChanRowCount parameter must be greater than 0", errors.New("parameter error"))
	}
	a1log := fmt.Sprintf("(%d) check JointIndexChanRowCount parameter message is {%d}.", logThreadSeq, cp.JointIndexChanRowCount)
	global.Wlog.Info(a1log)

	b1log := fmt.Sprintf("(%d)  start init queue-size values.", logThreadSeq)
	global.Wlog.Info(b1log)
	if cp.QueueDepth < int(1) {
		rc.getErr("queue-size parameter must be greater than 0", errors.New("parameter error"))
	}
	c1log := fmt.Sprintf("(%d) check queue-size parameter message is {%d}.", logThreadSeq, cp.QueueDepth)
	global.Wlog.Info(c1log)

	d1log := fmt.Sprintf("(%d) start init Ratio values.", logThreadSeq)
	global.Wlog.Info(d1log)
	if cp.Ratio < 1 && cp.Ratio > 100 {
		rc.getErr("Failed to get Ratio parameters", errors.New("parameter error"))
	}
	e1log := fmt.Sprintf("(%d) check Ratio parameter message is {%d}.", logThreadSeq, cp.Ratio)
	global.Wlog.Info(e1log)

	f1log := fmt.Sprintf("(%d) start init check mode values.", logThreadSeq)
	global.Wlog.Info(f1log)
	if cp.CheckMode == "count" {
		cp.Datafix = "no"
	}
	g1log := fmt.Sprintf("(%d) check check mode parameter message is {%s}.", logThreadSeq, cp.CheckMode)
	global.Wlog.Info(g1log)

	h1log := fmt.Sprintf("(%d) start init trx conn pool values.", logThreadSeq)
	global.Wlog.Info(h1log)
	cp.PoolMin = cp.Concurrency*3 + 10
	i1log := fmt.Sprintf("(%d) check trx conn pool message is {%d}.", logThreadSeq, cp.PoolMin)
	global.Wlog.Info(i1log)
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

func NewConfigInit() *ConfigParameter {
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

	global.Wlog.Info("(0) Initializing gt-checksum parameter.")
	fmt.Println("-- gt-checksum check parameter --")
	rc.checkPar(cp)
	return cp
}
