package inputArg

import (
	"errors"
	"fmt"
	"greatdbCheck/dbExec"
	"os"
	"regexp"
	"runtime"
	"strings"
)

type ConfigParameter struct {
	config                                       string //配置文件信息
	SourceJdbc, DestJdbc, SourceDrive, DestDrive string //源端的连接信息
	PoolMin, PoolMax                             int    //数据库连接池最小值
	Schema, Igschema                             string //待校验的库和忽略的库
	Table, Igtable                               string //待校验的表和忽略的表
	CheckNoIndexTable                            string //是否校验无索引表
	LowerCaseTableNames                          string //是否忽略校验表的大小写
	LogPath, LogFile, LogLevel                   string //关于日志输出信息配置
	Concurrency                                  int    //查询并发度
	SingleIndexChanRowCount                      int    //单索引列校验数据块长度
	JointIndexChanRowCount                       int    //多列索引校验数据块长度
	QueueDepth                                   int    //数据块长度
	Datafix, FixPath, FixFileName                string //差异数据修复的方式及配置
	IncCheckSwitch                               string //增量数据校验
	CheckMode                                    string //校验的方式，可以为count(*)或者是校验row数据
	CheckObject                                  string //校验的对象，可以是struct或者是data
	Ratio                                        int    //配置数据抽检时配置的比例
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
		rl.getErr("schema/table/ignoreSchema/ignoreTable Parameter setting error.", errors.New("parameter error"))
	}
}
var fileExsit = func(rl *readConf, logpath, logfile string) {
	sysType := runtime.GOOS
	var logFile string
	if sysType == "linux" {
		logFile = fmt.Sprintf("%s/%s", logpath, logfile)
	} else if sysType == "windows" {
		logFile = fmt.Sprintf("%s\\%s", logpath, logfile)
	}
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
		//errorinfo := fmt.Sprintf("%v error Msg: %v", msg, err)
		fmt.Println(err, ":", msg)
		os.Exit(0)
	}
}

/*
	校验输入参数是否合规
*/
func (rc *readConf) checkPar(cp *ConfigParameter) {
	if cp.SourceDrive == "oracle" {
		cp.SourceDrive = "godror"
	}
	if cp.DestDrive == "oracle" {
		cp.DestDrive = "godror"
	}
	tmpDbc := dbExec.DBConnStruct{}
	tmpDbc.DBDevice = cp.SourceDrive
	tmpDbc.JDBC = cp.SourceJdbc
	tmpDbc.OpenDB()

	tmpDbc.DBDevice = cp.DestDrive
	tmpDbc.JDBC = cp.DestJdbc
	tmpDbc.OpenDB()

	if cp.PoolMax < int(1) {
		rc.getErr("poolMax parameter must be greater than 0", errors.New("parameter error"))
	}
	if cp.PoolMin < int(1) {
		rc.getErr("poolMin parameter must be greater than 0", errors.New("parameter error"))
	}

	if strings.ToUpper(cp.Table) == "ALL" || strings.ToUpper(cp.Table) == "ALL" || strings.ToUpper(cp.Table) == "ALL" { //不法参数
		rc.getErr("table or ignoreTable Cannot be set to ALL.", errors.New("parameter error"))
	}
	schr, _ := regexp.Compile(`[0-9a-zA-Z!@_{}-]`)
	rexPat(rc, schr, cp.Schema, illegalParameterStatus)
	rexPat(rc, schr, cp.Igschema, illegalParameterStatus)
	//表级别的正则匹配
	tabr, _ := regexp.Compile(`[0-9a-zA-Z!@_{}-]\.[0-9a-zA-Z!@_{}-]`)
	rexPat(rc, tabr, cp.Table, illegalParameterStatus)
	rexPat(rc, tabr, cp.Igtable, illegalParameterStatus)

	if strings.ToUpper(cp.Schema) == "ALL" {
		cp.Schema = "*"
	}
	if strings.ToUpper(cp.Table) == "NIL" {
		cp.Table = ""
	}
	if strings.ToUpper(cp.Schema) == "NIL" {
		cp.Schema = ""
	}
	if strings.ToUpper(cp.Igschema) == "NIL" {
		cp.Igschema = ""
	}
	if strings.ToUpper(cp.Igtable) == "NIL" {
		cp.Igtable = ""
	}
	//判断校验库表参数
	if cp.Schema == "" && cp.Table == "" { //库为空，表为空
		rc.getErr("schema and  table cannot all be set to nil ", errors.New("parameter error"))
	} else if cp.Schema != "" && cp.Table == "" { //库不为空，表为空
		cp.Table = "*"
	} else if (cp.Schema != "") && cp.Table != "" { //库为空，表不为空
		cp.Schema = schemaTableFilter(cp.Schema, cp.Table)
	}

	//判断忽略库表参数
	cp.Igschema = schemaTableFilter(cp.Igschema, cp.Igtable)

	if cp.Schema == cp.Igschema {
		cp.Schema = ""
		cp.Igschema = ""
	} else {
		a := strings.Split(cp.Schema, ",")
		b := strings.Split(cp.Igschema, ",")
		var c, d = make(map[string]int), make(map[string]int)
		var e, f []string
		for _, i := range a {
			c[i] = 0
		}
		for _, i := range b {
			d[i] = 0
		}
		for k, _ := range c {
			if _, ok := d[k]; ok {
				delete(c, k)
				delete(d, k)
			} else {
				e = append(e, k)
			}
		}
		for k, _ := range d {
			f = append(f, k)
		}
		cp.Schema = strings.Join(e, ",")
		cp.Igschema = strings.Join(f, ",")
	}
	cp.CheckObject = strings.ToLower(cp.CheckObject)
	cp.CheckMode = strings.ToLower(cp.CheckMode)
	cp.CheckNoIndexTable = strings.ToLower(cp.CheckNoIndexTable)
	cp.LowerCaseTableNames = strings.ToLower(cp.LowerCaseTableNames)
	//判断日志输入参数
	if cp.LogPath == "" {
		rc.getErr("Failed to get logPath parameters", errors.New("parameter error"))
	} else {
		if exit, err2 := rc.pathExists(cp.LogPath); !exit {
			rc.getErr("The log Path parameters error.", err2)
		}
	}
	fileExsit(rc, cp.LogPath, cp.LogFile)

	if cp.Concurrency < int(1) {
		rc.getErr("concurrency parameter must be greater than 0", errors.New("parameter error"))
	}
	if cp.SingleIndexChanRowCount < int(1) {
		rc.getErr("singleIndexChanRowCount parameter must be greater than 0", errors.New("parameter error"))
	}
	if cp.JointIndexChanRowCount < int(1) {
		rc.getErr("jointIndexChanRowCount parameter must be greater than 0", errors.New("parameter error"))
	}
	if cp.QueueDepth < int(1) {
		rc.getErr("QueueDepth parameter must be greater than 0", errors.New("parameter error"))
	}
	//if cp.SingleIndexChanRowCount/cp.Concurrency > 5 {
	//	cp.SingleIndexChanRowCount = cp.SingleIndexChanRowCount / cp.Concurrency
	//}
	//if cp.JointIndexChanRowCount/cp.Concurrency > 5 {
	//	cp.JointIndexChanRowCount = cp.JointIndexChanRowCount / cp.Concurrency
	//}
	if cp.Ratio < 1 && cp.Ratio > 100 {
		rc.getErr("Failed to get Ratio parameters", errors.New("parameter error"))
	}
	if cp.FixFileName == "" {
		cp.FixFileName = "greatdbCheckDataFix.sql"
	}
	if cp.FixPath == "" {
		rc.getErr("Failed to get fixPath parameters", errors.New("parameter error"))
	} else {
		if exit, err2 := rc.pathExists(cp.FixPath); !exit {
			rc.getErr("The fix Path parameters error.", err2)
		}
	}
	fileExsit(rc, cp.FixPath, cp.FixFileName)
	if cp.PoolMin/cp.Concurrency < 3 || (cp.PoolMin/cp.Concurrency == 3 && cp.PoolMin%cp.Concurrency == 0) {
		rc.getErr("The poolMin parameter is at least three times the concurrency parameter.", errors.New("parameter error"))
	}
	if cp.CheckMode == "count" {
		cp.Datafix = "no"
	}

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
	rc.checkPar(cp)
	return cp
}
