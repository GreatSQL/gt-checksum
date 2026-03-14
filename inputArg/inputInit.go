package inputArg

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/go-log/log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"gopkg.in/ini.v1"
)

type FirstLevel struct {
	DSNs   *ini.Section
	Schema *ini.Section
	Rules  *ini.Section
	Logs   *ini.Section
	Repair *ini.Section
}
type DSNsS struct {
	SrcDSN    string
	DstDSN    string
	SrcDrive  string
	SrcJdbc   string
	DestDrive string
	DestJdbc  string
}
type SchemaS struct {
	Tables                  string
	IgnoreTables            string
	CheckNoIndexTable       string
	CaseSensitiveObjectName string
	SqlWhere                string
}

// RulesS defines checksum task execution and runtime control options.
type RulesS struct {
	ParallelThds   int
	ChanRowCount   int
	QueueSize      int
	CheckObject    string
	MemoryLimit    int
	ShowActualRows string
	// MariaDBJSONTargetType controls how MariaDB JSON alias columns are rewritten on MySQL targets.
	MariaDBJSONTargetType string
	IsRoutineCheck        bool // 标记是否同时检查存储过程和函数
}

type LogS struct {
	LogFile  string
	LogLevel string
}
type RepairS struct {
	Datafix         string
	FixTrxNum       int
	FixTrxSize      int
	InsertSqlSize   int
	DeleteSqlSize   int
	FixFileDir      string
	FixFileFINE     *os.File
	FixFilePerTable string
}
type SecondaryLevel struct {
	DsnsV   DSNsS
	SchemaV SchemaS
	RulesV  RulesS
	LogV    LogS
	RepairV RepairS
}
type ConnPool struct {
	PoolMin int
	PoolMax int
}

// ConfigParameter represents the runtime configuration state loaded from CLI and file.
type ConfigParameter struct {
	FirstL              FirstLevel
	SecondaryL          SecondaryLevel
	ConfFine            *ini.File
	ConnPoolV           ConnPool
	Config              string //配置文件信息
	CliFixTrxSize       int
	CliInsertSqlSize    int
	CliDeleteSqlSize    int
	CliShowActualRows   string
	LogThreadSeq        int64
	NoIndexTableTmpFile string
}

var rc ConfigParameter

func init() {
	if strings.HasSuffix(filepath.Base(os.Args[0]), ".test") {
		return
	}
	rc.cliHelp()
	fmt.Println("Initializing gt-checksum")
	fmt.Println("Reading configuration files")
	if rc.Config == "" {
		if _, err := os.Stat("gc.conf"); err == nil {
			rc.Config = "gc.conf"
			fmt.Println("Automatically loading configuration file 'gc.conf' from current directory")
		}
	}
	if rc.Config != "" {
		if !strings.Contains(rc.Config, "/") {
			sysType := runtime.GOOS
			if sysType == "linux" {
				rc.Config = fmt.Sprintf("./%s", rc.Config)
			} else if sysType == "windows" {
				rc.Config = fmt.Sprintf(".\\%s", rc.Config)
			}
		}
		rc.GetConfig()
	}
	//初始化日志文件
	fmt.Println("Opening log files")
	// 处理日期时间格式
	logFile := rc.SecondaryL.LogV.LogFile
	if strings.Contains(logFile, "%") {
		logFile = replaceDateTimeFormat(logFile)
	}
	global.Wlog = log.NewWlog(logFile, rc.SecondaryL.LogV.LogLevel)
	fmt.Println("Checking configuration options")
	rc.checkPar()
}

func ConfigInit(logThreadSeq int64) *ConfigParameter {
	rc.LogThreadSeq = logThreadSeq
	return &rc
}

// replaceDateTimeFormat 替换日期时间格式符为实际值
func replaceDateTimeFormat(filename string) string {
	now := time.Now()
	result := strings.ReplaceAll(filename, "%Y", now.Format("2006"))
	result = strings.ReplaceAll(result, "%m", now.Format("01"))
	result = strings.ReplaceAll(result, "%d", now.Format("02"))
	result = strings.ReplaceAll(result, "%H", now.Format("15"))
	result = strings.ReplaceAll(result, "%M", now.Format("04"))
	result = strings.ReplaceAll(result, "%S", now.Format("05"))
	result = strings.ReplaceAll(result, "%s", fmt.Sprintf("%d", now.Unix()))
	result = strings.ReplaceAll(result, "%F", now.Format("2006-01-02"))
	result = strings.ReplaceAll(result, "%T", now.Format("15:04:05"))
	return result
}

// GetGlobalConfig 返回全局配置的引用
func GetGlobalConfig() *ConfigParameter {
	return &rc
}
