package inputArg

import (
	"fmt"
	"gopkg.in/ini.v1"
	"gt-checksum/global"
	"gt-checksum/go-log/log"
	"os"
	"runtime"
	"strings"
)

type FirstLevel struct {
	DSNs   *ini.Section
	Schema *ini.Section
	Rules  *ini.Section
	Struct *ini.Section
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
	Tables              string
	IgnoreTables        string
	CheckNoIndexTable   string
	CaseSensitiveObjectName string
}
type RulesS struct {
	ParallelThds int
	ChanRowCount int
	QueueSize    int
	CheckMode    string
	Ratio        int
	CheckObject  string
}
type StructS struct {
	ScheckMod     string
	ScheckOrder   string
	ScheckFixRule string
}
type LogS struct {
	LogFile  string
	LogLevel string
}
type RepairS struct {
	Datafix     string
	FixTrxNum   int
	FixFileName string
	FixFileFINE *os.File
}
type SecondaryLevel struct {
	DsnsV   DSNsS
	SchemaV SchemaS
	RulesV  RulesS
	StructV StructS
	LogV    LogS
	RepairV RepairS
}
type ConnPool struct {
	PoolMin int
	PoolMax int
}
type ConfigParameter struct {
	FirstL              FirstLevel
	SecondaryL          SecondaryLevel
	ConfFine            *ini.File
	ConnPoolV           ConnPool
	Config              string //配置文件信息
	LogThreadSeq        int64
	NoIndexTableTmpFile string
}

var rc ConfigParameter

func init() {
	rc.cliHelp()
	fmt.Println("\ngt-checksum is initializing")
	fmt.Println("gt-checksum is reading configuration files")
	if rc.Config == "" {
		if _, err := os.Stat("gc.conf"); err == nil {
			rc.Config = "gc.conf"
			fmt.Println("gt-checksum: Automatically loading configuration file 'gc.conf' from current directory.")
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
		rc.getConfig()
	}
	//初始化日志文件
	fmt.Println("gt-checksum is opening log files")
	global.Wlog = log.NewWlog(rc.SecondaryL.LogV.LogFile, rc.SecondaryL.LogV.LogLevel)
	fmt.Println("gt-checksum is checking options")
	rc.checkPar()
}

func ConfigInit(logThreadSeq int64) *ConfigParameter {
	rc.LogThreadSeq = logThreadSeq
	return &rc
}
