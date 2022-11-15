package global

import (
	"bufio"
	"bytes"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs" /* 引入日志回滚功能 */
	"github.com/rifflock/lfshook"                       /* logrus本地文件系统钩子 */
	"github.com/sirupsen/logrus"                        /* logrus日志包 */
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

/* 创建logrus 日志实例 */
var Logger = logrus.New()

/* 定义日志级别 */
const LOG_TRACE = 0
const LOG_DEBUG = 1
const LOG_INFO = 2
const LOG_WARN = 3
const LOG_ERROR = 4
const LOG_FATAL = 5
const LOG_PANIC = 6

//Custom log format definition
/*logrus原生支持两种日志格式，一种是text，另一种是json格式，同时也支持自定义格式，当前根据自己需求进行自定义*/
/*首先定义一个结构体*/
type wlLogFormatter struct{}

/*定义一个方法*/
func (s *wlLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	var msg string
	timestamp := time.Now().Local().Format("2006/01/02 15:04:05")
	//HasCaller()为true才会有调用信息
	if entry.HasCaller() {
		fName := filepath.Base(entry.Caller.File)
		msg = fmt.Sprintf("%s [%s] [%s:%d %s] %s\n", timestamp, strings.ToUpper(entry.Level.String()), fName, entry.Caller.Line, entry.Caller.Function, entry.Message)
	} else {
		msg = fmt.Sprintf("%s [%s]  %s\n", timestamp, strings.ToUpper(entry.Level.String()), entry.Message)
	}
	b.WriteString(msg)
	return b.Bytes(), nil
}

/* 使用闭包特性，初始化带回滚功能的logrus日志环境 */
func LoggerToFile() func(int, ...interface{}) {
	/* 日志路径和名称 */
	logFilePath := "E:\\万里开源\\goProject\\tableCheckSum\\src\\wl-table-checkSum"
	logFileName := "table-checksum"
	partFileName := path.Join(logFilePath, logFileName)
	/* 禁止日志打印到标准输出stdout */
	devnull, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Printf("LoggerToFile open os.DevNull failed: ", err)
	}
	writernull := bufio.NewWriter(devnull)
	/*设置日志的输出方式，默认为两种，一种是os.stid，一种是io.write */
	Logger.SetOutput(writernull)
	/* */
	Logger.SetReportCaller(true)
	/* 设置日志输出格式 */
	Logger.SetFormatter(&wlLogFormatter{})
	/* 设置默认日志级别为 INFO */
	Logger.SetLevel(logrus.InfoLevel)

	/* 创建日志回滚实例，日志名称格式，日志回滚模式（日志每20M回滚，保留10个日志文件） */
	logWriter, err := rotatelogs.New(
		partFileName+".%Y%m%d.log",
		rotatelogs.WithLinkName(logFileName+".log"), /* 链接文件，链接到当前实际的日志文件  */
		//WithRotationTime设置日志分割时间，多长时间切割一次
		rotatelogs.WithRotationTime(time.Hour*24),
		//rotatelogs.WithRotationTime(time.Second),
		//WithMaxAge和WithRotationCount二者只能设置一个，
		//WithMaxAge设置文件清理前的最长保存时间
		//WithRotationCount设置文件清理前最多保存的个数
		//rotatelogs.WithMaxAge(time.Hour*24)
		rotatelogs.WithRotationSize(50*1024*1024),
		rotatelogs.WithRotationCount(10),
	)
	/* 日志输出到本地文件系统，不同级别都输出到相同的日志中 */
	writeMap := lfshook.WriterMap{
		logrus.InfoLevel:  logWriter,
		logrus.FatalLevel: logWriter,
		logrus.DebugLevel: logWriter,
		logrus.WarnLevel:  logWriter,
		logrus.ErrorLevel: logWriter,
		logrus.PanicLevel: logWriter,
	}
	/* 创建新的lfs钩子 */
	lfHook := lfshook.NewHook(writeMap, &wlLogFormatter{})

	/* logrus实例添加lfshook钩子 */
	Logger.AddHook(lfHook)

	/* 返回日志函数实例，这里可以根据level参数，实现不同级别的日志输出控制 */
	return func(level int, args ...interface{}) {
		loginfo := fmt.Sprintf("%v", args)
		switch level {
		case 0:
			Logger.Trace(loginfo)
		case 1:
			Logger.Debug(loginfo)
		case 2:
			Logger.Info(loginfo)
		case 3:
			Logger.Warn(loginfo)
		case 4:
			Logger.Error(loginfo)
		case 5:
			Logger.Fatal(loginfo)
		case 6:
			Logger.Panic(loginfo)
		}
	}
}

/* 创建一个日志函数实例（闭包） */
var testLog = LoggerToFile()

type loggsStruct struct{}
type loggsInter interface {
	TraceWrite()
	DebugWrite()
	InfoWrite()
	WarnWrite()
	ErrorWrite()
	FatalWrite()
	PanicWrite()
}

func (log *loggsStruct) TraceWrite(msg string) {
	testLog(LOG_TRACE, msg)
}
func (log *loggsStruct) DebugWrite(msg string) {
	testLog(LOG_DEBUG, msg)
}
func (log *loggsStruct) InfoWrite(msg string) {
	testLog(LOG_INFO, msg)
}
func (log *loggsStruct) WarnWrite(msg string) {
	testLog(LOG_WARN, msg)
}
func (log *loggsStruct) ErrorWrite(msg string) {
	testLog(LOG_ERROR, msg)
}
func (log *loggsStruct) FatalWrite(msg string) {
	testLog(LOG_FATAL, msg)
}
func (log *loggsStruct) PanicWrite(msg string) {
	testLog(LOG_PANIC, msg)
}
func Glog() loggsStruct {
	var log = loggsStruct{}
	return log
}
