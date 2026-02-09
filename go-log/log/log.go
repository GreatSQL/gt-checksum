package log

import (
	"fmt"
	"os"
	"time"
)

var logger = NewDefault(newStdHandler())

// SetDefaultLogger changes the global logger
func SetDefaultLogger(l *Logger) {
	logger = l
}

// SetLevel changes the logger level
func SetLevel(level Level) {
	logger.SetLevel(level)
}

// SetLevelByName changes the logger level by name
func SetLevelByName(name string) {
	logger.SetLevelByName(name)
}

// Fatal records the log with fatal level and exits
func Fatal(args ...interface{}) {
	logger.Output(2, LevelFatal, fmt.Sprint(args...))
	os.Exit(1)
}

// Fatalf records the log with fatal level and exits
func Fatalf(format string, args ...interface{}) {
	logger.Output(2, LevelFatal, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Fatalln records the log with fatal level and exits
func Fatalln(args ...interface{}) {
	logger.Output(2, LevelFatal, fmt.Sprintln(args...))
	os.Exit(1)
}

// Panic records the log with fatal level and panics
func Panic(args ...interface{}) {
	msg := fmt.Sprint(args...)
	logger.Output(2, LevelError, msg)
	panic(msg)
}

// Panicf records the log with fatal level and panics
func Panicf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	logger.Output(2, LevelError, msg)
	panic(msg)
}

// Panicln records the log with fatal level and panics
func Panicln(args ...interface{}) {
	msg := fmt.Sprintln(args...)
	logger.Output(2, LevelError, msg)
	panic(msg)
}

// Print records the log with trace level
func Print(args ...interface{}) {
	logger.Output(2, LevelTrace, fmt.Sprint(args...))
}

// Printf records the log with trace level
func Printf(format string, args ...interface{}) {
	logger.Output(2, LevelTrace, fmt.Sprintf(format, args...))
}

// Println records the log with trace level
func Println(args ...interface{}) {
	logger.Output(2, LevelTrace, fmt.Sprintln(args...))
}

// Debug records the log with debug level
func Debug(args ...interface{}) {
	logger.Output(2, LevelDebug, fmt.Sprint(args...))
}

// Debugf records the log with debug level
func Debugf(format string, args ...interface{}) {
	logger.Output(2, LevelDebug, fmt.Sprintf(format, args...))
}

// Debugln records the log with debug level
func Debugln(args ...interface{}) {
	logger.Output(2, LevelDebug, fmt.Sprintln(args...))
}

// Error records the log with error level
func Error(args ...interface{}) {
	logger.Output(2, LevelError, fmt.Sprint(args...))
}

// Errorf records the log with error level
func Errorf(format string, args ...interface{}) {
	logger.Output(2, LevelError, fmt.Sprintf(format, args...))
}

// Errorln records the log with error level
func Errorln(args ...interface{}) {
	logger.Output(2, LevelError, fmt.Sprintln(args...))
}

// Info records the log with info level
func Info(args ...interface{}) {
	logger.Output(2, LevelInfo, fmt.Sprint(args...))
}

// Infof records the log with info level
func Infof(format string, args ...interface{}) {
	logger.Output(2, LevelInfo, fmt.Sprintf(format, args...))
}

// Infoln records the log with info level
func Infoln(args ...interface{}) {
	logger.Output(2, LevelInfo, fmt.Sprintln(args...))
}

// Warn records the log with warn level
func Warn(args ...interface{}) {
	logger.Output(2, LevelWarn, fmt.Sprint(args...))
}

// Warnf records the log with warn level
func Warnf(format string, args ...interface{}) {
	logger.Output(2, LevelWarn, fmt.Sprintf(format, args...))
}

// Warnln records the log with warn level
func Warnln(args ...interface{}) {
	logger.Output(2, LevelWarn, fmt.Sprintln(args...))
}

func NewWlog(logfile, logLevel string) *Logger {
	// 检查 logfile 是否存在且不为空
	if _, err := os.Stat(logfile); err == nil {
		// 文件存在，检查是否为空
		fileInfo, err := os.Stat(logfile)
		if err == nil && fileInfo.Size() > 0 {
			// 文件不为空，将其重命名为 logfile-时间戳
			timestamp := time.Now().Format("20060102150405")
			oldLogFile := logfile + "-" + timestamp
			// 如果旧文件已经存在，删除它
			if _, err := os.Stat(oldLogFile); err == nil {
				os.Remove(oldLogFile)
			}
			// 重命名当前文件
			os.Rename(logfile, oldLogFile)
		}
	}

	// 创建或打开新的 logfile
	fp, err := os.OpenFile(logfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("open log file or create log file fail. Errof info: ", err)
		os.Exit(1)
	}
	h, err := NewStreamHandler(fp)
	if err != nil {
		Errorln("create log file StreamHandler fail. Errof info: ", err)
		os.Exit(1)
	}
	wlog := NewDefault(h)
	wlog.SetLevelByName(logLevel)
	return wlog
}
