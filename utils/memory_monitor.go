package utils

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// MemoryMonitor monitors the memory usage of the program asynchronously.
func MemoryMonitor(memoryLimit string, config *inputArg.ConfigParameter) {
	limitMB := parseMemoryLimit(memoryLimit)
	if limitMB == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			currentMB := getCurrentMemoryUsage()
			if currentMB >= limitMB {
				// 清理临时文件并记录日志
				cleanupTmpFileAndLog(currentMB, limitMB, config)
				fmt.Printf("\nFatal error: Current memory usage %dMB has reached the limit (%dMB). Exiting...\n", currentMB, limitMB)
				os.Exit(1)
			}
		}
	}()
}

func parseMemoryLimit(memoryLimit string) int {
	if memoryLimit == "" {
		return 0
	}

	memoryLimit = strings.ToUpper(memoryLimit)
	if strings.HasSuffix(memoryLimit, "MB") {
		value, err := strconv.Atoi(strings.TrimSuffix(memoryLimit, "MB"))
		if err != nil {
			return 0
		}
		if value < 100 {
			return 100
		}
		return value
	} else if strings.HasSuffix(memoryLimit, "GB") {
		value, err := strconv.Atoi(strings.TrimSuffix(memoryLimit, "GB"))
		if err != nil {
			return 0
		}
		return value * 1024
	}
	return 0
}

func getCurrentMemoryUsage() int {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.Alloc / 1024 / 1024)
}

// cleanupTmpFileAndLog 清理临时文件并记录内存溢出日志
func cleanupTmpFileAndLog(currentMB, limitMB int, config *inputArg.ConfigParameter) {
	// 使用配置中的临时文件名
	tmpFile := "tmp_file"
	if config != nil && config.NoIndexTableTmpFile != "" {
		tmpFile = config.NoIndexTableTmpFile
	}
	
	if _, err := os.Stat(tmpFile); err == nil {
		if err := os.Remove(tmpFile); err == nil {
			if global.Wlog != nil {
				global.Wlog.Error(fmt.Sprintf("Memory limit exceeded (%dMB/%dMB), %s cleaned up before exit", currentMB, limitMB, tmpFile))
			}
		} else {
			if global.Wlog != nil {
				global.Wlog.Error(fmt.Sprintf("Memory limit exceeded (%dMB/%dMB), failed to clean up %s: %v", currentMB, limitMB, tmpFile, err))
			}
		}
	} else {
		if global.Wlog != nil {
			global.Wlog.Error(fmt.Sprintf("Memory limit exceeded (%dMB/%dMB), no %s found to clean up", currentMB, limitMB, tmpFile))
		}
	}
}
