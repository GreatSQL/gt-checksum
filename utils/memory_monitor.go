package utils

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"


)

// MemoryMonitor monitors the memory usage of the program asynchronously.
func MemoryMonitor(memoryLimit string) {
	limitMB := parseMemoryLimit(memoryLimit)
	if limitMB == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			currentMB := getCurrentMemoryUsage()
			if currentMB >= limitMB {
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
