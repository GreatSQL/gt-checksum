package utils

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 参数变更通知机制
var (
	ParamChangedChan = make(chan struct{}, 1) // 用于通知参数变更的通道
	paramChangeMutex sync.Mutex               // 保护参数变更状态
	ParamChanged     bool                     // 标记参数是否已变更
)

// MemoryMonitor monitors the memory usage of the program asynchronously.
func MemoryMonitor(memoryLimit string, config *inputArg.ConfigParameter) {
	limitMB := parseMemoryLimit(memoryLimit)
	if limitMB == 0 {
		return
	}

	// 用于跟踪参数调整次数
	adjustmentCount := 0

	// 定义更保守的GC触发阈值，内存限制的60%就开始触发GC
	gcThresholdMB := int(float64(limitMB) * 0.6)

	// 定义更严格的内存警告阈值，内存限制的75%
	warnThresholdMB := int(float64(limitMB) * 0.75)

	// 定义内存下降阈值，当内存使用量下降到限制的80%以下时才允许再次调整
	memoryDecreaseThresholdMB := int(float64(limitMB) * 0.8)

	// 记录上次调整的时间
	lastAdjustmentTime := time.Now()

	// 标记是否刚刚进行过参数调整
	justAdjusted := false

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			// 获取当前内存使用情况
			currentMB := getCurrentMemoryUsage()
			currentVirtualMB := getVirtualMemory()

			// 输出详细的内存监控信息
			if adjustmentCount%20 == 0 { // 每1000ms输出一次
				global.Wlog.Debug(fmt.Sprintf("Memory usage: RSS=%dMB, Virtual=%dMB, Heap=%dMB, Limit=%dMB",
					currentMB, currentVirtualMB, getHeapMemory(), limitMB))
			}

			// 使用虚拟内存和RSS中的较大值进行监控
			monitorMB := currentMB
			if currentVirtualMB > monitorMB {
				monitorMB = currentVirtualMB
			}

			// 更激进的GC策略：
			// 1. 当内存达到60%限制时，触发GC
			// 2. 当内存达到75%限制时，连续触发多次GC
			// 3. 当内存达到90%限制时，触发GC并准备调整参数
			if monitorMB >= gcThresholdMB {
				gcCount := 1
				if monitorMB >= warnThresholdMB {
					gcCount = 3 // 更激进的GC
				}

				for i := 0; i < gcCount; i++ {
					// 手动触发GC，使用debug.FreeOSMemory()释放更多内存回操作系统
					debug.FreeOSMemory()
					runtime.GC()

					// 等待GC完成
					time.Sleep(50 * time.Millisecond)
				}

				gcMessage := fmt.Sprintf("Info: Memory usage approaching limit (RSS=%dMB, Virtual=%dMB), triggering garbage collection x%d", currentMB, currentVirtualMB, gcCount)
				fmt.Println(gcMessage)
				if global.Wlog != nil {
					global.Wlog.Warn(gcMessage)
				}

				// 再次检查内存使用
				currentMB = getCurrentMemoryUsage()
				currentVirtualMB = getVirtualMemory()
				monitorMB = currentMB
				if currentVirtualMB > monitorMB {
					monitorMB = currentVirtualMB
				}
			}

			// 如果刚刚进行过参数调整，检查内存是否已经下降到阈值以下
			if justAdjusted {
				if monitorMB < memoryDecreaseThresholdMB {
					justAdjusted = false
					memoryDecreaseMsg := fmt.Sprintf("Info: Memory usage decreased to RSS=%dMB, Virtual=%dMB, below threshold, ready for next adjustment if needed", currentMB, currentVirtualMB)
					fmt.Println(memoryDecreaseMsg)
					if global.Wlog != nil {
						global.Wlog.Warn(memoryDecreaseMsg)
					}
				} else if time.Since(lastAdjustmentTime) > 10*time.Second {
					// 如果超过10秒内存仍未下降，允许再次调整
					justAdjusted = false
					global.Wlog.Warn("Memory not decreasing after adjustment, allowing new adjustment")
				}
			}

			// 当内存超过75%限制或任何内存指标超过限制时，进行参数调整
			if (monitorMB >= warnThresholdMB || currentVirtualMB >= limitMB || currentMB >= limitMB) && !justAdjusted {
				// 清理临时文件并记录日志
				cleanupTmpFileAndLog(currentMB, limitMB, config)

				// 检查参数是否已经是最小值
				if config.SecondaryL.RulesV.ParallelThds <= 1 && config.SecondaryL.RulesV.QueueSize <= 1 && config.SecondaryL.RulesV.ChanRowCount <= 100 {
					// 所有参数已降至最小值，仍然内存超限，退出程序
					fmt.Printf("\nFatal error: Current memory usage RSS=%dMB, Virtual=%dMB has reached the limit (%dMB). Parameters already at minimal values. Exiting...\n", currentMB, currentVirtualMB, limitMB)
					os.Exit(1)
				}

				// 增加调整计数
				adjustmentCount++

				// 记录调整前的参数值
				prevParallelThds := config.SecondaryL.RulesV.ParallelThds
				prevQueueSize := config.SecondaryL.RulesV.QueueSize
				prevChunkSize := config.SecondaryL.RulesV.ChanRowCount

				// 采用更激进的参数调整策略
				// 所有参数都调整为原来的70%，更快地降低内存使用
				newParallelThds := max(1, int(float64(prevParallelThds)*0.7))
				newQueueSize := max(1, int(float64(prevQueueSize)*0.7))
				newChunkSize := max(100, int(float64(prevChunkSize)*0.7)) // 最小值为100，避免过小影响性能

				// 如果内存已经超过限制，进一步降低参数
				if monitorMB >= limitMB {
					newParallelThds = max(1, int(float64(newParallelThds)*0.8))
					newQueueSize = max(1, int(float64(newQueueSize)*0.8))
					newChunkSize = max(100, int(float64(newChunkSize)*0.8))
				}

				// 更新配置值
				config.SecondaryL.RulesV.ParallelThds = newParallelThds
				config.SecondaryL.RulesV.QueueSize = newQueueSize
				config.SecondaryL.RulesV.ChanRowCount = newChunkSize

				// 设置参数变更标志并发送通知
				paramChangeMutex.Lock()
				ParamChanged = true
				paramChangeMutex.Unlock()

				// 非阻塞地发送参数变更通知
				select {
				case ParamChangedChan <- struct{}{}:
					// 通知已发送
					fmt.Printf("Info: Parameter change notification sent, new values - ParallelThds: %d, QueueSize: %d, ChunkSize: %d\n",
						newParallelThds, newQueueSize, newChunkSize)
				default:
					// 通道已满，不阻塞
				}

				// 触发更彻底的GC，连续进行多次
				for i := 0; i < 3; i++ {
					debug.FreeOSMemory()
					runtime.GC()
					time.Sleep(30 * time.Millisecond)
				}

				// 输出调整信息
				adjustmentMsg := fmt.Sprintf("\nWarning: Memory usage exceeded limit (RSS=%dMB, Virtual=%dMB). Automatically reducing parameters:", currentMB, currentVirtualMB)
				adjustmentMsg += fmt.Sprintf("\n  - parallelThds: %d -> %d", prevParallelThds, newParallelThds)
				adjustmentMsg += fmt.Sprintf("\n  - queueSize: %d -> %d", prevQueueSize, newQueueSize)
				adjustmentMsg += fmt.Sprintf("\n  - chunkSize: %d -> %d", prevChunkSize, newChunkSize)
				adjustmentMsg += fmt.Sprintf("\n  - Garbage collection triggered after parameter adjustment")
				fmt.Println(adjustmentMsg)

				// 记录到日志
				if global.Wlog != nil {
					global.Wlog.Warn(adjustmentMsg)
				}

				// 增加等待时间，让系统有更多时间释放内存
				time.Sleep(800 * time.Millisecond)

				// 设置刚刚调整过的标志
				justAdjusted = true
				lastAdjustmentTime = time.Now()
			}

			// 增加调整计数
			adjustmentCount++
		}
	}()
}

// getVirtualMemory returns the virtual memory size in MB
func getVirtualMemory() int {
	// Read memory usage from /proc/self/statm
	file, err := os.Open("/proc/self/statm")
	if err != nil {
		// Fallback to runtime stats if we can't read /proc
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int(m.Alloc / 1024 / 1024)
	}
	defer file.Close()

	var size, resident, shared, text, lib, data, dt int
	_, err = fmt.Fscanf(file, "%d %d %d %d %d %d %d", &size, &resident, &shared, &text, &lib, &data, &dt)
	if err != nil {
		// Fallback to runtime stats if parsing fails
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int(m.Alloc / 1024 / 1024)
	}

	// Convert from pages to MB (each page is typically 4KB)
	return (size * 4) / 1024
}

// getHeapMemory returns the heap memory usage in MB
func getHeapMemory() int {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.Alloc / 1024 / 1024)
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// IsParamChanged 检查参数是否已变更
func IsParamChanged() bool {
	paramChangeMutex.Lock()
	defer paramChangeMutex.Unlock()
	return ParamChanged
}

// ResetParamChanged 重置参数变更标志
func ResetParamChanged() {
	paramChangeMutex.Lock()
	defer paramChangeMutex.Unlock()
	ParamChanged = false
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

// getCurrentMemoryUsage returns the current memory usage in MB
// It returns the maximum of RSS (resident set size) and allocated heap memory
func getCurrentMemoryUsage() int {
	// Get RSS memory from /proc/self/statm
	rss := getRSSMemory()

	// Get allocated heap memory from runtime
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapAlloc := int(m.Alloc / 1024 / 1024)

	// Use the larger of the two values for more conservative monitoring
	if rss > heapAlloc {
		return rss
	}
	return heapAlloc
}

// getRSSMemory returns the resident set size in MB
func getRSSMemory() int {
	// Read memory usage from /proc/self/statm
	// Format: size resident shared text lib data dt
	file, err := os.Open("/proc/self/statm")
	if err != nil {
		// Fallback to runtime stats if we can't read /proc
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int(m.Alloc / 1024 / 1024)
	}
	defer file.Close()

	var size, resident, shared, text, lib, data, dt int
	_, err = fmt.Fscanf(file, "%d %d %d %d %d %d %d", &size, &resident, &shared, &text, &lib, &data, &dt)
	if err != nil {
		// Fallback to runtime stats if parsing fails
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int(m.Alloc / 1024 / 1024)
	}

	// Convert from pages to MB (each page is typically 4KB)
	return (resident * 4) / 1024
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
				global.Wlog.Warn(fmt.Sprintf("Memory limit exceeded (%dMB/%dMB), %s cleaned up before exit", currentMB, limitMB, tmpFile))
			}
		} else {
			if global.Wlog != nil {
				global.Wlog.Warn(fmt.Sprintf("Memory limit exceeded (%dMB/%dMB), failed to clean up %s: %v", currentMB, limitMB, tmpFile, err))
			}
		}
	} else {
		if global.Wlog != nil {
			global.Wlog.Warn(fmt.Sprintf("Memory limit exceeded (%dMB/%dMB), no %s found to clean up", currentMB, limitMB, tmpFile))
		}
	}
}
