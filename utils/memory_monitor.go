package utils

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"runtime"
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

	// 定义GC触发阈值为内存限制的80%
	gcThresholdMB := int(float64(limitMB) * 0.8)

	// 定义内存下降阈值，当内存使用量下降到限制的90%以下时才允许再次调整
	memoryDecreaseThresholdMB := int(float64(limitMB) * 0.9)

	// 标记是否刚刚进行过参数调整
	justAdjusted := false

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			currentMB := getCurrentMemoryUsage()

			// 当内存使用接近限制时触发垃圾回收
			if currentMB >= gcThresholdMB && currentMB < limitMB {
				runtime.GC()
				gcMessage := fmt.Sprintf("Info: Memory usage %dMB approaching limit, triggering garbage collection", currentMB)
				//fmt.Println(gcMessage)
				if global.Wlog != nil {
					global.Wlog.Info(gcMessage)
				}

				// 等待GC完成
				time.Sleep(100 * time.Millisecond)
				// 再次检查内存使用
				currentMB = getCurrentMemoryUsage()
			}

			// 如果刚刚进行过参数调整，检查内存是否已经下降到阈值以下
			if justAdjusted && currentMB < memoryDecreaseThresholdMB {
				justAdjusted = false
				memoryDecreaseMsg := fmt.Sprintf("Info: Memory usage decreased to %dMB, below threshold, ready for next adjustment if needed", currentMB)
				fmt.Println(memoryDecreaseMsg)
				if global.Wlog != nil {
					global.Wlog.Info(memoryDecreaseMsg)
				}
			}

			// 只有当内存使用超过限制且不在调整后的冷却期时，才进行参数调整
			if currentMB >= limitMB && !justAdjusted {
				// 清理临时文件并记录日志
				cleanupTmpFileAndLog(currentMB, limitMB, config)

				// 检查参数是否已经是最小值
				if config.SecondaryL.RulesV.ParallelThds <= 1 && config.SecondaryL.RulesV.QueueSize <= 1 && config.SecondaryL.RulesV.ChanRowCount <= 100 {
					// 所有参数已降至最小值，仍然内存超限，退出程序
					fmt.Printf("\nFatal error: Current memory usage %dMB has reached the limit (%dMB). Parameters already at minimal values. Exiting...\n", currentMB, limitMB)
					os.Exit(1)
				}

				// 增加调整计数
				adjustmentCount++

				// 记录调整前的参数值
				prevParallelThds := config.SecondaryL.RulesV.ParallelThds
				prevQueueSize := config.SecondaryL.RulesV.QueueSize
				prevChunkSize := config.SecondaryL.RulesV.ChanRowCount

				// 计算新的参数值，采用更温和的调整策略
				// 所有参数都统一调整为原来的90%
				newParallelThds := max(1, int(float64(prevParallelThds)*0.9))
				newQueueSize := max(1, int(float64(prevQueueSize)*0.9))
				newChunkSize := max(100, int(float64(prevChunkSize)*0.9)) // 最小值为100，避免过小影响性能

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
					fmt.Printf("Info: Parameter change notification sent, new values - ParallelThds: %d, QueueSize: %d, ChunkSize: %d\n", newParallelThds, newQueueSize, newChunkSize)
				default:
					// 通道已满，不阻塞
				}

				// 触发垃圾回收
				runtime.GC()

				// 输出调整信息
				adjustmentMsg := fmt.Sprintf("\nWarning: Memory usage %dMB reached limit (%dMB). Automatically reducing parameters:", currentMB, limitMB)
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
				time.Sleep(500 * time.Millisecond)

				// 设置刚刚调整过的标志
				justAdjusted = true
			}
		}
	}()
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
