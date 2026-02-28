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

type RuntimeTuneSnapshot struct {
	ParallelThds int
	QueueSize    int
	ChunkSize    int
}

var runtimeTuneState = struct {
	sync.RWMutex
	value RuntimeTuneSnapshot
}{}

func initRuntimeTuneState(config *inputArg.ConfigParameter) {
	if config == nil {
		return
	}
	runtimeTuneState.Lock()
	runtimeTuneState.value = RuntimeTuneSnapshot{
		ParallelThds: config.SecondaryL.RulesV.ParallelThds,
		QueueSize:    config.SecondaryL.RulesV.QueueSize,
		ChunkSize:    config.SecondaryL.RulesV.ChanRowCount,
	}
	runtimeTuneState.Unlock()
}

func GetRuntimeTuneSnapshot() RuntimeTuneSnapshot {
	runtimeTuneState.RLock()
	defer runtimeTuneState.RUnlock()
	return runtimeTuneState.value
}

func setRuntimeTuneSnapshot(v RuntimeTuneSnapshot) {
	runtimeTuneState.Lock()
	runtimeTuneState.value = v
	runtimeTuneState.Unlock()
}

// MemoryMonitor monitors the memory usage of the program asynchronously.
func MemoryMonitor(memoryLimit string, config *inputArg.ConfigParameter) {
	limitMB := parseMemoryLimit(memoryLimit)
	if limitMB == 0 {
		return
	}
	initRuntimeTuneState(config)

	// 用于跟踪参数调整次数
	adjustmentCount := 0

	// 定义GC触发阈值为内存限制的92%
	gcThresholdMB := int(float64(limitMB) * 0.92)
	// 超过该阈值进入应急释放流程（120%）
	emergencyThresholdMB := int(float64(limitMB) * 1.2)
	// 超过该阈值且应急多次失败时才退出（避免直接退出）
	fatalThresholdMB := int(float64(limitMB) * 1.35)

	// 定义内存下降阈值，当内存使用量下降到限制的88%以下时才允许再次调整
	memoryDecreaseThresholdMB := int(float64(limitMB) * 0.88)

	// 标记是否刚刚进行过参数调整
	justAdjusted := false

	// 记录上次输出GC提示的时间，用于控制输出频率
	lastGCTime := time.Now()
	lastForcedGC := time.Now().Add(-10 * time.Second)
	lastEmergencyAt := time.Now().Add(-10 * time.Second)
	emergencyFailureCount := 0

	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			currentMB := getCurrentMemoryUsage()

			// 当内存使用接近限制时触发垃圾回收
			if currentMB >= gcThresholdMB && currentMB < limitMB {
				if time.Since(lastForcedGC) >= 2*time.Second {
					runtime.GC()
					lastForcedGC = time.Now()
				}

				// 控制GC提示信息输出频率，最多每2秒输出一次
				if time.Since(lastGCTime) >= 2*time.Second {
					gcMessage := fmt.Sprintf("Info: Memory usage %dMB approaching limit, triggering garbage collection", currentMB)
					//fmt.Println(gcMessage)
					if global.Wlog != nil {
						global.Wlog.Info(gcMessage)
					}
					// 更新上次输出GC提示的时间
					lastGCTime = time.Now()
				}

				// 再次检查内存使用
				currentMB = getCurrentMemoryUsage()
			}

			// 如果刚刚进行过参数调整，检查内存是否已经下降到阈值以下
			if justAdjusted && currentMB < memoryDecreaseThresholdMB {
				justAdjusted = false
				memoryDecreaseMsg := fmt.Sprintf("\nInfo: Memory usage decreased to %dMB, below threshold, ready for next adjustment if needed", currentMB)
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
				currentTune := GetRuntimeTuneSnapshot()
				minimalParams := currentTune.ParallelThds <= 1 &&
					currentTune.QueueSize <= 1 &&
					currentTune.ChunkSize <= 100
				if minimalParams {
					// 当达到120%时先尝试应急释放，而不是直接退出
					if currentMB >= emergencyThresholdMB {
						if time.Since(lastEmergencyAt) >= 2*time.Second {
							lastEmergencyAt = time.Now()
							currentMB = emergencyMemoryRelief(currentMB, limitMB)
							if currentMB < limitMB {
								emergencyFailureCount = 0
								justAdjusted = true
								continue
							}
							emergencyFailureCount++
						}
						if currentMB >= fatalThresholdMB && emergencyFailureCount >= 3 {
							fmt.Printf("\nFatal error: Current memory usage %dMB remains above emergency threshold after relief attempts (limit=%dMB, threshold=%dMB). Exiting...\n",
								currentMB, limitMB, emergencyThresholdMB)
							os.Exit(1)
						}
						// 继续运行，等待下一轮监控和应急释放
						continue
					}

					// 低于120%时仅尝试释放并继续，不做强退
					currentMB = emergencyMemoryRelief(currentMB, limitMB)
					if currentMB < limitMB {
						emergencyFailureCount = 0
						justAdjusted = true
					}
					continue
				}

				// 增加调整计数
				adjustmentCount++

				// 记录调整前的参数值
				prevParallelThds := currentTune.ParallelThds
				prevQueueSize := currentTune.QueueSize
				prevChunkSize := currentTune.ChunkSize

				// 计算新的参数值，采用更温和的调整策略
				// 所有参数都统一调整为原来的90%
				newParallelThds := max(1, int(float64(prevParallelThds)*0.9))
				newQueueSize := max(1, int(float64(prevQueueSize)*0.9))
				newChunkSize := max(100, int(float64(prevChunkSize)*0.9)) // 最小值为100，避免过小影响性能

				// 仅更新运行时参数快照，避免并发写配置对象
				setRuntimeTuneSnapshot(RuntimeTuneSnapshot{
					ParallelThds: newParallelThds,
					QueueSize:    newQueueSize,
					ChunkSize:    newChunkSize,
				})

				// 设置参数变更标志并发送通知
				paramChangeMutex.Lock()
				ParamChanged = true
				paramChangeMutex.Unlock()

				// 非阻塞地发送参数变更通知
				select {
				case ParamChangedChan <- struct{}{}:
					// 通知已发送
					fmt.Printf("\nInfo: Parameter change notification sent, new values - ParallelThds: %d, QueueSize: %d, ChunkSize: %d\n", newParallelThds, newQueueSize, newChunkSize)
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
				emergencyFailureCount = 0
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

func emergencyMemoryRelief(currentMB, limitMB int) int {
	// Run a short relief sequence to reclaim heap/OS pages before giving up.
	for i := 0; i < 2; i++ {
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(150 * time.Millisecond)
		currentMB = getCurrentMemoryUsage()
		if currentMB < limitMB {
			return currentMB
		}
	}
	return currentMB
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
