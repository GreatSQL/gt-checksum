package main

import (
	"context"
	"flag"
	"fmt"
	"gt-checksum/progress"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "repairDB execution failed:", err)
		os.Exit(1)
	}
}

func setupRepairSignalContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	stop := make(chan struct{})
	done := make(chan struct{})
	go handleRepairSignals(cancel, signals, stop, done)

	return ctx, func() {
		signal.Stop(signals)
		close(stop)
		cancel()
		<-done
	}
}

func handleRepairSignals(cancel context.CancelFunc, signals <-chan os.Signal, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	interrupted := false
	for {
		select {
		case sig, ok := <-signals:
			if !ok {
				return
			}
			if !interrupted {
				interrupted = true
				log.Printf("Received %s, stopping new SQL scheduling and waiting for in-flight files to finish\n", sig)
				cancel()
				continue
			}
			log.Printf("Received %s again; waiting for in-flight files to finish safely\n", sig)
		case <-stop:
			return
		}
	}
}

// run contains all repairDB logic and returns an error on failure.
// Using a dedicated run() function ensures deferred cleanup (logFile.Close) executes
// correctly on both success and error paths, since os.Exit bypasses deferred calls.
// Stage-level db.Close() is called explicitly (not via defer) inside the stage loop.
func run() (err error) {
	confFile := flag.String("conf", "gc.conf", "Config file path")
	force := flag.Bool("f", false, "Force execution without confirmation")
	forceLong := flag.Bool("force", false, "Force execution without confirmation")
	dryRun := flag.Bool("dry-run", false, "Dry run mode: show statistics only")
	resultFile := flag.String("result-file", "", "Custom output path for CSV report (default: result/repairDB-result-<timestamp>.csv)")
	flag.Parse()

	ctx, stopSignals := setupRepairSignalContext()
	defer stopSignals()

	forceMode := *force || *forceLong

	var specifiedFixFileDir string
	if len(flag.Args()) > 0 {
		specifiedFixFileDir = flag.Args()[0]
	}

	if specifiedFixFileDir != "" {
		if err := parseConfig(*confFile); err != nil {
			return fmt.Errorf("failed to parse config file: %v", err)
		}
		config.FixFileDir = specifiedFixFileDir
	} else {
		if err := parseConfig(*confFile); err != nil {
			return fmt.Errorf("failed to parse config file: %v", err)
		}
		if config.FixFileDir == "" {
			return fmt.Errorf("no fixFileDir specified in command line or config file")
		}
	}

	if _, err := os.Stat(config.FixFileDir); os.IsNotExist(err) {
		return fmt.Errorf("fixFileDir directory does not exist: %s", config.FixFileDir)
	}

	// Lock file lifecycle:
	// 1. Created at startup (in defer below)
	// 2. Exists while running or after completion/failure
	// 3. In resume mode: deleted if previous run failed (to allow restart)
	// 4. Recreated in defer at function exit
	lockPath := filepath.Join(config.FixFileDir, lockFileName)
	if err := checkLockFile(lockPath); err != nil {
		// Resume mode: check lock file content to decide whether to allow restart
		if config.Resume != "OFF" {
			lockContent, readErr := os.ReadFile(lockPath)
			if readErr != nil {
				return fmt.Errorf("failed to read lock file for resume check: %v", readErr)
			}

			// Empty lock file means previous execution completed successfully, no resume needed
			if len(lockContent) == 0 {
				fmt.Fprintf(os.Stderr, "Previous execution completed successfully. No resume needed.\n")
				fmt.Fprintf(os.Stderr, "To start a fresh run, remove the lock file: %s\n", lockPath)
				return fmt.Errorf("previous execution completed successfully, remove lock file to start fresh")
			}

			// Non-empty lock file means previous execution failed, can resume
			fmt.Fprintf(os.Stderr, "WARNING: %v\n", err)
			fmt.Fprintf(os.Stderr, "Resume mode enabled, previous execution failed. Resuming...\n")
			// Remove old lock file, will be recreated in defer
			if removeErr := os.Remove(lockPath); removeErr != nil && !os.IsNotExist(removeErr) {
				log.Printf("[WARN] Failed to remove old lock file: %v\n", removeErr)
			}
		} else {
			fmt.Fprintf(os.Stderr, "WARNING: %v\n", err)
			return fmt.Errorf("lock file check failed: %v", err)
		}
	}

	defer func() {
		var errMsg string
		if err != nil {
			errMsg = err.Error()
			fmt.Fprintf(os.Stderr, "Execution failed, writing lock file with error: %s\n", errMsg)
		} else {
			fmt.Fprintf(os.Stderr, "Execution completed successfully, writing empty lock file\n")
		}

		if writeErr := writeLockFile(lockPath, errMsg); writeErr != nil {
			fmt.Fprintf(os.Stderr, "CRITICAL WARNING: Failed to write lock file: %v\n", writeErr)
			fmt.Fprintf(os.Stderr, "Lock mechanism may not work for next execution!\n")
		}
	}()

	logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	logBinStr := "ON"
	if !config.LogBin {
		logBinStr = "OFF"
	}
	log.Printf("Configuration information:")
	log.Printf("  DstDSN: %s\n", config.DstDSN)
	log.Printf("  ParallelThds: %d\n", config.ParallelThds)
	log.Printf("  FixFileDir: %s\n", config.FixFileDir)
	log.Printf("  LogBin: %s\n", logBinStr)
	log.Printf("  LogFile: %s\n", config.LogFile)

	entries, err := os.ReadDir(config.FixFileDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}
	if len(entries) == 0 {
		log.Printf("fixFileDir directory is empty, exiting\n")
		log.Printf("repairDB executed successfully\n")
		return nil
	}

	var sqlFiles []string
	err = filepath.Walk(config.FixFileDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".sql") {
			sqlFiles = append(sqlFiles, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to traverse directory: %v", err)
	}

	if len(sqlFiles) == 0 {
		log.Printf("No .sql files found in fixFileDir directory\n")
		log.Printf("repairDB executed successfully\n")
		return nil
	}

	sqlFiles = uniqueFiles(sqlFiles)

	cf := classifySQLFiles(sqlFiles)
	stages := buildExecutionStages(cf)

	log.Printf("Stage classification: DELETE=%d TABLE=%d VIEW=%d ROUTINE=%d TRIGGER=%d UNKNOWN=%d\n",
		len(cf.Delete), len(cf.Table), len(cf.View), len(cf.Routine), len(cf.Trigger), len(cf.Unknown))

	if len(cf.Unknown) > 0 {
		sample := cf.Unknown
		if len(sample) > 3 {
			sample = sample[:3]
		}
		log.Printf("[WARN] %d file(s) could not be classified by type prefix and will execute last in UNKNOWN stage.\n", len(cf.Unknown))
		log.Printf("[WARN] UNKNOWN file examples: %s\n", strings.Join(sample, ", "))
	}

	stats, err := collectFixSQLStatistics(config.FixFileDir)
	if err != nil {
		return fmt.Errorf("收集统计信息失败: %v", err)
	}

	printStatisticsReport(stats)

	if *dryRun {
		log.Printf("Dry-run 模式，不执行修复操作\n")
		return nil
	}

	if !forceMode {
		confirmed, err := promptUserConfirmation()
		if err != nil {
			return err
		}
		if !confirmed {
			log.Printf("用户取消操作，退出\n")
			return nil
		}
	}

	if ctx.Err() != nil {
		return fmt.Errorf("repairDB interrupted before execution: %w", ctx.Err())
	}

	log.Printf("开始执行修复操作...\n")

	startTime := time.Now()

	// 断点续传：初始化进度文件
	var repairProgress *progress.RepairProgress
	progressPath := progress.RepairProgressFilePath(config.FixFileDir)

	if config.Resume != "OFF" {
		if p, err := progress.LoadRepairProgress(progressPath); err != nil {
			log.Printf("[WARN] Failed to load progress file: %v\n", err)
		} else if p != nil && p.IsRunning() {
			// 发现未完成的进度文件
			log.Printf("[RESUME] Found existing progress file: %s\n", progressPath)
			log.Printf("[RESUME] Previously executed files: %d\n", p.FileCount())
			log.Printf("[RESUME] Successful files: %d\n", p.SuccessCount())

			if config.Resume == "ASK" {
				fmt.Print("\nDo you want to resume from the last checkpoint? (y/n): ")
				var answer string
				fmt.Scanln(&answer)
				if strings.ToLower(strings.TrimSpace(answer)) == "y" {
					repairProgress = p
					log.Println("[RESUME] Resuming from checkpoint...")
				} else {
					log.Println("[RESUME] Starting fresh run...")
					if err := p.Remove(); err != nil {
						log.Printf("[WARN] Failed to remove old progress file: %v\n", err)
					}
				}
			} else {
				// resume == "ON"
				repairProgress = p
				log.Println("[RESUME] Auto-resuming from checkpoint...")
			}
		}
	}

	// 如果没有加载到已有进度文件，创建新的
	if repairProgress == nil && config.Resume != "OFF" {
		repairProgress = progress.NewRepairProgress(config.FixFileDir, progressPath)
		if saveErr := repairProgress.Save(); saveErr != nil {
			log.Printf("[WARN] Failed to create progress file: %v\n", saveErr)
			repairProgress = nil
		}
	}

	// SSL configuration
	dsn := parseDSN(config.DstDSN)
	hasSSL := config.SslMode != "" || config.SslCa != "" || config.SslCert != "" || config.SslKey != ""
	if hasSSL {
		sslMode := config.SslMode
		if sslMode == "" {
			sslMode = "PREFERRED"
		}
		tlsValue, err := setupSSLConfig(config.SslCa, config.SslCert, config.SslKey, sslMode)
		if err != nil {
			return fmt.Errorf("SSL configuration error: %v", err)
		}
		dsn = appendTLSToDSN(dsn, tlsValue)
		log.Printf("SSL mode: %s\n", sslMode)
	}

	var allResults []FileExecResult

	for _, stage := range stages {
		if ctx.Err() != nil {
			writeCSVIfPossible(allResults, time.Since(startTime), *resultFile)
			return fmt.Errorf("repairDB interrupted: %w", ctx.Err())
		}

		files := prepareStageFiles(stage)
		logExecutionPlan(stage.Name, files, config.FixFileDir)
		log.Printf("[%s] starting execution (%d files), concurrency: %d\n", stage.Name, len(files), config.ParallelThds)

		db, err := openExecutionDB(dsn)
		if err != nil {
			if len(allResults) > 0 {
				writeCSVIfPossible(allResults, time.Since(startTime), *resultFile)
			}
			return fmt.Errorf("[%s] failed to connect to database: %v", stage.Name, err)
		}
		stageResults, stageErr := parallelExecuteSQLFiles(ctx, db, files, stage.Name, repairProgress)
		allResults = append(allResults, stageResults...)
		db.Close()
		if stageErr != nil {
			writeCSVIfPossible(allResults, time.Since(startTime), *resultFile)
			return fmt.Errorf("[%s] execution failed: %v", stage.Name, stageErr)
		}

		log.Printf("[%s] execution completed\n", stage.Name)
	}

	totalTime := time.Since(startTime)

	writeCSVIfPossible(allResults, totalTime, *resultFile)

	// 断点续传：标记进度文件为已完成
	if repairProgress != nil {
		if err := repairProgress.MarkStatus(progress.StatusCompleted); err != nil {
			log.Printf("[WARN] Failed to mark progress as completed: %v\n", err)
		} else {
			log.Printf("Progress file marked as completed: %s\n", repairProgress.FilePath())
		}
	}

	minutes := int(totalTime.Minutes())
	seconds := totalTime.Seconds() - float64(minutes*60)
	formattedTime := fmt.Sprintf("%dm%.3fs", minutes, seconds)

	log.Printf("All SQL files execution completed, total time taken: %s\n", formattedTime)
	log.Printf("repairDB executed successfully\n")
	return nil
}

// writeCSVIfPossible writes the CSV report and logs a warning on failure.
// It does not return an error because CSV writing is non-fatal.
func writeCSVIfPossible(results []FileExecResult, totalTime time.Duration, cliResultFile string) {
	if len(results) == 0 {
		return
	}
	var resultPath string
	if cliResultFile != "" {
		resultPath = resolveRepairResultFilePath(cliResultFile)
	} else if config.ResultFile != "" {
		resultPath = resolveRepairResultFilePath(config.ResultFile)
	} else {
		resultPath = resolveRepairResultFilePath("")
	}
	if err := writeRepairCSVReport(results, totalTime, resultPath); err != nil {
		log.Printf("[WARN] Failed to write CSV report: %v\n", err)
	} else {
		log.Printf("CSV report written to: %s\n", resultPath)
	}
}
