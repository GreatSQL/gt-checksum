package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "repairDB execution failed:", err)
		os.Exit(1)
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

	lockPath := filepath.Join(config.FixFileDir, lockFileName)
	if err := checkLockFile(lockPath); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: %v\n", err)
		return fmt.Errorf("lock file check failed: %v", err)
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

	log.Printf("开始执行修复操作...\n")

	startTime := time.Now()

	dsn := parseDSN(config.DstDSN)
	var allResults []FileExecResult

	for _, stage := range stages {
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
		stageResults, stageErr := parallelExecuteSQLFiles(db, files, stage.Name)
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
