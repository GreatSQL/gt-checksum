package main

import (
	"fmt"
	"os"
	"strings"
)

// Global variables
var (
	config Config
)

// parseConfig parses the configuration file
func parseConfig(confFile string) error {
	content, err := os.ReadFile(confFile)
	if err != nil {
		return fmt.Errorf("Failed to read config file: %v", err)
	}

	logbinSet := false
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, ";") || line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "dstDSN":
			config.DstDSN = value
		case "parallelThds":
			fmt.Sscanf(value, "%d", &config.ParallelThds)
		case "fixFileDir":
			config.FixFileDir = value
		case "logbin":
			logbinSet = true
			switch strings.ToUpper(value) {
			case "OFF":
				config.LogBin = false
			case "ON":
				config.LogBin = true
			default:
				return fmt.Errorf("invalid value for logbin: %q (must be ON or OFF)", value)
			}
		case "resultFile":
			config.ResultFile = value
		}
	}

	if config.ParallelThds <= 0 {
		config.ParallelThds = 4
	}
	if config.FixFileDir == "" {
		config.FixFileDir = "./fixsql"
	}
	if !logbinSet {
		config.LogBin = true // default: keep sql_log_bin ON
	}
	config.LogFile = "repairDB.log"

	if config.DstDSN == "" {
		return fmt.Errorf("Missing dstDSN parameter in config file")
	}

	return nil
}

// parseDSN extracts the raw DSN from the "mysql|..." prefixed format
func parseDSN(dsn string) string {
	parts := strings.Split(dsn, "|")
	if len(parts) != 2 {
		return dsn
	}
	return parts[1]
}
