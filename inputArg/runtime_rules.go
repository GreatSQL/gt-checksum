package inputArg

import (
	"os"
	"strings"
	"sync"
	"time"
)

const defaultShowActualRows = "ON"

var showActualRowsRuntimeCache = struct {
	sync.RWMutex
	file     string
	modTime  time.Time
	lastRead time.Time
	value    string
}{
	value: defaultShowActualRows,
}

func normalizeOnOff(v string) string {
	v = strings.TrimSpace(v)
	if idx := strings.IndexAny(v, ";#"); idx >= 0 {
		v = strings.TrimSpace(v[:idx])
	}
	v = strings.Trim(v, `"'`)
	if fields := strings.Fields(v); len(fields) > 0 {
		v = fields[0]
	}
	v = strings.ToUpper(strings.TrimSpace(v))
	if v == "ON" {
		return "ON"
	}
	return "OFF"
}

func readLastConfigValue(configFile, paramName string) (string, bool) {
	content, err := os.ReadFile(configFile)
	if err != nil {
		return "", false
	}
	lines := strings.Split(string(content), "\n")
	var value string
	found := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") {
			continue
		}
		equalIndex := strings.Index(line, "=")
		if equalIndex <= 0 {
			continue
		}
		name := strings.TrimSpace(line[:equalIndex])
		if name != paramName {
			continue
		}
		value = strings.TrimSpace(line[equalIndex+1:])
		found = true
	}
	return value, found
}

// IsShowActualRowsEnabled returns current effective switch value.
// It supports runtime refresh from config file (mtime-based) without process restart.
func IsShowActualRowsEnabled() bool {
	cfg := GetGlobalConfig()
	if cfg == nil {
		return false
	}

	// CLI override has highest priority and is immutable for this process.
	if strings.TrimSpace(cfg.CliShowActualRows) != "" {
		return normalizeOnOff(cfg.CliShowActualRows) == "ON"
	}

	defaultValue := defaultShowActualRows
	configFile := strings.TrimSpace(cfg.Config)
	if configFile == "" {
		return defaultValue == "ON"
	}

	stat, err := os.Stat(configFile)
	if err != nil {
		return defaultValue == "ON"
	}

	showActualRowsRuntimeCache.RLock()
	sameFile := showActualRowsRuntimeCache.file == configFile
	sameModTime := showActualRowsRuntimeCache.modTime.Equal(stat.ModTime())
	cachedValue := showActualRowsRuntimeCache.value
	showActualRowsRuntimeCache.RUnlock()
	if sameFile && sameModTime {
		return normalizeOnOff(cachedValue) == "ON"
	}

	fileValue, found := readLastConfigValue(configFile, "showActualRows")
	newValue := defaultValue
	if found {
		newValue = normalizeOnOff(fileValue)
	}

	showActualRowsRuntimeCache.Lock()
	showActualRowsRuntimeCache.file = configFile
	showActualRowsRuntimeCache.modTime = stat.ModTime()
	showActualRowsRuntimeCache.lastRead = time.Now()
	showActualRowsRuntimeCache.value = newValue
	showActualRowsRuntimeCache.Unlock()

	cfg.SecondaryL.RulesV.ShowActualRows = newValue
	return newValue == "ON"
}
