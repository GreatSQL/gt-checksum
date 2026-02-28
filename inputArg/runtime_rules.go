package inputArg

import (
	"fmt"
	"gt-checksum/global"
	"os"
	"strings"
	"sync"
	"time"
)

const defaultShowActualRows = "ON"
const runtimeConfigRefreshInterval = 2 * time.Second

var showActualRowsRuntimeCache = struct {
	sync.RWMutex
	file     string
	modTime  time.Time
	lastRead time.Time
	lastWarn time.Time
	value    string
}{
	value: defaultShowActualRows,
}

func normalizeOnOff(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if idx := strings.IndexAny(v, ";#"); idx >= 0 {
		v = strings.TrimSpace(v[:idx])
	}
	v = strings.Trim(v, `"'`)
	if fields := strings.Fields(v); len(fields) > 0 {
		v = fields[0]
	}
	v = strings.ToUpper(strings.TrimSpace(v))
	if v == "ON" || v == "OFF" {
		return v, true
	}
	return "", false
}

func warnShowActualRows(message string) {
	showActualRowsRuntimeCache.Lock()
	if time.Since(showActualRowsRuntimeCache.lastWarn) < runtimeConfigRefreshInterval {
		showActualRowsRuntimeCache.Unlock()
		return
	}
	showActualRowsRuntimeCache.lastWarn = time.Now()
	showActualRowsRuntimeCache.Unlock()

	if global.Wlog != nil {
		global.Wlog.Warn(message)
	} else {
		fmt.Println(message)
	}
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
		return defaultShowActualRows == "ON"
	}

	// CLI override has highest priority and is immutable for this process.
	if override := strings.TrimSpace(cfg.CliShowActualRows); override != "" {
		if normalized, ok := normalizeOnOff(override); ok {
			return normalized == "ON"
		}
		warnShowActualRows(fmt.Sprintf("Invalid showActualRows CLI value [%s], fallback to default [%s]", override, defaultShowActualRows))
		return defaultShowActualRows == "ON"
	}

	defaultValue := defaultShowActualRows
	configFile := strings.TrimSpace(cfg.Config)
	if configFile == "" {
		return defaultValue == "ON"
	}

	showActualRowsRuntimeCache.RLock()
	if showActualRowsRuntimeCache.file == configFile && time.Since(showActualRowsRuntimeCache.lastRead) < runtimeConfigRefreshInterval {
		cachedValue := showActualRowsRuntimeCache.value
		showActualRowsRuntimeCache.RUnlock()
		return cachedValue == "ON"
	}
	showActualRowsRuntimeCache.RUnlock()

	stat, err := os.Stat(configFile)
	if err != nil {
		warnShowActualRows(fmt.Sprintf("Cannot stat config file [%s], fallback showActualRows=%s", configFile, defaultValue))
		showActualRowsRuntimeCache.Lock()
		showActualRowsRuntimeCache.file = configFile
		showActualRowsRuntimeCache.modTime = time.Time{}
		showActualRowsRuntimeCache.lastRead = time.Now()
		showActualRowsRuntimeCache.value = defaultValue
		showActualRowsRuntimeCache.Unlock()
		cfg.SecondaryL.RulesV.ShowActualRows = defaultValue
		return defaultValue == "ON"
	}

	showActualRowsRuntimeCache.RLock()
	sameFile := showActualRowsRuntimeCache.file == configFile
	sameModTime := showActualRowsRuntimeCache.modTime.Equal(stat.ModTime())
	cachedValue := showActualRowsRuntimeCache.value
	showActualRowsRuntimeCache.RUnlock()
	if sameFile && sameModTime {
		return cachedValue == "ON"
	}

	fileValue, found := readLastConfigValue(configFile, "showActualRows")
	newValue := defaultValue
	if found {
		if normalized, ok := normalizeOnOff(fileValue); ok {
			newValue = normalized
		} else {
			warnShowActualRows(fmt.Sprintf("Invalid showActualRows config value [%s], fallback to default [%s]", fileValue, defaultValue))
		}
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
