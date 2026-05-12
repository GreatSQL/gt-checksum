package main

import (
	"math/rand"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var deleteFileNameRegex = regexp.MustCompile(`^.+-DELETE-.+\.sql$`)
var numberedSQLFileRegex = regexp.MustCompile(`^(.+?-)(\d+)(\.sql)$`)

// stageOrder defines the fixed execution order for object-type stages.
var stageOrder = []string{"DELETE", "TABLE", "VIEW", "ROUTINE", "TRIGGER", "UNKNOWN"}

// executionStage describes a single phase of the repairDB execution pipeline.
type executionStage struct {
	Name    string
	Files   []string
	Shuffle bool // true: shuffle before execution (DML hotspot reduction); false: sort for audit readability
}

// classifiedFiles holds SQL file paths grouped by their execution stage.
type classifiedFiles struct {
	Delete  []string
	Table   []string
	View    []string
	Routine []string
	Trigger []string
	Unknown []string
}

// detectObjectStage returns the execution stage name for a single SQL file path.
func detectObjectStage(path string) string {
	base := filepath.Base(path)
	if deleteFileNameRegex.MatchString(base) {
		return "DELETE"
	}
	switch {
	case strings.HasPrefix(base, "table."):
		return "TABLE"
	case strings.HasPrefix(base, "view."):
		return "VIEW"
	case strings.HasPrefix(base, "routine."):
		return "ROUTINE"
	case strings.HasPrefix(base, "trigger."):
		return "TRIGGER"
	default:
		return "UNKNOWN"
	}
}

// detectObjectTypeFromContent returns the human-readable object type for a SQL file.
func detectObjectTypeFromContent(stage, content string) string {
	switch stage {
	case "TABLE", "DELETE":
		return "table"
	case "VIEW":
		return "view"
	case "TRIGGER":
		return "trigger"
	case "ROUTINE":
		upper := strings.ToUpper(content)
		if strings.Contains(upper, "CREATE FUNCTION") ||
			(strings.Contains(upper, "CREATE DEFINER") && strings.Contains(upper, " FUNCTION ")) {
			return "function"
		}
		return "procedure"
	default:
		return "unknown"
	}
}

// classifySQLFiles distributes SQL file paths into their respective execution stages.
func classifySQLFiles(files []string) classifiedFiles {
	var cf classifiedFiles
	for _, f := range files {
		switch detectObjectStage(f) {
		case "DELETE":
			cf.Delete = append(cf.Delete, f)
		case "TABLE":
			cf.Table = append(cf.Table, f)
		case "VIEW":
			cf.View = append(cf.View, f)
		case "ROUTINE":
			cf.Routine = append(cf.Routine, f)
		case "TRIGGER":
			cf.Trigger = append(cf.Trigger, f)
		default:
			cf.Unknown = append(cf.Unknown, f)
		}
	}
	return cf
}

// buildExecutionStages constructs the ordered stage table from classified files.
func buildExecutionStages(cf classifiedFiles) []executionStage {
	filesByStage := map[string][]string{
		"DELETE":  cf.Delete,
		"TABLE":   cf.Table,
		"VIEW":    cf.View,
		"ROUTINE": cf.Routine,
		"TRIGGER": cf.Trigger,
		"UNKNOWN": cf.Unknown,
	}
	shuffleByStage := map[string]bool{
		"TABLE": true,
	}
	var stages []executionStage
	for _, name := range stageOrder {
		files := filesByStage[name]
		if len(files) > 0 {
			stages = append(stages, executionStage{
				Name:    name,
				Files:   files,
				Shuffle: shuffleByStage[name],
			})
		}
	}
	return stages
}

// prepareStageFiles returns a copy of the stage's files in their execution order.
func prepareStageFiles(stage executionStage) []string {
	out := make([]string, len(stage.Files))
	copy(out, stage.Files)
	if stage.Shuffle {
		shuffleSQLFiles(out)
	} else {
		sort.Strings(out)
	}
	return out
}

func uniqueFiles(files []string) []string {
	seen := make(map[string]struct{}, len(files))
	result := make([]string, 0, len(files))
	for _, f := range files {
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		result = append(result, f)
	}
	return result
}

func shuffleSQLFiles(files []string) {
	if len(files) <= 1 {
		return
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rng.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})
}

func stageIndex(stage string) int {
	for i, s := range stageOrder {
		if s == stage {
			return i
		}
	}
	return len(stageOrder)
}

// extractSchemaAndObject parses schema and object name from a repair SQL file path.
func extractSchemaAndObject(filePath string) (schema, object string) {
	base := filepath.Base(filePath)
	name := strings.TrimSuffix(base, ".sql")

	if deleteFileNameRegex.MatchString(base) {
		dashIdx := strings.LastIndex(name, "-DELETE-")
		if dashIdx != -1 {
			name = name[:dashIdx]
		}
	}

	parts := strings.SplitN(name, ".", 3)
	if len(parts) == 3 && (parts[0] == "table" || parts[0] == "view" || parts[0] == "routine" || parts[0] == "trigger") {
		return parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", name
}
