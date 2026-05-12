package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/gosuri/uitable"
)

// formatSize formats file size in human-readable format
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// identifyStatementType identifies SQL statement type and estimates affected rows
func identifyStatementType(stmt string) (stmtType string, affectedRows int64) {
	lines := strings.Split(stmt, "\n")
	var cleanedLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "--") && trimmed != "" {
			cleanedLines = append(cleanedLines, line)
		}
	}

	cleanedStmt := strings.Join(cleanedLines, "\n")
	normalized := strings.TrimSpace(strings.ToUpper(cleanedStmt))
	if normalized == "" {
		return "UNKNOWN", 0
	}

	switch {
	case strings.HasPrefix(normalized, "INSERT"):
		valuesIdx := strings.Index(normalized, "VALUES")
		if valuesIdx == -1 {
			return "INSERT", 1
		}
		afterValues := cleanedStmt[valuesIdx:]
		rows := int64(strings.Count(afterValues, "("))
		if rows == 0 {
			rows = 1
		}
		return "INSERT", rows
	case strings.HasPrefix(normalized, "UPDATE"):
		return "UPDATE", 1
	case strings.HasPrefix(normalized, "DELETE"):
		return "DELETE", 1
	case strings.HasPrefix(normalized, "DROP"):
		return "DROP", 1
	case strings.HasPrefix(normalized, "ALTER"):
		return "ALTER", 1
	case strings.HasPrefix(normalized, "CREATE"):
		return "CREATE", 1
	default:
		return "UNKNOWN", 0
	}
}

// collectFixSQLStatistics collects statistics about fix SQL files
func collectFixSQLStatistics(fixFileDir string) (*FixSQLStatistics, error) {
	stats := &FixSQLStatistics{}

	var sqlFiles []string
	err := filepath.Walk(fixFileDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".sql") {
			sqlFiles = append(sqlFiles, path)
			stats.TotalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to traverse directory: %v", err)
	}

	stats.TotalFiles = len(sqlFiles)
	tableSet := make(map[string]bool)

	for _, file := range sqlFiles {
		stage := detectObjectStage(file)
		switch stage {
		case "DELETE":
			stats.DeleteFiles++
		case "TABLE":
			stats.TableFiles++
		case "VIEW":
			stats.ViewFiles++
		case "ROUTINE":
			stats.RoutineFiles++
		case "TRIGGER":
			stats.TriggerFiles++
		default:
			stats.UnknownFiles++
		}

		if stage == "TABLE" || stage == "DELETE" {
			content, err := os.ReadFile(file)
			if err != nil {
				continue
			}

			statements := splitSQLStatements(string(content))
			for _, stmt := range statements {
				stmtType, rows := identifyStatementType(stmt)
				switch stmtType {
				case "INSERT":
					stats.InsertRows += rows
				case "UPDATE":
					stats.UpdateRows += rows
				case "DELETE":
					stats.DeleteRows += rows
				case "DROP":
					stats.DropCount++
				case "ALTER":
					stats.AlterCount++
				case "CREATE":
					stats.CreateCount++
				}

				if stmtType == "INSERT" || stmtType == "UPDATE" || stmtType == "DELETE" {
					tableName := extractTableName(stmt)
					if tableName != "" {
						tableSet[tableName] = true
					}
				}
			}
		}

		if stage == "VIEW" || stage == "ROUTINE" || stage == "TRIGGER" {
			content, err := os.ReadFile(file)
			if err != nil {
				continue
			}

			statements := splitSQLStatements(string(content))
			for _, stmt := range statements {
				stmtType, _ := identifyStatementType(stmt)
				switch stmtType {
				case "DROP":
					stats.DropCount++
				case "ALTER":
					stats.AlterCount++
				case "CREATE":
					stats.CreateCount++
				}
			}
		}
	}

	stats.TableCount = len(tableSet)
	stats.EstimatedBinlogSize = estimateBinlogSize(stats.TotalSize, stats.DeleteRows, stats.InsertRows)

	return stats, nil
}

// extractTableName extracts table name from SQL statement (simple implementation)
func extractTableName(stmt string) string {
	normalized := strings.TrimSpace(stmt)
	upper := strings.ToUpper(normalized)

	var tableName string
	if strings.HasPrefix(upper, "INSERT") {
		parts := strings.Fields(normalized)
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "INTO" {
			tableName = parts[2]
		}
	} else if strings.HasPrefix(upper, "UPDATE") {
		parts := strings.Fields(normalized)
		if len(parts) >= 2 {
			tableName = parts[1]
		}
	} else if strings.HasPrefix(upper, "DELETE") {
		parts := strings.Fields(normalized)
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "FROM" {
			tableName = parts[2]
		}
	}

	tableName = strings.Trim(tableName, "`")

	if idx := strings.Index(tableName, "("); idx != -1 {
		tableName = tableName[:idx]
		tableName = strings.Trim(tableName, "`")
	}

	if idx := strings.Index(tableName, "."); idx != -1 {
		tableName = tableName[idx+1:]
		tableName = strings.Trim(tableName, "`")
	}

	return strings.ToUpper(tableName)
}

// estimateBinlogSize estimates binlog size based on SQL file size and operations
func estimateBinlogSize(totalFileSize int64, deleteRows, insertRows int64) int64 {
	coefficient := 1.3
	if deleteRows > insertRows {
		coefficient = 1.1
	}
	return int64(float64(totalFileSize) * coefficient)
}

// printStatisticsReport prints statistics report with table format and colors
func printStatisticsReport(stats *FixSQLStatistics) {
	fmt.Println()
	fmt.Println(color.CyanString("========== repairDB 预执行报告 =========="))

	table := uitable.New()
	table.MaxColWidth = 80
	table.Wrap = true

	table.AddRow(color.YellowString("统计项"), color.YellowString("数值"))
	table.AddRow("---", "---")

	table.AddRow("修复 SQL 文件总数", color.GreenString("%d", stats.TotalFiles))
	table.AddRow("文件总大小", color.GreenString("%s", formatSize(stats.TotalSize)))

	if stats.DeleteFiles > 0 {
		table.AddRow("DELETE 文件数", color.RedString("%d", stats.DeleteFiles))
	}
	if stats.TableFiles > 0 {
		table.AddRow("TABLE 文件数", color.GreenString("%d", stats.TableFiles))
	}
	if stats.ViewFiles > 0 {
		table.AddRow("VIEW 文件数", color.BlueString("%d", stats.ViewFiles))
	}
	if stats.RoutineFiles > 0 {
		table.AddRow("ROUTINE 文件数", color.BlueString("%d", stats.RoutineFiles))
	}
	if stats.TriggerFiles > 0 {
		table.AddRow("TRIGGER 文件数", color.BlueString("%d", stats.TriggerFiles))
	}
	if stats.UnknownFiles > 0 {
		table.AddRow("UNKNOWN 文件数", color.MagentaString("%d", stats.UnknownFiles))
	}

	table.AddRow("---", "---")

	if stats.TableCount > 0 {
		table.AddRow("涉及表数量", color.CyanString("%d", stats.TableCount))
	}
	if stats.InsertRows > 0 {
		table.AddRow("INSERT 行数", color.GreenString("%s", formatNumber(stats.InsertRows)))
	}
	if stats.UpdateRows > 0 {
		table.AddRow("UPDATE 行数", color.YellowString("%s", formatNumber(stats.UpdateRows)))
	}
	if stats.DeleteRows > 0 {
		table.AddRow("DELETE 行数", color.RedString("%s", formatNumber(stats.DeleteRows)))
	}

	if stats.DropCount > 0 {
		table.AddRow("DROP 对象数", color.RedString("%d", stats.DropCount))
	}
	if stats.AlterCount > 0 {
		table.AddRow("ALTER 对象数", color.YellowString("%d", stats.AlterCount))
	}
	if stats.CreateCount > 0 {
		table.AddRow("CREATE 对象数", color.GreenString("%d", stats.CreateCount))
	}

	if stats.EstimatedBinlogSize > 0 {
		table.AddRow("预估 binlog 大小", color.MagentaString("%s (预估)", formatSize(stats.EstimatedBinlogSize)))
	}

	fmt.Println(table)
	fmt.Println(color.CyanString("=========================================="))
	fmt.Println()
}

// formatNumber formats number with thousand separators
func formatNumber(n int64) string {
	str := strconv.FormatInt(n, 10)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// promptUserConfirmation prompts user for confirmation to continue execution
func promptUserConfirmation() (bool, error) {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print(color.YellowString("是否继续执行修复操作？(yes/no): "))
		input, err := reader.ReadString('\n')
		if err != nil {
			return false, fmt.Errorf("读取用户输入失败: %v", err)
		}

		input = strings.TrimSpace(strings.ToLower(input))

		switch input {
		case "yes", "y":
			return true, nil
		case "no", "n":
			return false, nil
		default:
			fmt.Println(color.RedString("无效输入，请输入 yes 或 no"))
		}
	}
}
