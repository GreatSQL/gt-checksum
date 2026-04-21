package mysql

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// GenerateRoutineFixSQL builds DROP + CREATE statements for procedure/function
// routineType should be "PROCEDURE" or "FUNCTION"
func GenerateRoutineFixSQL(sourceSchema, destSchema, name, routineType, sourceDef string) []string {
	upperType := strings.ToUpper(strings.TrimSpace(routineType))
	// 处理源端定义中的schema名称替换
	processedDef := processRoutineSchemaNames(sourceDef, sourceSchema, destSchema)
	createStmt := extractRoutineCreateStatement(processedDef)
	if !routineCreateLooksExecutable(createStmt, upperType) {
		return []string{
			fmt.Sprintf("-- WARNING: unable to auto-generate executable CREATE %s for `%s`.`%s`", upperType, destSchema, name),
			fmt.Sprintf("-- Suggested manual check: SHOW CREATE %s `%s`.`%s`;", upperType, sourceSchema, name),
		}
	}

	drop := fmt.Sprintf("DROP %s IF EXISTS %s.%s;", upperType, mysqlQuoteIdent(destSchema), mysqlQuoteIdent(name))
	return []string{drop, createStmt}
}

// GenerateTriggerFixSQL builds DROP + CREATE statements for trigger
func GenerateTriggerFixSQL(sourceSchema, destSchema, name, sourceDef string) []string {
	drop := fmt.Sprintf("DROP TRIGGER IF EXISTS %s.%s;", mysqlQuoteIdent(destSchema), mysqlQuoteIdent(name))

	// 处理源端定义中的schema名称替换
	processedDef := processTriggerSchemaNames(sourceDef, sourceSchema, destSchema)

	return []string{drop, strings.TrimSpace(processedDef)}
}

// CheckAndCleanupEmptyFixFile removes empty per-object fix files and files that
// contain only session preamble / transaction wrappers but no actual repair SQL.
func CheckAndCleanupEmptyFixFile(fixFileDir string) error {
	// 检查目录是否存在
	if _, err := os.Stat(fixFileDir); err != nil {
		// 目录不存在，不需要处理
		return nil
	}

	// 遍历目录中的所有.sql文件
	files, err := os.ReadDir(fixFileDir)
	if err != nil {
		return fmt.Errorf("failed to read fix file directory: %v", err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			filePath := fmt.Sprintf("%s/%s", fixFileDir, file.Name())

			info, err := file.Info()
			if err != nil {
				return fmt.Errorf("failed to stat fix SQL file %s: %v", file.Name(), err)
			}

			// 快速路径：物理为空直接删除，无需读内容
			if info.Size() == 0 {
				if err := os.Remove(filePath); err != nil {
					return fmt.Errorf("failed to remove empty fix SQL file %s: %v", file.Name(), err)
				}
				continue
			}

				// 慢路径：流式扫描非空文件，仅在必要时继续向后读取。
				hasActualFixSql, err := fixSQLFileHasActualStatements(filePath)
				if err != nil {
					return fmt.Errorf("failed to scan fix SQL file %s: %v", file.Name(), err)
				}

			if !hasActualFixSql {
				// 文件只包含preamble和事务控制语句，删除它
				if err := os.Remove(filePath); err != nil {
					return fmt.Errorf("failed to remove empty fix SQL file %s: %v", file.Name(), err)
				}
			}
		}
	}

	return nil
}

func fixSQLFileHasActualStatements(filePath string) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 8*1024*1024)

	for scanner.Scan() {
		trimmedLine := strings.TrimSpace(scanner.Text())
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "SET ") || trimmedLine == "BEGIN;" || trimmedLine == "COMMIT;" {
			continue
		}
		return true, nil
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// processRoutineSchemaNames 处理存储过程和函数定义中的schema名称替换
func processRoutineSchemaNames(sourceDef, sourceSchema, destSchema string) string {
	// 替换过程/函数名中的schema名称
	processedDef := strings.ReplaceAll(sourceDef, fmt.Sprintf("`%s`", sourceSchema), fmt.Sprintf("`%s`", destSchema))

	// 如果替换后没有包含目标schema名，说明原始语句没有schema名，需要手动添加
	if !strings.Contains(processedDef, fmt.Sprintf("`%s`", destSchema)) {
		// 查找"PROCEDURE"或"FUNCTION"关键字的位置
		procIndex := strings.Index(strings.ToUpper(processedDef), "PROCEDURE")
		funcIndex := strings.Index(strings.ToUpper(processedDef), "FUNCTION")

		var keywordIndex int
		var keywordType string
		if procIndex != -1 && funcIndex != -1 {
			if procIndex < funcIndex {
				keywordIndex = procIndex
				keywordType = "PROCEDURE"
			} else {
				keywordIndex = funcIndex
				keywordType = "FUNCTION"
			}
		} else if procIndex != -1 {
			keywordIndex = procIndex
			keywordType = "PROCEDURE"
		} else if funcIndex != -1 {
			keywordIndex = funcIndex
			keywordType = "FUNCTION"
		}

		if keywordIndex != -1 {
			// 从关键字之后开始处理
			afterKeyword := processedDef[keywordIndex+len(keywordType):]

			// 跳过可能的DEFINER子句
			afterKeyword = strings.TrimSpace(afterKeyword)
			if strings.HasPrefix(strings.ToUpper(afterKeyword), "DEFINER=") {
				// 跳过DEFINER子句直到下一个关键字
				parenCount := 0
				for i, char := range afterKeyword {
					if char == '(' {
						parenCount++
					} else if char == ')' {
						parenCount--
						if parenCount == 0 {
							afterKeyword = afterKeyword[i+1:]
							break
						}
					}
				}
			}

			// 跳过IF NOT EXISTS
			afterKeyword = strings.TrimSpace(afterKeyword)
			if strings.HasPrefix(strings.ToUpper(afterKeyword), "IF NOT EXISTS") {
				afterKeyword = afterKeyword[len("IF NOT EXISTS"):]
			}

			// 查找真正的过程/函数名
			afterKeyword = strings.TrimSpace(afterKeyword)

			// 跳过可能的DEFINER部分后，找到真正的函数名
			// 策略：找到第一个反引号，然后检查后面是否包含(字符(参数开始)
			funcNameStart := strings.Index(afterKeyword, "`")
			if funcNameStart != -1 {
				// 找到下一个反引号
				funcNameEnd := strings.Index(afterKeyword[funcNameStart+1:], "`")
				if funcNameEnd != -1 {
					// 提取候选名称
					candidateName := afterKeyword[funcNameStart+1 : funcNameStart+funcNameEnd+1]

					// 检查这个名称后面是否跟着(
					restAfterName := afterKeyword[funcNameStart+funcNameEnd+2:]
					restAfterName = strings.TrimSpace(restAfterName)

					// 如果后面跟着(或者是第一个出现在FUNCTION/PROCEDURE后面的反引号名称，认为这是真正的函数名
					if strings.HasPrefix(restAfterName, "(") {
						routineName := candidateName
						restOfDef := afterKeyword[funcNameStart+funcNameEnd+2:]

						// 在过程/函数名前添加schema名，确保格式为`destSchema`.`routineName`
						newRoutineName := fmt.Sprintf("`%s`.`%s`", destSchema, routineName)
						beforeKeyword := processedDef[:keywordIndex]
						processedDef = beforeKeyword + keywordType + " " + newRoutineName + restOfDef
					} else {
						// 如果没有找到合适的函数名，尝试查找FUNCTION/PROCEDURE关键字后的函数名
						// 这是针对特殊情况的备用逻辑
						funcKeyword := keywordType

						// 从关键字后开始查找真正的函数名
						keywordPos := strings.ToUpper(processedDef)
						funcStartPos := strings.Index(keywordPos, funcKeyword)
						if funcStartPos != -1 {
							funcStartPos += len(funcKeyword)
							afterFunc := processedDef[funcStartPos:]
							afterFunc = strings.TrimSpace(afterFunc)

							// 跳过IF NOT EXISTS
							if strings.HasPrefix(strings.ToUpper(afterFunc), "IF NOT EXISTS") {
								afterFunc = afterFunc[len("IF NOT EXISTS"):]
								afterFunc = strings.TrimSpace(afterFunc)
							}

							// 再次查找反引号
							if strings.HasPrefix(afterFunc, "`") {
								endQuoteIndex := strings.Index(afterFunc[1:], "`")
								if endQuoteIndex != -1 {
									routineName := afterFunc[1 : endQuoteIndex+1]
									restOfDef := afterFunc[endQuoteIndex+2:]

									// 添加schema名
									newRoutineName := fmt.Sprintf("`%s`.`%s`", destSchema, routineName)
									beforeFunc := processedDef[:funcStartPos]
									processedDef = beforeFunc + " " + newRoutineName + restOfDef
								}
							}
						}
					}
				}
			}
		}
	}

	return processedDef
}

func extractRoutineCreateStatement(sourceDef string) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(sourceDef, "\r", ""))
	if normalized == "" {
		return ""
	}

	lines := strings.Split(normalized, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(trimmed), "DELIMITER ") {
			continue
		}
		filtered = append(filtered, line)
	}

	normalized = strings.TrimSpace(strings.Join(filtered, "\n"))
	normalized = strings.TrimSpace(routineFixMetadataCommentPattern.ReplaceAllString(normalized, ""))
	if normalized == "" {
		return ""
	}

	upper := strings.ToUpper(normalized)
	if idx := strings.Index(upper, "CREATE "); idx >= 0 {
		normalized = strings.TrimSpace(normalized[idx:])
	}

	for {
		switch {
		case strings.HasSuffix(normalized, "$$"):
			normalized = strings.TrimSpace(strings.TrimSuffix(normalized, "$$"))
		case strings.HasSuffix(normalized, "$"):
			normalized = strings.TrimSpace(strings.TrimSuffix(normalized, "$"))
		case strings.HasSuffix(normalized, ";"):
			normalized = strings.TrimSpace(strings.TrimSuffix(normalized, ";"))
		default:
			return normalized
		}
	}
}

func routineCreateLooksExecutable(createStmt, routineType string) bool {
	normalized := strings.TrimSpace(createStmt)
	if normalized == "" {
		return false
	}

	upper := strings.ToUpper(normalized)
	if !strings.Contains(upper, "CREATE ") || !strings.Contains(upper, routineType) {
		return false
	}

	typeIndex := strings.Index(upper, routineType)
	if typeIndex == -1 {
		return false
	}

	openParenOffset := strings.Index(normalized[typeIndex:], "(")
	if openParenOffset == -1 {
		return false
	}
	openParenIndex := typeIndex + openParenOffset
	depth := 0
	closingParenIndex := -1
	for i := openParenIndex; i < len(normalized); i++ {
		switch normalized[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closingParenIndex = i
				break
			}
		}
	}
	if closingParenIndex == -1 {
		return false
	}

	rest := strings.TrimSpace(normalized[closingParenIndex+1:])
	return rest != ""
}

// processTriggerSchemaNames 处理触发器定义中的schema名称替换
func processTriggerSchemaNames(sourceDef, sourceSchema, destSchema string) string {
	// 首先尝试直接替换已有的schema名称
	processedDef := strings.ReplaceAll(sourceDef, fmt.Sprintf("`%s`.`", sourceSchema), fmt.Sprintf("`%s`.`", destSchema))

	// 检查是否需要添加schema前缀到触发器名
	if !strings.Contains(strings.ToUpper(processedDef), strings.ToUpper(fmt.Sprintf("TRIGGER `%s`.`", destSchema))) {
		// 查找TRIGGER关键字的位置（忽略大小写）
		triggerIndex := strings.Index(strings.ToUpper(processedDef), "TRIGGER")
		if triggerIndex != -1 {
			// 从TRIGGER之后开始查找触发器名
			searchStart := triggerIndex + len("TRIGGER")

			// 跳过可能的DEFINER部分
			remaining := processedDef[searchStart:]
			trimmed := strings.TrimSpace(remaining)

			// 如果包含DEFINER，找到其结束位置
			if strings.HasPrefix(strings.ToUpper(trimmed), "DEFINER=") {
				// 使用括号匹配来正确跳过整个DEFINER子句
				parenCount := 0
				for i, char := range trimmed {
					if char == '(' {
						parenCount++
					} else if char == ')' {
						parenCount--
						if parenCount == 0 {
							searchStart += i + 1
							break
						}
					}
				}
			}

			// 查找真正的触发器名
			remaining = processedDef[searchStart:]

			// 跳过可能的DEFINER部分后，找到真正的触发器名
			// 策略：找到第一个反引号，然后检查后面是否包含ON关键字
			triggerNameStart := strings.Index(remaining, "`")
			if triggerNameStart != -1 {
				// 找到下一个反引号
				triggerNameEnd := strings.Index(remaining[triggerNameStart+1:], "`")
				if triggerNameEnd != -1 {
					// 提取候选名称
					candidateName := remaining[triggerNameStart+1 : triggerNameStart+triggerNameEnd+1]

					// 检查这个名称后面是否包含ON关键字(触发器语法中ON表示表名开始)
					restAfterName := remaining[triggerNameStart+triggerNameEnd+2:]
					restAfterName = strings.TrimSpace(restAfterName)

					// 如果后面包含ON关键字，认为这是真正的触发器名
					if strings.Contains(strings.ToUpper(restAfterName), " ON ") {
						// 构建新的触发器名，添加schema前缀
						newTriggerName := fmt.Sprintf("`%s`.`%s`", destSchema, candidateName)

						// 替换原始触发器名
						before := processedDef[:searchStart+triggerNameStart]
						after := processedDef[searchStart+triggerNameStart+triggerNameEnd+2:]
						processedDef = before + newTriggerName + after
					}
				}
			} else {
				// 如果没有找到反引号包围的触发器名，尝试备用策略
				// 从CREATE TRIGGER关键字后开始查找
				keywordPos := strings.ToUpper(processedDef)
				triggerStartPos := strings.Index(keywordPos, "TRIGGER")
				if triggerStartPos != -1 {
					triggerStartPos += len("TRIGGER")
					afterTrigger := processedDef[triggerStartPos:]
					afterTrigger = strings.TrimSpace(afterTrigger)

					// 再次尝试查找反引号
					if strings.HasPrefix(afterTrigger, "`") {
						endQuoteIndex := strings.Index(afterTrigger[1:], "`")
						if endQuoteIndex != -1 {
							triggerName := afterTrigger[1 : endQuoteIndex+1]
							restOfDef := afterTrigger[endQuoteIndex+2:]

							// 添加schema名
							newTriggerName := fmt.Sprintf("`%s`.`%s`", destSchema, triggerName)
							beforeCreate := processedDef[:triggerStartPos]
							processedDef = beforeCreate + " " + newTriggerName + restOfDef
						}
					}
				}
			}
		}
	}

	// 确保表名也有正确的schema前缀
	processedDef = replaceTableSchemaInTrigger(processedDef, sourceSchema, destSchema)

	return processedDef
}

// replaceTableSchemaInTrigger 替换触发器ON子句中的表名schema
func replaceTableSchemaInTrigger(triggerDef, sourceSchema, destSchema string) string {
	onIndex := strings.Index(strings.ToUpper(triggerDef), " ON ")
	if onIndex != -1 {
		afterOn := triggerDef[onIndex+4:]
		forEachIndex := strings.Index(strings.ToUpper(afterOn), " FOR EACH ROW")
		if forEachIndex == -1 {
			return triggerDef
		}

		tableRef := strings.TrimSpace(afterOn[:forEachIndex])
		rest := afterOn[forEachIndex:]

		if tableRef == "" {
			return triggerDef
		}

		if !strings.Contains(tableRef, ".") {
			return triggerDef[:onIndex+4] + fmt.Sprintf("`%s`.%s", destSchema, tableRef) + rest
		}

		parts := strings.SplitN(tableRef, ".", 2)
		if len(parts) != 2 {
			return triggerDef
		}

		schemaPart := strings.Trim(parts[0], "` ")
		tablePart := parts[1]
		if strings.EqualFold(schemaPart, sourceSchema) {
			return triggerDef[:onIndex+4] + fmt.Sprintf("`%s`.%s", destSchema, tablePart) + rest
		}
	}

	return triggerDef
}
