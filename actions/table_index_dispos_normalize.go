package actions

import (
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"math"
	"strconv"
	"strings"
)

func isFloatingColumnType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	if t == "" {
		return false
	}
	// Cover common Oracle/MySQL floating types.
	return strings.HasPrefix(t, "FLOAT") ||
		strings.HasPrefix(t, "DOUBLE") ||
		strings.HasPrefix(t, "REAL") ||
		strings.HasPrefix(t, "BINARY_FLOAT") ||
		strings.HasPrefix(t, "BINARY_DOUBLE")
}

func parseNumericScale(dataType string) (int, bool) {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	left := strings.Index(t, "(")
	right := strings.LastIndex(t, ")")
	if left == -1 || right <= left+1 {
		return 0, false
	}
	args := strings.TrimSpace(t[left+1 : right])
	parts := strings.Split(args, ",")
	if len(parts) < 2 {
		return 0, false
	}
	scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || scale < 0 {
		return 0, false
	}
	return scale, true
}

func resolveFloatComparisonScale(sourceType, destType string) int {
	sourceFloat := isFloatComparisonType(sourceType)
	destFloat := isFloatComparisonType(destType)
	if !sourceFloat && !destFloat {
		return -1
	}
	sourceScale, sourceOK := parseNumericScale(sourceType)
	destScale, destOK := parseNumericScale(destType)
	if sourceOK && destOK {
		if sourceScale < destScale {
			return sourceScale
		}
		return destScale
	}
	if sourceOK {
		return sourceScale
	}
	if destOK {
		return destScale
	}
	// 当两端均为 FLOAT/BINARY_FLOAT 且无显式小数精度时，
	// 使用 float32 精度哨兵规范化：消除 Oracle 精确十进制（123.45）
	// 与 MySQL 二进制浮点（123.449997）的字符串差异，两者在 float32 层面实际相同。
	if isFloatComparisonType(sourceType) && isFloatComparisonType(destType) {
		return floatSinglePrecisionSentinel
	}
	return 6
}

func isFloatComparisonType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return strings.HasPrefix(t, "FLOAT") || strings.HasPrefix(t, "BINARY_FLOAT")
}

func buildFloatComparisonScales(sourceCols, destCols []map[string]string) []int {
	colCount := len(sourceCols)
	if len(destCols) < colCount {
		colCount = len(destCols)
	}
	if colCount == 0 {
		return nil
	}
	scales := make([]int, colCount)
	hasFloatColumn := false
	for i := 0; i < colCount; i++ {
		scale := resolveFloatComparisonScale(sourceCols[i]["dataType"], destCols[i]["dataType"])
		scales[i] = scale
		if scale >= 0 {
			hasFloatColumn = true
		}
	}
	if !hasFloatColumn {
		return nil
	}
	return scales
}

// floatSinglePrecisionSentinel 作为 scale 哨兵值，指示使用 float32 精度规范化。
// 用于处理 Oracle FLOAT（以精确十进制存储，如 123.45）与
// MySQL FLOAT（IEEE 754 单精度，返回 123.449997）的字符串表示不一致问题。
const floatSinglePrecisionSentinel = -3

func normalizeFloatComparisonValue(raw string, scale int) string {
	if scale == -1 {
		return raw
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == dataDispos.ValueNullPlaceholder || trimmed == dataDispos.ValueEmptyPlaceholder {
		return raw
	}
	if scale == floatSinglePrecisionSentinel {
		// 使用 float32 精度规范化：将两端值都下转为 float32 再格式化，
		// 消除 Oracle 精确十进制与 MySQL 二进制浮点的字符串表示差异。
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return raw
		}
		f32 := float32(f)
		result := strconv.FormatFloat(float64(f32), 'g', -1, 32)
		return result
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return raw
	}
	if scale > 15 {
		scale = 15
	}
	factor := math.Pow10(scale)
	rounded := math.Round(f*factor) / factor
	normalized := strconv.FormatFloat(rounded, 'f', scale, 64)
	normalized = strings.TrimRight(strings.TrimRight(normalized, "0"), ".")
	if normalized == "" || normalized == "-0" {
		normalized = "0"
	}
	return normalized
}

func normalizeRowsForFloatComparison(rows []string, scales []int) []string {
	if len(rows) == 0 || len(scales) == 0 {
		return rows
	}
	const columnSep = "/*go actions columnData*/"
	normalizedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		parts := strings.Split(row, columnSep)
		limit := len(scales)
		if len(parts) < limit {
			limit = len(parts)
		}
		changed := false
		for colIdx := 0; colIdx < limit; colIdx++ {
			if scales[colIdx] == -1 {
				continue
			}
			normalized := normalizeFloatComparisonValue(parts[colIdx], scales[colIdx])
			if normalized != parts[colIdx] {
				parts[colIdx] = normalized
				changed = true
			}
		}
		if changed {
			normalizedRows[rowIdx] = strings.Join(parts, columnSep)
		} else {
			normalizedRows[rowIdx] = row
		}
	}
	return normalizedRows
}

// remapDelToOriginalDest 将 Arrcmp 返回的 del 行（来自归一化后的 cleanDestData）
// 映射回归一化前的目标端原始行（origDest），确保 DELETE WHERE 使用 MySQL 实际存储值。
//
// 原理：normalizeRowsForFloatComparison / normalizeRowsForTemporalComparison 按位置
// 修改每行字段，因此 normalizedDest[i] 与 origDest[i] 一一对应。
// 本函数通过 "归一化行→原始行队列" 映射表逐行消费；对多条目标行在归一化后字面相同
// 的"歧义"场景（例如多行 float 归一化到同值且与主键无关）提供 FIFO 兜底并显式告警，
// 让运维可借助日志人工复核，避免把错误原始行写入 DELETE WHERE。
func remapDelToOriginalDest(normalizedDel, normalizedDest, origDest []string) []string {
	if len(normalizedDest) != len(origDest) {
		// 长度不一致时保守地返回原 del，避免越界
		return normalizedDel
	}
	// 构建 normalizedRow → []originalRow 队列映射
	normToOrig := make(map[string][]string, len(normalizedDest))
	ambiguous := make(map[string]bool)
	for i, norm := range normalizedDest {
		if queue, exists := normToOrig[norm]; exists {
			// 多条原始行归一化后字面相同：结果可能依赖 Arrcmp 顺序（潜在错位）
			if len(queue) > 0 && queue[0] != origDest[i] {
				ambiguous[norm] = true
			}
		}
		normToOrig[norm] = append(normToOrig[norm], origDest[i])
	}
	result := make([]string, 0, len(normalizedDel))
	for _, normRow := range normalizedDel {
		if queue, ok := normToOrig[normRow]; ok && len(queue) > 0 {
			if ambiguous[normRow] {
				global.Wlog.Warnf("remapDelToOriginalDest: normalized dest row matches %d distinct originals (float/time collapse); "+
					"FIFO selecting first remaining — verify generated DELETE WHERE targets the intended row", len(queue))
			}
			result = append(result, queue[0])
			normToOrig[normRow] = queue[1:]
		} else {
			// 找不到对应原始行时回退使用归一化行（不应发生）
			global.Wlog.Warnf("remapDelToOriginalDest: normalized del row not found in normalizedDest; falling back to normalized form")
			result = append(result, normRow)
		}
	}
	return result
}

// buildCharTrimFlags 检查源端（Oracle）列类型，对 CHAR/NCHAR 列返回 true，
// 指示在 Arrcmp 前需要对该列的值执行尾部空格裁剪。
// Oracle CHAR/NCHAR 存储时以空格填充至列定义长度（如 'A         '），
// 而 MySQL CHAR SELECT 时自动去除尾部空格（返回 'A'），
// 若不归一化则字符串不等，导致无限 diff 循环。
func buildCharTrimFlags(sourceCols []map[string]string) []bool {
	if len(sourceCols) == 0 {
		return nil
	}
	flags := make([]bool, len(sourceCols))
	hasChar := false
	for i, col := range sourceCols {
		t := strings.ToUpper(strings.TrimSpace(col["dataType"]))
		if strings.HasPrefix(t, "NCHAR") || strings.HasPrefix(t, "CHAR") {
			flags[i] = true
			hasChar = true
		}
	}
	if !hasChar {
		return nil
	}
	return flags
}

// normalizeRowsForCharComparison 对 Oracle CHAR/NCHAR 列的值裁剪尾部空格，
// 使其与 MySQL 自动去除尾部空格后的值一致。
func normalizeRowsForCharComparison(rows []string, flags []bool) []string {
	if len(rows) == 0 || len(flags) == 0 {
		return rows
	}
	const columnSep = "/*go actions columnData*/"
	normalizedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		parts := strings.Split(row, columnSep)
		limit := len(flags)
		if len(parts) < limit {
			limit = len(parts)
		}
		changed := false
		for colIdx := 0; colIdx < limit; colIdx++ {
			if !flags[colIdx] {
				continue
			}
			if parts[colIdx] == dataDispos.ValueNullPlaceholder || parts[colIdx] == dataDispos.ValueEmptyPlaceholder {
				continue
			}
			trimmed := strings.TrimRight(parts[colIdx], " ")
			if trimmed != parts[colIdx] {
				parts[colIdx] = trimmed
				changed = true
			}
		}
		if changed {
			normalizedRows[rowIdx] = strings.Join(parts, columnSep)
		} else {
			normalizedRows[rowIdx] = row
		}
	}
	return normalizedRows
}

func isTemporalComparableType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return t == "DATE" ||
		strings.Contains(t, "TIMESTAMP") ||
		strings.HasPrefix(t, "DATETIME") ||
		strings.HasPrefix(t, "TIME") ||
		strings.HasPrefix(t, "INTERVAL DAY") ||
		strings.HasPrefix(t, "YEAR")
}

func isTimeOnlyType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return t == "TIME" || strings.HasPrefix(t, "TIME(")
}

func classifyTemporalCompareKind(sourceType, destType string) string {
	s := strings.ToUpper(strings.TrimSpace(sourceType))
	d := strings.ToUpper(strings.TrimSpace(destType))

	if strings.HasPrefix(s, "INTERVAL DAY") && strings.HasPrefix(d, "TIME") {
		return "time"
	}
	if strings.HasPrefix(d, "INTERVAL DAY") && strings.HasPrefix(s, "TIME") {
		return "time"
	}
	if isTimeOnlyType(s) && isTimeOnlyType(d) {
		return "time"
	}
	if strings.HasPrefix(s, "YEAR") || strings.HasPrefix(d, "YEAR") {
		return "year"
	}
	if isTemporalComparableType(s) && isTemporalComparableType(d) {
		return "datetime"
	}
	return ""
}

func buildTemporalCompareKinds(sourceCols, destCols []map[string]string) []string {
	colCount := len(sourceCols)
	if len(destCols) < colCount {
		colCount = len(destCols)
	}
	if colCount == 0 {
		return nil
	}
	kinds := make([]string, colCount)
	hasTemporal := false
	for i := 0; i < colCount; i++ {
		kind := classifyTemporalCompareKind(sourceCols[i]["dataType"], destCols[i]["dataType"])
		kinds[i] = kind
		if kind != "" {
			hasTemporal = true
		}
	}
	if !hasTemporal {
		return nil
	}
	return kinds
}

func normalizeTemporalValue(raw, kind string) string {
	if kind == "" {
		return raw
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == dataDispos.ValueNullPlaceholder || trimmed == dataDispos.ValueEmptyPlaceholder {
		return raw
	}

	switch kind {
	case "year":
		i, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return raw
		}
		return strconv.FormatInt(i, 10)
	case "time":
		// Handle datetime-like string first to avoid misclassifying "YYYY-MM-DD HH:MM:SS" as interval.
		if m := temporalDatetimePrefixRe.FindStringSubmatch(trimmed); len(m) == 3 {
			return m[2]
		}

		// Oracle interval fallback path: value may be total seconds text.
		if temporalNumericSecondsRe.MatchString(trimmed) {
			secondsFloat, secErr := strconv.ParseFloat(trimmed, 64)
			if secErr == nil {
				// Defensive compatibility: some Oracle drivers expose INTERVAL as
				// duration nanoseconds integer. Convert to seconds before HH:MM:SS.
				if math.Abs(secondsFloat) >= 1e10 {
					secondsFloat = secondsFloat / 1e9
				}
				totalSeconds := int64(math.Round(secondsFloat))
				sign := ""
				if totalSeconds < 0 {
					sign = "-"
					totalSeconds = -totalSeconds
				}
				hours := totalSeconds / 3600
				minutes := (totalSeconds % 3600) / 60
				seconds := totalSeconds % 60
				return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
			}
		}

		if m := temporalIntervalRe.FindStringSubmatch(trimmed); len(m) == 5 {
			dayPart, dayErr := strconv.Atoi(m[1])
			hourPart, hourErr := strconv.Atoi(m[2])
			if dayErr == nil && hourErr == nil {
				totalHours := dayPart*24 + hourPart
				sign := ""
				if totalHours < 0 {
					sign = "-"
					totalHours = -totalHours
				}
				return fmt.Sprintf("%s%02d:%s:%s", sign, totalHours, m[3], m[4])
			}
		}

		if m := temporalTimeTokenRe.FindStringSubmatch(trimmed); len(m) == 2 {
			return m[1]
		}
		// Go duration style fallback, e.g. "12h30m29s".
		if m := temporalGoDurationRe.FindStringSubmatch(trimmed); len(m) == 4 {
			hours, hErr := strconv.Atoi(m[1])
			minutes, mErr := strconv.Atoi(m[2])
			secondsFloat, sErr := strconv.ParseFloat(m[3], 64)
			if hErr == nil && mErr == nil && sErr == nil {
				seconds := int(math.Round(secondsFloat))
				sign := ""
				if hours < 0 {
					sign = "-"
					hours = -hours
				}
				return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
			}
		}
		if len(trimmed) >= 19 && trimmed[10] == ' ' {
			return trimmed[11:19]
		}
		return raw
	case "datetime":
		if temporalDateOnlyRe.MatchString(trimmed) {
			return trimmed + " 00:00:00"
		}
		if m := temporalDateTimeRe.FindStringSubmatch(trimmed); len(m) == 3 {
			return m[1] + " " + m[2]
		}
		return raw
	default:
		return raw
	}
}

func normalizeRowsForTemporalComparison(rows []string, kinds []string) []string {
	if len(rows) == 0 || len(kinds) == 0 {
		return rows
	}
	const columnSep = "/*go actions columnData*/"
	normalizedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		parts := strings.Split(row, columnSep)
		limit := len(kinds)
		if len(parts) < limit {
			limit = len(parts)
		}
		changed := false
		for colIdx := 0; colIdx < limit; colIdx++ {
			kind := kinds[colIdx]
			if kind == "" {
				continue
			}
			normalized := normalizeTemporalValue(parts[colIdx], kind)
			if normalized != parts[colIdx] {
				parts[colIdx] = normalized
				changed = true
			}
		}
		if changed {
			normalizedRows[rowIdx] = strings.Join(parts, columnSep)
		} else {
			normalizedRows[rowIdx] = row
		}
	}
	return normalizedRows
}

func reconcileTemporalNullArtifacts(addRows, delRows []string, kinds []string, sourceCols, destCols []map[string]string) ([]string, []string, int) {
	if len(addRows) == 0 || len(delRows) == 0 || len(kinds) == 0 {
		return addRows, delRows, 0
	}
	artifactCols := buildIntervalTimeArtifactColumns(sourceCols, destCols, kinds)
	if len(artifactCols) == 0 {
		return addRows, delRows, 0
	}
	const colSep = "/*go actions columnData*/"
	delUsed := make([]bool, len(delRows))
	delParts := make([][]string, len(delRows))
	delBuckets := make(map[string][]int, len(delRows))
	for idx, delRow := range delRows {
		parts := strings.Split(delRow, colSep)
		delParts[idx] = parts
		key := buildTemporalReconcileKey(parts, artifactCols)
		delBuckets[key] = append(delBuckets[key], idx)
	}
	keepAdd := make([]string, 0, len(addRows))
	healed := 0

	for _, addRow := range addRows {
		addParts := strings.Split(addRow, colSep)
		key := buildTemporalReconcileKey(addParts, artifactCols)
		matched := -1
		for _, idx := range delBuckets[key] {
			if delUsed[idx] {
				continue
			}
			if rowsEqualWithTemporalArtifact(addParts, delParts[idx], artifactCols) {
				matched = idx
				break
			}
		}
		if matched >= 0 {
			delUsed[matched] = true
			healed++
			continue
		}
		keepAdd = append(keepAdd, addRow)
	}

	keepDel := make([]string, 0, len(delRows))
	for i, delRow := range delRows {
		if !delUsed[i] {
			keepDel = append(keepDel, delRow)
		}
	}
	return keepAdd, keepDel, healed
}

func buildTemporalReconcileKey(parts []string, artifactCols map[int]struct{}) string {
	var b strings.Builder
	for i, part := range parts {
		if _, isArtifactCol := artifactCols[i]; isArtifactCol {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("|")
		}
		b.WriteString(strconv.Itoa(i))
		b.WriteString("=")
		b.WriteString(part)
	}
	return b.String()
}

func buildIntervalTimeArtifactColumns(sourceCols, destCols []map[string]string, kinds []string) map[int]struct{} {
	cols := make(map[int]struct{})
	limit := len(kinds)
	if len(sourceCols) < limit {
		limit = len(sourceCols)
	}
	if len(destCols) < limit {
		limit = len(destCols)
	}
	for i := 0; i < limit; i++ {
		if kinds[i] != "time" {
			continue
		}
		sType := strings.ToUpper(strings.TrimSpace(sourceCols[i]["dataType"]))
		dType := strings.ToUpper(strings.TrimSpace(destCols[i]["dataType"]))
		if strings.HasPrefix(sType, "INTERVAL DAY") && isTimeOnlyType(dType) {
			cols[i] = struct{}{}
			continue
		}
		if strings.HasPrefix(dType, "INTERVAL DAY") && isTimeOnlyType(sType) {
			cols[i] = struct{}{}
		}
	}
	return cols
}

func rowsEqualWithTemporalArtifact(addParts, delParts []string, artifactCols map[int]struct{}) bool {
	if len(addParts) != len(delParts) {
		return false
	}
	for i := range addParts {
		a := addParts[i]
		d := delParts[i]
		if a == d {
			continue
		}
		if _, ok := artifactCols[i]; ok {
			aNull := isNullPlaceholderValue(a)
			dNull := isNullPlaceholderValue(d)
			if aNull || dNull {
				if aNull && dNull {
					continue
				}
				return false
			}
			if normalizeTemporalValue(a, "time") == normalizeTemporalValue(d, "time") {
				continue
			}
		}
		return false
	}
	return true
}

func isNullPlaceholderValue(v string) bool {
	return v == dataDispos.ValueNullPlaceholder || strings.EqualFold(strings.TrimSpace(v), "null")
}
