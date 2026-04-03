package actions

import "testing"

func TestColumnsModeSplitPKAndCompare_CaseInsensitive(t *testing.T) {
	filteredCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
		{"columnName": "note", "dataType": "varchar(32)"},
	}

	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredCols, []string{"ID"})
	if len(pkPositions) != 1 || pkPositions[0] != 0 {
		t.Fatalf("pkPositions = %v, want [0]", pkPositions)
	}
	if len(compareColNames) != 2 {
		t.Fatalf("compareColNames len = %d, want 2", len(compareColNames))
	}
	if compareColNames[0] != "amount" || compareColNames[1] != "note" {
		t.Fatalf("compareColNames = %v, want [amount note]", compareColNames)
	}
}

func TestColumnsModeExtractPKKey_CaseInsensitiveSplitKeepsDistinctRows(t *testing.T) {
	filteredCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
	}
	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredCols, []string{"ID"})
	if len(compareColNames) != 1 || compareColNames[0] != "amount" {
		t.Fatalf("compareColNames = %v, want [amount]", compareColNames)
	}

	row1 := "1/*go actions columnData*/10.00"
	row2 := "2/*go actions columnData*/20.00"
	key1 := columnsModeExtractPKKey(row1, pkPositions)
	key2 := columnsModeExtractPKKey(row2, pkPositions)
	if key1 == key2 {
		t.Fatalf("distinct PK rows produced same key: %q", key1)
	}
}

// TestColumnsModeSplitPKAndCompare_CompositePK 验证复合 PK（两列）场景下
// pkPositions 和 compareColNames 的正确性。
func TestColumnsModeSplitPKAndCompare_CompositePK(t *testing.T) {
	// 过滤后列顺序：id(PK), amount(compare), tenant_id(PK)
	filteredCols := []map[string]string{
		{"columnName": "id", "dataType": "bigint"},
		{"columnName": "amount", "dataType": "decimal(10,2)"},
		{"columnName": "tenant_id", "dataType": "varchar(64)"},
	}

	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredCols, []string{"id", "tenant_id"})
	if len(pkPositions) != 2 {
		t.Fatalf("pkPositions len = %d, want 2", len(pkPositions))
	}
	if pkPositions[0] != 0 || pkPositions[1] != 2 {
		t.Fatalf("pkPositions = %v, want [0 2]", pkPositions)
	}
	if len(compareColNames) != 1 || compareColNames[0] != "amount" {
		t.Fatalf("compareColNames = %v, want [amount]", compareColNames)
	}

	// 复合 PK key 编码：两列用 \x00 分隔，不同组合产生不同 key
	row1 := "1/*go actions columnData*/100.00/*go actions columnData*/T1"
	row2 := "1/*go actions columnData*/200.00/*go actions columnData*/T2"
	row3 := "2/*go actions columnData*/100.00/*go actions columnData*/T1"

	key1 := columnsModeExtractPKKey(row1, pkPositions) // PK = (1, T1)
	key2 := columnsModeExtractPKKey(row2, pkPositions) // PK = (1, T2)
	key3 := columnsModeExtractPKKey(row3, pkPositions) // PK = (2, T1)

	if key1 == key2 {
		t.Fatalf("rows with same id but different tenant_id produced same PK key: %q", key1)
	}
	if key1 == key3 {
		t.Fatalf("rows with different id but same tenant_id produced same PK key: %q", key1)
	}
	if key2 == key3 {
		t.Fatalf("rows with different id and different tenant_id produced same PK key: %q", key2)
	}
}

// TestColumnsModeExtractPKKey_OutOfBoundsSentinel 验证 pos 越界时返回哨兵值（不 panic，
// 不与合法空字符串 PK 值产生碰撞）。
func TestColumnsModeExtractPKKey_OutOfBoundsSentinel(t *testing.T) {
	// 行只有 1 列，但 pkPositions 要求读取位置 1（越界）
	row := "only-one-col"
	pkPositions := []int{0, 1}

	key := columnsModeExtractPKKey(row, pkPositions)

	// 第 1 个分量应为 "only-one-col"
	// 第 2 个分量应为哨兵值，而不是合法空字符串 ""
	parts := splitOnNUL(key)
	if len(parts) != 2 {
		t.Fatalf("expected 2 key components, got %d: %q", len(parts), key)
	}
	if parts[0] != "only-one-col" {
		t.Fatalf("parts[0] = %q, want %q", parts[0], "only-one-col")
	}
	if parts[1] == "" {
		t.Fatal("out-of-bounds PK component must not be empty string (would collide with legitimate empty-string PK)")
	}
	if parts[1] != pkKeyMissingMarker {
		t.Fatalf("out-of-bounds PK component = %q, want pkKeyMissingMarker", parts[1])
	}

	// 合法空字符串 PK 与缺失分量必须产生不同的 key
	rowWithEmptyPK := "/*go actions columnData*/"
	pkPositionsSingle := []int{0}
	keyEmpty := columnsModeExtractPKKey(rowWithEmptyPK, pkPositionsSingle)

	rowMissing := "only-first-col"
	pkPositionsMissing := []int{0, 1}
	keyMissing := columnsModeExtractPKKey(rowMissing, pkPositionsMissing)

	if keyEmpty == keyMissing {
		t.Fatalf("legitimate empty PK and missing PK component must produce different keys; both got %q", keyEmpty)
	}
}

// TestColumnsModeNormalizationSkipsPKCols_Float 验证 columns 模式下 PK 列（floatCompareScales[pos]=-1）
// 不会被 float 归一化修改，而 compare 列正常被归一化。
func TestColumnsModeNormalizationSkipsPKCols_Float(t *testing.T) {
	// col0 = FLOAT PK（屏蔽，-1），col1 = DOUBLE(10,2) compare col（scale=2）
	scales := []int{-1, 2}
	row := "1.123456789/*go actions columnData*/3.14159"
	result := normalizeRowsForFloatComparison([]string{row}, scales)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	const sep = "/*go actions columnData*/"
	parts := splitByColumnSep(result[0], sep)
	if len(parts) != 2 {
		t.Fatalf("expected 2 columns, got %d: %q", len(parts), result[0])
	}
	// PK 列（pos 0）不应被修改
	if parts[0] != "1.123456789" {
		t.Fatalf("PK column was unexpectedly normalized: got %q, want %q", parts[0], "1.123456789")
	}
	// compare 列（pos 1）应被四舍五入到 2 位小数
	if parts[1] != "3.14" {
		t.Fatalf("compare column not normalized correctly: got %q, want %q", parts[1], "3.14")
	}
}

// TestColumnsModeNormalizationSkipsPKCols_Temporal 验证 columns 模式下 PK 列（temporalCompareKinds[pos]=""）
// 不会被时间归一化截断，而 compare 列正常被截断到秒级。
// TIMESTAMP/DATETIME 类型在 classifyTemporalCompareKind 中被归类为 "datetime" kind。
func TestColumnsModeNormalizationSkipsPKCols_Temporal(t *testing.T) {
	// col0 = TIMESTAMP(6) PK（屏蔽，kind=""），col1 = TIMESTAMP compare col（kind="datetime"）
	// 注意：classifyTemporalCompareKind 对 TIMESTAMP 和 DATETIME 都返回 "datetime"，不是 "timestamp"
	kinds := []string{"", "datetime"}
	row := "2026-04-03 12:00:00.654321/*go actions columnData*/2026-04-03 12:00:00.123456"
	result := normalizeRowsForTemporalComparison([]string{row}, kinds)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}

	const sep = "/*go actions columnData*/"
	parts := splitByColumnSep(result[0], sep)
	if len(parts) != 2 {
		t.Fatalf("expected 2 columns, got %d: %q", len(parts), result[0])
	}
	// PK 列（pos 0）不应被截断，原始微秒精度保留
	if parts[0] != "2026-04-03 12:00:00.654321" {
		t.Fatalf("PK column was unexpectedly normalized: got %q, want original microsecond value", parts[0])
	}
	// compare 列（pos 1）应被截断到秒
	if parts[1] != "2026-04-03 12:00:00" {
		t.Fatalf("compare column not normalized correctly: got %q, want %q", parts[1], "2026-04-03 12:00:00")
	}
}

func TestColumnsModeNormalizationPipeline_MasksTemporalPKInRealFlow(t *testing.T) {
	filteredSrcCols := columnsModeFilteredCols(
		[]map[string]string{
			{"columnName": "note", "dataType": "varchar(32)"},
			{"columnName": "event_ts", "dataType": "timestamp(6)"},
			{"columnName": "updated_at", "dataType": "datetime(6)"},
		},
		[]string{"updated_at"},
		[]string{"event_ts"},
	)
	filteredDstCols := columnsModeFilteredCols(
		[]map[string]string{
			{"columnName": "updated_at", "dataType": "datetime"},
			{"columnName": "event_ts", "dataType": "timestamp"},
			{"columnName": "note", "dataType": "varchar(32)"},
		},
		[]string{"updated_at"},
		[]string{"event_ts"},
	)

	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredSrcCols, []string{"event_ts"})
	if len(pkPositions) != 1 || pkPositions[0] != 0 {
		t.Fatalf("pkPositions = %v, want [0]", pkPositions)
	}
	if len(compareColNames) != 1 || compareColNames[0] != "updated_at" {
		t.Fatalf("compareColNames = %v, want [updated_at]", compareColNames)
	}

	rows := []string{
		"2026-04-03 12:00:00.111111/*go actions columnData*/2026-04-03 13:00:00.654321",
		"2026-04-03 12:00:00.222222/*go actions columnData*/2026-04-03 13:00:00.999999",
	}

	unmaskedKinds := buildTemporalCompareKinds(filteredSrcCols, filteredDstCols)
	if len(unmaskedKinds) != 2 || unmaskedKinds[0] != "datetime" || unmaskedKinds[1] != "datetime" {
		t.Fatalf("unmaskedKinds = %v, want [datetime datetime]", unmaskedKinds)
	}
	unmaskedRows := normalizeRowsForTemporalComparison(rows, unmaskedKinds)
	if columnsModeExtractPKKey(unmaskedRows[0], pkPositions) == columnsModeExtractPKKey(unmaskedRows[1], pkPositions) {
		// Expected: without the real-flow PK mask, timestamp PK precision collapses to second-level.
	} else {
		t.Fatalf("expected unmasked temporal normalization to collapse PK precision, got rows %q and %q", unmaskedRows[0], unmaskedRows[1])
	}

	maskedKinds := append([]string(nil), unmaskedKinds...)
	for _, pos := range pkPositions {
		maskedKinds[pos] = ""
	}
	maskedRows := normalizeRowsForTemporalComparison(rows, maskedKinds)
	if columnsModeExtractPKKey(maskedRows[0], pkPositions) == columnsModeExtractPKKey(maskedRows[1], pkPositions) {
		t.Fatalf("masked temporal normalization still collapsed PK precision: %q vs %q", maskedRows[0], maskedRows[1])
	}

	parts := splitByColumnSep(maskedRows[0], "/*go actions columnData*/")
	if len(parts) != 2 {
		t.Fatalf("expected 2 columns, got %d: %q", len(parts), maskedRows[0])
	}
	if parts[0] != "2026-04-03 12:00:00.111111" {
		t.Fatalf("temporal PK should preserve microseconds, got %q", parts[0])
	}
	if parts[1] != "2026-04-03 13:00:00" {
		t.Fatalf("compare column should normalize to second precision, got %q", parts[1])
	}
}

func TestColumnsModeNormalizationPipeline_MasksFloatPKInRealFlow(t *testing.T) {
	filteredSrcCols := columnsModeFilteredCols(
		[]map[string]string{
			{"columnName": "id_float", "dataType": "float(10,2)"},
			{"columnName": "score", "dataType": "float(10,2)"},
			{"columnName": "note", "dataType": "varchar(32)"},
		},
		[]string{"score"},
		[]string{"id_float"},
	)
	filteredDstCols := columnsModeFilteredCols(
		[]map[string]string{
			{"columnName": "score", "dataType": "float(10,2)"},
			{"columnName": "note", "dataType": "varchar(32)"},
			{"columnName": "id_float", "dataType": "float(10,2)"},
		},
		[]string{"score"},
		[]string{"id_float"},
	)

	pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredSrcCols, []string{"id_float"})
	if len(pkPositions) != 1 || pkPositions[0] != 0 {
		t.Fatalf("pkPositions = %v, want [0]", pkPositions)
	}
	if len(compareColNames) != 1 || compareColNames[0] != "score" {
		t.Fatalf("compareColNames = %v, want [score]", compareColNames)
	}

	rows := []string{
		"1.234/*go actions columnData*/3.14159",
		"1.232/*go actions columnData*/2.71828",
	}

	unmaskedScales := buildFloatComparisonScales(filteredSrcCols, filteredDstCols)
	if len(unmaskedScales) != 2 || unmaskedScales[0] != 2 || unmaskedScales[1] != 2 {
		t.Fatalf("unmaskedScales = %v, want [2 2]", unmaskedScales)
	}
	unmaskedRows := normalizeRowsForFloatComparison(rows, unmaskedScales)
	if columnsModeExtractPKKey(unmaskedRows[0], pkPositions) == columnsModeExtractPKKey(unmaskedRows[1], pkPositions) {
		// Expected: without the real-flow PK mask, float PK precision collapses to scale 2.
	} else {
		t.Fatalf("expected unmasked float normalization to collapse PK precision, got rows %q and %q", unmaskedRows[0], unmaskedRows[1])
	}

	maskedScales := append([]int(nil), unmaskedScales...)
	for _, pos := range pkPositions {
		maskedScales[pos] = -1
	}
	maskedRows := normalizeRowsForFloatComparison(rows, maskedScales)
	if columnsModeExtractPKKey(maskedRows[0], pkPositions) == columnsModeExtractPKKey(maskedRows[1], pkPositions) {
		t.Fatalf("masked float normalization still collapsed PK precision: %q vs %q", maskedRows[0], maskedRows[1])
	}

	parts := splitByColumnSep(maskedRows[0], "/*go actions columnData*/")
	if len(parts) != 2 {
		t.Fatalf("expected 2 columns, got %d: %q", len(parts), maskedRows[0])
	}
	if parts[0] != "1.234" {
		t.Fatalf("float PK should preserve original precision, got %q", parts[0])
	}
	if parts[1] != "3.14" {
		t.Fatalf("compare column should normalize to scale 2, got %q", parts[1])
	}
}

// splitOnNUL 按 \x00 分割 PK key（与 columnsModeExtractPKKey 的 Join 对称）。
func splitOnNUL(s string) []string {
	return splitByColumnSep(s, "\x00")
}

func splitByColumnSep(s, sep string) []string {
	var result []string
	start := 0
	for {
		idx := findSubstr(s, sep, start)
		if idx < 0 {
			result = append(result, s[start:])
			break
		}
		result = append(result, s[start:idx])
		start = idx + len(sep)
	}
	return result
}

func findSubstr(s, sub string, from int) int {
	if len(sub) == 0 {
		return from
	}
	for i := from; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
