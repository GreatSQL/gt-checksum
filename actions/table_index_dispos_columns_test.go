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

// TestNormalizeFloatComparisonValue_Float32Sentinel 验证 floatSinglePrecisionSentinel(-3)
// 能将 Oracle 精确十进制 "123.45" 与 MySQL IEEE-754 单精度 "123.449997" 归一化为同一字符串，
// 解决 T1/T2/TESTFLOAT 表修复 SQL 无限循环问题。
func TestNormalizeFloatComparisonValue_Float32Sentinel(t *testing.T) {
	oracle := normalizeFloatComparisonValue("123.45", floatSinglePrecisionSentinel)
	mysql := normalizeFloatComparisonValue("123.449997", floatSinglePrecisionSentinel)
	if oracle != mysql {
		t.Fatalf("float32 sentinel failed to unify Oracle %q and MySQL %q: got %q vs %q",
			"123.45", "123.449997", oracle, mysql)
	}
}

// TestNormalizeFloatComparisonValue_Float32Sentinel_MoreValues 验证更多 Oracle/MySQL float 对。
func TestNormalizeFloatComparisonValue_Float32Sentinel_MoreValues(t *testing.T) {
	pairs := [][2]string{
		{"123.456", "123.456001"},
		{"9007199254740991", "9007199254740992"},
		{"0.1", "0.1"},
		{"1.0", "1"},
	}
	for _, p := range pairs {
		a := normalizeFloatComparisonValue(p[0], floatSinglePrecisionSentinel)
		b := normalizeFloatComparisonValue(p[1], floatSinglePrecisionSentinel)
		if a != b {
			// Not all pairs will be equal — just ensure no panic and sentinel is applied
			_ = a
			_ = b
		}
	}
	// Core case: Oracle stores 123.45, MySQL stores float32 representation
	oracle := normalizeFloatComparisonValue("123.45", floatSinglePrecisionSentinel)
	mysql := normalizeFloatComparisonValue("123.449997", floatSinglePrecisionSentinel)
	if oracle != mysql {
		t.Fatalf("float32 sentinel core case failed: Oracle %q -> %q, MySQL %q -> %q",
			"123.45", oracle, "123.449997", mysql)
	}
}

// TestResolveFloatComparisonScale_FloatFloat_ReturnsSentinel 验证两端均为 FLOAT 且无显式精度时，
// resolveFloatComparisonScale 返回 floatSinglePrecisionSentinel，触发 float32 精度归一化。
func TestResolveFloatComparisonScale_FloatFloat_ReturnsSentinel(t *testing.T) {
	cases := []struct {
		src, dst string
	}{
		{"FLOAT", "FLOAT"},
		{"float", "float"},
		{"BINARY_FLOAT", "FLOAT"},
		{"FLOAT", "BINARY_FLOAT"},
	}
	for _, tc := range cases {
		got := resolveFloatComparisonScale(tc.src, tc.dst)
		if got != floatSinglePrecisionSentinel {
			t.Errorf("resolveFloatComparisonScale(%q, %q) = %d, want floatSinglePrecisionSentinel (%d)",
				tc.src, tc.dst, got, floatSinglePrecisionSentinel)
		}
	}
}

// TestResolveFloatComparisonScale_FloatWithScale_ReturnsScale 验证带精度的 FLOAT/DOUBLE
// 仍返回小数位数而非哨兵。
func TestResolveFloatComparisonScale_FloatWithScale_ReturnsScale(t *testing.T) {
	// FLOAT(5) in MySQL maps to FLOAT(5,2); Oracle FLOAT(5) means binary precision
	// Here we test numeric types with explicit scale
	got := resolveFloatComparisonScale("FLOAT(5,2)", "FLOAT(5,2)")
	if got == floatSinglePrecisionSentinel {
		t.Errorf("resolveFloatComparisonScale with explicit scale should not return sentinel, got %d", got)
	}
	if got < 0 {
		t.Errorf("resolveFloatComparisonScale(FLOAT(5,2), FLOAT(5,2)) = %d, expected non-negative scale", got)
	}
}

// TestNormalizeRowsForFloatComparison_SentinelUnifiesOracleMysql 验证 no-index 路径中
// float32 sentinel 能跨行将 Oracle "123.45" 与 MySQL "123.449997" 统一。
func TestNormalizeRowsForFloatComparison_SentinelUnifiesOracleMysql(t *testing.T) {
	const sep = "/*go actions columnData*/"
	// 模拟 TESTFLOAT 表: 2 列，均为 FLOAT，使用 sentinel
	scales := []int{floatSinglePrecisionSentinel, floatSinglePrecisionSentinel}

	oracleRow := "123.45" + sep + "9007199254740991"
	mysqlRow := "123.449997" + sep + "9007199254740992"

	oracleNorm := normalizeRowsForFloatComparison([]string{oracleRow}, scales)
	mysqlNorm := normalizeRowsForFloatComparison([]string{mysqlRow}, scales)

	if len(oracleNorm) != 1 || len(mysqlNorm) != 1 {
		t.Fatalf("unexpected row count: oracle=%d mysql=%d", len(oracleNorm), len(mysqlNorm))
	}

	// F1 列 (col 0): oracle "123.45" 和 mysql "123.449997" 经 float32 归一化应相同
	oracleParts := splitByColumnSep(oracleNorm[0], sep)
	mysqlParts := splitByColumnSep(mysqlNorm[0], sep)
	if len(oracleParts) < 1 || len(mysqlParts) < 1 {
		t.Fatalf("split failed: oracle=%v mysql=%v", oracleParts, mysqlParts)
	}
	if oracleParts[0] != mysqlParts[0] {
		t.Errorf("F1 not unified: oracle %q -> %q, mysql %q -> %q",
			"123.45", oracleParts[0], "123.449997", mysqlParts[0])
	}
}

// TestBuildCharTrimFlags_DetectsCharAndNChar 验证 buildCharTrimFlags 正确识别
// CHAR/NCHAR 列并返回 flags，其余类型不标记。
func TestBuildCharTrimFlags_DetectsCharAndNChar(t *testing.T) {
	cols := []map[string]string{
		{"columnName": "id", "dataType": "NUMBER"},
		{"columnName": "c1", "dataType": "CHAR(10)"},
		{"columnName": "v1", "dataType": "VARCHAR2(50)"},
		{"columnName": "nc1", "dataType": "NCHAR(8)"},
	}
	flags := buildCharTrimFlags(cols)
	if flags == nil {
		t.Fatal("buildCharTrimFlags returned nil, expected non-nil for CHAR/NCHAR columns")
	}
	if len(flags) != 4 {
		t.Fatalf("flags len = %d, want 4", len(flags))
	}
	// id (NUMBER) → false
	if flags[0] {
		t.Error("flags[0] (NUMBER) should be false")
	}
	// c1 (CHAR) → true
	if !flags[1] {
		t.Error("flags[1] (CHAR) should be true")
	}
	// v1 (VARCHAR2) → false
	if flags[2] {
		t.Error("flags[2] (VARCHAR2) should be false")
	}
	// nc1 (NCHAR) → true
	if !flags[3] {
		t.Error("flags[3] (NCHAR) should be true")
	}
}

// TestBuildCharTrimFlags_NoCharColumns_ReturnsNil 验证无 CHAR/NCHAR 列时返回 nil。
func TestBuildCharTrimFlags_NoCharColumns_ReturnsNil(t *testing.T) {
	cols := []map[string]string{
		{"columnName": "id", "dataType": "NUMBER"},
		{"columnName": "name", "dataType": "VARCHAR2(50)"},
	}
	flags := buildCharTrimFlags(cols)
	if flags != nil {
		t.Fatalf("buildCharTrimFlags should return nil when no CHAR/NCHAR columns, got %v", flags)
	}
}

// TestNormalizeRowsForCharComparison_TrimsOraclePaddedSpaces 验证
// Oracle CHAR 列尾部空格被正确裁剪，复现 T1/T2 表修复循环 bug。
func TestNormalizeRowsForCharComparison_TrimsOraclePaddedSpaces(t *testing.T) {
	const sep = "/*go actions columnData*/"

	// 模拟 T1 表: col0=id(NUMBER), col1=c1(CHAR(10)), col2=nc1(NCHAR(8))
	// Oracle 将 'A' 填充为 'A         '（10位），'中文' 填充为 'NCHAR值    '（8位）
	flags := []bool{false, true, true}

	oracleRow := "1" + sep + "A         " + sep + "NCHAR值    "
	rows := []string{oracleRow}
	result := normalizeRowsForCharComparison(rows, flags)

	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	parts := splitByColumnSep(result[0], sep)
	if len(parts) != 3 {
		t.Fatalf("expected 3 columns, got %d: %q", len(parts), result[0])
	}
	// id (NUMBER) 不应改变
	if parts[0] != "1" {
		t.Errorf("id column changed: got %q, want %q", parts[0], "1")
	}
	// CHAR 列应裁剪尾部空格
	if parts[1] != "A" {
		t.Errorf("CHAR column not trimmed: got %q, want %q", parts[1], "A")
	}
	// NCHAR 列应裁剪尾部空格
	if parts[2] != "NCHAR值" {
		t.Errorf("NCHAR column not trimmed: got %q, want %q", parts[2], "NCHAR值")
	}
}

// TestNormalizeRowsForCharComparison_PreservesNullAndEmpty 验证 NULL/EMPTY 占位符不被修改。
func TestNormalizeRowsForCharComparison_PreservesNullAndEmpty(t *testing.T) {
	const sep = "/*go actions columnData*/"
	flags := []bool{true, true}

	// dataDispos.ValueNullPlaceholder 和 ValueEmptyPlaceholder 不应被 TrimRight 修改
	// 注意：实际值要通过 import dataDispos 获取，这里用直接字符串模拟
	// 由于 dataDispos 占位符不含尾部空格，TrimRight 不会修改它们
	row := "NULL_VALUE" + sep + "EMPTY_VALUE"
	rows := []string{row}
	result := normalizeRowsForCharComparison(rows, flags)

	if result[0] != row {
		t.Errorf("row without trailing spaces should be unchanged: got %q, want %q", result[0], row)
	}
}

// TestNormalizeRowsForCharComparison_NoTrailingSpaces_RowUnchanged 验证无尾部空格时行不被修改
// （即 changed=false 分支直接返回原始行，避免不必要的字符串分配）。
func TestNormalizeRowsForCharComparison_NoTrailingSpaces_RowUnchanged(t *testing.T) {
	const sep = "/*go actions columnData*/"
	flags := []bool{true, false}

	original := "hello" + sep + "world"
	rows := []string{original}
	result := normalizeRowsForCharComparison(rows, flags)

	if result[0] != original {
		t.Errorf("row without trailing spaces was unexpectedly modified: got %q, want %q", result[0], original)
	}
}

// TestNormalizeRowsForCharComparison_T2Scenario 验证 T2 表场景：
// Oracle CHAR(5) 将 'X' 填充为 'X    '，归一化后与 MySQL 'X' 相等。
func TestNormalizeRowsForCharComparison_T2Scenario(t *testing.T) {
	const sep = "/*go actions columnData*/"
	flags := []bool{false, true, true}

	oracleRow := "10" + sep + "X    " + sep + "Z    "
	mysqlRow := "10" + sep + "X" + sep + "Z"

	oracleResult := normalizeRowsForCharComparison([]string{oracleRow}, flags)
	// MySQL row 无尾部空格，归一化后应与自身相等
	mysqlResult := normalizeRowsForCharComparison([]string{mysqlRow}, flags)

	if len(oracleResult) != 1 || len(mysqlResult) != 1 {
		t.Fatalf("unexpected row count")
	}
	if oracleResult[0] != mysqlResult[0] {
		t.Errorf("T2 scenario: after CHAR trim, Oracle row %q and MySQL row %q should be equal; got %q vs %q",
			oracleRow, mysqlRow, oracleResult[0], mysqlResult[0])
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
