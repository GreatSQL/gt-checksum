package schemacompat

import (
	"testing"
)

// makeFuncToken 构造函数索引 token，格式：/*expr*/EXPRESSION/*seq*/N/*type*//*prefix*/0
func makeFuncToken(expr, seq string) string {
	return "/*expr*/" + expr + "/*seq*/" + seq + "/*type*//*prefix*/0"
}

// ---------- CanonicalizeMySQLIndexes 函数索引解析 ----------

func TestCanonicalizeMySQLIndexes_FuncIndex_Parsed(t *testing.T) {
	indexMap := map[string][]string{
		"idx_5": {makeFuncToken("(ABS(`price`))", "1")},
	}
	visMap := map[string]string{"idx_5": "VISIBLE"}
	indexes := CanonicalizeMySQLIndexes(indexMap, visMap)
	if len(indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(indexes))
	}
	idx := indexes[0]
	if idx.Name != "idx_5" {
		t.Errorf("expected Name=idx_5, got %q", idx.Name)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "" {
		t.Errorf("expected Columns=[\"\"], got %v", idx.Columns)
	}
	if len(idx.NormalizedExpressions) != 1 {
		t.Fatalf("expected NormalizedExpressions len=1, got %d", len(idx.NormalizedExpressions))
	}
	// 规范化后应为小写
	want := "(abs(`price`))"
	if idx.NormalizedExpressions[0] != want {
		t.Errorf("expected NormalizedExpressions[0]=%q, got %q", want, idx.NormalizedExpressions[0])
	}
}

func TestCanonicalizeMySQLIndexes_FuncIndex_MultiExpr(t *testing.T) {
	// 多个函数索引按序号排序
	indexMap := map[string][]string{
		"idx_multi": {
			makeFuncToken("(ABS(`b`))", "2"),
			makeFuncToken("(LOWER(`a`))", "1"),
		},
	}
	visMap := map[string]string{"idx_multi": "VISIBLE"}
	indexes := CanonicalizeMySQLIndexes(indexMap, visMap)
	if len(indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(indexes))
	}
	idx := indexes[0]
	if len(idx.NormalizedExpressions) != 2 {
		t.Fatalf("expected 2 exprs, got %d", len(idx.NormalizedExpressions))
	}
	// seq=1 在前，seq=2 在后
	if idx.NormalizedExpressions[0] != "(lower(`a`))" {
		t.Errorf("first expr should be (lower(`a`)), got %q", idx.NormalizedExpressions[0])
	}
	if idx.NormalizedExpressions[1] != "(abs(`b`))" {
		t.Errorf("second expr should be (abs(`b`)), got %q", idx.NormalizedExpressions[1])
	}
}

func TestCanonicalizeMySQLIndexes_RegularIndex_HasEmptyExprs(t *testing.T) {
	// 普通索引的 NormalizedExpressions 全为空字符串
	indexMap := map[string][]string{
		"idx_price": {makeToken("price", "1", "decimal(10,2)", 0)},
	}
	visMap := map[string]string{"idx_price": "VISIBLE"}
	indexes := CanonicalizeMySQLIndexes(indexMap, visMap)
	idx := indexes[0]
	if len(idx.NormalizedExpressions) != 1 || idx.NormalizedExpressions[0] != "" {
		t.Errorf("regular index should have empty NormalizedExpressions, got %v", idx.NormalizedExpressions)
	}
}

// ---------- DecideIndexCompatibility 函数索引比对 ----------

func TestDecideIndexCompatibility_FuncVsRegular_Mismatch(t *testing.T) {
	// 源端函数索引 vs 目标端普通索引 → 应为不匹配
	src := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`price`))"},
	}
	dst := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{"price"},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{""},
	}
	dec := DecideIndexCompatibility(src, dst)
	if !dec.IsMismatch() {
		t.Errorf("expected mismatch (func vs regular), but got Equal. Reason: %s", dec.Reason)
	}
}

func TestDecideIndexCompatibility_SameFuncExpr_Equal(t *testing.T) {
	// 两端相同函数索引 → Equal
	src := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`price`))"},
	}
	dst := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`price`))"},
	}
	dec := DecideIndexCompatibility(src, dst)
	if dec.IsMismatch() {
		t.Errorf("expected Equal (same func expr), got mismatch. Reason: %s", dec.Reason)
	}
}

func TestDecideIndexCompatibility_DifferentFuncExpr_Mismatch(t *testing.T) {
	// 两端函数索引表达式不同 → Mismatch
	src := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`price`))"},
	}
	dst := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`qty`))"},
	}
	dec := DecideIndexCompatibility(src, dst)
	if !dec.IsMismatch() {
		t.Errorf("expected mismatch (different func expr), got Equal. Reason: %s", dec.Reason)
	}
}

func TestDecideIndexCompatibility_RegularVsFunc_Mismatch(t *testing.T) {
	// 源端普通索引 vs 目标端函数索引 → 应为不匹配（反向验证）
	src := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{"price"},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{""},
	}
	dst := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`price`))"},
	}
	dec := DecideIndexCompatibility(src, dst)
	if !dec.IsMismatch() {
		t.Errorf("expected mismatch (regular vs func), got Equal. Reason: %s", dec.Reason)
	}
}

func TestDecideIndexCompatibility_FuncIndex_ColumnCountMismatch(t *testing.T) {
	// 含函数列时，列数不同 → Mismatch
	src := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{"", "col_b"},
		PrefixLength:          []int{0, 0},
		NormalizedExpressions: []string{"(abs(`price`))", ""},
	}
	dst := CanonicalIndex{
		Name:                  "idx_5",
		Columns:               []string{""},
		PrefixLength:          []int{0},
		NormalizedExpressions: []string{"(abs(`price`))"},
	}
	dec := DecideIndexCompatibility(src, dst)
	if !dec.IsMismatch() {
		t.Errorf("expected mismatch (column count differs), got Equal. Reason: %s", dec.Reason)
	}
}

func TestDecideIndexCompatibility_MixedFuncAndRegular_Equal(t *testing.T) {
	// 混合索引（第1列函数，第2列普通），两端相同 → Equal
	src := CanonicalIndex{
		Name:                  "idx_mix",
		Columns:               []string{"", "col_b"},
		PrefixLength:          []int{0, 0},
		NormalizedExpressions: []string{"(abs(`price`))", ""},
	}
	dst := CanonicalIndex{
		Name:                  "idx_mix",
		Columns:               []string{"", "col_b"},
		PrefixLength:          []int{0, 0},
		NormalizedExpressions: []string{"(abs(`price`))", ""},
	}
	dec := DecideIndexCompatibility(src, dst)
	if dec.IsMismatch() {
		t.Errorf("expected Equal (same mixed index), got mismatch. Reason: %s", dec.Reason)
	}
}
