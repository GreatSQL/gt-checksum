package schemacompat

import (
	"fmt"
	"testing"
)

// makeToken 构造标准格式的索引 token：colName/*seq*/N/*type*/T/*prefix*/P
func makeToken(col, seq, colType string, prefix int) string {
	return col + "/*seq*/" + seq + "/*type*/" + colType + "/*prefix*/" + fmt.Sprintf("%d", prefix)
}

// makeTokenNoPrefix 构造旧格式 token（不含 /*prefix*/ 段），用于向后兼容测试。
func makeTokenNoPrefix(col, seq, colType string) string {
	return col + "/*seq*/" + seq + "/*type*/" + colType
}

func TestCanonicalizeMySQLIndexes_PrefixParsing(t *testing.T) {
	t.Run("prefix_length_parsed_correctly", func(t *testing.T) {
		indexMap := map[string][]string{
			"idx_4": {makeToken("goods_name", "1", "varchar(50)", 20)},
		}
		visMap := map[string]string{"idx_4": "VISIBLE"}
		indexes := CanonicalizeMySQLIndexes(indexMap, visMap)
		if len(indexes) != 1 {
			t.Fatalf("expected 1 index, got %d", len(indexes))
		}
		idx := indexes[0]
		if len(idx.PrefixLength) != 1 || idx.PrefixLength[0] != 20 {
			t.Errorf("expected PrefixLength=[20], got %v", idx.PrefixLength)
		}
	})

	t.Run("no_prefix_gives_zero", func(t *testing.T) {
		indexMap := map[string][]string{
			"idx_4": {makeToken("goods_name", "1", "varchar(50)", 0)},
		}
		visMap := map[string]string{"idx_4": "VISIBLE"}
		indexes := CanonicalizeMySQLIndexes(indexMap, visMap)
		if len(indexes[0].PrefixLength) != 1 || indexes[0].PrefixLength[0] != 0 {
			t.Errorf("expected PrefixLength=[0], got %v", indexes[0].PrefixLength)
		}
	})

	t.Run("old_token_no_prefix_segment_backward_compat", func(t *testing.T) {
		indexMap := map[string][]string{
			"idx_old": {makeTokenNoPrefix("name", "1", "varchar(100)")},
		}
		visMap := map[string]string{"idx_old": "VISIBLE"}
		indexes := CanonicalizeMySQLIndexes(indexMap, visMap)
		if len(indexes[0].PrefixLength) != 1 || indexes[0].PrefixLength[0] != 0 {
			t.Errorf("old token: expected PrefixLength=[0], got %v", indexes[0].PrefixLength)
		}
	})
}

func TestDecideIndexCompatibility_PrefixMismatch(t *testing.T) {
	makeIndex := func(name, col string, prefix int) CanonicalIndex {
		return CanonicalIndex{
			Name:         name,
			Columns:      []string{col},
			PrefixLength: []int{prefix},
			Visibility:   IndexVisibilityVisible,
		}
	}

	t.Run("prefix20_vs_full_column_is_mismatch", func(t *testing.T) {
		src := makeIndex("idx_4", "goods_name", 20)
		dst := makeIndex("idx_4", "goods_name", 0)
		dec := DecideIndexCompatibility(src, dst)
		if !dec.IsMismatch() {
			t.Errorf("expected mismatch (prefix 20 vs 0), got compatible: %s", dec.Reason)
		}
	})

	t.Run("full_column_vs_prefix20_is_mismatch", func(t *testing.T) {
		src := makeIndex("idx_4", "goods_name", 0)
		dst := makeIndex("idx_4", "goods_name", 20)
		dec := DecideIndexCompatibility(src, dst)
		if !dec.IsMismatch() {
			t.Errorf("expected mismatch (prefix 0 vs 20), got compatible: %s", dec.Reason)
		}
	})

	t.Run("same_prefix_is_compatible", func(t *testing.T) {
		src := makeIndex("idx_4", "goods_name", 20)
		dst := makeIndex("idx_4", "goods_name", 20)
		dec := DecideIndexCompatibility(src, dst)
		if dec.IsMismatch() {
			t.Errorf("expected compatible (same prefix 20), got mismatch: %s", dec.Reason)
		}
	})

	t.Run("unique_index_prefix_mismatch", func(t *testing.T) {
		src := CanonicalIndex{
			Name:         "uk_1",
			Type:         "UNIQUE",
			Columns:      []string{"email"},
			PrefixLength: []int{50},
			Visibility:   IndexVisibilityVisible,
		}
		dst := CanonicalIndex{
			Name:         "uk_1",
			Type:         "UNIQUE",
			Columns:      []string{"email"},
			PrefixLength: []int{0},
			Visibility:   IndexVisibilityVisible,
		}
		dec := DecideIndexCompatibility(src, dst)
		if !dec.IsMismatch() {
			t.Errorf("unique index: expected mismatch (prefix 50 vs 0), got compatible: %s", dec.Reason)
		}
	})

	t.Run("multi_col_partial_prefix_mismatch", func(t *testing.T) {
		// (a(10), b(5)) vs (a(10), b) — b 的前缀不同
		src := CanonicalIndex{
			Name:         "idx_multi",
			Columns:      []string{"a", "b"},
			PrefixLength: []int{10, 5},
			Visibility:   IndexVisibilityVisible,
		}
		dst := CanonicalIndex{
			Name:         "idx_multi",
			Columns:      []string{"a", "b"},
			PrefixLength: []int{10, 0},
			Visibility:   IndexVisibilityVisible,
		}
		dec := DecideIndexCompatibility(src, dst)
		if !dec.IsMismatch() {
			t.Errorf("multi-col: expected mismatch (b: prefix 5 vs 0), got compatible: %s", dec.Reason)
		}
	})

	t.Run("multi_col_all_same_prefix_compatible", func(t *testing.T) {
		src := CanonicalIndex{
			Name:         "idx_multi",
			Columns:      []string{"a", "b"},
			PrefixLength: []int{10, 5},
			Visibility:   IndexVisibilityVisible,
		}
		dst := CanonicalIndex{
			Name:         "idx_multi",
			Columns:      []string{"a", "b"},
			PrefixLength: []int{10, 5},
			Visibility:   IndexVisibilityVisible,
		}
		dec := DecideIndexCompatibility(src, dst)
		if dec.IsMismatch() {
			t.Errorf("multi-col: expected compatible (same prefixes), got mismatch: %s", dec.Reason)
		}
	})
}
