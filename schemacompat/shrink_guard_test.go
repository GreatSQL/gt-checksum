package schemacompat

import "testing"

func TestBuildColumnShrinkGuard_VarcharShrink(t *testing.T) {
	source := CanonicalColumn{NormalizedType: "varchar(100)"}
	target := CanonicalColumn{NormalizedType: "varchar(200)"}
	guard, ok := BuildColumnShrinkGuard(source, target)
	if !ok {
		t.Fatal("expected shrink guard for varchar(100)→varchar(200)")
	}
	if guard.Metric != ColumnShrinkMetricCharLength {
		t.Fatalf("expected CHAR_LENGTH metric, got %s", guard.Metric)
	}
	if guard.Limit != 100 {
		t.Fatalf("expected limit 100, got %d", guard.Limit)
	}
}

func TestBuildColumnShrinkGuard_NoShrink(t *testing.T) {
	source := CanonicalColumn{NormalizedType: "varchar(200)"}
	target := CanonicalColumn{NormalizedType: "varchar(100)"}
	_, ok := BuildColumnShrinkGuard(source, target)
	if ok {
		t.Fatal("should not detect shrink when source is wider")
	}
}

func TestBuildColumnShrinkGuard_SameWidth(t *testing.T) {
	source := CanonicalColumn{NormalizedType: "varchar(100)"}
	target := CanonicalColumn{NormalizedType: "varchar(100)"}
	_, ok := BuildColumnShrinkGuard(source, target)
	if ok {
		t.Fatal("should not detect shrink for same width")
	}
}

func TestBuildColumnShrinkGuard_BinaryShrink(t *testing.T) {
	source := CanonicalColumn{NormalizedType: "varbinary(50)"}
	target := CanonicalColumn{NormalizedType: "varbinary(100)"}
	guard, ok := BuildColumnShrinkGuard(source, target)
	if !ok {
		t.Fatal("expected shrink guard for varbinary(50)→varbinary(100)")
	}
	if guard.Metric != ColumnShrinkMetricOctetLength {
		t.Fatalf("expected OCTET_LENGTH metric, got %s", guard.Metric)
	}
}

func TestBuildColumnShrinkGuard_CrossTypeMismatch(t *testing.T) {
	source := CanonicalColumn{NormalizedType: "varchar(50)"}
	target := CanonicalColumn{NormalizedType: "varbinary(100)"}
	_, ok := BuildColumnShrinkGuard(source, target)
	if ok {
		t.Fatal("should not guard across type groups (character vs binary)")
	}
}

func TestBuildColumnShrinkGuard_NonWidthType(t *testing.T) {
	source := CanonicalColumn{NormalizedType: "int"}
	target := CanonicalColumn{NormalizedType: "bigint"}
	_, ok := BuildColumnShrinkGuard(source, target)
	if ok {
		t.Fatal("should not guard non-width-limited types")
	}
}

func TestStripMySQLMetadataOnlyExtraTokens_Empty(t *testing.T) {
	if got := StripMySQLMetadataOnlyExtraTokens(""); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestStripMySQLMetadataOnlyExtraTokens_NoDefaultGenerated(t *testing.T) {
	input := "on update CURRENT_TIMESTAMP"
	got := StripMySQLMetadataOnlyExtraTokens(input)
	if got != input {
		t.Fatalf("expected %q unchanged, got %q", input, got)
	}
}

func TestStripMySQLMetadataOnlyExtraTokens_StripDefaultGenerated(t *testing.T) {
	input := "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
	got := StripMySQLMetadataOnlyExtraTokens(input)
	expected := "on update CURRENT_TIMESTAMP"
	if got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestStripMySQLMetadataOnlyExtraTokens_StripLowerCase(t *testing.T) {
	input := "default_generated"
	got := StripMySQLMetadataOnlyExtraTokens(input)
	if got != "" {
		t.Fatalf("expected empty after stripping sole default_generated, got %q", got)
	}
}
