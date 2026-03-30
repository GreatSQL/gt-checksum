package inputArg

import "testing"

// ---------------------------------------------------------------------------
// tablePatternHasUnsupportedStar
//
// Background: Only '%' is supported as a partial wildcard in table-name
// segments.  The full ".*" suffix is the one valid use of '*' (meaning "all
// tables in a schema").  Any other '*' in the table portion is a user mistake
// that used to pass validation silently and produce wrong results.
// ---------------------------------------------------------------------------

func TestTablePatternHasUnsupportedStar_PartialStar_True(t *testing.T) {
	cases := []string{
		"sbtest.t*",
		"db1.t*",
		"db.prefix_*",
		"db.*suffix",
		"db.a*b",
	}
	for _, c := range cases {
		if !tablePatternHasUnsupportedStar(c) {
			t.Errorf("expected true for %q (partial '*' in table name)", c)
		}
	}
}

func TestTablePatternHasUnsupportedStar_ValidPatterns_False(t *testing.T) {
	// Note: tablePatternHasUnsupportedStar receives individual schema.table
	// parts already split on ':' by the caller.  Do NOT pass full mapping
	// strings like "db.*:db2.*" here; pass each side separately instead.
	cases := []string{
		"sbtest.*",   // full db.* — expand all tables
		"db1.*",      // full db.* — expand all tables
		"db1.t%",     // % wildcard — supported
		"db1.t1",     // exact table name
		"db1.t_name", // exact table name with underscore
		"",           // empty string — no dot, returns false safely
		"notadot",    // no dot — not a schema.table pattern
	}
	for _, c := range cases {
		if tablePatternHasUnsupportedStar(c) {
			t.Errorf("expected false for %q (should be valid or not applicable)", c)
		}
	}
}

// TestTablePatternHasUnsupportedStar_MappingTargetStar covers the exact case
// reported in the audit: the destination side of a mapping pattern.
// Note: tablePatternHasUnsupportedStar operates on individual schema.table
// parts; the caller splits on ':' before invoking it.
func TestTablePatternHasUnsupportedStar_MappingTargetStar(t *testing.T) {
	// Simulate what checkUnsupportedStarInOption does:
	// for "db1.t%:db2.t*" → srcPart="db1.t%", dstPart="db2.t*"
	srcPart := "db1.t%"
	dstPart := "db2.t*"
	if tablePatternHasUnsupportedStar(srcPart) {
		t.Errorf("src part %q should be valid", srcPart)
	}
	if !tablePatternHasUnsupportedStar(dstPart) {
		t.Errorf("dst part %q should be flagged as unsupported '*'", dstPart)
	}
}

func TestTablePatternHasUnsupportedStar_BothSidesStar(t *testing.T) {
	// "db1.t*:db2.t*" — both sides invalid
	srcPart := "db1.t*"
	dstPart := "db2.t*"
	if !tablePatternHasUnsupportedStar(srcPart) {
		t.Errorf("src part %q should be flagged", srcPart)
	}
	if !tablePatternHasUnsupportedStar(dstPart) {
		t.Errorf("dst part %q should be flagged", dstPart)
	}
}

func TestTablePatternHasUnsupportedStar_ValidMappingBothWildcard(t *testing.T) {
	// "db1.*:db2.*" — both sides use full db.* which is legal
	srcPart := "db1.*"
	dstPart := "db2.*"
	if tablePatternHasUnsupportedStar(srcPart) {
		t.Errorf("src part %q should be valid (full db.*)", srcPart)
	}
	if tablePatternHasUnsupportedStar(dstPart) {
		t.Errorf("dst part %q should be valid (full db.*)", dstPart)
	}
}

func TestTablePatternHasUnsupportedStar_EmptyDstPart(t *testing.T) {
	// When no ':' is in the pattern, dstPart is "".
	// tablePatternHasUnsupportedStar("") must return false without panicking.
	if tablePatternHasUnsupportedStar("") {
		t.Error("empty string should return false")
	}
}
