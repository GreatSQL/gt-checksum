package actions

import "testing"

// ---------------------------------------------------------------------------
// EvaluateDataCheckPreflight — regression tests for missing-table scenarios
//
// Background: TableColumnNameCheck appends pod entries AND adds table keys to
// abnormalTableList when a table is missing on the source or both sides.  The
// data-mode main path calls EvaluateDataCheckPreflight(validCount, abnormalCount, …)
// to decide whether to proceed with checksum or skip with a DDL-abnormal report.
//
// A previous fix accidentally cleared abnormalTableList for these cases, causing
// EvaluateDataCheckPreflight to receive (validCount=0, abnormalCount=0) and return
// Fatal:"No valid tables in checklist" instead of SkipChecksum.
// These tests guard that regression.
// ---------------------------------------------------------------------------

// TestPreflight_SourceMissing_SkipChecksum asserts that when the source table
// does not exist (validCount=0, abnormalCount=1) the preflight decision is
// SkipChecksum, not Fatal.
func TestPreflight_SourceMissing_SkipChecksum(t *testing.T) {
	dec := EvaluateDataCheckPreflight(0, 1, false)
	if dec.Fatal {
		t.Fatalf("expected SkipChecksum for source-missing table, got Fatal: %q", dec.Message)
	}
	if !dec.SkipChecksum {
		t.Fatalf("expected SkipChecksum=true for source-missing table, got false (message: %q)", dec.Message)
	}
}

// TestPreflight_BothMissing_SkipChecksum asserts that when both source and
// target tables do not exist the preflight decision is SkipChecksum, not Fatal.
func TestPreflight_BothMissing_SkipChecksum(t *testing.T) {
	dec := EvaluateDataCheckPreflight(0, 2, false)
	if dec.Fatal {
		t.Fatalf("expected SkipChecksum for both-missing tables, got Fatal: %q", dec.Message)
	}
	if !dec.SkipChecksum {
		t.Fatalf("expected SkipChecksum=true for both-missing tables, got false (message: %q)", dec.Message)
	}
}

// TestPreflight_NoTables_Fatal asserts the truly-empty case (no valid tables,
// no abnormal tables) still returns Fatal so the caller exits cleanly.
func TestPreflight_NoTables_Fatal(t *testing.T) {
	dec := EvaluateDataCheckPreflight(0, 0, false)
	if !dec.Fatal {
		t.Fatalf("expected Fatal for empty checklist, got SkipChecksum=%v message=%q", dec.SkipChecksum, dec.Message)
	}
}

// TestPreflight_ValidTables_Proceed asserts that when at least one valid table
// exists the preflight decision is to proceed (neither Fatal nor SkipChecksum).
func TestPreflight_ValidTables_Proceed(t *testing.T) {
	dec := EvaluateDataCheckPreflight(3, 0, false)
	if dec.Fatal || dec.SkipChecksum {
		t.Fatalf("expected proceed for valid tables, got Fatal=%v SkipChecksum=%v message=%q",
			dec.Fatal, dec.SkipChecksum, dec.Message)
	}
}

// TestPreflight_MixedValid_Proceed asserts that when some tables are valid and
// some are abnormal the preflight decision is to proceed with checksum.
func TestPreflight_MixedValid_Proceed(t *testing.T) {
	dec := EvaluateDataCheckPreflight(2, 1, false)
	if dec.Fatal || dec.SkipChecksum {
		t.Fatalf("expected proceed for mixed valid/abnormal, got Fatal=%v SkipChecksum=%v",
			dec.Fatal, dec.SkipChecksum)
	}
}

// TestPreflight_InvisibleMismatch_SkipChecksum asserts that invisible-column
// mismatch with no valid tables triggers SkipChecksum (not Fatal).
func TestPreflight_InvisibleMismatch_SkipChecksum(t *testing.T) {
	dec := EvaluateDataCheckPreflight(0, 1, true)
	if dec.Fatal {
		t.Fatalf("expected SkipChecksum for invisible mismatch, got Fatal: %q", dec.Message)
	}
	if !dec.SkipChecksum {
		t.Fatalf("expected SkipChecksum=true for invisible mismatch, got false")
	}
}
