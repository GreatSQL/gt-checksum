package actions

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// dummyDB returns a non-nil *sql.DB without establishing a connection.
// It is safe for tests that only exercise in-memory cache short-circuits.
func dummyDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "unused:unused@tcp(127.0.0.1:1)/x")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Test_DataModeOracleSlow_TableExistenceCacheHit verifies that
// tableExistsByDrive short-circuits via the preload cache without touching
// the DB connection (we pass nil db to ensure cache path is exercised).
// Regression for /report-bug data-mode-oracle-slow.
func Test_DataModeOracleSlow_TableExistenceCacheHit(t *testing.T) {
	st := &schemaTable{
		tableExistenceCache: map[string]map[string]struct{}{
			"oracle|SBTEST": {
				"T9":  {},
				"T99": {},
			},
			"mysql|GT_CHECKSUM": {
				"TESTBIN":    {},
				"TESTFLOAT":  {},
				"TESTSTRING": {},
			},
		},
	}

	cases := []struct {
		drive, schema, table, kind string
		want                       bool
	}{
		{"oracle", "sbtest", "t9", "table", true},
		{"oracle", "sbtest", "t9", "", true},
		{"oracle", "SBTEST", "MISSING", "table", false},
		{"mysql", "gt_checksum", "testbin", "table", true},
		{"mysql", "gt_checksum", "nosuch", "table", false},
	}
	for _, c := range cases {
		got, err := st.tableExistsByDrive(nil, c.drive, c.schema, c.table, c.kind)
		if err != nil {
			t.Fatalf("%s.%s (%s): unexpected err %v", c.schema, c.table, c.drive, err)
		}
		if got != c.want {
			t.Fatalf("%s.%s (%s): got %v want %v", c.schema, c.table, c.drive, got, c.want)
		}
	}
}

// Test_DataModeOracleSlow_TableExistenceCacheBypassForView ensures the cache
// only applies to BASE TABLE lookups; VIEW kind must fall through to the
// live query path. We detect fallthrough by recovering from the nil-db panic.
func Test_DataModeOracleSlow_TableExistenceCacheBypassForView(t *testing.T) {
	st := &schemaTable{
		tableExistenceCache: map[string]map[string]struct{}{
			"mysql|GT_CHECKSUM": {"V1": {}},
		},
		caseSensitiveObjectName: "yes",
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("VIEW lookup unexpectedly served from table cache (no live query attempted)")
		}
	}()
	_, _ = st.tableExistsByDrive(nil, "mysql", "gt_checksum", "v1", "view")
}

// Test_DataModeOracleSlow_CachedPartitionsShortCircuit verifies that when
// Oracle schema-level preload has been performed, cachedPartitions returns
// empty without issuing any DB calls for tables known to be non-partitioned.
func Test_DataModeOracleSlow_CachedPartitionsShortCircuit(t *testing.T) {
	st := &schemaTable{
		partitionedTableCacheLoaded: map[string]bool{
			"oracle|SBTEST": true,
		},
		partitionedTableCache: map[string]map[string]struct{}{
			"oracle|SBTEST": {
				"T_PART": {}, // only T_PART is partitioned
			},
		},
	}

	db := dummyDB(t)
	// Non-partitioned table: must short-circuit via cache, not query db.
	parts, err := st.cachedPartitions(db, "oracle", "sbtest", "t9", 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("expected empty partitions for non-partitioned table, got %v", parts)
	}

	// Subsequent lookup must be served from partitionsCache (primed above).
	parts2, err := st.cachedPartitions(db, "oracle", "sbtest", "t9", 0)
	if err != nil {
		t.Fatalf("unexpected err on second call: %v", err)
	}
	if len(parts2) != 0 {
		t.Fatalf("expected cached empty partitions, got %v", parts2)
	}
}

// Test_DataModeOracleSlow_CachedPartitionsNoPreloadFallsThrough guards
// against overly aggressive short-circuiting: if preload hasn't marked the
// schema loaded, we must not return empty. With a non-nil stcls but no
// preload state, cachedPartitions must proceed to the live query — which
// the cachedPartitions `db == nil` guard delegates to tc.Query().Partitions,
// producing an error. We accept either err!=nil or panic as evidence that
// short-circuit did not fire.
func Test_DataModeOracleSlow_CachedPartitionsNoPreloadFallsThrough(t *testing.T) {
	st := &schemaTable{} // no preload state
	db := dummyDB(t)
	defer func() { _ = recover() }()
	parts, err := st.cachedPartitions(db, "oracle", "sbtest", "t9", 0)
	// Without preload, we must hit the live query which fails against our
	// dummy DB — an error (or panic) is the required signal.
	if err == nil && len(parts) == 0 {
		t.Fatalf("cachedPartitions returned empty without preload; expected fall-through to live query")
	}
}
