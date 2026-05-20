package actions

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"gt-checksum/schemacompat"
)

// TestTableExistenceCache_SeparateSourceAndTarget 验证当源端和目标端使用相同
// drive 和 schema 时，tableExistenceCache 不会混淆两端的表。
// 回归测试：dTypeMapping-tbl-not-exists bug（缓存键冲突导致源端"看到"目标端表）。
func TestTableExistenceCache_SeparateSourceAndTarget(t *testing.T) {
	srcDB, err := sql.Open("mysql", "unused:unused@tcp(127.0.0.1:1)/src")
	if err != nil {
		t.Fatalf("sql.Open src: %v", err)
	}
	defer srcDB.Close()

	dstDB, err := sql.Open("mysql", "unused:unused@tcp(127.0.0.1:2)/dst")
	if err != nil {
		t.Fatalf("sql.Open dst: %v", err)
	}
	defer dstDB.Close()

	// 模拟 preloadTableExistence 的结果：源端有 t4，目标端有 t9
	st := &schemaTable{
		tableExistenceCache: make(map[string]map[string]struct{}),
	}

	srcKey := tableExistenceCacheKey(srcDB, "mysql", "SBTEST")
	dstKey := tableExistenceCacheKey(dstDB, "mysql", "SBTEST")

	// 两个 key 必须不同
	if srcKey == dstKey {
		t.Fatalf("cache keys should differ: src=%q, dst=%q", srcKey, dstKey)
	}

	// 模拟源端只有 t4
	st.tableExistenceCache[srcKey] = map[string]struct{}{"T4": {}}
	// 模拟目标端有 t9
	st.tableExistenceCache[dstKey] = map[string]struct{}{"T9": {}}

	// 源端查找 t9 应该返回 false
	srcHasT9, err := st.tableExistsByDrive(srcDB, "mysql", "sbtest", "t9", "table")
	if err != nil {
		t.Fatalf("src t9: %v", err)
	}
	if srcHasT9 {
		t.Errorf("source should NOT have t9, got true")
	}

	// 目标端查找 t9 应该返回 true
	dstHasT9, err := st.tableExistsByDrive(dstDB, "mysql", "sbtest", "t9", "table")
	if err != nil {
		t.Fatalf("dst t9: %v", err)
	}
	if !dstHasT9 {
		t.Errorf("target should have t9, got false")
	}

	// 源端查找 t4 应该返回 true
	srcHasT4, err := st.tableExistsByDrive(srcDB, "mysql", "sbtest", "t4", "table")
	if err != nil {
		t.Fatalf("src t4: %v", err)
	}
	if !srcHasT4 {
		t.Errorf("source should have t4, got false")
	}

	// 目标端查找 t4 应该返回 false
	dstHasT4, err := st.tableExistsByDrive(dstDB, "mysql", "sbtest", "t4", "table")
	if err != nil {
		t.Fatalf("dst t4: %v", err)
	}
	if dstHasT4 {
		t.Errorf("target should NOT have t4, got true")
	}
}

// TestTableExistenceCacheKey_NilDB 验证 nil db 的缓存键一致性。
func TestTableExistenceCacheKey_NilDB(t *testing.T) {
	// nil db 的缓存键应该始终一致
	key1 := tableExistenceCacheKey(nil, "mysql", "SBTEST")
	key2 := tableExistenceCacheKey(nil, "mysql", "sbtest")
	if key1 != key2 {
		t.Errorf("nil db keys should be case-insensitive on schema: %q vs %q", key1, key2)
	}
	expected := fmt.Sprintf("%p|%s|%s", (*sql.DB)(nil), "mysql", "SBTEST")
	if key1 != expected {
		t.Errorf("nil db key: got %q, want %q", key1, expected)
	}
}

func TestApplyDTypeMappingOverrides_AutoInc(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: INT
      target_type: BIGINT
      nullable: false
      autoinc: true
`
	tmpFile := t.TempDir() + "/test-autoinc.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := schemacompat.LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { schemacompat.GlobalDTypeMappingRules = nil }()

	// 模拟修复场景：源端 int(10) unsigned auto_increment，目标端 decimal(25,0)
	// repairAttrs[0] 已被 repairPlan 改写为目标类型 "bigint"（不含 AUTO_INCREMENT）
	repairAttrs := []string{"bigint", "null", "null", "YES", "null", ""}
	// sourceType 为原始源端类型，包含 auto_increment
	applyDTypeMappingOverrides(repairAttrs, "id", false, false, "int(10) unsigned auto_increment", "sbtest", "t9")

	// 验证 autoinc 覆盖生效
	if repairAttrs[3] != "NO" {
		t.Errorf("nullable override: got %q, want %q", repairAttrs[3], "NO")
	}
	// 核心验证：repairAttrs[0] 应包含 AUTO_INCREMENT
	found := false
	for _, part := range []string{"AUTO_INCREMENT", "auto_increment"} {
		if contains(repairAttrs[0], part) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("autoinc override: repairAttrs[0] = %q, want it to contain AUTO_INCREMENT", repairAttrs[0])
	}
}

func TestApplyDTypeMappingOverrides_AutoIncFalse(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: BIGINT
      target_type: INT
      autoinc: false
`
	tmpFile := t.TempDir() + "/test-autoinc-false.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := schemacompat.LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { schemacompat.GlobalDTypeMappingRules = nil }()

	// 源端 bigint 不带 AUTO_INCREMENT，规则 autoinc=false 匹配
	// repairAttrs[0] 被错误地包含了 AUTO_INCREMENT（模拟异常场景），规则应移除它
	repairAttrs := []string{"int AUTO_INCREMENT", "null", "null", "NO", "null", ""}
	applyDTypeMappingOverrides(repairAttrs, "cnt", false, false, "bigint", "sbtest", "t9")

	if contains(repairAttrs[0], "AUTO_INCREMENT") || contains(repairAttrs[0], "auto_increment") {
		t.Errorf("autoinc=false should strip AUTO_INCREMENT, got repairAttrs[0] = %q", repairAttrs[0])
	}
}

func TestApplyDTypeMappingOverrides_AutoIncNil(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: INT
      target_type: BIGINT
`
	tmpFile := t.TempDir() + "/test-autoinc-nil.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := schemacompat.LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { schemacompat.GlobalDTypeMappingRules = nil }()

	// 规则未指定 autoinc，repairAttrs[0] 不应被修改
	repairAttrs := []string{"bigint", "null", "null", "NO", "null", ""}
	applyDTypeMappingOverrides(repairAttrs, "k", false, false, "int(11)", "sbtest", "t9")

	if repairAttrs[0] != "bigint" {
		t.Errorf("autoinc=nil should not modify type, got repairAttrs[0] = %q", repairAttrs[0])
	}
}

// TestApplyDTypeMappingOverrides_Unsigned 验证 unsigned 覆盖属性正确写入 repairAttrs[0]。
// 回归测试：修复 dTypeMapping 规则带 unsigned=true 时修复 SQL 不包含 UNSIGNED 的 bug。
func TestApplyDTypeMappingOverrides_Unsigned(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: INT
      target_type: BIGINT
      nullable: false
      unsigned: true
      autoinc: true
`
	tmpFile := t.TempDir() + "/test-unsigned.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := schemacompat.LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { schemacompat.GlobalDTypeMappingRules = nil }()

	// 模拟修复场景：源端 int(10) unsigned auto_increment，目标端 bigint
	// repairAttrs[0] 已被 repairPlan 改写为 "bigint"（不含 UNSIGNED 和 AUTO_INCREMENT）
	repairAttrs := []string{"bigint", "null", "null", "YES", "null", ""}
	applyDTypeMappingOverrides(repairAttrs, "id", false, false, "int(10) unsigned auto_increment", "sbtest", "t9")

	// 验证 nullable 覆盖
	if repairAttrs[3] != "NO" {
		t.Errorf("nullable override: got %q, want %q", repairAttrs[3], "NO")
	}
	// 验证 unsigned 覆盖
	if !contains(repairAttrs[0], "unsigned") {
		t.Errorf("unsigned override: repairAttrs[0] = %q, want it to contain unsigned", repairAttrs[0])
	}
	// 验证 autoinc 覆盖
	if !contains(repairAttrs[0], "AUTO_INCREMENT") && !contains(repairAttrs[0], "auto_increment") {
		t.Errorf("autoinc override: repairAttrs[0] = %q, want it to contain AUTO_INCREMENT", repairAttrs[0])
	}
}

