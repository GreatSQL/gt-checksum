package schemacompat

import (
	"os"
	"strings"
	"testing"

	"gt-checksum/global"
)

// ---- BuildMappingContext ----

func TestBuildMappingContext(t *testing.T) {
	tests := []struct {
		rawType    string
		nullable   bool
		colName    string
		wantSrc    string
		wantP      int
		wantS      int
		wantUnsign bool
	}{
		{"NUMBER(10,2)", false, "price", "NUMBER", 10, 2, false},
		{"NUMBER(19)", true, "id", "NUMBER", 19, 0, false},
		{"NUMBER", false, "n", "NUMBER", 0, 0, false},
		{"VARCHAR2(100)", true, "name", "VARCHAR2", 100, 0, false},
		{"BIGINT UNSIGNED", false, "cnt", "BIGINT", 0, 0, true},
		{"DECIMAL(38,10)", false, "val", "DECIMAL", 38, 10, false},
		{"FLOAT", true, "f", "FLOAT", 0, 0, false},
	}
	for _, tt := range tests {
		ctx := BuildMappingContext(tt.rawType, tt.nullable, tt.colName, false, "", "")
		if ctx.SourceType != tt.wantSrc {
			t.Errorf("BuildMappingContext(%q).SourceType = %q, want %q", tt.rawType, ctx.SourceType, tt.wantSrc)
		}
		if ctx.Precision != tt.wantP {
			t.Errorf("BuildMappingContext(%q).Precision = %d, want %d", tt.rawType, ctx.Precision, tt.wantP)
		}
		if ctx.Scale != tt.wantS {
			t.Errorf("BuildMappingContext(%q).Scale = %d, want %d", tt.rawType, ctx.Scale, tt.wantS)
		}
		if ctx.Unsigned != tt.wantUnsign {
			t.Errorf("BuildMappingContext(%q).Unsigned = %v, want %v", tt.rawType, ctx.Unsigned, tt.wantUnsign)
		}
		if ctx.Nullable != tt.nullable {
			t.Errorf("BuildMappingContext(%q).Nullable = %v, want %v", tt.rawType, ctx.Nullable, tt.nullable)
		}
	}
}

// ---- parseCondition ----

func TestParseCondition(t *testing.T) {
	tests := []struct {
		expr    string
		ctx     MappingContext
		want    bool
		wantErr bool
	}{
		{"p <= 19 and s = 0", MappingContext{Precision: 10, Scale: 0}, true, false},
		{"p <= 19 and s = 0", MappingContext{Precision: 20, Scale: 0}, false, false},
		{"p <= 19 and s = 0", MappingContext{Precision: 10, Scale: 2}, false, false},
		{"p > 19 or s > 0", MappingContext{Precision: 20, Scale: 0}, true, false},
		{"p > 19 or s > 0", MappingContext{Precision: 10, Scale: 2}, true, false},
		{"p > 19 or s > 0", MappingContext{Precision: 10, Scale: 0}, false, false},
		{"nullable = true", MappingContext{Nullable: true}, true, false},
		{"nullable = true", MappingContext{Nullable: false}, false, false},
		{"nullable = false", MappingContext{Nullable: false}, true, false},
		{"unsigned = true", MappingContext{Unsigned: true}, true, false},
		{"unsigned = false", MappingContext{Unsigned: false}, true, false},
		{"p >= 1 and p <= 9", MappingContext{Precision: 5}, true, false},
		{"p >= 1 and p <= 9", MappingContext{Precision: 10}, false, false},
		{"p != 0", MappingContext{Precision: 5}, true, false},
		{"p != 0", MappingContext{Precision: 0}, false, false},
		{"", MappingContext{}, true, false}, // empty condition always matches
	}
	for _, tt := range tests {
		got, err := parseCondition(tt.expr, tt.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseCondition(%q) error = %v, wantErr %v", tt.expr, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("parseCondition(%q, p=%d s=%d) = %v, want %v",
				tt.expr, tt.ctx.Precision, tt.ctx.Scale, got, tt.want)
		}
	}
}

// ---- MatchUserRule ----

func boolPtr(b bool) *bool { return &b }

func TestMatchUserRule_FirstMatch(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "NUMBER", TargetType: "BIGINT", Condition: "p <= 19 and s = 0"},
		{SourceType: "NUMBER", TargetType: "DECIMAL"},
	}

	ctx := BuildMappingContext("NUMBER(10)", false, "id", false, "", "")
	got, idx, ok := MatchUserRule(rules, ctx)
	if !ok || got != "BIGINT" || idx != 1 {
		t.Errorf("expected BIGINT idx=1, got %q idx=%d ok=%v", got, idx, ok)
	}

	ctx2 := BuildMappingContext("NUMBER(25,2)", false, "price", false, "", "")
	got2, idx2, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 || got2 != "DECIMAL" || idx2 != 2 {
		t.Errorf("expected DECIMAL idx=2, got %q idx=%d ok=%v", got2, idx2, ok2)
	}
}

func TestMatchUserRule_NoMatch(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "FLOAT", TargetType: "DOUBLE"},
	}
	ctx := BuildMappingContext("NUMBER(10)", false, "id", false, "", "")
	_, _, ok := MatchUserRule(rules, ctx)
	if ok {
		t.Error("expected no match for NUMBER against FLOAT rule")
	}
}

func TestMatchUserRule_NullableIsOverrideNotFilter(t *testing.T) {
	nullable := true
	rules := []TypeMappingRule{
		{SourceType: "CHAR", TargetType: "VARCHAR", Nullable: &nullable},
	}
	ctx := BuildMappingContext("char(120)", false, "c", false, "", "")
	got, _, ok := MatchUserRule(rules, ctx)
	if !ok {
		t.Error("nullable is override attribute: rule should match even when source column is NOT NULL")
	}
	if got != "VARCHAR" {
		t.Errorf("expected target_type=VARCHAR, got %q", got)
	}
	ctx2 := BuildMappingContext("char(120)", true, "c", false, "", "")
	_, _, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 {
		t.Error("nullable is override attribute: rule should match when source column is nullable too")
	}
}

func TestMatchUserRuleWithOverrides_NullableDefault(t *testing.T) {
	nullableTrue := true
	rules := []TypeMappingRule{
		{
			SourceType: "CHAR",
			TargetType: "VARCHAR",
			Nullable:   &nullableTrue,
			Default:    "abc",
		},
	}
	ctx := BuildMappingContext("char(120)", false, "c", false, "", "")
	rule, idx, ok := MatchUserRuleWithOverrides(rules, ctx)
	if !ok || rule == nil {
		t.Fatal("expected match, got no match")
	}
	if idx != 1 {
		t.Errorf("expected ruleIndex=1, got %d", idx)
	}
	if rule.Nullable == nil || !*rule.Nullable {
		t.Error("expected rule.Nullable=true")
	}
	if rule.Default != "abc" {
		t.Errorf("expected rule.Default=%q, got %v", "abc", rule.Default)
	}
}

func TestMatchUserRuleWithOverrides_NoMatch(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT"},
	}
	ctx := BuildMappingContext("char(120)", false, "c", false, "", "")
	rule, _, ok := MatchUserRuleWithOverrides(rules, ctx)
	if ok || rule != nil {
		t.Error("expected no match for CHAR against INT rule")
	}
}

func TestMatchUserRule_NullableFilter(t *testing.T) {
	nullable := true
	rules := []TypeMappingRule{
		{SourceType: "NUMBER", TargetType: "BIGINT", Nullable: &nullable},
	}
	ctx := BuildMappingContext("NUMBER(10)", false, "id", false, "", "")
	_, _, ok := MatchUserRule(rules, ctx)
	if !ok {
		t.Error("nullable is now an override attribute: rule should match NOT NULL columns too")
	}
	ctx2 := BuildMappingContext("NUMBER(10)", true, "id", false, "", "")
	_, _, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 {
		t.Error("nullable is now an override attribute: rule should match nullable columns")
	}
}

func TestMatchUserRule_ColumnPattern(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "NUMBER", TargetType: "BIGINT", ColumnPattern: "^id_"},
	}
	ctx := BuildMappingContext("NUMBER(10)", false, "id_user", false, "", "")
	_, _, ok := MatchUserRule(rules, ctx)
	if !ok {
		t.Error("expected match for column name matching ^id_")
	}
	ctx2 := BuildMappingContext("NUMBER(10)", false, "price", false, "", "")
	_, _, ok2 := MatchUserRule(rules, ctx2)
	if ok2 {
		t.Error("expected no match for column name not matching ^id_")
	}
}

// ---- CanonicalizeColumnForComparison with mysql_upgrade rules ----

func TestCanonicalizeColumnForComparison_MySQLUpgrade(t *testing.T) {
	GlobalDTypeMappingRules = &DTypeMappingConfig{
		DTypeMapping: ScenarioRules{
			MySQLUpgrade: []TypeMappingRule{
				{SourceType: "INT", TargetType: "BIGINT"},
				{SourceType: "CHAR", TargetType: "VARCHAR"},
			},
		},
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	mysql56 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 5, Minor: 6}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0}

	attrsInt := []string{"int(11)", "null", "null", "NO", "0", ""}
	col := CanonicalizeColumnForComparison("k", attrsInt, mysql56, mysql80, "", "", "", "")
	if col.NormalizedType != "bigint" {
		t.Errorf("source int(11) with INT→BIGINT rule: NormalizedType = %q, want %q", col.NormalizedType, "bigint")
	}

	attrsChar := []string{"char(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	col2 := CanonicalizeColumnForComparison("c", attrsChar, mysql56, mysql80, "", "", "", "")
	if col2.NormalizedType != "varchar(120)" {
		t.Errorf("source char(120) with CHAR→VARCHAR rule: NormalizedType = %q, want %q", col2.NormalizedType, "varchar(120)")
	}

	colDst := CanonicalizeColumnForComparison("k", attrsInt, mysql80, mysql56, "", "", "", "")
	if colDst.NormalizedType != "int" {
		t.Errorf("target int with INT→BIGINT rule should NOT be mapped: NormalizedType = %q, want %q", colDst.NormalizedType, "int")
	}

	colDst2 := CanonicalizeColumnForComparison("c", attrsChar, mysql80, mysql56, "", "", "", "")
	if colDst2.NormalizedType != "char(120)" {
		t.Errorf("target char(120) with CHAR→VARCHAR rule should NOT be mapped: NormalizedType = %q, want %q", colDst2.NormalizedType, "char(120)")
	}
}

func TestCanonicalizeColumnForComparison_NoRules(t *testing.T) {
	GlobalDTypeMappingRules = nil

	mysqlSrc := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL}
	mysqlDst := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL}

	attrsInt := []string{"int(11)", "null", "null", "NO", "0", ""}
	col := CanonicalizeColumnForComparison("k", attrsInt, mysqlSrc, mysqlDst, "", "", "", "")
	if col.NormalizedType == "bigint" {
		t.Errorf("without rules: NormalizedType should not be bigint, got %q", col.NormalizedType)
	}
}

// ---- ApplyPrecisionToTargetType ----

func TestApplyPrecisionToTargetType(t *testing.T) {
	tests := []struct {
		targetType string
		ctx        MappingContext
		want       string
	}{
		{"DECIMAL", MappingContext{SourceType: "NUMBER", Precision: 10, Scale: 2}, "decimal(10,2)"},
		{"DECIMAL", MappingContext{SourceType: "NUMBER", Precision: 10, Scale: 0}, "decimal(10,0)"},
		{"BIGINT", MappingContext{SourceType: "NUMBER", Precision: 10, Scale: 0}, "bigint"},
		{"VARCHAR", MappingContext{SourceType: "VARCHAR2", Precision: 100}, "varchar(100)"},
		{"DECIMAL(20,5)", MappingContext{SourceType: "NUMBER", Precision: 10, Scale: 2}, "decimal(20,5)"},
		{"DOUBLE", MappingContext{SourceType: "FLOAT"}, "double"},
		// 测试 target_type 已含 precision 但无 scale 时保留源端 scale（修复无限循环问题）
		{"DECIMAL(65)", MappingContext{SourceType: "NUMBER", Precision: 38, Scale: 5}, "decimal(65,5)"},
		{"DECIMAL(65)", MappingContext{SourceType: "NUMBER", Precision: 10, Scale: 2}, "decimal(65,2)"},
		{"DECIMAL(65)", MappingContext{SourceType: "NUMBER", Precision: 10, Scale: 0}, "decimal(65)"},
		{"DECIMAL(38)", MappingContext{SourceType: "NUMBER", Precision: 38, Scale: 0}, "decimal(38)"},
	}
	for _, tt := range tests {
		got := ApplyPrecisionToTargetType(tt.targetType, tt.ctx)
		if got != tt.want {
			t.Errorf("ApplyPrecisionToTargetType(%q, p=%d s=%d) = %q, want %q",
				tt.targetType, tt.ctx.Precision, tt.ctx.Scale, got, tt.want)
		}
	}
}

func TestMatchUserRule_UnsignedIgnoredForNonNumericType(t *testing.T) {
	unsignedTrue := true
	rules := []TypeMappingRule{
		{SourceType: "CHAR", TargetType: "VARCHAR", Unsigned: &unsignedTrue},
	}
	for i := range rules {
		srcBase := strings.ToUpper(strings.Fields(rules[i].SourceType)[0])
		if !unsignedCapableTypes[srcBase] {
			rules[i].Unsigned = nil
		}
	}

	ctx := BuildMappingContext("char(120)", false, "c", false, "", "")
	got, _, ok := MatchUserRule(rules, ctx)
	if !ok {
		t.Error("expected match for CHAR→VARCHAR after clearing unsigned condition")
	}
	if got != "VARCHAR" {
		t.Errorf("expected target_type=VARCHAR, got %q", got)
	}
}

func TestCanonicalizeColumnForComparison_NullableOverride(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: CHAR
      target_type: VARCHAR
      nullable: true
`
	tmpFile := t.TempDir() + "/test-nullable.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	src56 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 5, Minor: 6}
	dst80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0}

	srcAttrs := []string{"char(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	srcCanonical := CanonicalizeColumnForComparison("c", srcAttrs, src56, dst80, "", "", "", "")

	if srcCanonical.NormalizedType != "varchar(120)" {
		t.Errorf("NormalizedType: got %q, want %q", srcCanonical.NormalizedType, "varchar(120)")
	}
	if !srcCanonical.Nullable {
		t.Errorf("Nullable: got false, want true (rule nullable=true should override source NOT NULL)")
	}
}

func TestCanonicalizeColumnForComparison_DefaultOverride(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: CHAR
      target_type: VARCHAR
      nullable: true
      default: 'abc'
`
	tmpFile := t.TempDir() + "/test-default.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	src56 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 5, Minor: 6}
	dst80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0}

	srcAttrs := []string{"char(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	srcCanonical := CanonicalizeColumnForComparison("c", srcAttrs, src56, dst80, "", "", "", "")

	if srcCanonical.DefaultValue != "abc" {
		t.Errorf("DefaultValue: got %q, want %q", srcCanonical.DefaultValue, "abc")
	}
}

func TestMatchUserRule_AutoIncFilter(t *testing.T) {
	autoincTrue := true
	autoincFalse := false
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT", AutoInc: &autoincTrue},
		{SourceType: "INT", TargetType: "INT"},
	}

	ctx := BuildMappingContext("int(11)", false, "id", true, "", "")
	got, idx, ok := MatchUserRule(rules, ctx)
	if !ok || got != "BIGINT" || idx != 1 {
		t.Errorf("autoinc=true col: expected BIGINT idx=1, got %q idx=%d ok=%v", got, idx, ok)
	}

	ctx2 := BuildMappingContext("int(11)", false, "cnt", false, "", "")
	got2, idx2, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 || got2 != "INT" || idx2 != 2 {
		t.Errorf("non-autoinc col: expected INT idx=2, got %q idx=%d ok=%v", got2, idx2, ok2)
	}

	rules2 := []TypeMappingRule{
		{SourceType: "BIGINT", TargetType: "INT", AutoInc: &autoincFalse},
	}
	ctxAI := BuildMappingContext("bigint", false, "id", true, "", "")
	_, _, okAI := MatchUserRule(rules2, ctxAI)
	if okAI {
		t.Error("autoinc=false rule should NOT match AUTO_INCREMENT column")
	}
	ctxNoAI := BuildMappingContext("bigint", false, "cnt", false, "", "")
	_, _, okNoAI := MatchUserRule(rules2, ctxNoAI)
	if !okNoAI {
		t.Error("autoinc=false rule should match non-AUTO_INCREMENT column")
	}
}

// TestMatchUserRule_UnsignedIsOverrideNotFilter 验证 unsigned 是目标端覆盖属性，不过滤源端
func TestMatchUserRule_UnsignedIsOverrideNotFilter(t *testing.T) {
	unsignedTrue := true
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT", Unsigned: &unsignedTrue, Object: "sbtest.t9.k"},
	}
	// 源列 k 是 int(11)，非 unsigned；规则应命中（unsigned 是目标覆盖属性，不过滤源端）
	ctx := BuildMappingContext("int(11)", false, "k", false, "sbtest", "t9")
	got, _, ok := MatchUserRule(rules, ctx)
	if !ok {
		t.Error("unsigned is override attribute: rule should match non-unsigned source column")
	}
	if got != "BIGINT" {
		t.Errorf("expected target_type=BIGINT, got %q", got)
	}
	// 源列是 unsigned 的情况也应命中
	ctx2 := BuildMappingContext("int(10) unsigned", false, "k", false, "sbtest", "t9")
	_, _, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 {
		t.Error("unsigned is override attribute: rule should also match unsigned source column")
	}
}

// TestMatchUserRule_UnsignedConditionViaConditionExpr 验证通过 condition 表达式过滤源端 unsigned
func TestMatchUserRule_UnsignedConditionViaConditionExpr(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT", Condition: "unsigned = 1"},
		{SourceType: "INT", TargetType: "INT"},
	}
	// 源列 unsigned，命中第一条规则
	ctxUnsigned := BuildMappingContext("int(10) unsigned", false, "k", false, "", "")
	got, idx, ok := MatchUserRule(rules, ctxUnsigned)
	if !ok || got != "BIGINT" || idx != 1 {
		t.Errorf("unsigned source: expected BIGINT idx=1, got %q idx=%d ok=%v", got, idx, ok)
	}
	// 源列非 unsigned，命中第二条规则
	ctxSigned := BuildMappingContext("int(11)", false, "k", false, "", "")
	got2, idx2, ok2 := MatchUserRule(rules, ctxSigned)
	if !ok2 || got2 != "INT" || idx2 != 2 {
		t.Errorf("signed source: expected INT idx=2, got %q idx=%d ok=%v", got2, idx2, ok2)
	}
}

// TestIsDTypeMappingCoveredTransition_NormalizedEqualWithRule 验证 NormalizedEqual 场景下
// 规则要求不同目标类型时 IsDTypeMappingCoveredTransition 返回 false
func TestIsDTypeMappingCoveredTransition_NormalizedEqualWithRule(t *testing.T) {
	unsignedTrue := true
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT", Unsigned: &unsignedTrue, Object: "sbtest.t9.k"},
	}
	// source=int(11), dest=int：规则要求 BIGINT UNSIGNED，dest 是 INT → 不覆盖
	covered := IsDTypeMappingCoveredTransition(rules, "int(11)", "int", false, "k", false, "sbtest", "t9")
	if covered {
		t.Error("int(11)->int should NOT be covered when rule requires BIGINT")
	}
	// source=int(11), dest=bigint（无 unsigned）：规则要求 unsigned，dest 缺少该属性 → 不覆盖
	covered2 := IsDTypeMappingCoveredTransition(rules, "int(11)", "bigint", false, "k", false, "sbtest", "t9")
	if covered2 {
		t.Error("int(11)->bigint (no unsigned) should NOT be covered when rule requires BIGINT UNSIGNED")
	}
	// source=int(11), dest=bigint unsigned：类型和 unsigned 均满足 → 覆盖
	covered3 := IsDTypeMappingCoveredTransition(rules, "int(11)", "bigint unsigned", false, "k", false, "sbtest", "t9")
	if !covered3 {
		t.Error("int(11)->bigint unsigned should be covered when rule requires BIGINT UNSIGNED")
	}
}

func TestParseCondition_AutoInc(t *testing.T) {
	tests := []struct {
		expr string
		ctx  MappingContext
		want bool
	}{
		{"autoinc = true", MappingContext{AutoInc: true}, true},
		{"autoinc = true", MappingContext{AutoInc: false}, false},
		{"autoinc = false", MappingContext{AutoInc: false}, true},
		{"autoinc = false", MappingContext{AutoInc: true}, false},
		{"autoinc = true and unsigned = false", MappingContext{AutoInc: true, Unsigned: false}, true},
		{"autoinc = true and unsigned = false", MappingContext{AutoInc: false, Unsigned: false}, false},
	}
	for _, tt := range tests {
		got, err := parseCondition(tt.expr, tt.ctx)
		if err != nil {
			t.Errorf("parseCondition(%q) unexpected error: %v", tt.expr, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseCondition(%q, autoinc=%v) = %v, want %v",
				tt.expr, tt.ctx.AutoInc, got, tt.want)
		}
	}
}

func TestLoadDTypeMappingFile_AutoIncIgnoredForNonInteger(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: CHAR
      target_type: VARCHAR
      autoinc: true
`
	tmpFile := t.TempDir() + "/test-autoinc.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	rules := GlobalDTypeMappingRules.DTypeMapping.MySQLUpgrade
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].AutoInc != nil {
		t.Errorf("expected AutoInc to be nil for CHAR source_type, got %v", *rules[0].AutoInc)
	}
}

func TestLoadDTypeMappingFile_AutoIncValidForInteger(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: INT
      target_type: BIGINT
      autoinc: true
    - source_type: INT
      target_type: INT
`
	tmpFile := t.TempDir() + "/test-autoinc-int.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	rules := GlobalDTypeMappingRules.DTypeMapping.MySQLUpgrade
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}
	if rules[0].AutoInc == nil || !*rules[0].AutoInc {
		t.Error("expected AutoInc=true for INT source_type rule")
	}
	if rules[1].AutoInc != nil {
		t.Errorf("expected AutoInc=nil for second rule, got %v", *rules[1].AutoInc)
	}
}

func TestLoadDTypeMappingFile_UnsignedIgnoredForNonNumeric(t *testing.T) {
	yamlContent := `dTypeMapping:
  mysql_upgrade:
    - source_type: CHAR
      target_type: VARCHAR
      unsigned: true
`
	tmpFile := t.TempDir() + "/test-dtype.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	if err := LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed: %v", err)
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	rules := GlobalDTypeMappingRules.DTypeMapping.MySQLUpgrade
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].Unsigned != nil {
		t.Errorf("expected Unsigned to be nil for CHAR source_type, got %v", *rules[0].Unsigned)
	}
}

func TestIsDTypeMappingCoveredTransition_AttributeCheck(t *testing.T) {
	unsignedTrue := true
	autoincTrue := true
	rules := []TypeMappingRule{
		{
			SourceType: "INT",
			TargetType: "BIGINT",
			Unsigned:   &unsignedTrue,
			AutoInc:    &autoincTrue,
		},
	}

	// 场景1：目标列 bigint（缺少 unsigned 和 auto_increment）→ 应返回 false
	covered := IsDTypeMappingCoveredTransition(rules, "int(10) unsigned auto_increment", "bigint", false, "id", true, "", "")
	if covered {
		t.Error("expected NOT covered when dest=bigint is missing UNSIGNED and AUTO_INCREMENT")
	}

	// 场景2：目标列 bigint unsigned（有 unsigned，缺少 auto_increment）→ 应返回 false
	covered2 := IsDTypeMappingCoveredTransition(rules, "int(10) unsigned auto_increment", "bigint unsigned", false, "id", true, "", "")
	if covered2 {
		t.Error("expected NOT covered when dest=bigint unsigned is missing AUTO_INCREMENT")
	}

	// 场景3：目标列 bigint unsigned auto_increment → 应返回 true
	covered3 := IsDTypeMappingCoveredTransition(rules, "int(10) unsigned auto_increment", "bigint unsigned auto_increment", false, "id", true, "", "")
	if !covered3 {
		t.Error("expected covered when dest=bigint unsigned auto_increment matches all attributes")
	}

	// 场景4：规则无 unsigned/autoinc 条件，目标列 bigint → 应返回 true
	rulesNoAttr := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT"},
	}
	covered4 := IsDTypeMappingCoveredTransition(rulesNoAttr, "int(11)", "bigint", false, "k", false, "", "")
	if !covered4 {
		t.Error("expected covered when rule has no unsigned/autoinc conditions")
	}
}

func TestCanonicalizeColumnForComparison_MariaDBToMySQL(t *testing.T) {
	unsignedTrue := true
	autoincTrue := true
	nullableTrue := true
	GlobalDTypeMappingRules = &DTypeMappingConfig{
		DTypeMapping: ScenarioRules{
			MariaDBToMySQL: []TypeMappingRule{
				{SourceType: "CHAR", TargetType: "VARCHAR", Nullable: &nullableTrue, Default: "abc"},
				{SourceType: "INT", TargetType: "BIGINT", Unsigned: &unsignedTrue, AutoInc: &autoincTrue},
			},
		},
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	mariaDB1011 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 11}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0}

	attrsInt := []string{"int(10) unsigned auto_increment", "null", "null", "NO", "0", ""}
	col := CanonicalizeColumnForComparison("id", attrsInt, mariaDB1011, mysql80, "", "", "", "")
	if col.NormalizedType != "bigint" {
		t.Errorf("MariaDB source int(10) unsigned auto_increment with INT→BIGINT rule: NormalizedType = %q, want %q",
			col.NormalizedType, "bigint")
	}
	if !col.AutoIncrement {
		t.Errorf("MariaDB source int(10) unsigned auto_increment: AutoIncrement = %v, want true", col.AutoIncrement)
	}

	attrsChar := []string{"char(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	col2 := CanonicalizeColumnForComparison("c", attrsChar, mariaDB1011, mysql80, "", "", "", "")
	if col2.NormalizedType != "varchar(120)" {
		t.Errorf("MariaDB source char(120) with CHAR→VARCHAR rule: NormalizedType = %q, want %q",
			col2.NormalizedType, "varchar(120)")
	}
	if !col2.Nullable {
		t.Errorf("MariaDB source char(120) with nullable=true rule: Nullable = %v, want true", col2.Nullable)
	}
	if col2.DefaultValue != "abc" {
		t.Errorf("MariaDB source char(120) with default='abc' rule: DefaultValue = %q, want %q", col2.DefaultValue, "abc")
	}

	attrsIntPlain := []string{"int(11)", "null", "null", "NO", "0", ""}
	col3 := CanonicalizeColumnForComparison("k", attrsIntPlain, mariaDB1011, mysql80, "", "", "", "")
	if col3.NormalizedType != "int" {
		t.Errorf("MariaDB source int(11) without autoinc should not match INT→BIGINT rule: NormalizedType = %q, want %q",
			col3.NormalizedType, "int")
	}

	colDst := CanonicalizeColumnForComparison("c", attrsChar, mysql80, mariaDB1011, "", "", "", "")
	if colDst.NormalizedType != "char(120)" {
		t.Errorf("MySQL target char(120) should NOT be mapped: NormalizedType = %q, want %q",
			colDst.NormalizedType, "char(120)")
	}
}

func TestDecideColumnDefinitionCompatibility_MariaDBToMySQL_DTypeMapping(t *testing.T) {
	nullableTrue := true
	unsignedTrue := true
	autoincTrue := true
	GlobalDTypeMappingRules = &DTypeMappingConfig{
		DTypeMapping: ScenarioRules{
			MariaDBToMySQL: []TypeMappingRule{
				{SourceType: "CHAR", TargetType: "VARCHAR", Nullable: &nullableTrue, Default: "abc"},
				{SourceType: "INT", TargetType: "BIGINT", Unsigned: &unsignedTrue, AutoInc: &autoincTrue},
			},
		},
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	mariaDB1011 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMariaDB, Major: 10, Minor: 11}
	mysql80 := global.MySQLVersionInfo{Flavor: global.DatabaseFlavorMySQL, Major: 8, Minor: 0}

	srcAttrs := []string{"char(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	srcCanonical := CanonicalizeColumnForComparison("c", srcAttrs, mariaDB1011, mysql80, "", "", "", "")

	dstAttrs := []string{"char(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	dstCanonical := CanonicalizeColumnForComparison("c", dstAttrs, mysql80, mariaDB1011, "", "", "", "")

	decision := DecideColumnDefinitionCompatibility(srcCanonical, dstCanonical)
	if !decision.IsMismatch() {
		t.Errorf("expected mismatch for varchar(120) vs char(120), got State=%v Reason=%s", decision.State, decision.Reason)
	}

	srcAttrsIntAI := []string{"int(10) unsigned auto_increment", "null", "null", "NO", "0", ""}
	srcIntAI := CanonicalizeColumnForComparison("id", srcAttrsIntAI, mariaDB1011, mysql80, "", "", "", "")

	dstAttrsIntAI := []string{"int unsigned auto_increment", "null", "null", "NO", "0", ""}
	dstIntAI := CanonicalizeColumnForComparison("id", dstAttrsIntAI, mysql80, mariaDB1011, "", "", "", "")

	decision2 := DecideColumnDefinitionCompatibility(srcIntAI, dstIntAI)
	if !decision2.IsMismatch() {
		t.Errorf("expected mismatch for bigint vs int, got State=%v Reason=%s", decision2.State, decision2.Reason)
	}

	srcAttrsIntPlain := []string{"int(11)", "null", "null", "NO", "0", ""}
	srcIntPlain := CanonicalizeColumnForComparison("k", srcAttrsIntPlain, mariaDB1011, mysql80, "", "", "", "")
	if srcIntPlain.NormalizedType != "int" {
		t.Errorf("MariaDB source int(11) without autoinc should NOT match INT→BIGINT rule: NormalizedType = %q, want %q",
			srcIntPlain.NormalizedType, "int")
	}

	dstAttrsVarchar := []string{"varchar(120)", "utf8mb4", "utf8mb4_general_ci", "NO", "", ""}
	dstVarchar := CanonicalizeColumnForComparison("c", dstAttrsVarchar, mysql80, mariaDB1011, "", "", "", "")
	decision3 := DecideColumnDefinitionCompatibility(srcCanonical, dstVarchar)
	if decision3.IsMismatch() {
		t.Errorf("expected compatible for varchar(120) vs varchar(120), got State=%v Reason=%s", decision3.State, decision3.Reason)
	}
}

// ---- Object 字段测试 ----

func TestParseObjectPattern(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
		wantColumn string
		wantWild   bool
		wantErr    bool
	}{
		// 合法格式
		{"sbtest.*", "sbtest", "*", "", true, false},
		{"sbtest.t9", "sbtest", "t9", "", false, false},
		{"sbtest.t9.c", "sbtest", "t9", "c", false, false},
		{"MYDB.users.name", "MYDB", "users", "name", false, false},
		// 非法格式
		{"*.t9", "", "", "", false, true},         // schema 不能是 *
		{"sbtest.*.c", "", "", "", false, true},    // wildcard 不允许 column
		{"sbtest", "", "", "", false, true},         // 至少需要 schema.table
		{"", "", "", "", false, true},               // 空字符串
		{" .t9", "", "", "", false, true},           // 空 schema
		{"sbtest. ", "", "", "", false, true},       // 空 table
		{"sbtest.t9.", "", "", "", false, true},     // 空 column
	}
	for _, tt := range tests {
		op, err := ParseObjectPattern(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseObjectPattern(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if tt.wantErr {
			continue
		}
		if op.Schema != tt.wantSchema {
			t.Errorf("ParseObjectPattern(%q).Schema = %q, want %q", tt.input, op.Schema, tt.wantSchema)
		}
		if op.Table != tt.wantTable {
			t.Errorf("ParseObjectPattern(%q).Table = %q, want %q", tt.input, op.Table, tt.wantTable)
		}
		if op.Column != tt.wantColumn {
			t.Errorf("ParseObjectPattern(%q).Column = %q, want %q", tt.input, op.Column, tt.wantColumn)
		}
		if op.TableIsWildcard != tt.wantWild {
			t.Errorf("ParseObjectPattern(%q).TableIsWildcard = %v, want %v", tt.input, op.TableIsWildcard, tt.wantWild)
		}
	}
}

func TestObjectPattern_Matches(t *testing.T) {
	tests := []struct {
		pattern  string
		schema   string
		table    string
		column   string
		want     bool
	}{
		// schema 级匹配
		{"sbtest.*", "sbtest", "t9", "c", true},
		{"sbtest.*", "sbtest", "any_table", "any_col", true},
		{"sbtest.*", "other", "t9", "c", false},
		// 表级匹配
		{"sbtest.t9", "sbtest", "t9", "c", true},
		{"sbtest.t9", "sbtest", "t9", "k", true},
		{"sbtest.t9", "sbtest", "t8", "c", false},
		{"sbtest.t9", "other", "t9", "c", false},
		// 列级匹配
		{"sbtest.t9.c", "sbtest", "t9", "c", true},
		{"sbtest.t9.c", "sbtest", "t9", "k", false},
		{"sbtest.t9.c", "sbtest", "t8", "c", false},
		// 大小写不敏感
		{"SBTEST.*", "sbtest", "t9", "c", true},
		{"sbtest.T9", "sbtest", "t9", "c", true},
		{"sbtest.t9.C", "sbtest", "t9", "c", true},
	}
	for _, tt := range tests {
		op, err := ParseObjectPattern(tt.pattern)
		if err != nil {
			t.Fatalf("ParseObjectPattern(%q) unexpected error: %v", tt.pattern, err)
		}
		got := op.Matches(tt.schema, tt.table, tt.column)
		if got != tt.want {
			t.Errorf("ObjectPattern(%q).Matches(%q, %q, %q) = %v, want %v",
				tt.pattern, tt.schema, tt.table, tt.column, got, tt.want)
		}
	}
}

func TestMatchUserRule_ObjectFilter(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "CHAR", TargetType: "VARCHAR", Object: "sbtest.t9"},
		{SourceType: "CHAR", TargetType: "TEXT"},
	}

	// sbtest.t9 的 CHAR 列 → 匹配第一条规则 VARCHAR
	ctx := BuildMappingContext("char(120)", false, "c", false, "sbtest", "t9")
	got, idx, ok := MatchUserRule(rules, ctx)
	if !ok || got != "VARCHAR" || idx != 1 {
		t.Errorf("expected VARCHAR idx=1 for sbtest.t9, got %q idx=%d ok=%v", got, idx, ok)
	}

	// sbtest.t8 的 CHAR 列 → 不匹配第一条，匹配第二条 TEXT
	ctx2 := BuildMappingContext("char(120)", false, "c", false, "sbtest", "t8")
	got2, idx2, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 || got2 != "TEXT" || idx2 != 2 {
		t.Errorf("expected TEXT idx=2 for sbtest.t8, got %q idx=%d ok=%v", got2, idx2, ok2)
	}

	// other.t9 的 CHAR 列 → 不匹配第一条，匹配第二条 TEXT
	ctx3 := BuildMappingContext("char(120)", false, "c", false, "other", "t9")
	got3, idx3, ok3 := MatchUserRule(rules, ctx3)
	if !ok3 || got3 != "TEXT" || idx3 != 2 {
		t.Errorf("expected TEXT idx=2 for other.t9, got %q idx=%d ok=%v", got3, idx3, ok3)
	}
}

func TestMatchUserRule_ObjectBackwardCompat(t *testing.T) {
	// 无 object 字段的规则应匹配所有对象
	rules := []TypeMappingRule{
		{SourceType: "CHAR", TargetType: "VARCHAR"},
	}

	ctx := BuildMappingContext("char(120)", false, "c", false, "any_schema", "any_table")
	got, _, ok := MatchUserRule(rules, ctx)
	if !ok || got != "VARCHAR" {
		t.Errorf("rule without object should match any schema/table, got %q ok=%v", got, ok)
	}
}

func TestMatchUserRule_ObjectSchemaWildcard(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "CHAR", TargetType: "VARCHAR", Object: "sbtest.*"},
	}

	// sbtest 下任意表 → 匹配
	ctx := BuildMappingContext("char(120)", false, "c", false, "sbtest", "t99")
	got, _, ok := MatchUserRule(rules, ctx)
	if !ok || got != "VARCHAR" {
		t.Errorf("expected VARCHAR for sbtest.*, got %q ok=%v", got, ok)
	}

	// 其他 schema → 不匹配
	ctx2 := BuildMappingContext("char(120)", false, "c", false, "other", "t99")
	_, _, ok2 := MatchUserRule(rules, ctx2)
	if ok2 {
		t.Error("expected no match for other schema")
	}
}

func TestMatchUserRule_ObjectColumnSpecific(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT", Object: "sbtest.t9.k"},
		{SourceType: "INT", TargetType: "INT"},
	}

	// sbtest.t9.k → 匹配第一条 BIGINT
	ctx := BuildMappingContext("int(11)", false, "k", false, "sbtest", "t9")
	got, idx, ok := MatchUserRule(rules, ctx)
	if !ok || got != "BIGINT" || idx != 1 {
		t.Errorf("expected BIGINT idx=1 for sbtest.t9.k, got %q idx=%d ok=%v", got, idx, ok)
	}

	// sbtest.t9.c → 不匹配第一条，匹配第二条 INT
	ctx2 := BuildMappingContext("int(11)", false, "c", false, "sbtest", "t9")
	got2, idx2, ok2 := MatchUserRule(rules, ctx2)
	if !ok2 || got2 != "INT" || idx2 != 2 {
		t.Errorf("expected INT idx=2 for sbtest.t9.c, got %q idx=%d ok=%v", got2, idx2, ok2)
	}
}

func TestIsDTypeMappingCoveredTransition_WithObject(t *testing.T) {
	rules := []TypeMappingRule{
		{SourceType: "INT", TargetType: "BIGINT", Object: "sbtest.t9"},
	}

	// sbtest.t9 的 INT→BIGINT → 应该被覆盖
	covered := IsDTypeMappingCoveredTransition(rules, "int(11)", "bigint", false, "k", false, "sbtest", "t9")
	if !covered {
		t.Error("expected covered for sbtest.t9 INT→BIGINT")
	}

	// sbtest.t8 的 INT→BIGINT → 不应被覆盖（object 不匹配）
	covered2 := IsDTypeMappingCoveredTransition(rules, "int(11)", "bigint", false, "k", false, "sbtest", "t8")
	if covered2 {
		t.Error("expected NOT covered for sbtest.t8 (object filter)")
	}
}

func TestLoadDTypeMappingFile_ObjectValidation(t *testing.T) {
	// 合法 object
	yamlContent := `dTypeMapping:
  mariadb_to_mysql:
    - source_type: CHAR
      target_type: VARCHAR
      object: sbtest.t9.c
`
	tmpFile := t.TempDir() + "/test-object-valid.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	if err := LoadDTypeMappingFile(tmpFile); err != nil {
		t.Fatalf("LoadDTypeMappingFile failed on valid object: %v", err)
	}
	defer func() { GlobalDTypeMappingRules = nil }()

	rules := GlobalDTypeMappingRules.DTypeMapping.MariaDBToMySQL
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].Object != "sbtest.t9.c" {
		t.Errorf("expected Object=%q, got %q", "sbtest.t9.c", rules[0].Object)
	}
}

func TestLoadDTypeMappingFile_ObjectInvalid(t *testing.T) {
	// 非法 object: wildcard 后有 column
	yamlContent := `dTypeMapping:
  mariadb_to_mysql:
    - source_type: CHAR
      target_type: VARCHAR
      object: sbtest.*.c
`
	tmpFile := t.TempDir() + "/test-object-invalid.yaml"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	err := LoadDTypeMappingFile(tmpFile)
	if err == nil {
		t.Error("expected error for invalid object pattern sbtest.*.c")
	}
}
