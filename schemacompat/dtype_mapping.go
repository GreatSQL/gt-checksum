package schemacompat

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// TypeMappingRule 单条用户自定义类型映射规则
type TypeMappingRule struct {
	SourceType    string      `yaml:"source_type" json:"source_type"`
	TargetType    string      `yaml:"target_type" json:"target_type"`
	Condition     string      `yaml:"condition,omitempty" json:"condition,omitempty"`
	ColumnPattern string      `yaml:"column_pattern,omitempty" json:"column_pattern,omitempty"`
	Object        string      `yaml:"object,omitempty" json:"object,omitempty"`
	Nullable      *bool       `yaml:"nullable,omitempty" json:"nullable,omitempty"`
	Unsigned      *bool       `yaml:"unsigned,omitempty" json:"unsigned,omitempty"`
	AutoInc       *bool       `yaml:"autoinc,omitempty" json:"autoinc,omitempty"`
	Default       interface{} `yaml:"default,omitempty" json:"default,omitempty"`
	Description   string      `yaml:"description,omitempty" json:"description,omitempty"`
}

// ObjectPattern 解析后的 object 字段结构，用于匹配 schema.table.column 层级
type ObjectPattern struct {
	Schema          string // 精确匹配（大小写不敏感）
	Table           string // 精确匹配或 "*"（通配所有表）
	Column          string // 精确匹配或 ""（通配所有列）
	TableIsWildcard bool   // Table == "*"
}

// ParseObjectPattern 解析 object 字段字符串为 ObjectPattern
// 支持格式：schema.*、schema.table、schema.table.column
func ParseObjectPattern(s string) (ObjectPattern, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return ObjectPattern{}, fmt.Errorf("object pattern is empty")
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) < 2 {
		return ObjectPattern{}, fmt.Errorf("object %q: expected at least \"schema.table\" format", s)
	}
	schema := strings.TrimSpace(parts[0])
	if schema == "" {
		return ObjectPattern{}, fmt.Errorf("object %q: schema part is empty", s)
	}
	if schema == "*" {
		return ObjectPattern{}, fmt.Errorf("object %q: wildcard \"*\" is only allowed in table position (e.g. \"schema.*\")", s)
	}
	table := strings.TrimSpace(parts[1])
	if table == "" {
		return ObjectPattern{}, fmt.Errorf("object %q: table part is empty", s)
	}
	op := ObjectPattern{Schema: schema, Table: table}
	if table == "*" {
		op.TableIsWildcard = true
		if len(parts) == 3 {
			return ObjectPattern{}, fmt.Errorf("object %q: wildcard \"*\" in table position does not allow column specification", s)
		}
	} else if len(parts) == 3 {
		col := strings.TrimSpace(parts[2])
		if col == "" {
			return ObjectPattern{}, fmt.Errorf("object %q: column part is empty", s)
		}
		op.Column = col
	}
	return op, nil
}

// Matches 判断给定的 schema.table.column 是否匹配此 object 模式
// 所有比较大小写不敏感
func (op ObjectPattern) Matches(schema, table, column string) bool {
	if !strings.EqualFold(op.Schema, schema) {
		return false
	}
	if op.TableIsWildcard {
		return true
	}
	if !strings.EqualFold(op.Table, table) {
		return false
	}
	if op.Column == "" {
		return true
	}
	return strings.EqualFold(op.Column, column)
}

// ScenarioRules 按迁移场景分组的规则集
type ScenarioRules struct {
	OracleToMySQL  []TypeMappingRule `yaml:"oracle_to_mysql" json:"oracle_to_mysql"`
	MySQLUpgrade   []TypeMappingRule `yaml:"mysql_upgrade" json:"mysql_upgrade"`
	MariaDBToMySQL []TypeMappingRule `yaml:"mariadb_to_mysql" json:"mariadb_to_mysql"`
}

// DTypeMappingConfig 规则文件顶层结构
type DTypeMappingConfig struct {
	DTypeMapping ScenarioRules `yaml:"dTypeMapping" json:"dTypeMapping"`
}

// MappingContext 规则匹配时的列上下文
type MappingContext struct {
	SourceType    string // 原始类型名（大写，不含精度），如 "NUMBER"
	Precision     int    // p
	Scale         int    // s
	Nullable      bool
	Unsigned      bool
	AutoInc       bool
	DefaultIsNull bool
	ColumnName    string
	Schema        string // 当前列所属 schema
	Table         string // 当前列所属 table
}

// GlobalDTypeMappingRules 全局用户自定义类型映射规则，启动时加载，之后只读
var GlobalDTypeMappingRules *DTypeMappingConfig

// validMySQLTypes MySQL 合法目标类型白名单（大写）
var validMySQLTypes = map[string]bool{
	"TINYINT": true, "SMALLINT": true, "MEDIUMINT": true, "INT": true,
	"INTEGER": true, "BIGINT": true, "FLOAT": true, "DOUBLE": true,
	"DECIMAL": true, "NUMERIC": true, "BIT": true, "BOOL": true,
	"BOOLEAN": true, "DATE": true, "DATETIME": true, "TIMESTAMP": true,
	"TIME": true, "YEAR": true, "CHAR": true, "VARCHAR": true,
	"BINARY": true, "VARBINARY": true, "TINYBLOB": true, "BLOB": true,
	"MEDIUMBLOB": true, "LONGBLOB": true, "TINYTEXT": true, "TEXT": true,
	"MEDIUMTEXT": true, "LONGTEXT": true, "ENUM": true, "SET": true,
	"JSON": true, "GEOMETRY": true, "POINT": true, "LINESTRING": true,
	"POLYGON": true, "MULTIPOINT": true, "MULTILINESTRING": true,
	"MULTIPOLYGON": true, "GEOMETRYCOLLECTION": true,
}

// unsignedCapableTypes 支持 UNSIGNED 属性的 MySQL 数据类型（大写）
var unsignedCapableTypes = map[string]bool{
	"TINYINT": true, "SMALLINT": true, "MEDIUMINT": true, "INT": true,
	"INTEGER": true, "BIGINT": true, "FLOAT": true, "DOUBLE": true,
	"DECIMAL": true, "NUMERIC": true,
}

// autoincCapableTypes 支持 AUTO_INCREMENT 属性的 MySQL 数据类型（大写）
var autoincCapableTypes = map[string]bool{
	"TINYINT": true, "SMALLINT": true, "MEDIUMINT": true, "INT": true,
	"INTEGER": true, "BIGINT": true,
}

// LoadDTypeMappingFile 从 YAML 或 JSON 文件加载用户自定义类型映射规则
// 按文件扩展名自动识别格式；加载后校验每条规则的 target_type 合法性
func LoadDTypeMappingFile(path string) error {
	if path == "" {
		return nil
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("dTypeMapping: cannot resolve path %q: %w", path, err)
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("dTypeMapping: cannot read file %q: %w", absPath, err)
	}

	var cfg DTypeMappingConfig
	ext := strings.ToLower(filepath.Ext(absPath))
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &cfg); err != nil {
			return fmt.Errorf("dTypeMapping: JSON parse error in %q: %w", absPath, err)
		}
	default: // .yaml / .yml 及其他均按 YAML 解析
		if err := parseYAMLConfig(data, &cfg); err != nil {
			return fmt.Errorf("dTypeMapping: YAML parse error in %q: %w", absPath, err)
		}
	}

	// 校验所有规则的 target_type 合法性，并预编译 column_pattern 正则
	allRules := [][]TypeMappingRule{
		cfg.DTypeMapping.OracleToMySQL,
		cfg.DTypeMapping.MySQLUpgrade,
		cfg.DTypeMapping.MariaDBToMySQL,
	}
	for _, rules := range allRules {
		for i := range rules {
			r := &rules[i]
			baseType := strings.ToUpper(strings.Fields(r.TargetType)[0])
			// 去掉括号内精度，如 DECIMAL(10,2) → DECIMAL
			if idx := strings.IndexByte(baseType, '('); idx >= 0 {
				baseType = baseType[:idx]
			}
			if !validMySQLTypes[baseType] {
				fmt.Printf("[WARNING] dTypeMapping: target_type %q is not a recognized MySQL type (source_type=%q)\n",
					r.TargetType, r.SourceType)
			}
			// 预校验 condition 语法
			if r.Condition != "" {
				dummyCtx := MappingContext{}
				if _, err := parseCondition(r.Condition, dummyCtx); err != nil {
					return fmt.Errorf("dTypeMapping: invalid condition %q for source_type=%q: %w",
						r.Condition, r.SourceType, err)
				}
			}
			// 预校验 column_pattern 正则
			if r.ColumnPattern != "" {
				if _, err := regexp.Compile(r.ColumnPattern); err != nil {
					return fmt.Errorf("dTypeMapping: invalid column_pattern %q for source_type=%q: %w",
						r.ColumnPattern, r.SourceType, err)
				}
			}
			// 预校验 object 格式
			if r.Object != "" {
				if _, err := ParseObjectPattern(r.Object); err != nil {
					return fmt.Errorf("dTypeMapping: invalid object %q for source_type=%q: %w",
						r.Object, r.SourceType, err)
				}
			}
			// 校验 unsigned 覆盖属性是否适用于 target_type（目标端覆盖属性，非源端过滤条件）
			if r.Unsigned != nil {
				tgtBase := strings.ToUpper(strings.Fields(r.TargetType)[0])
				if idx := strings.IndexByte(tgtBase, '('); idx >= 0 {
					tgtBase = tgtBase[:idx]
				}
				if !unsignedCapableTypes[tgtBase] {
					fmt.Printf("[WARNING] dTypeMapping: target_type %q does not support UNSIGNED attribute; ignoring unsigned=%v override in rule (source_type=%q -> target_type=%q)\n",
						r.TargetType, *r.Unsigned, r.SourceType, r.TargetType)
					r.Unsigned = nil
				}
			}
			// 校验 autoinc 条件是否适用于 source_type（仅整数类型支持 AUTO_INCREMENT）
			if r.AutoInc != nil {
				srcBase := strings.ToUpper(strings.Fields(r.SourceType)[0])
				if idx := strings.IndexByte(srcBase, '('); idx >= 0 {
					srcBase = srcBase[:idx]
				}
				if !autoincCapableTypes[srcBase] {
					fmt.Printf("[WARNING] dTypeMapping: source_type %q does not support AUTO_INCREMENT attribute; ignoring autoinc=%v condition in rule (source_type=%q -> target_type=%q)\n",
						r.SourceType, *r.AutoInc, r.SourceType, r.TargetType)
					r.AutoInc = nil
				}
			}
		}
	}

	GlobalDTypeMappingRules = &cfg
	return nil
}

// BuildMappingContext 从原始类型字符串和列属性构建 MappingContext
// rawType 示例："NUMBER(10,2)"、"VARCHAR2(100)"、"BIGINT UNSIGNED"
// autoInc：列是否带有 AUTO_INCREMENT 属性（MySQL 整数列专用）
// schema、table：当前列所属的 schema 和表名，用于 object 模式匹配
func BuildMappingContext(rawType string, nullable bool, columnName string, autoInc bool, schema, table string) MappingContext {
	upper := strings.ToUpper(strings.TrimSpace(rawType))
	unsigned := strings.Contains(upper, "UNSIGNED")

	// 提取基础类型名（去掉精度和修饰词）
	baseName := upper
	if idx := strings.IndexByte(baseName, '('); idx >= 0 {
		baseName = baseName[:idx]
	}
	baseName = strings.Fields(baseName)[0]

	// 提取精度 p 和小数位 s
	p, s := 0, 0
	if start := strings.IndexByte(upper, '('); start >= 0 {
		if end := strings.IndexByte(upper, ')'); end > start {
			inner := upper[start+1 : end]
			parts := strings.SplitN(inner, ",", 2)
			p, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
			if len(parts) == 2 {
				s, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		}
	}

	return MappingContext{
		SourceType: baseName,
		Precision:  p,
		Scale:      s,
		Nullable:   nullable,
		Unsigned:   unsigned,
		AutoInc:    autoInc,
		ColumnName: columnName,
		Schema:     schema,
		Table:      table,
	}
}

// matchRuleConditions 检查规则的过滤条件（不含 nullable/default/unsigned，它们是覆盖属性）
// unsigned 指定目标端列应具备的属性，不作为源端过滤条件；如需按源端 unsigned 过滤，使用 condition: unsigned = 1
func matchRuleConditions(r TypeMappingRule, ctx MappingContext) bool {
	// object 过滤条件（优先短路）
	if r.Object != "" {
		op, err := ParseObjectPattern(r.Object)
		if err != nil || !op.Matches(ctx.Schema, ctx.Table, ctx.ColumnName) {
			return false
		}
	}
	if !strings.EqualFold(r.SourceType, ctx.SourceType) {
		return false
	}
	// 条件表达式
	if r.Condition != "" {
		ok, err := parseCondition(r.Condition, ctx)
		if err != nil || !ok {
			return false
		}
	}
	// autoinc 过滤条件
	if r.AutoInc != nil && *r.AutoInc != ctx.AutoInc {
		return false
	}
	// column_pattern 正则匹配
	if r.ColumnPattern != "" {
		matched, err := regexp.MatchString(r.ColumnPattern, ctx.ColumnName)
		if err != nil || !matched {
			return false
		}
	}
	return true
}

// MatchUserRule 在规则列表中查找第一条匹配规则（first-match 语义）
// 返回 (targetType, ruleIndex, matched)，ruleIndex 从 1 开始
// 注意：nullable 和 default 字段是覆盖属性，不作为过滤条件
func MatchUserRule(rules []TypeMappingRule, ctx MappingContext) (string, int, bool) {
	for i, r := range rules {
		if !matchRuleConditions(r, ctx) {
			continue
		}
		return r.TargetType, i + 1, true
	}
	return "", 0, false
}

// IsDTypeMappingCoveredTransition 判断 sourceRawType→destRawType 的类型转换是否被规则列表覆盖。
// 仅检查 source_type 匹配且 target_type 与目标端实际类型一致，不考虑 nullable/default 覆盖属性。
// 用于在列比较阶段决定是否将类型差异视为"已知可接受的迁移转换"而跳过 diff 标记。
// 当规则要求 unsigned 或 autoinc 属性但目标列缺少这些属性时，返回 false 以便走修复路径。
func IsDTypeMappingCoveredTransition(rules []TypeMappingRule, sourceRawType, destRawType string, nullable bool, colName string, autoInc bool, schema, table string) bool {
	if len(rules) == 0 {
		return false
	}
	ctx := BuildMappingContext(sourceRawType, nullable, colName, autoInc, schema, table)
	for _, r := range rules {
		if !matchRuleConditions(r, ctx) {
			continue
		}
		// target_type 与目标端实际基础类型一致即视为覆盖
		mappedBase := strings.ToUpper(strings.Fields(r.TargetType)[0])
		if idx := strings.IndexByte(mappedBase, '('); idx >= 0 {
			mappedBase = mappedBase[:idx]
		}
		destBase := strings.ToUpper(strings.Fields(destRawType)[0])
		if idx := strings.IndexByte(destBase, '('); idx >= 0 {
			destBase = destBase[:idx]
		}
		if mappedBase == destBase {
			// 检查规则要求的属性是否已在目标列上存在
			destUpper := strings.ToUpper(destRawType)
			if r.Unsigned != nil && *r.Unsigned && !strings.Contains(destUpper, "UNSIGNED") {
				return false
			}
			if r.AutoInc != nil && *r.AutoInc && !strings.Contains(destUpper, "AUTO_INCREMENT") {
				return false
			}
			return true
		}
	}
	return false
}

// MatchUserRuleWithOverrides 在规则列表中查找第一条匹配规则，返回完整规则指针
// 调用方可通过返回的规则读取 Nullable/Default 覆盖属性并应用到修复 SQL
func MatchUserRuleWithOverrides(rules []TypeMappingRule, ctx MappingContext) (*TypeMappingRule, int, bool) {
	for i, r := range rules {
		if !matchRuleConditions(r, ctx) {
			continue
		}
		return &rules[i], i + 1, true
	}
	return nil, 0, false
}

// ApplyPrecisionToTargetType 将源类型的精度参数拼接到目标类型
// 例如：targetType="DECIMAL", ctx.Precision=18, ctx.Scale=4 → "decimal(18,4)"
// 若 targetType 已含精度（如 "DECIMAL(10,2)"），则直接返回小写形式
// 特殊处理：当 targetType 已含 precision 但无 scale（如 "DECIMAL(65)"），
// 且源类型有 scale 时，保留源端的 scale 以避免精度丢失
func ApplyPrecisionToTargetType(targetType string, ctx MappingContext) string {
	lower := strings.ToLower(strings.TrimSpace(targetType))
	// 已含精度
	if strings.Contains(lower, "(") {
		// 检查是否已含完整的 (precision,scale) 格式
		// 若只有 (precision) 而源端有 scale，则补充 scale
		if ctx.Scale > 0 {
			// 提取目标类型的 precision 部分
			start := strings.IndexByte(lower, '(')
			end := strings.IndexByte(lower, ')')
			if start >= 0 && end > start {
				inner := lower[start+1 : end]
				// 检查是否已包含逗号（即已有 scale）
				if !strings.Contains(inner, ",") {
					// 只有 precision，补充源端的 scale
					baseType := lower[:start]
					precision := strings.TrimSpace(inner)
					return fmt.Sprintf("%s(%s,%d)", baseType, precision, ctx.Scale)
				}
			}
		}
		return lower
	}
	base := strings.ToUpper(lower)
	switch base {
	case "DECIMAL", "NUMERIC":
		if ctx.Precision > 0 {
			if ctx.Scale > 0 {
				return fmt.Sprintf("%s(%d,%d)", lower, ctx.Precision, ctx.Scale)
			}
			return fmt.Sprintf("%s(%d,0)", lower, ctx.Precision)
		}
	case "VARCHAR", "CHAR", "VARBINARY", "BINARY":
		if ctx.Precision > 0 {
			return fmt.Sprintf("%s(%d)", lower, ctx.Precision)
		}
	}
	return lower
}

// ---- 条件表达式求值器 ----
// 支持变量：p（精度）、s（小数位）、nullable（bool）、unsigned（bool）
// 运算符：=、!=、<、<=、>、>=
// 逻辑：and（优先级高）、or

type condToken struct {
	kind string // "ident", "number", "op", "and", "or", "eof"
	val  string
}

func tokenizeCondition(expr string) []condToken {
	var tokens []condToken
	i := 0
	for i < len(expr) {
		ch := expr[i]
		if unicode.IsSpace(rune(ch)) {
			i++
			continue
		}
		// 数字
		if ch >= '0' && ch <= '9' {
			j := i
			for j < len(expr) && (expr[j] >= '0' && expr[j] <= '9') {
				j++
			}
			tokens = append(tokens, condToken{"number", expr[i:j]})
			i = j
			continue
		}
		// 标识符或关键字
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_' {
			j := i
			for j < len(expr) && (expr[j] >= 'a' && expr[j] <= 'z' || expr[j] >= 'A' && expr[j] <= 'Z' || expr[j] == '_' || expr[j] >= '0' && expr[j] <= '9') {
				j++
			}
			word := expr[i:j]
			lower := strings.ToLower(word)
			switch lower {
			case "and":
				tokens = append(tokens, condToken{"and", "and"})
			case "or":
				tokens = append(tokens, condToken{"or", "or"})
			case "true":
				tokens = append(tokens, condToken{"number", "1"})
			case "false":
				tokens = append(tokens, condToken{"number", "0"})
			default:
				tokens = append(tokens, condToken{"ident", lower})
			}
			i = j
			continue
		}
		// 运算符
		if i+1 < len(expr) {
			two := expr[i : i+2]
			if two == "<=" || two == ">=" || two == "!=" {
				tokens = append(tokens, condToken{"op", two})
				i += 2
				continue
			}
		}
		if ch == '<' || ch == '>' || ch == '=' {
			tokens = append(tokens, condToken{"op", string(ch)})
			i++
			continue
		}
		// 未知字符跳过
		i++
	}
	tokens = append(tokens, condToken{"eof", ""})
	return tokens
}

// parseCondition 解析并求值条件表达式
func parseCondition(expr string, ctx MappingContext) (bool, error) {
	if strings.TrimSpace(expr) == "" {
		return true, nil
	}
	tokens := tokenizeCondition(expr)
	pos := 0

	var parseOr func() (bool, error)
	var parseAnd func() (bool, error)
	var parseCmp func() (bool, error)

	resolveIdent := func(name string) (int, error) {
		switch name {
		case "p":
			return ctx.Precision, nil
		case "s":
			return ctx.Scale, nil
		case "nullable":
			if ctx.Nullable {
				return 1, nil
			}
			return 0, nil
		case "unsigned":
			if ctx.Unsigned {
				return 1, nil
			}
			return 0, nil
		case "autoinc":
			if ctx.AutoInc {
				return 1, nil
			}
			return 0, nil
		}
		return 0, fmt.Errorf("unknown variable %q", name)
	}

	parseCmp = func() (bool, error) {
		if pos >= len(tokens) {
			return false, fmt.Errorf("unexpected end of expression")
		}
		tok := tokens[pos]
		var lval int
		var err error
		if tok.kind == "ident" {
			lval, err = resolveIdent(tok.val)
			if err != nil {
				return false, err
			}
			pos++
		} else if tok.kind == "number" {
			lval, _ = strconv.Atoi(tok.val)
			pos++
		} else {
			return false, fmt.Errorf("expected identifier or number, got %q", tok.val)
		}
		if pos >= len(tokens) || tokens[pos].kind != "op" {
			return false, fmt.Errorf("expected operator after %q", tok.val)
		}
		op := tokens[pos].val
		pos++
		if pos >= len(tokens) {
			return false, fmt.Errorf("expected value after operator %q", op)
		}
		rtok := tokens[pos]
		var rval int
		if rtok.kind == "ident" {
			rval, err = resolveIdent(rtok.val)
			if err != nil {
				return false, err
			}
		} else if rtok.kind == "number" {
			rval, _ = strconv.Atoi(rtok.val)
		} else {
			return false, fmt.Errorf("expected identifier or number after operator, got %q", rtok.val)
		}
		pos++
		switch op {
		case "=":
			return lval == rval, nil
		case "!=":
			return lval != rval, nil
		case "<":
			return lval < rval, nil
		case "<=":
			return lval <= rval, nil
		case ">":
			return lval > rval, nil
		case ">=":
			return lval >= rval, nil
		}
		return false, fmt.Errorf("unknown operator %q", op)
	}

	parseAnd = func() (bool, error) {
		left, err := parseCmp()
		if err != nil {
			return false, err
		}
		for pos < len(tokens) && tokens[pos].kind == "and" {
			pos++
			right, err := parseCmp()
			if err != nil {
				return false, err
			}
			left = left && right
		}
		return left, nil
	}

	parseOr = func() (bool, error) {
		left, err := parseAnd()
		if err != nil {
			return false, err
		}
		for pos < len(tokens) && tokens[pos].kind == "or" {
			pos++
			right, err := parseAnd()
			if err != nil {
				return false, err
			}
			left = left || right
		}
		return left, nil
	}

	result, err := parseOr()
	if err != nil {
		return false, err
	}
	if pos < len(tokens) && tokens[pos].kind != "eof" {
		return false, fmt.Errorf("unexpected token %q at position %d", tokens[pos].val, pos)
	}
	return result, nil
}

// parseYAMLConfig 解析 dTypeMapping 专用 YAML 格式，不依赖外部库。
// 支持格式：顶层 dTypeMapping 键，三个场景子键，每个场景下的规则列表。
func parseYAMLConfig(data []byte, cfg *DTypeMappingConfig) error {
	lines := strings.Split(string(data), "\n")

	// 找到 dTypeMapping: 行
	dtypeMappingLine := -1
	dtypeMappingIndent := 0
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.TrimRight(line, "\r"))
		if trimmed == "dTypeMapping:" {
			dtypeMappingLine = i
			dtypeMappingIndent = yamlLeadingSpaces(line)
			break
		}
	}
	if dtypeMappingLine < 0 {
		return nil
	}

	i := dtypeMappingLine + 1
	for i < len(lines) {
		line := strings.TrimRight(lines[i], "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			i++
			continue
		}
		indent := yamlLeadingSpaces(line)
		if indent <= dtypeMappingIndent {
			break
		}

		var scenarioRules *[]TypeMappingRule
		switch trimmed {
		case "oracle_to_mysql:":
			scenarioRules = &cfg.DTypeMapping.OracleToMySQL
		case "mysql_upgrade:":
			scenarioRules = &cfg.DTypeMapping.MySQLUpgrade
		case "mariadb_to_mysql:":
			scenarioRules = &cfg.DTypeMapping.MariaDBToMySQL
		default:
			i++
			continue
		}
		scenarioIndent := indent
		i++

		// 解析该场景下的规则列表
		for i < len(lines) {
			line = strings.TrimRight(lines[i], "\r")
			trimmed = strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				i++
				continue
			}
			indent = yamlLeadingSpaces(line)
			if indent <= scenarioIndent {
				break
			}
			if !strings.HasPrefix(trimmed, "- ") {
				i++
				continue
			}

			// 新规则：解析 "- key: value" 首行
			rule := TypeMappingRule{}
			ruleIndent := indent
			yamlParseRuleField(&rule, strings.TrimPrefix(trimmed, "- "))
			i++

			// 解析规则的后续字段行
			for i < len(lines) {
				line = strings.TrimRight(lines[i], "\r")
				trimmed = strings.TrimSpace(line)
				if trimmed == "" || strings.HasPrefix(trimmed, "#") {
					i++
					continue
				}
				indent = yamlLeadingSpaces(line)
				if indent <= ruleIndent || strings.HasPrefix(trimmed, "- ") {
					break
				}
				yamlParseRuleField(&rule, trimmed)
				i++
			}
			*scenarioRules = append(*scenarioRules, rule)
		}
	}
	return nil
}

// yamlLeadingSpaces 计算行首空格数（tab 按 2 格计）
func yamlLeadingSpaces(s string) int {
	count := 0
	for _, c := range s {
		switch c {
		case ' ':
			count++
		case '\t':
			count += 2
		default:
			return count
		}
	}
	return count
}

// yamlParseRuleField 解析 "key: value" 并填充到 TypeMappingRule
func yamlParseRuleField(rule *TypeMappingRule, s string) {
	idx := strings.Index(s, ": ")
	var key, value string
	if idx >= 0 {
		key = strings.TrimSpace(s[:idx])
		value = strings.TrimSpace(s[idx+2:])
	} else if strings.HasSuffix(strings.TrimSpace(s), ":") {
		return // 空值键，忽略
	} else {
		return
	}

	// 去除首尾引号
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') ||
			(value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}

	switch key {
	case "source_type":
		rule.SourceType = value
	case "target_type":
		rule.TargetType = value
	case "condition":
		rule.Condition = value
	case "column_pattern":
		rule.ColumnPattern = value
	case "object":
		rule.Object = value
	case "description":
		rule.Description = value
	case "nullable":
		b := strings.ToLower(value) == "true"
		rule.Nullable = &b
	case "unsigned":
		b := strings.ToLower(value) == "true"
		rule.Unsigned = &b
	case "autoinc":
		b := strings.ToLower(value) == "true"
		rule.AutoInc = &b
	case "default":
		lower := strings.ToLower(value)
		if lower == "null" || lower == "~" || value == "" {
			rule.Default = nil
		} else if lower == "true" {
			rule.Default = true
		} else if lower == "false" {
			rule.Default = false
		} else if iv, err := strconv.ParseInt(value, 10, 64); err == nil {
			rule.Default = iv
		} else if fv, err := strconv.ParseFloat(value, 64); err == nil {
			rule.Default = fv
		} else {
			rule.Default = value
		}
	}
}

// formatDTypeMappingDefault 将 TypeMappingRule.Default 格式化为字符串，用于写入 CanonicalColumn.DefaultValue
func formatDTypeMappingDefault(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return "1"
		}
		return "0"
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// PrintDTypeMappingPreview 将已加载的用户规则以表格形式输出到 stdout，便于验证。
// 若未加载任何规则，输出提示信息。
func PrintDTypeMappingPreview() {
	if GlobalDTypeMappingRules == nil {
		fmt.Println("[dTypeMapping] No rules loaded. Set dTypeMappingFile in config to enable custom type mapping.")
		return
	}

	type scenarioEntry struct {
		name  string
		rules []TypeMappingRule
	}
	scenarios := []scenarioEntry{
		{"oracle_to_mysql", GlobalDTypeMappingRules.DTypeMapping.OracleToMySQL},
		{"mysql_upgrade", GlobalDTypeMappingRules.DTypeMapping.MySQLUpgrade},
		{"mariadb_to_mysql", GlobalDTypeMappingRules.DTypeMapping.MariaDBToMySQL},
	}

	for _, sc := range scenarios {
		if len(sc.rules) == 0 {
			continue
		}
		fmt.Printf("\n[dTypeMapping] Scenario: %s (%d rules)\n", sc.name, len(sc.rules))
		fmt.Printf("  %-4s  %-20s  %-20s  %-20s  %-8s  %-30s  %s\n", "No.", "source_type", "target_type", "object", "autoinc", "condition", "description")
		fmt.Printf("  %-4s  %-20s  %-20s  %-20s  %-8s  %-30s  %s\n",
			"----", "--------------------", "--------------------", "--------------------",
			"--------", "------------------------------", "-----------")
		for i, r := range sc.rules {
			cond := r.Condition
			if cond == "" {
				cond = "(any)"
			}
			desc := r.Description
			if desc == "" {
				desc = "-"
			}
			obj := r.Object
			if obj == "" {
				obj = "(any)"
			}
			autoinc := "-"
			if r.AutoInc != nil {
				if *r.AutoInc {
					autoinc = "true"
				} else {
					autoinc = "false"
				}
			}
			fmt.Printf("  %-4d  %-20s  %-20s  %-20s  %-8s  %-30s  %s\n",
				i+1, r.SourceType, r.TargetType, obj, autoinc, cond, desc)
		}
	}
	fmt.Println()
}
