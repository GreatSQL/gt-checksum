package inputArg

import (
	"fmt"
	"strings"
)

func canonicalColumnsParamKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// ColumnPair maps one source-side column to one target-side column for the compare and fix operations.
type ColumnPair struct {
	SourceColumn string // column name selected from source table
	TargetColumn string // column name selected from target table
}

// TableColumnPlan holds the resolved column-selection plan for a single source→target table pair.
// It is populated at startup from the `columns` config parameter and carried into the compare pipeline.
type TableColumnPlan struct {
	SourceSchema string
	SourceTable  string
	TargetSchema string
	TargetTable  string

	// Pairs is the ordered list of column mappings.
	// In SimpleMode all Pairs[i].SourceColumn == Pairs[i].TargetColumn.
	Pairs []ColumnPair

	// SimpleMode is true when the columns parameter used syntax 1 (plain comma-separated list,
	// e.g. "c1,c2") meaning the same column names exist on both sides.
	SimpleMode bool
}

// SourceColumns returns the ordered list of source column names from the plan.
func (p *TableColumnPlan) SourceColumns() []string {
	cols := make([]string, len(p.Pairs))
	for i, pair := range p.Pairs {
		cols[i] = pair.SourceColumn
	}
	return cols
}

// TargetColumns returns the ordered list of target column names from the plan.
func (p *TableColumnPlan) TargetColumns() []string {
	cols := make([]string, len(p.Pairs))
	for i, pair := range p.Pairs {
		cols[i] = pair.TargetColumn
	}
	return cols
}

// parseColumnsParam parses the raw `columns` config value and returns a TableColumnPlan.
// tablesRaw is the raw `tables` config value; it is used for validation and for inferring
// schema/table names when the simple-list syntax (syntax 1) is used.
// Returns nil, nil when columnsRaw is empty (columns mode disabled).
func parseColumnsParam(columnsRaw, tablesRaw string) (*TableColumnPlan, error) {
	columnsRaw = strings.TrimSpace(columnsRaw)
	if columnsRaw == "" {
		return nil, nil
	}

	elements := splitAndTrimCSV(columnsRaw)
	if len(elements) == 0 {
		return nil, fmt.Errorf("columns parameter is empty after trimming")
	}

	// Decide syntax by whether any element contains a dot.
	if strings.Contains(elements[0], ".") {
		return parseFullQualifiedColumns(elements, tablesRaw)
	}
	return parseSimpleColumns(elements, tablesRaw)
}

// parseSimpleColumns handles syntax 1: "c1,c2,c3"
// tables must have exactly one concrete compare pair (no wildcards).
func parseSimpleColumns(elements []string, tablesRaw string) (*TableColumnPlan, error) {
	tablePairs := splitAndTrimCSV(tablesRaw)
	if len(tablePairs) != 1 {
		return nil, fmt.Errorf(
			"columns simple-list syntax (e.g. \"c1,c2\") requires tables to specify exactly one compare pair, got %d pair(s); use the fully-qualified syntax (\"schema.table.column:schema.table.column\") for multiple pairs",
			len(tablePairs),
		)
	}
	pair := tablePairs[0]
	if strings.Contains(pair, "*") {
		return nil, fmt.Errorf(
			"columns parameter cannot be used with wildcard table patterns (e.g. \"db.*\"); specify an exact table pair",
		)
	}

	srcSchema, srcTable, dstSchema, dstTable, err := parseTablePair(pair)
	if err != nil {
		return nil, fmt.Errorf("cannot parse tables value %q for columns parameter: %w", pair, err)
	}

	seen := make(map[string]struct{})
	var pairs []ColumnPair
	for _, col := range elements {
		if col == "" {
			return nil, fmt.Errorf("columns parameter contains an empty column name")
		}
		key := canonicalColumnsParamKey(col)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("columns parameter contains duplicate column name %q", col)
		}
		seen[key] = struct{}{}
		pairs = append(pairs, ColumnPair{SourceColumn: col, TargetColumn: col})
	}

	return &TableColumnPlan{
		SourceSchema: srcSchema,
		SourceTable:  srcTable,
		TargetSchema: dstSchema,
		TargetTable:  dstTable,
		Pairs:        pairs,
		SimpleMode:   true,
	}, nil
}

// parseFullQualifiedColumns handles syntax 2/3: "db1.t1.c1:db2.t2.c2, ..."
// All elements must reference the same source.table → target.table pair.
func parseFullQualifiedColumns(elements []string, tablesRaw string) (*TableColumnPlan, error) {
	var srcSchema, srcTable, dstSchema, dstTable string
	seenSrc := make(map[string]struct{})
	seenDst := make(map[string]struct{})
	var pairs []ColumnPair

	for i, elem := range elements {
		if !strings.Contains(elem, ":") {
			return nil, fmt.Errorf(
				"columns element %d %q must have format \"srcSchema.srcTable.srcColumn:dstSchema.dstTable.dstColumn\"",
				i+1, elem,
			)
		}
		halves := strings.SplitN(elem, ":", 2)
		srcPart := strings.TrimSpace(halves[0])
		dstPart := strings.TrimSpace(halves[1])

		srcSch, srcTbl, srcCol, err := parseSchemaTableColumn(srcPart)
		if err != nil {
			return nil, fmt.Errorf("columns element %d source %q: %w", i+1, srcPart, err)
		}
		dstSch, dstTbl, dstCol, err := parseSchemaTableColumn(dstPart)
		if err != nil {
			return nil, fmt.Errorf("columns element %d target %q: %w", i+1, dstPart, err)
		}

		if i == 0 {
			srcSchema, srcTable = srcSch, srcTbl
			dstSchema, dstTable = dstSch, dstTbl
		} else {
			if srcSch != srcSchema || srcTbl != srcTable {
				return nil, fmt.Errorf(
					"columns element %d references source table %s.%s but earlier elements reference %s.%s; all column pairs must belong to the same table pair",
					i+1, srcSch, srcTbl, srcSchema, srcTable,
				)
			}
			if dstSch != dstSchema || dstTbl != dstTable {
				return nil, fmt.Errorf(
					"columns element %d references target table %s.%s but earlier elements reference %s.%s; all column pairs must belong to the same table pair",
					i+1, dstSch, dstTbl, dstSchema, dstTable,
				)
			}
		}

		srcKey := canonicalColumnsParamKey(srcCol)
		dstKey := canonicalColumnsParamKey(dstCol)
		if _, dup := seenSrc[srcKey]; dup {
			return nil, fmt.Errorf("columns parameter: duplicate source column %q", srcCol)
		}
		if _, dup := seenDst[dstKey]; dup {
			return nil, fmt.Errorf("columns parameter: duplicate target column %q", dstCol)
		}
		seenSrc[srcKey] = struct{}{}
		seenDst[dstKey] = struct{}{}
		pairs = append(pairs, ColumnPair{SourceColumn: srcCol, TargetColumn: dstCol})
	}

	if err := validateTablePairInTables(srcSchema, srcTable, dstSchema, dstTable, tablesRaw); err != nil {
		return nil, err
	}

	// Determine SimpleMode: all pairs have the same column name on both sides.
	simple := true
	for _, p := range pairs {
		if p.SourceColumn != p.TargetColumn {
			simple = false
			break
		}
	}

	return &TableColumnPlan{
		SourceSchema: srcSchema,
		SourceTable:  srcTable,
		TargetSchema: dstSchema,
		TargetTable:  dstTable,
		Pairs:        pairs,
		SimpleMode:   simple,
	}, nil
}

// parseSchemaTableColumn splits "schema.table.column" into its three components.
func parseSchemaTableColumn(s string) (schema, table, column string, err error) {
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("expected \"schema.table.column\", got %q", s)
	}
	schema = strings.TrimSpace(parts[0])
	table = strings.TrimSpace(parts[1])
	column = strings.TrimSpace(parts[2])
	if schema == "" || table == "" || column == "" {
		return "", "", "", fmt.Errorf("schema, table, and column must all be non-empty in %q", s)
	}
	return schema, table, column, nil
}

// parseTablePair parses "srcSchema.srcTable[:dstSchema.dstTable]".
// When no ':' is present the source pair is reused as the destination.
func parseTablePair(pair string) (srcSchema, srcTable, dstSchema, dstTable string, err error) {
	pair = strings.TrimSpace(pair)
	if strings.Contains(pair, ":") {
		halves := strings.SplitN(pair, ":", 2)
		srcSchema, srcTable, err = parseSchemaDotTable(strings.TrimSpace(halves[0]))
		if err != nil {
			return "", "", "", "", fmt.Errorf("source part: %w", err)
		}
		dstSchema, dstTable, err = parseSchemaDotTable(strings.TrimSpace(halves[1]))
		if err != nil {
			return "", "", "", "", fmt.Errorf("target part: %w", err)
		}
	} else {
		srcSchema, srcTable, err = parseSchemaDotTable(pair)
		if err != nil {
			return "", "", "", "", err
		}
		dstSchema, dstTable = srcSchema, srcTable
	}
	return
}

// parseSchemaDotTable splits "schema.table" into schema and table.
func parseSchemaDotTable(s string) (schema, table string, err error) {
	// Allow at most one dot — take first dot as delimiter so table names with
	// more complex characters are handled gracefully.
	idx := strings.Index(s, ".")
	if idx < 0 {
		return "", "", fmt.Errorf("expected \"schema.table\" format, got %q", s)
	}
	schema = strings.TrimSpace(s[:idx])
	table = strings.TrimSpace(s[idx+1:])
	if schema == "" || table == "" {
		return "", "", fmt.Errorf("schema and table must both be non-empty in %q", s)
	}
	return schema, table, nil
}

// validateTablePairInTables verifies that the srcSchema.srcTable→dstSchema.dstTable pair
// appears (exactly) in the raw tables parameter string.
func validateTablePairInTables(srcSchema, srcTable, dstSchema, dstTable, tablesRaw string) error {
	for _, pair := range splitAndTrimCSV(tablesRaw) {
		pSrcSchema, pSrcTable, pDstSchema, pDstTable, err := parseTablePair(pair)
		if err != nil {
			continue
		}
		if pSrcSchema == srcSchema && pSrcTable == srcTable &&
			pDstSchema == dstSchema && pDstTable == dstTable {
			return nil
		}
	}
	return fmt.Errorf(
		"columns references table pair %s.%s:%s.%s which is not listed in the tables parameter",
		srcSchema, srcTable, dstSchema, dstTable,
	)
}

// splitAndTrimCSV splits s by commas and trims whitespace from each element,
// discarding empty elements.
func splitAndTrimCSV(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
