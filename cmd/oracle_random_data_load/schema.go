package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
)

func loadTableColumns(ctx context.Context, db *sql.DB, schema, table string) ([]columnMeta, error) {
	const sqlText = `
SELECT
  column_name,
  data_type,
  data_length,
  char_length,
  char_col_decl_length,
  char_used,
  data_precision,
  data_scale,
  nullable
FROM all_tab_columns
WHERE owner = :1
  AND table_name = :2
ORDER BY column_id`
	rows, err := db.QueryContext(ctx, sqlText, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []columnMeta
	for rows.Next() {
		var c columnMeta
		var nullable string
		var charLength sql.NullInt64
		var charDeclLength sql.NullInt64
		var charUsed sql.NullString
		if err := rows.Scan(&c.Name, &c.DataType, &c.Length, &charLength, &charDeclLength, &charUsed, &c.Precision, &c.Scale, &nullable); err != nil {
			return nil, err
		}
		c.Name = strings.ToUpper(strings.TrimSpace(c.Name))
		c.DataType = strings.ToUpper(strings.TrimSpace(c.DataType))
		if charLength.Valid {
			c.CharLength = charLength.Int64
		}
		if charDeclLength.Valid {
			c.CharDeclLength = charDeclLength.Int64
		}
		if charUsed.Valid {
			c.CharUsed = strings.ToUpper(strings.TrimSpace(charUsed.String))
		}
		c.Nullable = strings.EqualFold(nullable, "Y")
		columns = append(columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func loadPrimaryKeyColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	const sqlText = `
SELECT cols.column_name
FROM all_constraints cons
JOIN all_cons_columns cols
  ON cons.owner = cols.owner
 AND cons.constraint_name = cols.constraint_name
 AND cons.table_name = cols.table_name
WHERE cons.owner = :1
  AND cons.table_name = :2
  AND cons.constraint_type = 'P'
ORDER BY cols.position`
	rows, err := db.QueryContext(ctx, sqlText, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		result = append(result, strings.ToUpper(strings.TrimSpace(col)))
	}
	return result, rows.Err()
}

func buildPrimaryKeyPlans(ctx context.Context, db *sql.DB, schema, table string, columns []columnMeta, totalRows int64, seed int64) (map[string]pkGenerationPlan, error) {
	plans := make(map[string]pkGenerationPlan)
	tableRef := fmt.Sprintf("%s.%s", oracleIdentifier(schema), oracleIdentifier(table))
	r := rand.New(rand.NewSource(seed ^ 0x5A17C3))

	for _, c := range columns {
		if !c.IsPK {
			continue
		}
		dataType := strings.ToUpper(strings.TrimSpace(c.DataType))
		scale := int64(0)
		if c.Scale.Valid {
			scale = c.Scale.Int64
		}

		switch {
		case strings.HasPrefix(dataType, "NUMBER"):
			if scale <= 0 {
				maxVal, err := queryMaxInt64PK(ctx, db, tableRef, c.Name)
				if err != nil {
					return nil, fmt.Errorf("pk column %s max-value query failed: %w", c.Name, err)
				}
				step := int64(1 + r.Intn(17))
				if step%2 == 0 {
					step++
				}
				randomGap := int64(1 + r.Intn(1000))
				base, overflow := safeAddInt64(maxVal, randomGap)
				if overflow {
					return nil, fmt.Errorf("pk column %s base overflow, max existing value=%d", c.Name, maxVal)
				}
				if totalRows > 0 {
					last, ov := safeMulAddInt64(base, step, totalRows-1)
					if ov {
						return nil, fmt.Errorf("pk column %s value range overflows int64 (base=%d step=%d rows=%d)", c.Name, base, step, totalRows)
					}
					_ = last
				}
				plans[c.Name] = pkGenerationPlan{
					Kind:    "number_int",
					BaseInt: base,
					StepInt: step,
				}
				log.Printf("PK plan: column=%s mode=number_int base=%d step=%d", c.Name, base, step)
			} else {
				scaleDigits := int(minInt64(scale, 9))
				stepFloat := 1.0 / math.Pow10(scaleDigits)
				maxVal, err := queryMaxFloat64PK(ctx, db, tableRef, c.Name)
				if err != nil {
					return nil, fmt.Errorf("pk column %s max-value query failed: %w", c.Name, err)
				}
				base := maxVal + stepFloat*float64(1+r.Intn(100))
				plans[c.Name] = pkGenerationPlan{
					Kind:      "number_float",
					BaseFloat: base,
					StepFloat: stepFloat,
				}
				log.Printf("PK plan: column=%s mode=number_float base=%f step=%f", c.Name, base, stepFloat)
			}
		case strings.Contains(dataType, "CHAR"):
			maxLen, _ := stringLengthLimit(c)
			if maxLen <= 0 {
				maxLen = 32
			}
			prefix := fmt.Sprintf("PK%s", strings.ToUpper(strconv.FormatInt(seed^int64(len(c.Name)), 36)))
			plans[c.Name] = pkGenerationPlan{
				Kind:   "string",
				Prefix: prefix,
				MaxLen: maxLen,
			}
			log.Printf("PK plan: column=%s mode=string prefix=%s max_len=%d", c.Name, prefix, maxLen)
		case strings.HasPrefix(dataType, "FLOAT"), dataType == "BINARY_FLOAT", dataType == "BINARY_DOUBLE":
			maxVal, err := queryMaxFloat64PK(ctx, db, tableRef, c.Name)
			if err != nil {
				return nil, fmt.Errorf("pk column %s max-value query failed: %w", c.Name, err)
			}
			step := 0.0001
			base := maxVal + step*float64(1+r.Intn(1000))
			plans[c.Name] = pkGenerationPlan{
				Kind:      "float",
				BaseFloat: base,
				StepFloat: step,
			}
			log.Printf("PK plan: column=%s mode=float base=%f step=%f", c.Name, base, step)
		default:
			log.Printf("WARN pk column=%s has unsupported type for precomputed uniqueness plan (%s), using fallback generator", c.Name, dataType)
		}
	}
	return plans, nil
}

func queryMaxInt64PK(ctx context.Context, db *sql.DB, tableRef, columnName string) (int64, error) {
	query := fmt.Sprintf("SELECT TO_CHAR(MAX(%s)) FROM %s", oracleIdentifier(columnName), tableRef)
	var maxStr sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&maxStr); err != nil {
		return 0, err
	}
	if !maxStr.Valid || strings.TrimSpace(maxStr.String) == "" {
		return 0, nil
	}
	raw := strings.TrimSpace(maxStr.String)
	raw = strings.ReplaceAll(raw, ",", "")
	if dot := strings.IndexByte(raw, '.'); dot >= 0 {
		raw = raw[:dot]
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int64 from %q: %w", maxStr.String, err)
	}
	return v, nil
}

func queryMaxFloat64PK(ctx context.Context, db *sql.DB, tableRef, columnName string) (float64, error) {
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", oracleIdentifier(columnName), tableRef)
	var maxVal sql.NullFloat64
	if err := db.QueryRowContext(ctx, query).Scan(&maxVal); err != nil {
		return 0, err
	}
	if !maxVal.Valid {
		return 0, nil
	}
	return maxVal.Float64, nil
}

func filterInsertColumns(cols []columnMeta, pkSet, excludeSet map[string]struct{}) ([]columnMeta, []string, error) {
	insertCols := make([]columnMeta, 0, len(cols))
	skipped := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, ok := pkSet[c.Name]; ok {
			c.IsPK = true
		}
		if _, excluded := excludeSet[c.Name]; excluded {
			skipped = append(skipped, fmt.Sprintf("Column %s excluded by user option", c.Name))
			continue
		}
		if isSupportedDataType(c.DataType) {
			insertCols = append(insertCols, c)
			continue
		}
		if c.Nullable {
			skipped = append(skipped, fmt.Sprintf("Column %s (%s) unsupported and nullable, skipped", c.Name, c.DataType))
			continue
		}
		return nil, skipped, fmt.Errorf("column %s has unsupported type %s and is NOT NULL; use -exclude-columns if DB default can fill it", c.Name, c.DataType)
	}
	return insertCols, skipped, nil
}

func isSupportedDataType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	switch {
	case strings.HasPrefix(t, "NUMBER"),
		strings.HasPrefix(t, "FLOAT"),
		t == "BINARY_FLOAT",
		t == "BINARY_DOUBLE",
		strings.Contains(t, "CHAR"),
		strings.HasPrefix(t, "DATE"),
		strings.HasPrefix(t, "TIMESTAMP"),
		strings.HasSuffix(t, "CLOB"),
		strings.HasSuffix(t, "BLOB"),
		strings.HasPrefix(t, "RAW"),
		strings.HasPrefix(t, "LONG RAW"):
		return true
	default:
		return false
	}
}
