package main

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func generateRow(columns []columnMeta, rowSeq uint64, r *rand.Rand, nullRate float64, timeRangeDays int, pkPlans map[string]pkGenerationPlan) ([]interface{}, error) {
	vals := make([]interface{}, 0, len(columns))
	for _, c := range columns {
		if c.Nullable && !c.IsPK && r.Float64() < nullRate {
			vals = append(vals, nil)
			continue
		}
		v, err := generateValue(c, rowSeq, r, timeRangeDays, pkPlans)
		if err != nil {
			return nil, fmt.Errorf("column=%s type=%s: %w", c.Name, c.DataType, err)
		}
		normalizedValue, changed, changeDetail := normalizeColumnValue(c, v)
		if changed {
			_ = changeDetail
		}
		vals = append(vals, normalizedValue)
	}
	return vals, nil
}

func generatePrimaryKeyValue(c columnMeta, rowSeq uint64, r *rand.Rand, plan pkGenerationPlan) interface{} {
	switch plan.Kind {
	case "number_int":
		v, overflow := safeMulAddInt64(plan.BaseInt, plan.StepInt, int64(rowSeq-1))
		if overflow {
			return int64(rowSeq + uint64(r.Int63n(1000000)))
		}
		return v
	case "number_float":
		return plan.BaseFloat + float64(rowSeq-1)*plan.StepFloat
	case "float":
		return plan.BaseFloat + float64(rowSeq-1)*plan.StepFloat
	case "string":
		return buildUniqueStringPK(plan.Prefix, rowSeq, plan.MaxLen)
	default:
		t := strings.ToUpper(strings.TrimSpace(c.DataType))
		switch {
		case strings.HasPrefix(t, "NUMBER"):
			if c.Scale.Valid && c.Scale.Int64 > 0 {
				base := float64(rowSeq)
				frac := float64(r.Intn(99)) / 100.0
				return base + frac
			}
			return int64(rowSeq)
		case strings.Contains(t, "CHAR"):
			maxLen, byteSemantic := stringLengthLimit(c)
			if maxLen <= 0 {
				maxLen = 32
			}
			value := buildUniqueStringPK("PK", rowSeq, maxLen)
			if byteSemantic {
				return truncateStringByBytes(value, maxLen)
			}
			return truncateString(value, maxLen)
		case strings.HasPrefix(t, "FLOAT"), t == "BINARY_FLOAT", t == "BINARY_DOUBLE":
			return float64(rowSeq) + (r.Float64() * 0.0001)
		default:
			return int64(rowSeq)
		}
	}
}

func buildUniqueStringPK(prefix string, rowSeq uint64, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	seq := strings.ToUpper(strconv.FormatUint(rowSeq, 36))
	value := prefix + seq
	if len(value) <= maxLen {
		return value
	}
	if maxLen <= len(seq) {
		return seq[len(seq)-maxLen:]
	}
	headLen := maxLen - len(seq)
	if headLen < 0 {
		headLen = 0
	}
	if headLen > len(prefix) {
		headLen = len(prefix)
	}
	return prefix[:headLen] + seq
}
func generateValue(c columnMeta, rowSeq uint64, r *rand.Rand, timeRangeDays int, pkPlans map[string]pkGenerationPlan) (interface{}, error) {
	t := strings.ToUpper(strings.TrimSpace(c.DataType))
	if c.IsPK {
		plan := pkPlans[c.Name]
		return generatePrimaryKeyValue(c, rowSeq, r, plan), nil
	}

	switch {
	case strings.HasPrefix(t, "NUMBER"):
		return randomNumberValue(c, rowSeq, r), nil
	case strings.HasPrefix(t, "FLOAT"), t == "BINARY_FLOAT", t == "BINARY_DOUBLE":
		return randomFloatValue(c, rowSeq, r), nil
	case strings.Contains(t, "CHAR"):
		return randomStringValue(c, rowSeq, r), nil
	case strings.HasPrefix(t, "DATE"):
		return randomDateTimeValue(r, timeRangeDays), nil
	case strings.HasPrefix(t, "TIMESTAMP"):
		return randomDateTimeValue(r, timeRangeDays), nil
	case strings.HasSuffix(t, "CLOB"):
		return randomParagraph(randomLength(r, 32, 200), r), nil
	case strings.HasSuffix(t, "BLOB"), strings.HasPrefix(t, "RAW"), strings.HasPrefix(t, "LONG RAW"):
		return randomBytesValue(c, r), nil
	default:
		return nil, fmt.Errorf("unsupported type")
	}
}

func randomNumberValue(c columnMeta, rowSeq uint64, r *rand.Rand) interface{} {
	scale := int64(0)
	if c.Scale.Valid {
		scale = c.Scale.Int64
	}
	precision := int64(0)
	if c.Precision.Valid {
		precision = c.Precision.Int64
	}

	if c.IsPK {
		if scale > 0 {
			base := float64(rowSeq)
			frac := float64(r.Intn(99)) / 100.0
			return base + frac
		}
		return int64(rowSeq)
	}

	if precision <= 0 {
		if scale > 0 {
			return randomScaledFloat(r, scale, 1000000)
		}
		n := r.Int63()
		if r.Intn(10) < 2 {
			n = -n
		}
		return n
	}

	if scale <= 0 {
		maxDigits := minInt64(precision, 18)
		maxV := int64(math.Pow10(int(maxDigits))) - 1
		if maxV <= 0 {
			maxV = 1000000
		}
		n := r.Int63n(maxV + 1)
		if r.Intn(10) < 2 {
			n = -n
		}
		return n
	}

	intDigits := precision - scale
	if intDigits <= 0 {
		intDigits = 1
	}
	maxInt := int64(math.Pow10(int(minInt64(intDigits, 12)))) - 1
	if maxInt <= 0 {
		maxInt = 100000
	}
	intPart := float64(r.Int63n(maxInt + 1))
	fracDigits := int(minInt64(scale, 9))
	fracMax := pow10Int64(fracDigits) - 1
	fracPart := float64(0)
	if fracMax > 0 {
		fracPart = float64(r.Int63n(fracMax+1)) / float64(fracMax+1)
	}
	sign := 1.0
	if r.Intn(10) < 2 {
		sign = -1.0
	}
	value := sign * (intPart + fracPart)
	return roundFloat(value, fracDigits)
}

func randomFloatValue(c columnMeta, rowSeq uint64, r *rand.Rand) interface{} {
	if c.IsPK {
		return float64(rowSeq) + (r.Float64() * 0.0001)
	}
	base := (r.Float64()*2 - 1) * 1000000
	return math.Round(base*1000000) / 1000000
}

func randomStringValue(c columnMeta, rowSeq uint64, r *rand.Rand) interface{} {
	length, byteSemantic := stringLengthLimit(c)
	if length <= 0 {
		length = 32
	}
	trim := func(in string) string {
		if byteSemantic {
			return truncateStringByBytes(in, length)
		}
		return truncateString(in, length)
	}
	if c.IsPK {
		s := fmt.Sprintf("K%020d", rowSeq)
		return trim(s)
	}

	if length < 10 {
		return trim(randomFirstName(r))
	}
	if length < 30 {
		full := randomFirstName(r) + " " + randomLastName(r)
		return trim(full)
	}
	return trim(randomParagraph(randomLength(r, 20, 100), r))
}

func randomDateTimeValue(r *rand.Rand, rangeDays int) time.Time {
	if rangeDays <= 0 {
		rangeDays = defaultTimeRangeDays
	}
	end := time.Now()
	start := end.AddDate(0, 0, -rangeDays)
	delta := end.Unix() - start.Unix()
	randomSec := r.Int63n(delta + 1)
	return time.Unix(start.Unix()+randomSec, 0)
}

func randomBytesValue(c columnMeta, r *rand.Rand) []byte {
	maxLen := int(c.Length)
	if maxLen <= 0 {
		maxLen = 64
	}
	maxLen = minInt(maxLen, 128)
	minLen := 1
	if maxLen >= 8 {
		minLen = 8
	}
	size := randomLength(r, minLen, maxLen)
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

func normalizeColumnValue(c columnMeta, value interface{}) (interface{}, bool, string) {
	if value == nil {
		return nil, false, ""
	}
	dataType := strings.ToUpper(strings.TrimSpace(c.DataType))
	if strings.Contains(dataType, "CHAR") {
		s, ok := value.(string)
		if !ok {
			return value, false, ""
		}
		limit, byteSemantic := stringLengthLimit(c)
		if limit <= 0 {
			return value, false, ""
		}
		normalized := s
		if byteSemantic {
			normalized = truncateStringByBytes(s, limit)
			if len(normalized) != len(s) {
				return normalized, true, fmt.Sprintf("byte-length %d -> %d (limit=%d)", len(s), len(normalized), limit)
			}
			return normalized, false, ""
		}
		normalized = truncateString(s, limit)
		if utf8.RuneCountInString(normalized) != utf8.RuneCountInString(s) {
			return normalized, true, fmt.Sprintf("char-length %d -> %d (limit=%d)", utf8.RuneCountInString(s), utf8.RuneCountInString(normalized), limit)
		}
		return normalized, false, ""
	}

	if strings.HasPrefix(dataType, "RAW") || strings.HasPrefix(dataType, "LONG RAW") {
		raw, ok := value.([]byte)
		if !ok || c.Length <= 0 {
			return value, false, ""
		}
		limit := int(c.Length)
		if len(raw) <= limit {
			return value, false, ""
		}
		normalized := make([]byte, limit)
		copy(normalized, raw[:limit])
		return normalized, true, fmt.Sprintf("raw length %d -> %d (limit=%d)", len(raw), len(normalized), limit)
	}

	return value, false, ""
}
