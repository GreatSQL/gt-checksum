package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

var simpleOracleIdentifierPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)

func safeAddInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, true
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, true
	}
	return a + b, false
}

func safeMulAddInt64(base, step, n int64) (int64, bool) {
	if n < 0 {
		return base, false
	}
	if step != 0 && n > math.MaxInt64/step {
		return 0, true
	}
	prod := step * n
	return safeAddInt64(base, prod)
}

func oracleIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return trimmed
	}
	if strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"") {
		return trimmed
	}
	if simpleOracleIdentifierPattern.MatchString(trimmed) {
		return strings.ToUpper(trimmed)
	}
	return `"` + strings.ReplaceAll(trimmed, `"`, `""`) + `"`
}

func buildInsertAllSQL(tableRef string, colNames []string, rowCount int) string {
	var sb strings.Builder
	sb.Grow(rowCount * (len(colNames)*8 + 64))
	sb.WriteString("INSERT ALL")
	argPos := 1
	for i := 0; i < rowCount; i++ {
		sb.WriteString("\n  INTO ")
		sb.WriteString(tableRef)
		sb.WriteString(" (")
		sb.WriteString(strings.Join(colNames, ","))
		sb.WriteString(") VALUES (")
		for j := range colNames {
			if j > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(":")
			sb.WriteString(strconv.Itoa(argPos))
			argPos++
		}
		sb.WriteString(")")
	}
	sb.WriteString("\nSELECT 1 FROM DUAL")
	return sb.String()
}

func formatRowForLog(columns []columnMeta, row []interface{}) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i := range row {
		if i > 0 {
			sb.WriteString(", ")
		}
		colName := fmt.Sprintf("col%d", i+1)
		if i < len(columns) {
			colName = columns[i].Name
		}
		sb.WriteString(colName)
		sb.WriteByte('=')
		sb.WriteString(formatValueForLog(row[i]))
	}
	sb.WriteByte('}')
	return sb.String()
}

func formatValueForLog(value interface{}) string {
	if value == nil {
		return "<nil>"
	}
	switch v := value.(type) {
	case string:
		s := v
		if utf8.RuneCountInString(s) > 40 {
			s = truncateString(s, 40) + "..."
		}
		return strconv.Quote(s)
	case []byte:
		if len(v) == 0 {
			return "0x"
		}
		maxLen := minInt(len(v), 16)
		return fmt.Sprintf("0x%X(len=%d)", v[:maxLen], len(v))
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func monitorProgress(cfg config, st *stats, total int64, start time.Time, done <-chan struct{}) {
	ticker := time.NewTicker(cfg.ProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			generated := atomic.LoadUint64(&st.GeneratedRows)
			inserted := atomic.LoadUint64(&st.InsertedRows)
			failed := atomic.LoadUint64(&st.FailedRows)
			okBatches := atomic.LoadUint64(&st.OKBatches)
			failBatches := atomic.LoadUint64(&st.FailBatches)
			retries := atomic.LoadUint64(&st.Retries)
			batchRetries := atomic.LoadUint64(&st.BatchRetries)
			rowRetries := atomic.LoadUint64(&st.RowRetries)
			elapsed := time.Since(start).Seconds()
			if elapsed <= 0 {
				elapsed = 1
			}
			processed := inserted + failed
			progress := 100.0 * float64(processed) / float64(total)
			if progress > 100 {
				progress = 100
			}
			rate := float64(inserted) / elapsed
			log.Printf("progress=%.2f%% generated=%d inserted=%d failed=%d ok_batches=%d fail_batches=%d retries=%d(batch=%d,row=%d) rate=%.1f rows/s",
				progress, generated, inserted, failed, okBatches, failBatches, retries, batchRetries, rowRetries, rate)
		}
	}
}

func printSummary(cfg config, st *stats, start time.Time, runErr error) {
	generated := atomic.LoadUint64(&st.GeneratedRows)
	inserted := atomic.LoadUint64(&st.InsertedRows)
	failed := atomic.LoadUint64(&st.FailedRows)
	okBatches := atomic.LoadUint64(&st.OKBatches)
	failBatches := atomic.LoadUint64(&st.FailBatches)
	retries := atomic.LoadUint64(&st.Retries)
	batchRetries := atomic.LoadUint64(&st.BatchRetries)
	rowRetries := atomic.LoadUint64(&st.RowRetries)
	elapsed := time.Since(start)
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1
	}

	log.Printf("========== oracle-random-data-load summary ==========")
	log.Printf("target: %s.%s", cfg.Schema, cfg.Table)
	log.Printf("rows target=%d generated=%d inserted=%d failed=%d", cfg.Rows, generated, inserted, failed)
	log.Printf("batches ok=%d failed=%d retries=%d(batch=%d,row=%d)", okBatches, failBatches, retries, batchRetries, rowRetries)
	log.Printf("elapsed=%s throughput=%.1f rows/s", elapsed.Truncate(time.Millisecond).String(), float64(inserted)/sec)
	if runErr != nil {
		log.Printf("result=FAILED error=%v", runErr)
	} else if failed > 0 {
		log.Printf("result=PARTIAL_SUCCESS continue_on_error=true")
	} else {
		log.Printf("result=SUCCESS")
	}
}

func nonBlockingSendErr(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}

func applyDBPoolSettings(db *sql.DB, cfg config) {
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = maxInt(cfg.Workers*2, 8)
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = maxInt(cfg.Workers, 4)
	}
	if maxIdle > maxOpen {
		log.Printf("WARN db-max-idle-conns (%d) > db-max-open-conns (%d), auto-adjusting idle to %d", maxIdle, maxOpen, maxOpen)
		maxIdle = maxOpen
	}
	connLifetime := cfg.ConnMaxLifetime
	if connLifetime <= 0 {
		connLifetime = time.Duration(defaultConnLifetimeMin) * time.Minute
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connLifetime)
}

func capBatchSizeByBindLimit(batchSize, columnCount int) (int, error) {
	if columnCount <= 0 {
		return batchSize, nil
	}
	if columnCount > oracleMaxBindVariables {
		return 0, fmt.Errorf("column count %d exceeds Oracle bind-variable limit %d", columnCount, oracleMaxBindVariables)
	}
	maxBatchByBind := oracleMaxBindVariables / columnCount
	if maxBatchByBind < 1 {
		maxBatchByBind = 1
	}
	if batchSize > maxBatchByBind {
		return maxBatchByBind, nil
	}
	return batchSize, nil
}

func randomLength(r *rand.Rand, minV, maxV int) int {
	if maxV < minV {
		maxV = minV
	}
	return minV + r.Intn(maxV-minV+1)
}

func truncateString(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen])
}

func truncateStringByBytes(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	idx := 0
	for idx < len(s) {
		_, size := utf8.DecodeRuneInString(s[idx:])
		if size <= 0 {
			break
		}
		if idx+size > maxBytes {
			break
		}
		idx += size
	}
	return s[:idx]
}

func randomParagraph(length int, r *rand.Rand) string {
	words := []string{
		"alpha", "beta", "gamma", "delta", "omega", "oracle", "mysql",
		"random", "loader", "batch", "worker", "schema", "table", "column",
		"checksum", "sample", "vector", "signal", "storage", "engine",
		"thread", "commit", "rollback", "window", "stream", "profile",
		"index", "latency", "throughput", "segment", "planner", "cursor",
		"snapshot", "cluster", "consistency", "durability", "replica", "query",
		"partition", "transaction", "recover", "archive", "metrics", "memory",
		"compression", "channel", "pipeline", "adapter", "runtime", "scheduler",
	}
	var b strings.Builder
	for b.Len() < length {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[r.Intn(len(words))])
	}
	return truncateString(b.String(), length)
}

func randomFirstName(r *rand.Rand) string {
	first := []string{
		"Olivia", "Liam", "Emma", "Noah", "Ava", "Lucas", "Mia", "Ethan", "Ivy", "Mason",
		"Sophia", "Logan", "Amelia", "James", "Harper", "Benjamin", "Ella", "Henry", "Luna", "Jack",
		"Grace", "Owen", "Chloe", "Levi", "Nora", "Elijah", "Zoe", "Daniel", "Aria", "Isaac",
	}
	return first[r.Intn(len(first))]
}

func randomLastName(r *rand.Rand) string {
	last := []string{
		"Smith", "Johnson", "Brown", "Davis", "Wilson", "Taylor", "Anderson", "Thomas", "Clark", "Moore",
		"Martin", "Lee", "Perez", "White", "Harris", "Sanchez", "Allen", "Young", "King", "Wright",
		"Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Campbell", "Mitchell", "Roberts", "Carter",
	}
	return last[r.Intn(len(last))]
}

func stringLengthLimit(c columnMeta) (maxLen int, byteSemantic bool) {
	dataType := strings.ToUpper(strings.TrimSpace(c.DataType))
	if strings.HasPrefix(dataType, "NCHAR") || strings.HasPrefix(dataType, "NVARCHAR2") {
		if c.CharDeclLength > 0 {
			return int(c.CharDeclLength), false
		}
		if c.CharLength > 0 {
			return int(c.CharLength), false
		}
		if c.Length > 0 {
			// In Oracle, NCHAR/NVARCHAR2 data_length may be bytes. 11g metadata can
			// occasionally miss char_length; fall back to byte/2 for UTF-16 storage.
			estimatedChars := int(c.Length / 2)
			if estimatedChars > 0 {
				return estimatedChars, false
			}
			return int(c.Length), false
		}
		return 32, false
	}

	if strings.Contains(dataType, "CHAR") {
		if c.CharUsed == "C" {
			if c.CharDeclLength > 0 {
				return int(c.CharDeclLength), false
			}
			if c.CharLength > 0 {
				return int(c.CharLength), false
			}
			if c.Length > 0 {
				return int(c.Length), false
			}
		}
		if c.Length > 0 {
			return int(c.Length), true
		}
		if c.CharLength > 0 {
			return int(c.CharLength), false
		}
		return 32, false
	}

	if c.Length > 0 {
		return int(c.Length), true
	}
	return 32, true
}

func pow10Int64(digits int) int64 {
	if digits <= 0 {
		return 1
	}
	if digits > 18 {
		digits = 18
	}
	result := int64(1)
	for i := 0; i < digits; i++ {
		result *= 10
	}
	return result
}

func roundFloat(value float64, scale int) float64 {
	if scale <= 0 {
		return math.Round(value)
	}
	factor := math.Pow10(scale)
	return math.Round(value*factor) / factor
}

func randomScaledFloat(r *rand.Rand, scale int64, maxAbs float64) float64 {
	effectiveScale := int(scale)
	if effectiveScale < 0 {
		effectiveScale = 0
	}
	if effectiveScale > 9 {
		effectiveScale = 9
	}
	sign := 1.0
	if r.Intn(10) < 2 {
		sign = -1.0
	}
	value := sign * (r.Float64() * maxAbs)
	return roundFloat(value, effectiveScale)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
