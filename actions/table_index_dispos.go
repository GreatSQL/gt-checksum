package actions

import (
	"bufio"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"gt-checksum/utils"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	chanString      chan string
	chanMap         chan map[string]string
	chanBool        chan bool
	chanDiffDataS   chan DifferencesDataStruct
	chanSliceString chan []string
	chanStruct      chan struct{}
)

var (
	lock sync.Mutex

	// 用于跟踪已经输出过目标表为空提示的表，避免重复输出
	emptyTableWarned = make(map[string]bool)
	emptyTableMutex  sync.Mutex

	// 全局主键值跟踪机制 - 修复重复DELETE/INSERT冲突问题
	deleteMutex       sync.Mutex                  // 保护并发访问deletePrimaryKeys map的互斥锁
	deletePrimaryKeys = make(map[uint64]struct{}) // 全局已处理的DELETE主键值去重（hash key）

	insertMutex              sync.Mutex                  // 保护并发访问insertedPrimaryKeys map的互斥锁
	insertedPrimaryKeys      = make(map[uint64]struct{}) // 全局已处理的INSERT主键值跟踪（hash key）
	tableMemoryPeaks         sync.Map
	forcedGCMutex            sync.Mutex
	lastForcedGCAt           time.Time
	temporalDatetimePrefixRe = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})`)
	temporalNumericSecondsRe = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)
	temporalIntervalRe       = regexp.MustCompile(`^([+-]?\d+)\s+(\d{1,2}):(\d{2}):(\d{2})(?:\.\d+)?$`)
	temporalTimeTokenRe      = regexp.MustCompile(`\b(\d{1,3}:\d{2}:\d{2})\b`)
	temporalGoDurationRe     = regexp.MustCompile(`^(-?\d+)h(\d+)m(\d+(?:\.\d+)?)s$`)
	temporalDateOnlyRe       = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	temporalDateTimeRe       = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})`)
	oracleSimpleIdentRe      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)
)

type tableMemoryPeak struct {
	Stage       string
	AllocMB     uint64
	HeapInuseMB uint64
	HeapObjects uint64
	NumGC       uint32
}

func adaptWhereForDrive(where, drive string) string {
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		return strings.ReplaceAll(where, "`", "\"")
	}
	return where
}

// buildChunkRangeWhere 组合外层 WHERE 与本次分片的范围谓词。
// 当 low/high 为空字符串时跳过对应边界，避免在 Oracle 上生成 `col >= ''`
// 这类谓词触发 ORA-01722: invalid number（Oracle 将空串视为 NULL，
// 数值列的隐式转换会失败）。
func buildChunkRangeWhere(outer, col, low, high string, highInclusive bool) string {
	var preds []string
	if low != "" {
		preds = append(preds, fmt.Sprintf("`%s` >= '%s'", col, low))
	}
	if high != "" {
		op := "<"
		if highInclusive {
			op = "<="
		}
		preds = append(preds, fmt.Sprintf("`%s` %s '%s'", col, op, high))
	}
	chunk := strings.Join(preds, " and ")
	switch {
	case outer != "" && chunk != "":
		return fmt.Sprintf("%s and %s", outer, chunk)
	case outer != "":
		return outer
	case chunk != "":
		return chunk
	default:
		return "1=1"
	}
}

func oracleMetadataMatchExpr(column, value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return fmt.Sprintf("UPPER(%s)=UPPER('%s')", column, escaped)
}

// ResetMemoryPeakStats clears per-table peak memory metrics for a new checksum run.
func ResetMemoryPeakStats() {
	tableMemoryPeaks = sync.Map{}
}

// LogMemoryPeakSummary prints per-table memory peak summary to log.
func LogMemoryPeakSummary() {
	type item struct {
		table string
		peak  tableMemoryPeak
	}
	var items []item
	tableMemoryPeaks.Range(func(key, value interface{}) bool {
		table, ok := key.(string)
		if !ok {
			return true
		}
		peak, ok := value.(tableMemoryPeak)
		if !ok {
			return true
		}
		items = append(items, item{table: table, peak: peak})
		return true
	})
	if len(items) == 0 {
		return
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].table < items[j].table
	})
	global.Wlog.Info("MEM_PEAK_SUMMARY: begin")
	for _, it := range items {
		global.Wlog.Info(fmt.Sprintf("MEM_PEAK table=%s peakStage=%s Alloc=%dMB HeapInuse=%dMB HeapObjects=%d NumGC=%d",
			it.table,
			it.peak.Stage,
			it.peak.AllocMB,
			it.peak.HeapInuseMB,
			it.peak.HeapObjects,
			it.peak.NumGC,
		))
	}
	global.Wlog.Info("MEM_PEAK_SUMMARY: end")
}

func updateTableMemoryPeak(tableKey string, peak tableMemoryPeak) {
	if tableKey == "" {
		return
	}
	existingValue, ok := tableMemoryPeaks.Load(tableKey)
	if !ok {
		tableMemoryPeaks.Store(tableKey, peak)
		return
	}
	existing, ok := existingValue.(tableMemoryPeak)
	if !ok {
		tableMemoryPeaks.Store(tableKey, peak)
		return
	}
	if peak.AllocMB > existing.AllocMB || (peak.AllocMB == existing.AllocMB && peak.HeapInuseMB > existing.HeapInuseMB) {
		tableMemoryPeaks.Store(tableKey, peak)
	}
}

/*
初始化差异数据信息结构体
*/
func InitDifferencesDataStruct() DifferencesDataStruct {
	return DifferencesDataStruct{}
}

func hashDedupeKey(raw string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(raw))
	return h.Sum64()
}

func markDeleteKeyIfAbsent(raw string, enabled bool) bool {
	if !enabled {
		return true
	}
	key := hashDedupeKey(raw)
	deleteMutex.Lock()
	defer deleteMutex.Unlock()
	if _, exists := deletePrimaryKeys[key]; exists {
		return false
	}
	deletePrimaryKeys[key] = struct{}{}
	return true
}

func hasDeleteKey(raw string, enabled bool) bool {
	if !enabled {
		return false
	}
	key := hashDedupeKey(raw)
	deleteMutex.Lock()
	_, exists := deletePrimaryKeys[key]
	deleteMutex.Unlock()
	return exists
}

func markInsertKeyIfAbsent(raw string, enabled bool) bool {
	if !enabled {
		return true
	}
	key := hashDedupeKey(raw)
	insertMutex.Lock()
	defer insertMutex.Unlock()
	if _, exists := insertedPrimaryKeys[key]; exists {
		return false
	}
	insertedPrimaryKeys[key] = struct{}{}
	return true
}

func hasInsertKey(raw string, enabled bool) bool {
	if !enabled {
		return false
	}
	key := hashDedupeKey(raw)
	insertMutex.Lock()
	_, exists := insertedPrimaryKeys[key]
	insertMutex.Unlock()
	return exists
}

func isIntegerColumnType(columnType string) bool {
	ct := strings.ToLower(strings.TrimSpace(columnType))
	if ct == "" {
		return false
	}
	// Oracle NUMBER(p,0) 亦为整数。识别后可走数值分片 fast path，避免进入 GROUP BY
	// 递归路径，后者在目标端为 MySQL BIT 列时会把原始字节当作 chunk 边界值回传
	// Oracle NUMBER 谓词，触发 ORA-01722: invalid number。
	if strings.HasPrefix(ct, "number(") {
		rest := strings.TrimSuffix(strings.TrimPrefix(ct, "number("), ")")
		parts := strings.Split(rest, ",")
		if len(parts) == 2 {
			scale := strings.TrimSpace(parts[1])
			if scale == "0" {
				return true
			}
		}
		return false
	}
	return strings.HasPrefix(ct, "tinyint") ||
		strings.HasPrefix(ct, "smallint") ||
		strings.HasPrefix(ct, "mediumint") ||
		strings.HasPrefix(ct, "int") ||
		strings.HasPrefix(ct, "bigint")
}

func (sp *SchedulePlan) getSourceColumnType(columnName string) string {
	candidates := []string{
		fmt.Sprintf("%s_gtchecksum_%s", sp.sourceSchema, sp.table),
		fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table),
	}
	for _, key := range candidates {
		colInfo, ok := sp.tableAllCol[key]
		if !ok {
			continue
		}
		for _, col := range colInfo.SColumnInfo {
			name := col["columnName"]
			if !strings.EqualFold(name, columnName) {
				continue
			}
			if t := col["dataType"]; t != "" {
				return t
			}
			if t := col["columnType"]; t != "" {
				return t
			}
		}
	}
	return ""
}

func quoteIdentifierByDrive(name, drive string) string {
	name = strings.TrimSpace(name)
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		if len(name) >= 2 && strings.HasPrefix(name, "\"") && strings.HasSuffix(name, "\"") {
			unquoted := strings.ReplaceAll(name[1:len(name)-1], "\"\"", "\"")
			return fmt.Sprintf("\"%s\"", strings.ReplaceAll(unquoted, "\"", "\"\""))
		}
		if oracleSimpleIdentRe.MatchString(name) {
			return strings.ToUpper(name)
		}
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
	}
	return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
}

func qualifiedTableByDrive(schema, table, drive string) string {
	return fmt.Sprintf("%s.%s", quoteIdentifierByDrive(schema, drive), quoteIdentifierByDrive(table, drive))
}

func queryTableMinMaxInt64ByDrive(db *sql.DB, drive, schema, table, columnName, where string) (int64, int64, bool, error) {
	columnExpr := quoteIdentifierByDrive(columnName, drive)
	query := ""
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		query = fmt.Sprintf("SELECT CAST(MIN(%s) AS VARCHAR2(64)), CAST(MAX(%s) AS VARCHAR2(64)) FROM %s",
			columnExpr, columnExpr, qualifiedTableByDrive(schema, table, drive))
		if where != "" {
			query = fmt.Sprintf("%s WHERE %s", query, adaptWhereForDrive(where, drive))
		}
	} else {
		query = fmt.Sprintf("SELECT CAST(MIN(%s) AS CHAR), CAST(MAX(%s) AS CHAR) FROM %s",
			columnExpr, columnExpr, qualifiedTableByDrive(schema, table, drive))
		if where != "" {
			query = fmt.Sprintf("%s WHERE %s", query, where)
		}
	}

	var minStr, maxStr sql.NullString
	if err := db.QueryRow(query).Scan(&minStr, &maxStr); err != nil {
		return 0, 0, false, err
	}
	if !minStr.Valid || !maxStr.Valid || strings.TrimSpace(minStr.String) == "" || strings.TrimSpace(maxStr.String) == "" {
		return 0, 0, false, nil
	}
	minVal, err := strconv.ParseInt(strings.TrimSpace(minStr.String), 10, 64)
	if err != nil {
		return 0, 0, false, nil
	}
	maxVal, err := strconv.ParseInt(strings.TrimSpace(maxStr.String), 10, 64)
	if err != nil {
		return 0, 0, false, nil
	}
	return minVal, maxVal, true, nil
}

func queryTableRowsEstimateByDrive(db *sql.DB, drive, schema, table string) uint64 {
	var (
		query     string
		tableRows sql.NullInt64
	)
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		query = fmt.Sprintf("SELECT NUM_ROWS FROM ALL_TABLES WHERE %s AND %s",
			oracleMetadataMatchExpr("OWNER", schema),
			oracleMetadataMatchExpr("TABLE_NAME", table),
		)
		if err := db.QueryRow(query).Scan(&tableRows); err != nil {
			return 0
		}
	} else {
		query = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? LIMIT 1"
		if err := db.QueryRow(query, schema, table).Scan(&tableRows); err != nil {
			return 0
		}
	}
	if !tableRows.Valid || tableRows.Int64 <= 0 {
		return 0
	}
	return uint64(tableRows.Int64)
}

func queryColumnHasNullByDrive(db *sql.DB, drive, schema, table, columnName, where string) (bool, error) {
	columnExpr := quoteIdentifierByDrive(columnName, drive)
	query := fmt.Sprintf("SELECT 1 FROM %s", qualifiedTableByDrive(schema, table, drive))
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		nullPredicate := fmt.Sprintf("%s IS NULL", columnExpr)
		if where != "" {
			query = fmt.Sprintf("%s WHERE (%s) AND %s AND ROWNUM = 1", query, adaptWhereForDrive(where, drive), nullPredicate)
		} else {
			query = fmt.Sprintf("%s WHERE %s AND ROWNUM = 1", query, nullPredicate)
		}
	} else {
		nullPredicate := fmt.Sprintf("%s IS NULL", columnExpr)
		if where != "" {
			query = fmt.Sprintf("%s WHERE (%s) AND %s LIMIT 1", query, where, nullPredicate)
		} else {
			query = fmt.Sprintf("%s WHERE %s LIMIT 1", query, nullPredicate)
		}
	}

	var one int
	err := db.QueryRow(query).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func buildNumericChunkWhereClauses(columnName, baseWhere, drive string, minVal, maxVal int64, chunkRows int, estimatedRows uint64, includeNull bool) []string {
	if chunkRows <= 0 || maxVal < minVal {
		return nil
	}
	colExpr := quoteIdentifierByDrive(columnName, drive)

	targetChunks := 1
	if estimatedRows > 0 {
		targetChunks = int((estimatedRows + uint64(chunkRows) - 1) / uint64(chunkRows))
	} else {
		span := maxVal - minVal + 1
		targetChunks = int((span + int64(chunkRows) - 1) / int64(chunkRows))
	}
	if targetChunks < 1 {
		targetChunks = 1
	}

	span := maxVal - minVal + 1
	step := (span + int64(targetChunks) - 1) / int64(targetChunks)
	if step < 1 {
		step = 1
	}

	clauses := make([]string, 0, targetChunks+1)
	if includeNull {
		nullClause := fmt.Sprintf("%s IS NULL", colExpr)
		if baseWhere != "" {
			nullClause = fmt.Sprintf("%s and %s", baseWhere, nullClause)
		}
		clauses = append(clauses, nullClause)
	}

	for start := minVal; start <= maxVal; {
		next := start + step
		var clause string
		if next <= maxVal {
			clause = fmt.Sprintf("%s >= %d and %s < %d", colExpr, start, colExpr, next)
		} else {
			clause = fmt.Sprintf("%s >= %d", colExpr, start)
		}
		if baseWhere != "" {
			clause = fmt.Sprintf("%s and %s", baseWhere, clause)
		}
		clauses = append(clauses, clause)

		if next <= start {
			break
		}
		start = next
	}
	return clauses
}

func (sp *SchedulePlan) generateFirstLevelNumericChunks(sdb, ddb *sql.DB, level, queryNum int, where string, logThreadSeq int64) ([]string, bool) {
	if level != 0 || queryNum <= 0 {
		return nil, false
	}
	if where != "" {
		return nil, false
	}
	sourceDriveSupported := strings.EqualFold(sp.sdrive, "mysql") || strings.EqualFold(sp.sdrive, "godror") || strings.EqualFold(sp.sdrive, "oracle")
	destDriveSupported := strings.EqualFold(sp.ddrive, "mysql") || strings.EqualFold(sp.ddrive, "godror") || strings.EqualFold(sp.ddrive, "oracle")
	if !sourceDriveSupported || !destDriveSupported {
		return nil, false
	}
	if level >= len(sp.columnName) {
		return nil, false
	}

	column := sp.columnName[level]
	columnType := sp.getSourceColumnType(column)
	if !isIntegerColumnType(columnType) {
		return nil, false
	}

	destTable := sp.getDestTableName()
	sMin, sMax, sHasRows, sErr := queryTableMinMaxInt64ByDrive(sdb, sp.sdrive, sp.sourceSchema, sp.table, column, where)
	if sErr != nil {
		return nil, false
	}
	dMin, dMax, dHasRows, dErr := queryTableMinMaxInt64ByDrive(ddb, sp.ddrive, sp.destSchema, destTable, column, where)
	if dErr != nil {
		return nil, false
	}
	if !sHasRows && !dHasRows {
		return []string{}, true
	}

	minVal := sMin
	maxVal := sMax
	if !sHasRows {
		minVal = dMin
		maxVal = dMax
	}
	if dHasRows {
		if dMin < minVal {
			minVal = dMin
		}
		if dMax > maxVal {
			maxVal = dMax
		}
	}

	sEstRows := queryTableRowsEstimateByDrive(sdb, sp.sdrive, sp.sourceSchema, sp.table)
	dEstRows := queryTableRowsEstimateByDrive(ddb, sp.ddrive, sp.destSchema, destTable)
	estRows := sEstRows
	if dEstRows > estRows {
		estRows = dEstRows
	}

	sHasNull, _ := queryColumnHasNullByDrive(sdb, sp.sdrive, sp.sourceSchema, sp.table, column, where)
	dHasNull, _ := queryColumnHasNullByDrive(ddb, sp.ddrive, sp.destSchema, destTable, column, where)

	clauses := buildNumericChunkWhereClauses(column, where, sp.sdrive, minVal, maxVal, queryNum, estRows, sHasNull || dHasNull)
	if len(clauses) == 0 {
		return nil, false
	}

	vlog := fmt.Sprintf("(%d) Numeric range chunking enabled for %s.%s.%s, chunks=%d, span=[%d,%d], estRows=%d, drives=%s=>%s",
		logThreadSeq, sp.sourceSchema, sp.table, column, len(clauses), minVal, maxVal, estRows, sp.sdrive, sp.ddrive)
	global.Wlog.Info(vlog)
	return clauses, true
}

/*
递归查询索引列数据，并按照单次校验块的大小来切割索引列数据，生成查询的where条件
*/
func (sp *SchedulePlan) recursiveIndexColumn(sqlWhere chanString, sdb, ddb *sql.DB, level, queryNum int, where string, selectColumn map[string]map[string]string, logThreadSeq int64) {
	var (
		sqlwhere       string //查询sql的where条件
		d, c           int    //索引列每一行group重复值的累加值，临时变量
		e, g           string //定义每个chunk的初始值和结尾值,e为起始值，g为数据查询的动态指针值
		vlog           string //日志输出变量
		autoIncSeq     uint64
		partFirstValue = true
		curryCount     int64
	)

	// Floating-point indexed columns are unsafe for range-bound chunk predicates
	// (e.g. WHERE f1 >= '123.45') due binary representation drift.
	// Fall back to a safe single chunk predicate to preserve correctness.
	if level < len(sp.columnName) && sp.isFloatingIndexColumn(sp.columnName[level]) {
		safeWhere := strings.TrimSpace(where)
		if safeWhere == "" {
			safeWhere = "1=1"
		}
		vlog = fmt.Sprintf("(%d) Floating index fallback enabled for %s.%s column %s at level %d, using safe where: %s",
			logThreadSeq, sp.schema, sp.table, sp.columnName[level], level, safeWhere)
		global.Wlog.Warn(vlog)
		sqlWhere <- safeWhere
		if level == 0 {
			close(sqlWhere)
		}
		return
	}

	// Fast path for integer leading columns (MySQL/Oracle):
	// build chunk ranges from min/max + row estimate and skip full GROUP BY key materialization.
	if clauses, ok := sp.generateFirstLevelNumericChunks(sdb, ddb, level, queryNum, where, logThreadSeq); ok {
		for _, clause := range clauses {
			if level < len(sp.columnName)-1 {
				sp.recursiveIndexColumn(sqlWhere, sdb, ddb, level+1, queryNum, clause, selectColumn, logThreadSeq)
				continue
			}
			sqlWhere <- clause
		}
		if level == 0 {
			close(sqlWhere)
		}
		return
	}

	//获取索引列的数据类型
	a := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)].SColumnInfo
	//查询源目标端索引列数据
	idxc := dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName, SelectColumn: selectColumn[sp.sdrive], ColData: a}
	vlog = fmt.Sprintf("(%d) Querying source table %s.%s index column %s with WHERE: %s", logThreadSeq, sp.sourceSchema, sp.table, sp.columnName[level], where)
	global.Wlog.Debug(vlog)
	// 对于复合主键，查询符合前一个索引列条件的索引值，而不是所有可能的值
	// 这确保了递归查询的效率
	sourceWhereForGroup := adaptWhereForDrive(where, sp.sdrive)
	SdataChan1, err := idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(sdb, sourceWhereForGroup, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}
	idxcDest := dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: sp.getDestTableName(), ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName, SelectColumn: selectColumn[sp.ddrive], ColData: a}
	vlog = fmt.Sprintf("(%d) Querying target table %s.%s index column %s with WHERE: %s", logThreadSeq, sp.destSchema, sp.getDestTableName(), sp.columnName[level], where)
	global.Wlog.Debug(vlog)
	// 对于复合主键，查询符合前一个索引列条件的索引值，而不是所有可能的值
	// 这确保了递归查询的效率
	destWhereForGroup := adaptWhereForDrive(where, sp.ddrive)
	DdataChan1, err := idxcDest.TableIndexColumn().TmpTableColumnGroupDataDispos(ddb, destWhereForGroup, sp.columnName[level], logThreadSeq)
	if err != nil {
		return
	}

	// 修复：对于复合主键，确保递归时生成完整的WHERE条件
	if len(sp.columnName) > 1 {
		// 仅在复合主键的第一列时执行此逻辑
		if level == 0 {
			vlog = fmt.Sprintf("(%d) Handling composite primary key %s for %s.%s", logThreadSeq, strings.Join(sp.columnName, ","), sp.schema, sp.table)
			global.Wlog.Debug(vlog)
		}
	}

	cMerge := dataDispos.DataInfo{ChanQueueDepth: sp.mqQueueDepth}
	ascUniqSDDataChan := cMerge.ChangeMerge(SdataChan1, DdataChan1)

	//处理原目标端索引列数据的集合，并按照单次校验数据块大小来进行数据截取，如果是多列索引，则需要递归查询截取
	for {
		select {
		case cc, ok := <-ascUniqSDDataChan:
			autoIncSeq++
			var key, value string
			for k, v := range cc {
				key = k
				value = fmt.Sprintf("%v", v)
			}
			if !ok {
				// 修复：在通道关闭前，检查是否还有未处理的边界数据需要查询
				// 这确保了当总数据量正好是chunkSize的整数倍时，最后一条记录不会被遗漏
				global.Wlog.Debugf("DEBUG_CHANNEL_CLOSE: level=%d, e='%s', e!=''=%v\n", level, e, e != "")

				// 当d==0且e不为空时，说明上一个chunk刚好在边界处结束，
				// e被设置为下一个值但从未被包含在任何chunk中，需要补发一个最终chunk
				if e != "" {
					sqlwhere = buildChunkRangeWhere(where, sp.columnName[level], e, "", false)
					global.Wlog.Debugf("(%d) Final chunk emitted for remaining boundary value: %s", logThreadSeq, sqlwhere)
					sqlWhere <- sqlwhere
					sqlwhere = ""
				}

				if level == 0 {
					close(sqlWhere)
				}
				vlog = fmt.Sprintf("(%d) Completed WHERE condition processing for index column %s in %s.%s", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
				global.Wlog.Debug(vlog)
				return
			}
			shouldLogDetail := autoIncSeq <= 10 || key == dataDispos.StreamEndMarker || autoIncSeq%500 == 0
			if shouldLogDetail {
				vlog = fmt.Sprintf("(%d) Index column %s level %d - WHERE: %s, value: %s, count: %v", logThreadSeq, sp.columnName[level], level, where, key, value)
				global.Wlog.Debug(vlog)
			}
			if key == dataDispos.ValueNullPlaceholder || key == dataDispos.ValueEmptyPlaceholder {
				vlog = fmt.Sprintf("(%d) Processing NULL values for index column %s level %d", logThreadSeq, sp.columnName[level], level)
				global.Wlog.Debug(vlog)
				if e != "" { //假如null或者entry不是首行，则先处理原有数据条件
					if key != dataDispos.StreamEndMarker {
						g = key
					}
					if e == g {
						sqlwhere = fmt.Sprintf(" `%v` >= '%v' and `%v` <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					} else {
						sqlwhere = fmt.Sprintf(" `%v` >= '%v' and `%v` <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
					}
					if where != "" {
						sqlwhere = fmt.Sprintf("%s %s", where, sqlwhere)
					}
					//global.Wlog.Debug("DEBUG_WHERE1: %s", sqlwhere)
					sqlWhere <- sqlwhere

					sqlwhere, e, g = "", "", ""
				}
				var whereExist string
				if where != "" {
					whereExist = fmt.Sprintf("%s and ", where)
				}
				if key == dataDispos.ValueEmptyPlaceholder {
					sqlwhere = fmt.Sprintf("%s `%s` = '' ", whereExist, sp.columnName[level])
				}
				if key == dataDispos.ValueNullPlaceholder {
					sqlwhere = fmt.Sprintf("%s `%s` is null ", whereExist, sp.columnName[level])
				}
				//global.Wlog.Debug("DEBUG_WHERE6: %s", sqlwhere)
				partFirstValue = true
				vlog = fmt.Sprintf("(%d) NULL values processed for index column %s level %d - WHERE: %s", logThreadSeq, sp.columnName[level], level, sqlwhere)
				global.Wlog.Debug(vlog)
				sqlWhere <- sqlwhere

				sqlwhere = ""
			} else {
				//获取联合索引或单列索引的首值
				if key != dataDispos.StreamEndMarker && e == "" {
					e = key
					global.Wlog.Debugf("DEBUG_FIRST_VALUE: First key from merged data stream is '%s'\n", key)
				}
				vlog = fmt.Sprintf("(%d) Index column %s level %d starting value: %s", logThreadSeq, sp.columnName[level], level, e)
				global.Wlog.Debug(vlog)

				// 如果是level=0的前几个值，额外记录调试信息
				if level == 0 && autoIncSeq <= 3 {
					global.Wlog.Debugf("DEBUG_DATA_STREAM_%d: key='%s', value='%s', current e='%s'\n", autoIncSeq, key, value, e)
				}
				//获取每行的count值,并将count值记录及每次动态的值
				if key != dataDispos.StreamEndMarker {
					c, _ = strconv.Atoi(value)
					g = key
					if level == 0 {
						curryCount = curryCount + int64(c)
					}
					// group count(*)的值进行累加
					d = d + c
				}
				//判断行数累加值是否小于要校验的值，并且是最后一条索引列数据
				if d < queryNum && d > 0 && key == dataDispos.StreamEndMarker {
					vlog = fmt.Sprintf("(%d) Processing end of index column %s level %d", logThreadSeq, sp.columnName[level], level)
					global.Wlog.Debug(vlog)
					// 修复：对于最后一段数据，使用没有上界的条件以确保包含所有剩余记录；
					// 若起始值 e 为空（如全表仅有 NULL/空首值），跳过 `>= ''` 以免
					// Oracle 数值列触发 ORA-01722。
					sqlwhere = buildChunkRangeWhere(where, sp.columnName[level], e, "", false)
					if partFirstValue {
						partFirstValue = false
					}
					//global.Wlog.Debug("DEBUG_WHERE7: %s", sqlwhere)

					sqlWhere <- sqlwhere

					sqlwhere = ""
					e = "" // 防止通道关闭时重复发送最终chunk
					vlog = fmt.Sprintf("(%d) Completed processing end of index column %s level %d - WHERE: %s", logThreadSeq, sp.columnName[level], level, sqlwhere)
					global.Wlog.Debug(vlog)
				}
			}
			//判断行数累加值是否>=要校验的值
			if d >= queryNum {
				//判断联合索引列深度
				if level < len(sp.columnName)-1 { //如果不是最后一列，继续递归处理
					// 修复：对于复合主键，确保递归时传递完整的WHERE条件；
					// 若 e/g 为空则跳过对应边界，避免 Oracle ORA-01722。
					newWhere := buildChunkRangeWhere(where, sp.columnName[level], e, g, false)
					//global.Wlog.Debug("DEBUG_WHERE3: %s", newWhere)

					level++ //索引列层数递增
					//进入下一层的索引计算
					sp.recursiveIndexColumn(sqlWhere, sdb, ddb, level, queryNum, newWhere, selectColumn, logThreadSeq)
					level-- //回到上一层
					if key != dataDispos.StreamEndMarker {
						e = key
					}
				} else { //如果是最后一列，直接输出当前索引列深度的条件
					if d == c && c >= queryNum { //单行索引列数据的group值大于并发数
						var whereExist string
						if where != "" {
							whereExist = fmt.Sprintf("%s and ", where)
						}
						sqlwhere = fmt.Sprintf("%s `%v` = '%v' ", whereExist, sp.columnName[level], g)
					} else {
						// 若 e/g 为空则跳过对应边界，避免 Oracle ORA-01722。
						sqlwhere = buildChunkRangeWhere(where, sp.columnName[level], e, g, false)
						if partFirstValue {
							partFirstValue = false
						}
					}
					//global.Wlog.Debug("DEBUG_WHERE2: %s", sqlwhere)

					sqlWhere <- sqlwhere

					if key != dataDispos.StreamEndMarker {
						e = key
					}
					sqlwhere = ""
				}
				d = c //累加值重置为当前行的行数，因为当前行属于下一个分片
			}
		}
	}
}

func (sp *SchedulePlan) indexColumnDispos(sqlWhere chanString, selectColumn map[string]map[string]string) {
	var (
		vlog         string
		logThreadSeq int64
		where        string
	)
	time.Sleep(time.Nanosecond * 2)
	logThreadSeq = rand.Int63()
	vlog = fmt.Sprintf("(%d) Generating query sequence for table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)

	// 获取全局配置中的sqlWhere条件
	globalConfig := inputArg.GetGlobalConfig()
	if globalConfig != nil && globalConfig.SecondaryL.SchemaV.SqlWhere != "" {
		where = globalConfig.SecondaryL.SchemaV.SqlWhere
		vlog = fmt.Sprintf("(%d) Using sqlWhere condition: %s", logThreadSeq, globalConfig.SecondaryL.SchemaV.SqlWhere)
		global.Wlog.Info(vlog)

		// 检查表中是否存在WHERE条件中引用的所有列
		sdb := sp.sdbPool.Get(logThreadSeq)
		if !sp.checkColumnsExistInWhere(sdb, sp.sourceSchema, sp.table, where, logThreadSeq) {
			// 表中不存在WHERE条件中引用的列，跳过该表
			sp.sdbPool.Put(sdb, logThreadSeq)
			vlog = fmt.Sprintf("(%d) Skipping table %s.%s: columns referenced in WHERE condition do not exist", logThreadSeq, sp.sourceSchema, sp.table)
			global.Wlog.Warn(vlog)
			// 只对源数据库的表添加跳过记录，避免映射关系中的目标表重复添加
			global.AddSkippedTable(sp.sourceSchema, sp.table, "data", "columns referenced in WHERE condition do not exist")
			close(sqlWhere)
			return
		}
		sp.sdbPool.Put(sdb, logThreadSeq)
	}

	//查询表索引列数据并且生成查询的where条件
	sdb := sp.sdbPool.Get(logThreadSeq)
	ddb := sp.ddbPool.Get(logThreadSeq)
	sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, where, selectColumn, logThreadSeq)
	sp.sdbPool.Put(sdb, logThreadSeq)
	sp.ddbPool.Put(ddb, logThreadSeq)
	vlog = fmt.Sprintf("(%d) Query sequence generated for table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
}

/*
检查WHERE条件中引用的所有列是否在表中存在
*/
func (sp *SchedulePlan) checkColumnsExistInWhere(db *sql.DB, schema, table, where string, logThreadSeq int64) bool {
	// 提取WHERE条件中的所有列名
	columns := extractColumnsFromWhere(where)
	if len(columns) == 0 {
		// 没有引用任何列，认为检查通过
		return true
	}

	// 检查每个列是否在表中存在
	for _, column := range columns {
		// 构建查询检查列是否存在
		query := ""
		if sp.sdrive == "godror" {
			query = fmt.Sprintf("SELECT COUNT(*) FROM all_tab_columns WHERE %s AND %s AND %s",
				oracleMetadataMatchExpr("owner", schema),
				oracleMetadataMatchExpr("table_name", table),
				oracleMetadataMatchExpr("column_name", column),
			)
		} else {
			query = fmt.Sprintf("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'", schema, table, column)
		}
		var count int
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			vlog := fmt.Sprintf("(%d) Failed to check if column %s exists in table %s.%s: %v", logThreadSeq, column, schema, table, err)
			global.Wlog.Error(vlog)
			return false
		}
		if count == 0 {
			vlog := fmt.Sprintf("(%d) Column %s does not exist in table %s.%s", logThreadSeq, column, schema, table)
			global.Wlog.Warn(vlog)
			return false
		}
	}

	return true
}

/*
从WHERE条件中提取所有列名
*/
func extractColumnsFromWhere(where string) []string {
	// 改进的列名提取逻辑
	var columns []string

	// 简化版本：只处理常见的操作符左侧的列名
	// 支持的操作符：=, !=, <, >, <=, >=, LIKE, IN, BETWEEN
	// 匹配模式：标识符 + 可选空格 + 操作符
	operatorPatterns := []string{
		"=", "!=", "<", ">", "<=", ">=", "LIKE", "IN", "BETWEEN",
	}

	for _, op := range operatorPatterns {
		// 构建正则表达式：匹配标识符（列名）后跟操作符
		pattern := fmt.Sprintf(`\b([a-zA-Z_][a-zA-Z0-9_]*)\s*%s`, regexp.QuoteMeta(op))
		re := regexp.MustCompile(pattern)
		matches := re.FindAllStringSubmatch(where, -1)
		for _, match := range matches {
			if len(match) > 1 {
				columns = append(columns, match[1])
			}
		}
	}

	// 过滤掉可能的关键字
	keywords := map[string]bool{
		"AND": true, "OR": true, "NOT": true, "IN": true, "LIKE": true, "BETWEEN": true,
		"IS": true, "NULL": true, "TRUE": true, "FALSE": true,
		"SELECT": true, "FROM": true, "WHERE": true, "GROUP": true, "ORDER": true, "LIMIT": true,
	}

	// 去重并过滤关键字和纯数字
	seen := make(map[string]bool)
	var result []string
	for _, column := range columns {
		// 过滤纯数字（值）
		isNumber := true
		for _, char := range column {
			if !('0' <= char && char <= '9') {
				isNumber = false
				break
			}
		}
		if isNumber {
			continue
		}

		// 过滤关键字
		lowerColumn := strings.ToUpper(column)
		if !keywords[lowerColumn] && !seen[column] {
			result = append(result, column)
			seen[column] = true
		}
	}

	return result
}

/*
针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型
*/
// Deprecated: 请使用queryTableSqlSeparate函数替代
func (sp *SchedulePlan) queryTableSql(sqlWhere chanString, selectSql chanMap, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	// 保持向后兼容
	sp.queryTableSqlSeparate(sqlWhere, make(chanMap), make(chanMap), cc1, sc, logThreadSeq)
	var (
		vlog string
		err  error
	)

	// 使用函数创建通道，以便在参数变更时重新初始化
	createCurryChan := func() chanStruct {
		return make(chanStruct, sp.concurrency)
	}

	curry := createCurryChan()
	autoSeq := int64(0)
	vlog = fmt.Sprintf("(%d) Processing block data checksum queries", logThreadSeq)
	global.Wlog.Debug(vlog)

	for {
		select {
		// 监听参数变更通知
		case <-utils.ParamChangedChan:
			// 检查并更新SchedulePlan的参数
			// 从运行时快照读取最新参数值，避免并发读写全局配置
			runtimeTune := utils.GetRuntimeTuneSnapshot()
			if runtimeTune.ParallelThds > 0 && runtimeTune.ChunkSize > 0 {
				sp.concurrency = runtimeTune.ParallelThds
				sp.chunkSize = runtimeTune.ChunkSize
				// 关闭旧通道并创建新通道
				close(curry)
				curry = createCurryChan()
				utils.ResetParamChanged()
				fmt.Printf("(%d) Parameters updated - concurrency: %d, chunkSize: %d\n", logThreadSeq, sp.concurrency, sp.chunkSize)
			}
		case c, ok := <-sqlWhere:
			if !ok {
				if len(curry) == 0 {
					sc <- autoSeq
					close(sc)
					close(selectSql)
					return
				}
			} else {
				autoSeq++
				curry <- struct{}{}
				sdb := sp.sdbPool.Get(logThreadSeq)
				ddb := sp.ddbPool.Get(logThreadSeq)
				//查询该表的列名和列信息
				go func(c1 string, sd, dd *sql.DB, sdbPool, ddbPool *global.Pool) {
					var selectSqlMap = make(map[string]string)
					defer func() {
						sdbPool.Put(sd, logThreadSeq)
						ddbPool.Put(dd, logThreadSeq)
						<-curry
					}()
					// 为源端生成WHERE条件
					sourceWhere := strings.Replace(c1, fmt.Sprintf("%s.%s", sp.destSchema, sp.table), fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), -1)
					sourceWhere = strings.Replace(sourceWhere, fmt.Sprintf("`%s`.`%s`", sp.destSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), -1)
					sourceWhere = adaptWhereForDrive(sourceWhere, sp.sdrive)

					// 源端使用sourceSchema和sourceTable
					idxc := dbExec.IndexColumnStruct{
						Schema:                  sp.sourceSchema,
						Table:                   sp.table,
						TableColumn:             cc1.SColumnInfo,
						Sqlwhere:                sourceWhere,
						Drivce:                  sp.sdrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						ColData:                 cc1.SColumnInfo,
						CompareColumns:          sp.columnPlanSourceCols, // nil = 全列模式
					}
					lock.Lock()
					selectSqlMap[sp.sdrive], err = idxc.TableIndexColumn().GeneratingQuerySql(sd, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to generate source query SQL for %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
						global.Wlog.Error(vlog)
						lock.Unlock()
						return
					}
					lock.Unlock()

					// 确保目标数据库存在
					ddb := sp.ddbPool.Get(logThreadSeq)
					_, err = ddb.Exec(fmt.Sprintf("USE `%s`", sp.destSchema))
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Target database %s does not exist", logThreadSeq, sp.destSchema)
						global.Wlog.Error(vlog)
						sp.ddbPool.Put(ddb, logThreadSeq)
						return
					}
					sp.ddbPool.Put(ddb, logThreadSeq)

					// 为目标端生成WHERE条件
					_destTableName := sp.getDestTableName()
					destWhere := strings.Replace(c1, fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), fmt.Sprintf("%s.%s", sp.destSchema, _destTableName), -1)
					destWhere = strings.Replace(destWhere, fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.destSchema, _destTableName), -1)
					destWhere = adaptWhereForDrive(destWhere, sp.ddrive)

					// 目标端使用destSchema和destTable
					idxcDest := dbExec.IndexColumnStruct{
						Schema:                  sp.destSchema,
						Table:                   _destTableName,
						TableColumn:             cc1.DColumnInfo,
						Sqlwhere:                destWhere,
						Drivce:                  sp.ddrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						ColData:                 cc1.DColumnInfo,
						CompareColumns:          sp.columnPlanTargetCols, // nil = 全列模式
					}
					// 添加对目标表存在的检查
					ddb = sp.ddbPool.Get(logThreadSeq)
					_, err = ddb.Exec(fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", sp.destSchema, _destTableName))
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Target table %s.%s does not exist", logThreadSeq, sp.destSchema, _destTableName)
						global.Wlog.Error(vlog)
						sp.ddbPool.Put(ddb, logThreadSeq)
						return
					}
					sp.ddbPool.Put(ddb, logThreadSeq)
					lock.Lock()
					selectSqlMap[sp.ddrive], err = idxcDest.TableIndexColumn().GeneratingQuerySql(dd, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to generate destination query SQL for %s.%s: %v", logThreadSeq, sp.destSchema, sp.table, err)
						global.Wlog.Error(vlog)
						return
					}
					lock.Unlock()
					vlog = fmt.Sprintf("(%d) Block data checksum queries completed", logThreadSeq)
					global.Wlog.Debug(vlog)
					selectSql <- selectSqlMap
				}(c, sdb, ddb, sp.sdbPool, sp.ddbPool)
			}
		}
	}
}

/*
针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型，并执行sql语句
*/
// Deprecated: 请使用queryTableDataSeparate函数替代
func (sp *SchedulePlan) queryTableData(selectSql chanMap, diffQueryData chanDiffDataS, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	// 保持向后兼容
	sp.queryTableDataSeparate(selectSql, make(chanMap), diffQueryData, cc1, sc, logThreadSeq)
	var (
		vlog               string
		aa                 = &CheckSumTypeStruct{}
		differencesData    = InitDifferencesDataStruct()
		autoSeq1, autoSeq2 int64
	)

	// 使用函数创建通道，以便在参数变更时重新初始化
	createCurryChan := func() chanStruct {
		return make(chanStruct, sp.concurrency)
	}

	curry := createCurryChan()
	sp.bar = &Bar{}
	// 始终使用rows模式
	if sp.tableMaxRows > 0 {
		barTotal := int64(sp.tableMaxRows / uint64(sp.chanrowCount))
		if sp.tableMaxRows%uint64(sp.chanrowCount) > 0 {
			barTotal += 1
		}
		sp.bar.NewOption(0, barTotal, "Processing")
	}

	for {
		select {
		// 监听参数变更通知
		case <-utils.ParamChangedChan:
			// 检查并更新SchedulePlan的参数
			// 从运行时快照读取最新参数值，避免并发读写全局配置
			runtimeTune := utils.GetRuntimeTuneSnapshot()
			if runtimeTune.ParallelThds > 0 && runtimeTune.ChunkSize > 0 {
				sp.concurrency = runtimeTune.ParallelThds
				sp.chunkSize = runtimeTune.ChunkSize
				// 关闭旧通道并创建新通道
				close(curry)
				curry = createCurryChan()
				utils.ResetParamChanged()
				fmt.Printf("(%d) Parameters updated - concurrency: %d, chunkSize: %d\n", logThreadSeq, sp.concurrency, sp.chunkSize)
			}
		case d, ok := <-sc:
			if ok {
				sp.bar.NewOption(0, d, "Processing")
			}
		case c, ok := <-selectSql:
			if !ok {
				if len(curry) == 0 {
					sp.bar.Finish()
					time.Sleep(time.Millisecond)
					close(diffQueryData)
					return
				}
			} else {
				autoSeq1++
				// 源端检查使用sourceSchema
				idxc := dbExec.IndexColumnStruct{
					Schema:                  sp.sourceSchema,
					Table:                   sp.table,
					TableColumn:             cc1.SColumnInfo,
					Sqlwhere:                c[sp.sdrive],
					Drivce:                  sp.sdrive,
					CaseSensitiveObjectName: sp.caseSensitiveObjectName,
					ColData:                 cc1.SColumnInfo,
				}
				curry <- struct{}{}
				go func(c1 map[string]string, cc1 global.TableAllColumnInfoS) {
					defer func() {
						<-curry
					}()
					//查询源端表数据
					vlog = fmt.Sprintf("(%d) Querying source table %s.%s block data", logThreadSeq, sp.sourceSchema, sp.table)
					global.Wlog.Debug(vlog)
					sdb := sp.sdbPool.Get(logThreadSeq)
					stt, err := idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					vlog = fmt.Sprintf("(%d) Source table %s.%s query result", logThreadSeq, sp.sourceSchema, sp.table)
					global.Wlog.Debug(vlog)
					sp.sdbPool.Put(sdb, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) Failed to query source table %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
						global.Wlog.Error(vlog)
						return
					}

					// 目标端检查使用destSchema
					idxcDest := dbExec.IndexColumnStruct{
						Schema:                  sp.destSchema,
						Table:                   sp.table,
						Sqlwhere:                c1[sp.ddrive],
						TableColumn:             cc1.DColumnInfo,
						Drivce:                  sp.ddrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						ColData:                 cc1.DColumnInfo,
					}
					ddb := sp.ddbPool.Get(logThreadSeq)
					dtt, err := idxcDest.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					vlog = fmt.Sprintf("(%d) Target table %s.%s query result", logThreadSeq, sp.destSchema, sp.table)
					global.Wlog.Debug(vlog)
					sp.ddbPool.Put(ddb, logThreadSeq)
					if err != nil {
						vlog = fmt.Sprintf("(%d) Failed to query target table %s.%s: %v", logThreadSeq, sp.destSchema, sp.table, err)
						global.Wlog.Error(vlog)
						return
					}
					vlog = fmt.Sprintf("(%d) Checking block data consistency for %s.%s", logThreadSeq, sp.sourceSchema, sp.table)
					global.Wlog.Debug(vlog)
					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						vlog = fmt.Sprintf("(%d) Data inconsistency found in %s.%s - Query: %s", logThreadSeq, sp.schema, sp.table, c1)
						global.Wlog.Debug(vlog)
						differencesData.Table = sp.table
						differencesData.Schema = sp.schema
						differencesData.SqlWhere = c1
						differencesData.TableColumnInfo = cc1
						differencesData.indexColumnType = sp.indexColumnType
						if differencesData.Schema != "" && differencesData.Table != "" {
							diffQueryData <- differencesData
						}
					} else {
						vlog = fmt.Sprintf("(%d) Data consistent in %s.%s - Query: %s", logThreadSeq, sp.schema, sp.table, c1)
						global.Wlog.Debug(vlog)
					}
					stt, dtt = "", ""
					vlog = fmt.Sprintf("(%d) Block data checksum completed for %s.%s", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debug(vlog)
				}(c, cc1)
			}
		}
		if autoSeq1 > autoSeq2 {
			sp.bar.Play(autoSeq1)
			autoSeq2 = autoSeq1
		}
	}
}

/*
差异数据的二次校验，并生成修复语句
*/
func (sp *SchedulePlan) AbnormalDataDispos(diffQueryData chanDiffDataS, cc chanString, logThreadSeq int64) {
	var (
		vlog             string
		aa               = &CheckSumTypeStruct{}
		curry            = make(chanStruct, sp.concurrency)
		totalInsertCount int64 // 全局INSERT语句计数器
	)
	isUniqueIndex := strings.HasPrefix(sp.indexColumnType, "pri_") || strings.HasPrefix(sp.indexColumnType, "uni_")
	// For unique/primary indexed compare flow, chunk ranges are non-overlapping in practice.
	// Keep global PK dedupe only for non-unique flows to reduce large hash-map residency.
	useGlobalKeyDedupe := !isUniqueIndex

	// 在处理前清空所有全局去重映射，确保每次运行都有干净的状态
	deleteMutex.Lock()
	deletePrimaryKeys = make(map[uint64]struct{})
	deleteMutex.Unlock()

	insertMutex.Lock()
	insertedPrimaryKeys = make(map[uint64]struct{}) // 关键修复：清空INSERT主键跟踪映射
	insertMutex.Unlock()
	vlog = fmt.Sprintf("(%d) Processing differences and generating repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	logStageMemory("diff-compare-start", logThreadSeq, sp.schema, sp.table)

	for {
		select {
		case c, ok := <-diffQueryData:
			if !ok {
				if len(curry) == 0 {
					logStageMemory("diff-compare-end", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Debugf("DEBUG_FINAL_COUNT_%d: Total INSERT statements generated for %s.%s: %d\n",
						logThreadSeq, sp.schema, sp.table, totalInsertCount)
					vlog = fmt.Sprintf("(%d) Completed difference processing and repair statements for %s.%s", logThreadSeq, sp.schema, sp.table)
					global.Wlog.Info(vlog)
					close(cc)
					return
				}
			} else {
				sdb := sp.sdbPool.Get(logThreadSeq)
				ddb := sp.ddbPool.Get(logThreadSeq)
				curry <- struct{}{}
				go func(c1 DifferencesDataStruct, sdb, ddb *sql.DB) {
					defer func() {
						<-curry
						sp.sdbPool.Put(sdb, logThreadSeq)
						sp.ddbPool.Put(ddb, logThreadSeq)
					}()
					// 使用映射后的源端和目标端schema和table
					sourceSchema := sp.sourceSchema
					destSchema := sp.destSchema
					table := sp.table

					// 获取列数据时使用原始schema.table组合
					colData := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sourceSchema, table)]

					// 处理源端SQL条件，确保使用正确的源端数据范围
					var sourceSqlWhere string

					// 修复：使用分批查询逻辑，避免全表查询导致内存消耗过大
					// 基于现有的WHERE条件进行查询，这些条件已经由recursiveIndexColumn正确分片
					var destSqlWhere string // 在更外层声明变量
					// 使用原始的WHERE条件，这些条件已经按照chunkSize正确分片
					sourceSqlWhere = c1.SqlWhere["src"]
					destSqlWhere = c1.SqlWhere["dst"]

					// 确保使用正确的schema
					if strings.Contains(sourceSqlWhere, fmt.Sprintf("`%s`", destSchema)) {
						sourceSqlWhere = strings.Replace(sourceSqlWhere,
							fmt.Sprintf("`%s`", destSchema),
							fmt.Sprintf("`%s`", sourceSchema), -1)
					}
					if strings.Contains(sourceSqlWhere, fmt.Sprintf("%s.", destSchema)) {
						sourceSqlWhere = strings.Replace(sourceSqlWhere,
							fmt.Sprintf("%s.", destSchema),
							fmt.Sprintf("%s.", sourceSchema), -1)
					}

					// 处理目标端SQL条件，确保使用目标端schema
					if strings.Contains(destSqlWhere, fmt.Sprintf("`%s`", sourceSchema)) {
						destSqlWhere = strings.Replace(destSqlWhere,
							fmt.Sprintf("`%s`", sourceSchema),
							fmt.Sprintf("`%s`", destSchema), -1)
					}
					if strings.Contains(destSqlWhere, fmt.Sprintf("%s.", sourceSchema)) {
						destSqlWhere = strings.Replace(destSqlWhere,
							fmt.Sprintf("%s.", sourceSchema),
							fmt.Sprintf("%s.", destSchema), -1)
					}

					// 重要修复：添加去重逻辑，防止分片数据重复处理
					// 每个WHERE条件应该是独立的，不应该有重叠
					vlog = fmt.Sprintf("(%d) Using chunked query - Source: %s, Target: %s", logThreadSeq, sourceSqlWhere, destSqlWhere)
					global.Wlog.Debug(vlog)

					// Log for debugging
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
					global.Wlog.Debug(vlog)
					vlog = fmt.Sprintf("(%d) AbnormalDataDispos - Target SQL condition: %s", logThreadSeq, destSqlWhere)
					global.Wlog.Debug(vlog)

					// 源端查询使用sourceSchema和table
					var (
						stt, dtt string
						err      error
					)
					idxc := dbExec.IndexColumnStruct{
						Schema:                  sourceSchema,
						Table:                   table,
						TableColumn:             colData.SColumnInfo,
						Drivce:                  sp.sdrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						Sqlwhere:                sourceSqlWhere, // 使用处理后的源端SQL条件
						ColumnName:              sp.columnName,
						CompareColumns:          sp.columnPlanSourceCols,
					}
					stt, err = idxc.TableIndexColumn().GeneratingQueryCriteria(sdb, logThreadSeq)
					if err != nil {
						global.Wlog.Warn(fmt.Sprintf("(%d) failed to query source chunk by criteria for %s.%s, fallback to raw SQL query, err=%v", logThreadSeq, sourceSchema, table, err))
						fallbackSourceSQL := strings.TrimSpace(c1.SqlWhere["src"])
						if strings.HasPrefix(strings.ToUpper(fallbackSourceSQL), "SELECT") {
							sourceRows, fallbackErr := queryRowsDataBySQL(sdb, fallbackSourceSQL, sp.sdrive, logThreadSeq)
							if fallbackErr != nil {
								global.Wlog.Error(fmt.Sprintf("(%d) source fallback query failed for %s.%s, mark table as diff, err=%v", logThreadSeq, sourceSchema, table, fallbackErr))
								lock.Lock()
								if sp.pods != nil {
									sp.pods.DIFFS = "yes"
								}
								lock.Unlock()
								return
							}
							stt = strings.Join(sourceRows, "/*go actions rowData*/")
						} else {
							global.Wlog.Error(fmt.Sprintf("(%d) source fallback SQL is unavailable for %s.%s, mark table as diff", logThreadSeq, sourceSchema, table))
							lock.Lock()
							if sp.pods != nil {
								sp.pods.DIFFS = "yes"
							}
							lock.Unlock()
							return
						}
					}

					// 目标端查询使用destSchema和table
					destTable := sp.getDestTableName()
					idxcDest := dbExec.IndexColumnStruct{
						Schema:                  destSchema,
						Table:                   destTable,
						TableColumn:             colData.DColumnInfo,
						Drivce:                  sp.ddrive,
						CaseSensitiveObjectName: sp.caseSensitiveObjectName,
						Sqlwhere:                destSqlWhere, // 使用处理后的目标端SQL条件
						ColumnName:              sp.columnName,
						CompareColumns:          sp.columnPlanTargetCols,
					}
					dtt, err = idxcDest.TableIndexColumn().GeneratingQueryCriteria(ddb, logThreadSeq)
					if err != nil {
						global.Wlog.Warn(fmt.Sprintf("(%d) failed to query dest chunk by criteria for %s.%s, fallback to raw SQL query, err=%v", logThreadSeq, destSchema, destTable, err))
						fallbackDestSQL := strings.TrimSpace(c1.SqlWhere["dst"])
						if strings.HasPrefix(strings.ToUpper(fallbackDestSQL), "SELECT") {
							destRows, fallbackErr := queryRowsDataBySQL(ddb, fallbackDestSQL, sp.ddrive, logThreadSeq)
							if fallbackErr != nil {
								global.Wlog.Error(fmt.Sprintf("(%d) dest fallback query failed for %s.%s, mark table as diff, err=%v", logThreadSeq, destSchema, destTable, fallbackErr))
								lock.Lock()
								if sp.pods != nil {
									sp.pods.DIFFS = "yes"
								}
								lock.Unlock()
								return
							}
							dtt = strings.Join(destRows, "/*go actions rowData*/")
						} else {
							global.Wlog.Error(fmt.Sprintf("(%d) dest fallback SQL is unavailable for %s.%s, mark table as diff", logThreadSeq, destSchema, destTable))
							lock.Lock()
							if sp.pods != nil {
								sp.pods.DIFFS = "yes"
							}
							lock.Unlock()
							return
						}
					}

					if aa.CheckMd5(stt) != aa.CheckMd5(dtt) {
						vlog = fmt.Sprintf("(%d) Data checksum mismatch for %s.%s, need to find specific differences", logThreadSeq, c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)
						waitForMemoryBudget(0.92)

						// 重要优化：精确比较数据，只找出真正需要修复的记录
						// 1. 将源端和目标端数据转换为切片
						sourceData := strings.Split(stt, "/*go actions rowData*/")
						destData := strings.Split(dtt, "/*go actions rowData*/")

						// 2. 使用优化的Arrcmp实现，只返回真正需要修复的记录
						// 先清理空记录，保留重复记录（不进行去重）
						cleanSourceData := make([]string, 0, len(sourceData))
						cleanDestData := make([]string, 0, len(destData))

						for _, data := range sourceData {
							// 只检查是否为空记录，不使用TrimSpace，保留原始数据中的空格
							if data != "" && data != "/*go actions rowData*/" {
								cleanSourceData = append(cleanSourceData, data)
							}
						}

						for _, data := range destData {
							// 只检查是否为空记录，不使用TrimSpace，保留原始数据中的空格
							if data != "" && data != "/*go actions rowData*/" {
								cleanDestData = append(cleanDestData, data)
							}
						}

						// 3. 记录去重前后的数据量
						vlog = fmt.Sprintf("(%d) Data deduplication - Source: %d->%d, Dest: %d->%d for %s.%s",
							logThreadSeq, len(sourceData), len(cleanSourceData), len(destData), len(cleanDestData), c1.Schema, c1.Table)
						global.Wlog.Debug(vlog)

						// 避免在大差异场景输出大文本日志，防止日志构造额外放大内存占用
						if len(cleanSourceData) > 0 {
							global.Wlog.Debugf("DEBUG_SOURCE_DATA_%d: sourceRecords=%d (sample suppressed)", logThreadSeq, len(cleanSourceData))
						}

						// 检查去重是否真的有效
						// 只有当源数据确实有内容时，才检查重复记录
						if len(sourceData) != len(cleanSourceData) {
							// 检查是否只有一个空字符串（源表为空的情况）
							if len(sourceData) == 1 && sourceData[0] == "" {
								// 源表为空，不是真正的重复记录
								global.Wlog.Debugf("(%d) Source data is empty, skipping duplicate check for %s.%s", logThreadSeq, c1.Schema, c1.Table)
							} else {
								duplicateCount := len(sourceData) - len(cleanSourceData)
								vlog = fmt.Sprintf("(%d) Found %d duplicate records in source data for %s.%s", logThreadSeq, duplicateCount, c1.Schema, c1.Table)
								global.Wlog.Warn(vlog)
							}
						}

						if len(destData) != len(cleanDestData) {
							// 检查是否只有一个空字符串（目标表为空的情况）
							if len(destData) == 1 && destData[0] == "" {
								// 目标表为空，不是真正的重复记录
								global.Wlog.Debugf("(%d) Destination table %s.%s is empty, skipping duplicate check", logThreadSeq, c1.Schema, c1.Table)

								// 每个表只输出一次目标表为空的提示
								tableKey := fmt.Sprintf("%s.%s", c1.Schema, c1.Table)
								emptyTableMutex.Lock()
								if !emptyTableWarned[tableKey] {
									// 输出目标表为空的提示
									vlog = fmt.Sprintf("(%d) Destination table %s.%s is empty, all source records will be added", logThreadSeq, c1.Schema, c1.Table)
									global.Wlog.Warn(vlog)
									// 标记该表已输出提示
									emptyTableWarned[tableKey] = true
								}
								emptyTableMutex.Unlock()
							} else {
								duplicateCount := len(destData) - len(cleanDestData)
								vlog = fmt.Sprintf("(%d) Found %d duplicate records in dest data for %s.%s", logThreadSeq, duplicateCount, c1.Schema, c1.Table)
								global.Wlog.Warn(vlog)
							}
						}

						// 4. 使用Arrcmp进行精确比较
						// columns 模式下，查询结果已按 PK+compareColumns 顺序裁剪，必须使用过滤后的列元信息，
						// 否则 buildFloatComparisonScales / buildTemporalCompareKinds 产生的位置索引与实际列位置不符。
						effectiveSrcCols := colData.SColumnInfo
						effectiveDstCols := colData.DColumnInfo
						if len(sp.columnPlanSourceCols) > 0 {
							effectiveSrcCols = columnsModeFilteredCols(colData.SColumnInfo, sp.columnPlanSourceCols, sp.columnName)
							effectiveDstCols = columnsModeFilteredCols(colData.DColumnInfo, sp.columnPlanTargetCols, sp.columnName)
						}

						// columns 模式下，PK 列不能参与归一化。
						// columnsModeExtractPKKey() 从 Arrcmp 返回的行（即归一化后的行）中提取 PK key；
						// 若 PK 列是 TIMESTAMP(6)/TIME(6)/FLOAT 等类型，归一化会截断精度，导致原本不同
						// 的两行映射到同一 PK key，进而错误地触发 two-sided UPDATE 或吞掉真实差异。
						// 修复方案：提前计算 PK 列在过滤后列列表中的位置，然后将这些位置从
						// float/temporal 归一化向量中屏蔽（float: -1，temporal: ""），确保 PK 值
						// 在 Arrcmp 前后保持原始精度，而仅对 compare 列做归一化。
						var earlyPKPositions []int
						if len(sp.columnPlanSourceCols) > 0 {
							earlyPKPositions, _ = columnsModeSplitPKAndCompare(effectiveSrcCols, sp.columnName)
						}

						// 归一化前保存目标端原始行快照：归一化仅用于比对，
						// DELETE WHERE 子句必须使用 MySQL 实际存储值，否则无法命中目标行。
						origCleanDestData := append([]string(nil), cleanDestData...)
						anyNormApplied := false

						floatCompareScales := buildFloatComparisonScales(effectiveSrcCols, effectiveDstCols)
						for _, pos := range earlyPKPositions {
							if pos < len(floatCompareScales) {
								floatCompareScales[pos] = -1 // 跳过 PK 列 float 归一化
							}
						}
						if len(floatCompareScales) > 0 {
							cleanSourceData = normalizeRowsForFloatComparison(cleanSourceData, floatCompareScales)
							cleanDestData = normalizeRowsForFloatComparison(cleanDestData, floatCompareScales)
							anyNormApplied = true
							global.Wlog.Debugf("(%d) Applied float normalization for %s.%s before Arrcmp", logThreadSeq, c1.Schema, c1.Table)
						}
						temporalCompareKinds := buildTemporalCompareKinds(effectiveSrcCols, effectiveDstCols)
						for _, pos := range earlyPKPositions {
							if pos < len(temporalCompareKinds) {
								temporalCompareKinds[pos] = "" // 跳过 PK 列时间归一化
							}
						}
						if len(temporalCompareKinds) > 0 {
							cleanSourceData = normalizeRowsForTemporalComparison(cleanSourceData, temporalCompareKinds)
							cleanDestData = normalizeRowsForTemporalComparison(cleanDestData, temporalCompareKinds)
							anyNormApplied = true
							global.Wlog.Debugf("(%d) Applied temporal normalization for %s.%s before Arrcmp", logThreadSeq, c1.Schema, c1.Table)
						}
						charTrimFlags := buildCharTrimFlags(effectiveSrcCols)
						if len(charTrimFlags) > 0 {
							cleanSourceData = normalizeRowsForCharComparison(cleanSourceData, charTrimFlags)
							global.Wlog.Debugf("(%d) Applied CHAR trailing-space normalization for %s.%s before Arrcmp", logThreadSeq, c1.Schema, c1.Table)
						}

						if len(cleanSourceData) == len(cleanDestData) &&
							hashRowsIgnoringOrder(cleanSourceData) == hashRowsIgnoringOrder(cleanDestData) {
							// Normalization made both row multisets equal, so this chunk requires no fix DML.
							global.Wlog.Debugf("(%d) Normalized row sets are equal for %s.%s, skip diff DML generation",
								logThreadSeq, c1.Schema, c1.Table)
							return
						}
						add, del := aa.Arrcmp(cleanSourceData, cleanDestData)
						if len(add) > 0 && len(del) > 0 && len(temporalCompareKinds) > 0 {
							var healed int
							add, del, healed = reconcileTemporalNullArtifacts(add, del, temporalCompareKinds, effectiveSrcCols, effectiveDstCols)
							if healed > 0 {
								global.Wlog.Warnf("(%d) Reconciled %d temporal null artifacts for %s.%s (INTERVAL/TIME scan compatibility)",
									logThreadSeq, healed, c1.Schema, c1.Table)
							}
						}
						// 将 del 中经归一化处理后的行替换回目标端原始存储值。
						// 归一化（float/temporal）使 Arrcmp 能正确识别语义等价行，但修复 SQL
						// 的 DELETE WHERE 条件必须使用 MySQL 实际存储值才能精确命中目标行。
						if anyNormApplied && len(del) > 0 {
							del = remapDelToOriginalDest(del, cleanDestData, origCleanDestData)
						}
						stt, dtt = "", ""

						// 5. 记录发现的差异数量
						vlog = fmt.Sprintf("CHUNK_AUDIT: source=%d dest=%d add=%d del=%d table=%s.%s where=%s",
							len(cleanSourceData), len(cleanDestData), len(add), len(del), c1.Schema, c1.Table, sourceSqlWhere)
						global.Wlog.Debug(vlog)

						// 添加调试信息：检查差异数量的合理性
						expectedAddCount := len(cleanSourceData) - len(cleanDestData)
						if len(cleanDestData) == 0 {
							global.Wlog.Debugf("DEBUG_DIFF_ANALYSIS_%d: Expected add count: %d (source=%d, dest=0), Actual add count: %d\n",
								logThreadSeq, len(cleanSourceData), len(cleanSourceData), len(add))
						} else {
							global.Wlog.Debugf("DEBUG_DIFF_ANALYSIS_%d: Expected add count: %d (source=%d, dest=%d), Actual add count: %d\n",
								logThreadSeq, expectedAddCount, len(cleanSourceData), len(cleanDestData), len(add))
						}

						if len(add) > expectedAddCount+10 {
							global.Wlog.Debugf("DEBUG_ADD_DATA_%d: addCount=%d expected=%d (sample suppressed)",
								logThreadSeq, len(add), expectedAddCount)
						}
						if len(cleanSourceData) == 1 && len(cleanDestData) == 1 && len(add) == 1 && len(del) == 1 {
							global.Wlog.Warnf("ROW_COMPARE_SAMPLE_%d table=%s.%s sourceRow=%q destRow=%q addRow=%q delRow=%q",
								logThreadSeq, c1.Schema, c1.Table, cleanSourceData[0], cleanDestData[0], add[0], del[0])
						}

						// 6. 比较记录数量差异的日志记录
						// Arrcmp已经完成了精确的集合差异计算，不再对add数组进行截断
						if len(del) == 1 && len(add) > 100 {
							vlog = fmt.Sprintf("(%d) Note: 1 record to delete and %d to add for %s.%s (this is expected for large data differences)", logThreadSeq, len(add), c1.Schema, c1.Table)
							global.Wlog.Debug(vlog)
						}
						if len(del) > 0 || len(add) > 0 {
							// 确保使用正确的源和目标schema
							sourceSchema := sp.sourceSchema
							destSchema := sp.destSchema
							if sourceSchema == "" {
								sourceSchema = c1.Schema
							}
							if destSchema == "" {
								destSchema = c1.Schema
							}

							// 添加对空IndexColumn的检查
							indexColumns := sp.columnName
							if len(indexColumns) == 0 {
								// 如果没有索引列，使用所有列作为条件
								indexColumns = make([]string, 0, len(colData.DColumnInfo))
								for _, colInfo := range colData.DColumnInfo {
									if colName, ok := colInfo["columnName"]; ok {
										indexColumns = append(indexColumns, colName)
									}
								}
							}

							// 处理源端和目标端SQL条件
							// 获取原始SQL条件
							originalSourceSqlWhere := c1.SqlWhere["src"]
							originalDestSqlWhere := c1.SqlWhere["dst"]

							// 处理源端SQL条件，确保使用源端schema
							sourceSqlWhere := originalSourceSqlWhere
							// 如果源端SQL条件中包含目标端schema，替换为源端schema
							if strings.Contains(sourceSqlWhere, fmt.Sprintf("`%s`", destSchema)) {
								sourceSqlWhere = strings.Replace(sourceSqlWhere,
									fmt.Sprintf("`%s`", destSchema),
									fmt.Sprintf("`%s`", sourceSchema), -1)
							}
							if strings.Contains(sourceSqlWhere, fmt.Sprintf("%s.", destSchema)) {
								sourceSqlWhere = strings.Replace(sourceSqlWhere,
									fmt.Sprintf("%s.", destSchema),
									fmt.Sprintf("%s.", sourceSchema), -1)
							}

							// 处理目标端SQL条件，确保使用目标端schema
							destSqlWhere := originalDestSqlWhere
							// 如果目标端SQL条件中包含源端schema，替换为目标端schema
							if strings.Contains(destSqlWhere, fmt.Sprintf("`%s`", sourceSchema)) {
								destSqlWhere = strings.Replace(destSqlWhere,
									fmt.Sprintf("`%s`", sourceSchema),
									fmt.Sprintf("`%s`", destSchema), -1)
							}
							if strings.Contains(destSqlWhere, fmt.Sprintf("%s.", sourceSchema)) {
								destSqlWhere = strings.Replace(destSqlWhere,
									fmt.Sprintf("%s.", sourceSchema),
									fmt.Sprintf("%s.", destSchema), -1)
							}

							// Log for debugging
							vlog = fmt.Sprintf("(%d) DataFixSql - Source SQL condition: %s", logThreadSeq, sourceSqlWhere)
							global.Wlog.Debug(vlog)
							vlog = fmt.Sprintf("(%d) DataFixSql - Target SQL condition: %s", logThreadSeq, destSqlWhere)
							global.Wlog.Debug(vlog)

							// 修复SQL生成时使用正确的schema映射
							dbf := dbExec.DataAbnormalFixStruct{
								Schema:                  destSchema,   // 目标schema
								SourceSchema:            sourceSchema, // 源端schema，用于处理数据库映射关系
								Table:                   destTable,    // 使用目标端表名
								ColData:                 colData.DColumnInfo,
								Sqlwhere:                destSqlWhere, // 使用处理后的目标端SQL条件
								DestDevice:              sp.ddrive,
								IndexColumn:             indexColumns,
								DatafixType:             sp.datafixType,
								CaseSensitiveObjectName: sp.caseSensitiveObjectName,
							}
							if strings.HasPrefix(c1.indexColumnType, "pri") {
								dbf.IndexType = "pri"
							} else if strings.HasPrefix(c1.indexColumnType, "uni") {
								dbf.IndexType = "uni"
							} else {
								dbf.IndexType = "mul"
							}

							// columns 模式：按行 key 状态路由（two-sided / source-only / target-only）
							if len(sp.columnPlanSourceCols) > 0 {
								// 构建过滤后列列表（PK ∪ compareColumns，保持原表列顺序）
								filteredSrcCols := columnsModeFilteredCols(colData.SColumnInfo, sp.columnPlanSourceCols, indexColumns)
								filteredDstCols := columnsModeFilteredCols(colData.DColumnInfo, sp.columnPlanTargetCols, indexColumns)

								// 找出 PK 列在过滤后列列表中的位置，并拆出 compare 列。
								// 这里必须大小写不敏感，否则会与 orderColumnsForCompare /
								// FixUpdateSqlExec 的归一化语义不一致，导致 two-sided 配对错误。
								pkPositions, compareColNames := columnsModeSplitPKAndCompare(filteredSrcCols, indexColumns)

								// 构建源→目标列名映射，用于生成 UPDATE SET 子句时使用目标端列名。
								// 键统一小写，与 FixUpdateSqlExec 中的查找逻辑保持大小写不敏感一致。
								var srcToDstCol map[string]string
								if !sp.columnPlanSimpleMode && len(sp.columnPlanSourceCols) > 0 {
									srcToDstCol = make(map[string]string, len(sp.columnPlanSourceCols))
									for i, src := range sp.columnPlanSourceCols {
										if i < len(sp.columnPlanTargetCols) {
											srcToDstCol[strings.ToLower(src)] = sp.columnPlanTargetCols[i]
										}
									}
								}

								// 按 PK key 索引 add / del 行
								addByPK := make(map[string]string, len(add))
								for _, row := range add {
									addByPK[columnsModeExtractPKKey(row, pkPositions)] = row
								}
								delByPK := make(map[string]string, len(del))
								for _, row := range del {
									delByPK[columnsModeExtractPKKey(row, pkPositions)] = row
								}

								// Two-sided 行：生成 UPDATE；source-only 行：仅通知
								var chunkSrcOnly, chunkDstOnly int
								for pkKey, srcRow := range addByPK {
									if _, exists := delByPK[pkKey]; exists {
										// two-sided：source 与 target 的 compare 列有差异 → UPDATE
										if sp.datafixType == "no" {
											// compare-only 模式：只记录差异，不生成修复 SQL（Oracle 等不支持 UPDATE 的目标端在此路径安全退出）
											lock.Lock()
											if sp.pods != nil {
												sp.pods.DIFFS = "yes"
											}
											lock.Unlock()
										} else {
											dbf.RowData = srcRow
											dbf.ColData = filteredSrcCols
											sqlstr, err := dbf.DataAbnormalFix().FixUpdateSqlExec(ddb, srcRow, compareColNames, srcToDstCol, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate UPDATE sql error (columns-mode).", c1.Schema, c1.Table), err)
											} else if sqlstr != "" {
												cc <- sqlstr
											}
										}
									} else {
										// source-only：target 中不存在此 key，在 columns 模式下不生成 INSERT，仅计数
										lock.Lock()
										if sp.pods != nil {
											sp.pods.DIFFS = "yes"
										}
										lock.Unlock()
										chunkSrcOnly++
										// 计数，用于后续生成 advisory 提示文件
										if sp.sourceOnlyAdvisory != nil {
											sp.sourceOnlyAdvisory.mu.Lock()
											sp.sourceOnlyAdvisory.sourceOnlyCount++
											sp.sourceOnlyAdvisory.mu.Unlock()
										}
									}
								}

								// Target-only 行：由 extraRowsSyncToSource 控制
								for pkKey, dstRow := range delByPK {
									if _, exists := addByPK[pkKey]; !exists {
										if sp.extraRowsSyncToSource == "ON" {
											dbf.RowData = dstRow
											dbf.ColData = filteredDstCols
											sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate DELETE sql error (columns-mode target-only).", c1.Schema, c1.Table), err)
											} else if sqlstr != "" {
												cc <- sqlstr
											}
										} else {
											// 不生成 DELETE，但仍标记为差异，确保 Diffs=yes
											lock.Lock()
											if sp.pods != nil {
												sp.pods.DIFFS = "yes"
											}
											lock.Unlock()
											chunkDstOnly++
											// 计数，用于后续生成 advisory 提示文件
											if sp.sourceOnlyAdvisory != nil {
												sp.sourceOnlyAdvisory.mu.Lock()
												sp.sourceOnlyAdvisory.targetOnlyCount++
												sp.sourceOnlyAdvisory.mu.Unlock()
											}
										}
									}
								}
								// 每个 chunk 汇总一次，避免大差异时产生大量逐行日志
								if chunkSrcOnly > 0 || chunkDstOnly > 0 {
									global.Wlog.Warn(fmt.Sprintf("(%d) [columns-mode] %s.%s chunk diff: %d source-only row(s) skipped (no INSERT generated), %d target-only row(s) skipped (set extraRowsSyncToSource=ON to generate DELETE)",
										logThreadSeq, c1.Schema, c1.Table, chunkSrcOnly, chunkDstOnly))
								}
								return // columns 模式路由完成，跳过常规 INSERT/DELETE 生成
							}

							// 关键修复：确保DELETE语句一定在INSERT语句之前生成
							// 先处理所有DELETE语句
							if len(del) > 0 {
								vlog = fmt.Sprintf("(%d) Generating DELETE statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								global.Wlog.Debugf("DEBUG_SQL_ORDER_%d: Processing %d DELETE statements first for %s.%s\n",
									logThreadSeq, len(del), c1.Schema, c1.Table)

								deleteSqlSize := sp.deleteSqlSize

								// 分组处理DELETE语句，每fixTrxNum条合并一次
								for batchStart := 0; batchStart < len(del); batchStart += sp.fixTrxNum {
									batchEnd := batchStart + sp.fixTrxNum
									if batchEnd > len(del) {
										batchEnd = len(del)
									}
									batchDel := del[batchStart:batchEnd]

									// 处理单字段主键和多字段联合主键的批量DELETE
									var primaryCols []string
									var isSinglePrimary bool
									var primaryCol string
									if len(dbf.IndexColumn) > 0 {
										primaryCols = dbf.IndexColumn // 获取所有主键列
										isSinglePrimary = len(primaryCols) == 1
										if isSinglePrimary {
											primaryCol = primaryCols[0] // 使用唯一的主键列
										}
									}

									// 对于MySQL，合并DELETE语句
									if sp.ddrive == "mysql" {
										// 只有当IndexType为pri或uni时，才使用主键合并逻辑
										if len(dbf.IndexColumn) > 0 && (dbf.IndexType == "pri" || dbf.IndexType == "uni") {

											// 收集所有DELETE语句的主键值，并进行去重
											var primaryValues []string
											processedPrimaryValues := make(map[string]struct{}) // 局部去重，避免同一批次内重复
											for _, i := range batchDel {
												dbf.RowData = i
												sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
												if err != nil {
													sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
													continue
												}

												// 提取WHERE条件中的值
												if strings.Contains(sqlstr, "WHERE") {
													wherePart := strings.Split(sqlstr, "WHERE")[1]
													wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))

													var primaryKey string
													var primaryValue string

													if isSinglePrimary {
														// 单字段主键：提取单个值
														key := fmt.Sprintf("`%s` = '", primaryCol)
														if strings.Contains(wherePart, key) {
															part := strings.Split(wherePart, key)[1]
															if strings.Contains(part, "'") {
																value := strings.Split(part, "'")[0]
																primaryValue = "'" + value + "'"
																primaryKey = fmt.Sprintf("%s.%s.%s:%s", c1.Schema, c1.Table, primaryCol, value)
															}
														}
													} else {
														// 多字段联合主键：提取所有主键值组合
														var valueList []string
														var keyList []string
														foundAllValues := true
														for _, col := range primaryCols {
															// 构建匹配模式：`col` = 'value'
															pattern := fmt.Sprintf("`%s` = '", col)
															index := strings.Index(wherePart, pattern)
															if index == -1 {
																foundAllValues = false
																break
															}
															// 提取值
															afterPattern := wherePart[index+len(pattern):]
															valueEnd := strings.Index(afterPattern, "'")
															if valueEnd == -1 {
																foundAllValues = false
																break
															}
															value := afterPattern[:valueEnd]
															valueList = append(valueList, "'"+value+"'")
															keyList = append(keyList, fmt.Sprintf("%s:%s", col, value))
															// 从剩余字符串中查找下一个主键条件
															wherePart = afterPattern[valueEnd+1:]
														}
														if foundAllValues {
															// 构建值组合字符串：('val1', 'val2', 'val3')
															primaryValue = "(" + strings.Join(valueList, ", ") + ")"
															// 构建唯一键：schema.table.col1:val1,col2:val2
															primaryKey = fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
														}
													}

													// 检查该主键值是否已经处理过（全局去重）
													if primaryKey != "" {
														exists := hasDeleteKey(primaryKey, useGlobalKeyDedupe)

														// 同时检查局部去重，避免同一批次内重复
														_, localExists := processedPrimaryValues[primaryKey]

														// 关键修复：检查该主键是否已经被INSERT过
														inserted := hasInsertKey(primaryKey, useGlobalKeyDedupe)

														// 如果该主键已经被INSERT过，或者已经被DELETE过，或者在本批次内重复，则跳过
														if !exists && !localExists && !inserted {
															// 添加到全局去重map
															markDeleteKeyIfAbsent(primaryKey, useGlobalKeyDedupe)

															// 添加到局部去重map
															processedPrimaryValues[primaryKey] = struct{}{}

															// 添加到主键值列表
															primaryValues = append(primaryValues, primaryValue)
														}
													}
												}
											}

											// 如果成功提取了多个值，根据长度限制生成合并的DELETE语句
											if len(primaryValues) > 0 {
												// 生成基础SQL部分
												var baseSql string
												if isSinglePrimary {
													// 单字段主键：WHERE `col` IN (
													// 使用目标schema而非源schema
													baseSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` IN (", sp.destSchema, destTable, primaryCol)
												} else {
													// 多字段联合主键：WHERE (`col1`, `col2`, `col3`) IN (
													colNames := make([]string, len(primaryCols))
													for i, col := range primaryCols {
														colNames[i] = fmt.Sprintf("`%s`", col)
													}
													// 使用目标schema和目标表名
													baseSql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE (%s) IN (", sp.destSchema, destTable, strings.Join(colNames, ", "))
												}
												baseSqlLen := len(baseSql)
												closeBracketLen := len(");")

												// 根据长度限制合并值
												var currentValues []string
												currentLength := baseSqlLen

												for i, value := range primaryValues {
													valueLen := len(value)
													separatorLen := 0
													if i > 0 {
														separatorLen = 2 // 逗号和空格的长度 ", "
													}

													// 检查添加当前值是否会超过长度限制
													if currentLength+separatorLen+valueLen+closeBracketLen > deleteSqlSize {
														// 如果当前已经有值，先生成并发送当前的合并SQL
														if len(currentValues) > 0 {
															mergedSql := fmt.Sprintf("%s%s);", baseSql, strings.Join(currentValues, ", "))
															cc <- mergedSql
															// 重置当前值列表和长度
															currentValues = []string{value}
															currentLength = baseSqlLen + valueLen
														} else {
															// 如果单个值就超过限制，单独处理这条记录
															// 查找对应的原始记录并单独执行
															dbf.RowData = batchDel[i]
															sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
															if err != nil {
																sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
															}
															if sqlstr != "" {
																cc <- sqlstr
															}
														}
													} else {
														// 添加当前值到合并列表
														currentValues = append(currentValues, value)
														currentLength += separatorLen + valueLen
													}
												}

												// 处理剩余的值
												if len(currentValues) > 0 {
													mergedSql := fmt.Sprintf("%s%s);", baseSql, strings.Join(currentValues, ", "))
													cc <- mergedSql
												}
											} else {
												// 如果无法合并，回退到单独执行
												for _, i := range batchDel {
													dbf.RowData = i
													sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
													if err != nil {
														sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
														continue
													}

													// 提取WHERE条件中的主键值，用于去重
													var primaryKey string
													if strings.Contains(sqlstr, "WHERE") {
														wherePart := strings.Split(sqlstr, "WHERE")[1]
														wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))

														if isSinglePrimary {
															key := fmt.Sprintf("`%s` = '", primaryCol)
															if strings.Contains(wherePart, key) {
																part := strings.Split(wherePart, key)[1]
																if strings.Contains(part, "'") {
																	value := strings.Split(part, "'")[0]
																	primaryKey = fmt.Sprintf("%s.%s.%s:%s", c1.Schema, c1.Table, primaryCol, value)
																}
															}
														} else {
															// 多字段联合主键：提取所有主键值组合
															var keyList []string
															foundAllValues := true
															for _, col := range primaryCols {
																pattern := fmt.Sprintf("`%s` = '", col)
																index := strings.Index(wherePart, pattern)
																if index == -1 {
																	foundAllValues = false
																	break
																}
																afterPattern := wherePart[index+len(pattern):]
																valueEnd := strings.Index(afterPattern, "'")
																if valueEnd == -1 {
																	foundAllValues = false
																	break
																}
																value := afterPattern[:valueEnd]
																keyList = append(keyList, fmt.Sprintf("%s:%s", col, value))
																wherePart = afterPattern[valueEnd+1:]
															}
															if foundAllValues {
																primaryKey = fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
															}
														}
													}

													// 检查该主键值是否已经处理过
													if primaryKey != "" {
														if markDeleteKeyIfAbsent(primaryKey, useGlobalKeyDedupe) {
															// 发送SQL语句
															if sqlstr != "" {
																cc <- sqlstr
															}
														}
													} else {
														// 如果无法提取主键值，直接发送SQL语句
														if sqlstr != "" {
															cc <- sqlstr
														}
													}
												}
											}
										} else {
											// 对于无主键或普通索引（mul），统计相同记录的数量，生成带正确LIMIT的DELETE语句
											rowCountMap := make(map[string]int)
											for _, i := range batchDel {
												rowCountMap[i]++
											}

											for rowData, count := range rowCountMap {
												dbf.RowData = rowData
												sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
												if err != nil {
													sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
													continue
												}

												// 修改SQL语句，将LIMIT 1改为LIMIT count
												if strings.Contains(sqlstr, "LIMIT 1") {
													sqlstr = strings.Replace(sqlstr, "LIMIT 1", fmt.Sprintf("LIMIT %d", count), 1)
												}

												// 使用修改后的SQL作为去重键
												if markDeleteKeyIfAbsent(sqlstr, useGlobalKeyDedupe) {
													// 发送SQL语句
													if sqlstr != "" {
														cc <- sqlstr
													}
												}
											}
										}
									} else {
										// 对于非MySQL数据库，暂时保持单独执行
										for _, i := range batchDel {
											dbf.RowData = i
											sqlstr, err := dbf.DataAbnormalFix().FixDeleteSqlExec(ddb, sp.ddrive, logThreadSeq)
											if err != nil {
												sp.getErr(fmt.Sprintf("\ndest: checksum table %s.%s generate DELETE sql error.", c1.Schema, c1.Table), err)
												continue
											}

											// 提取WHERE条件中的主键值，用于去重
											var primaryKey string
											if strings.Contains(sqlstr, "WHERE") {
												wherePart := strings.Split(sqlstr, "WHERE")[1]
												wherePart = strings.TrimSpace(strings.TrimSuffix(wherePart, ";"))

												if isSinglePrimary {
													key := fmt.Sprintf("`%s` = '", primaryCol)
													if strings.Contains(wherePart, key) {
														part := strings.Split(wherePart, key)[1]
														if strings.Contains(part, "'") {
															value := strings.Split(part, "'")[0]
															primaryKey = fmt.Sprintf("%s.%s.%s:%s", c1.Schema, c1.Table, primaryCol, value)
														}
													}
												} else {
													// 多字段联合主键：提取所有主键值组合
													var keyList []string
													foundAllValues := true
													for _, col := range primaryCols {
														pattern := fmt.Sprintf("`%s` = '", col)
														index := strings.Index(wherePart, pattern)
														if index == -1 {
															foundAllValues = false
															break
														}
														afterPattern := wherePart[index+len(pattern):]
														valueEnd := strings.Index(afterPattern, "'")
														if valueEnd == -1 {
															foundAllValues = false
															break
														}
														value := afterPattern[:valueEnd]
														keyList = append(keyList, fmt.Sprintf("%s:%s", col, value))
														wherePart = afterPattern[valueEnd+1:]
													}
													if foundAllValues {
														primaryKey = fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
													}
												}
											}

											// 检查该主键值是否已经处理过
											if primaryKey != "" {
												exists := hasDeleteKey(primaryKey, useGlobalKeyDedupe)

												// 关键修复：检查该主键是否已经被INSERT过
												inserted := hasInsertKey(primaryKey, useGlobalKeyDedupe)

												if !exists && !inserted {
													// 添加到全局去重map
													markDeleteKeyIfAbsent(primaryKey, useGlobalKeyDedupe)

													// 发送SQL语句
													if sqlstr != "" {
														cc <- sqlstr
													}
												}
											} else {
												// 对于无法提取主键值的情况，使用完整SQL作为去重键
												if markDeleteKeyIfAbsent(sqlstr, useGlobalKeyDedupe) {
													// 发送SQL语句
													if sqlstr != "" {
														cc <- sqlstr
													}
												}
											}
										}
									}
								}
								vlog = fmt.Sprintf("(%d) DELETE statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
							// DELETE语句处理完成后，再处理INSERT语句
							if len(add) > 0 {
								vlog = fmt.Sprintf("(%d) Generating INSERT statements for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
								global.Wlog.Debugf("DEBUG_SQL_ORDER_%d: Processing %d INSERT statements after DELETE for %s.%s\n",
									logThreadSeq, len(add), c1.Schema, c1.Table)

								// 分组处理INSERT语句，每fixTrxNum条合并一次
								for batchStart := 0; batchStart < len(add); batchStart += sp.fixTrxNum {
									batchEnd := batchStart + sp.fixTrxNum
									if batchEnd > len(add) {
										batchEnd = len(add)
									}
									batchAdd := add[batchStart:batchEnd]

									// INSERT去重已由insertedPrimaryKeys机制保证，不再限制batchAdd大小

									// 生成单独的INSERT语句，避免多线程并发下的重复冲突
									global.Wlog.Debugf("DEBUG_INSERT_LOOP_%d: Starting INSERT generation for %d records in batch for %s.%s\n",
										logThreadSeq, len(batchAdd), c1.Schema, c1.Table)

									insertCount := 0
									duplicateCount := 0
									for batchIndex, i := range batchAdd {
										dbf.RowData = i
										sqlstr, err := dbf.DataAbnormalFix().FixInsertSqlExec(ddb, sp.ddrive, logThreadSeq)
										if err != nil {
											sp.getErr(fmt.Sprintf("dest: checksum table %s.%s generate INSERT sql error.", c1.Schema, c1.Table), err)
										} else if sqlstr != "" {
											// 关键修复：进行INSERT去重检查，防止跨chunk重复生成INSERT语句
											// 使用RowData（以/*go actions columnData*/分隔）提取主键值进行去重
											isDuplicate := false
											if len(dbf.IndexColumn) > 0 {
												rowParts := strings.Split(dbf.RowData, "/*go actions columnData*/")

												// 关键修复：构建列名到位置的映射，因为RowData的列顺序可能与IndexColumn不同
												// RowData的列顺序由SELECT语句决定（通常来自ColData/SColumnInfo），
												// 而不是主键列的顺序。直接用rowParts[idx]会取到错误的列值
												colNameToIdx := make(map[string]int)
												for ci, colInfo := range dbf.ColData {
													if name, ok := colInfo["columnName"]; ok {
														colNameToIdx[name] = ci
													}
												}

												if len(rowParts) >= len(dbf.ColData) {
													var keyList []string
													allFound := true
													for _, col := range dbf.IndexColumn {
														if colIdx, ok := colNameToIdx[col]; ok && colIdx < len(rowParts) {
															keyList = append(keyList, fmt.Sprintf("%s:%s", col, rowParts[colIdx]))
														} else {
															// 如果找不到列位置，跳过去重检查
															allFound = false
															break
														}
													}
													if allFound {
														// 关键修复：如果主键列中包含NULL值，跳过去重检查
														// 在MySQL中 NULL != NULL，UNIQUE KEY允许多个NULL值
														hasNullKey := false
														for _, kv := range keyList {
															kvParts := strings.SplitN(kv, ":", 2)
															if len(kvParts) == 2 {
																val := strings.TrimSpace(kvParts[1])
																if val == "" || val == dataDispos.ValueNullPlaceholder || strings.EqualFold(val, "NULL") {
																	hasNullKey = true
																	break
																}
															}
														}
														// NULL行不参与去重(MySQL中NULL!=NULL)，仅对非NULL行进行去重
														if !hasNullKey {
															primaryKey := fmt.Sprintf("%s.%s.%s", c1.Schema, c1.Table, strings.Join(keyList, ","))
															alreadyInserted := !markInsertKeyIfAbsent(primaryKey, useGlobalKeyDedupe)
															if alreadyInserted {
																isDuplicate = true
															}
														} // end if !hasNullKey
													}
												}
											}
											if isDuplicate {
												duplicateCount++
												continue
											}
											insertCount++
											// 记录生成的SQL语句
											vlog = fmt.Sprintf("(%d) Generated INSERT statement for %s.%s", logThreadSeq, c1.Schema, c1.Table)
											global.Wlog.Debug(vlog)

											// 如果是前几条记录，输出调试信息
											if insertCount <= 5 {
												sqlPreview := sqlstr
												if len(sqlstr) > 50 {
													sqlPreview = sqlstr[:50] + "..."
												}
												global.Wlog.Debugf("DEBUG_INSERT_DETAIL_%d: Batch[%d] - Insert count %d - SQL starts with: %s\n",
													logThreadSeq, batchIndex, insertCount, sqlPreview)
											}

											cc <- sqlstr
											totalInsertCount++
										}
									}

									if duplicateCount > 0 {
										global.Wlog.Debugf("DEBUG_INSERT_LOOP_%d: Generated %d INSERT statements, skipped %d duplicates for batch with %d records in %s.%s (Total so far: %d)\n",
											logThreadSeq, insertCount, duplicateCount, len(batchAdd), c1.Schema, c1.Table, totalInsertCount)
									} else {
										global.Wlog.Debugf("DEBUG_INSERT_LOOP_%d: Generated %d INSERT statements for batch with %d records in %s.%s (Total so far: %d)\n",
											logThreadSeq, insertCount, len(batchAdd), c1.Schema, c1.Table, totalInsertCount)
									}
								}
								vlog = fmt.Sprintf("(%d) INSERT statements generated for %s.%s", logThreadSeq, c1.Schema, c1.Table)
								global.Wlog.Debug(vlog)
							}
						}
					}
				}(c, sdb, ddb)
			}
		}
	}
}

func (sp *SchedulePlan) DataFixDispos(fixSQL chanString, logThreadSeq int64) {
	var (
		vlog        string
		deleteCount int
		insertCount int
	)

	// 修复：清空全局writtenSqlMap，确保只针对当前表去重，避免跨表影响
	writtenSqlMap = sync.Map{}

	vlog = fmt.Sprintf("(%d) Applying repair statements to target table %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	logStageMemory("fixsql-write-start", logThreadSeq, sp.schema, sp.table)

	maxFileSizeBytes := int64(sp.fixTrxSize) * 1024 * 1024
	if maxFileSizeBytes <= 0 {
		maxFileSizeBytes = 4 * 1024 * 1024
	}
	maxStmtPerFile := sp.fixTrxNum
	if maxStmtPerFile <= 0 {
		maxStmtPerFile = 1000
	}
	stageBatchStmt := maxStmtPerFile
	stageBatchBytes := maxFileSizeBytes
	// Keep streaming batches bounded, but allow a larger upper cap to reduce tiny-batch CPU overhead.
	if stageBatchBytes > 32*1024*1024 {
		stageBatchBytes = 32 * 1024 * 1024
	}

	isUniqueKey := strings.HasPrefix(sp.indexColumnType, "pri_") || strings.HasPrefix(sp.indexColumnType, "uni_")
	var (
		deleteWriter *sqlRollingWriter
		insertWriter *sqlRollingWriter
		updateWriter *sqlRollingWriter
	)
	if sp.datafixType != "table" {
		deleteWriter = sp.newSQLRollingWriter("DELETE", maxStmtPerFile, maxFileSizeBytes, logThreadSeq)
		insertWriter = sp.newSQLRollingWriter("INSERT", maxStmtPerFile, maxFileSizeBytes, logThreadSeq)
		updateWriter = sp.newSQLRollingWriter("UPDATE", maxStmtPerFile, maxFileSizeBytes, logThreadSeq)
	}
	if deleteWriter != nil {
		defer deleteWriter.close()
	}
	if insertWriter != nil {
		defer insertWriter.close()
	}
	if updateWriter != nil {
		defer updateWriter.close()
	}

	processDeleteBatch := func(batch []string) error {
		optimized := optimizeSqlStatements(batch, sp.fixTrxNum, isUniqueKey, sp.deleteSqlSize, sp.insertSqlSize)
		if len(optimized) == 0 {
			return nil
		}
		if sp.datafixType == "table" {
			writeOptimizedSqlChunk(optimized, sp.datafixType, nil, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			return nil
		}
		return deleteWriter.write(optimized)
	}
	processInsertBatch := func(batch []string) error {
		optimized := optimizeSqlStatements(batch, sp.fixTrxNum, false, sp.deleteSqlSize, sp.insertSqlSize)
		if len(optimized) == 0 {
			return nil
		}
		if sp.datafixType == "table" {
			writeOptimizedSqlChunk(optimized, sp.datafixType, nil, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			return nil
		}
		return insertWriter.write(optimized)
	}
	processUpdateBatch := func(batch []string) error {
		if len(batch) == 0 {
			return nil
		}
		if sp.datafixType == "table" {
			writeOptimizedSqlChunk(batch, sp.datafixType, nil, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			return nil
		}
		return updateWriter.write(batch)
	}

	global.Wlog.Info(fmt.Sprintf("(%d) Writing per-object fixsql for %s.%s",
		logThreadSeq, sp.schema, sp.table))
	deleteBatch := make([]string, 0, stageBatchStmt)
	insertBatch := make([]string, 0, stageBatchStmt)
	updateBatch := make([]string, 0, stageBatchStmt)
	var deleteBatchBytes int64
	var insertBatchBytes int64
	var updateBatchBytes int64
	var updateCount int

	flushDelete := func() {
		if len(deleteBatch) == 0 {
			return
		}
		if err := processDeleteBatch(deleteBatch); err != nil {
			sp.getErr(fmt.Sprintf("Failed streaming DELETE fixsql for %s.%s", sp.schema, sp.table), err)
		}
		deleteBatch = deleteBatch[:0]
		deleteBatchBytes = 0
	}
	flushInsert := func() {
		if len(insertBatch) == 0 {
			return
		}
		if err := processInsertBatch(insertBatch); err != nil {
			sp.getErr(fmt.Sprintf("Failed streaming INSERT fixsql for %s.%s", sp.schema, sp.table), err)
		}
		insertBatch = insertBatch[:0]
		insertBatchBytes = 0
	}
	flushUpdate := func() {
		if len(updateBatch) == 0 {
			return
		}
		if err := processUpdateBatch(updateBatch); err != nil {
			sp.getErr(fmt.Sprintf("Failed streaming UPDATE fixsql for %s.%s", sp.schema, sp.table), err)
		}
		updateBatch = updateBatch[:0]
		updateBatchBytes = 0
	}

	for v := range fixSQL {
		sqlType := detectFixSQLType(v)
		if sqlType == "" {
			continue
		}
		sp.pods.DIFFS = "yes"
		sqlBytes := int64(len(v) + 1)
		switch sqlType {
		case "DELETE":
			if len(deleteBatch) > 0 && (len(deleteBatch) >= stageBatchStmt || deleteBatchBytes+sqlBytes > stageBatchBytes) {
				flushDelete()
			}
			deleteBatch = append(deleteBatch, v)
			deleteBatchBytes += sqlBytes
			deleteCount++
		case "INSERT":
			if len(insertBatch) > 0 && (len(insertBatch) >= stageBatchStmt || insertBatchBytes+sqlBytes > stageBatchBytes) {
				flushInsert()
			}
			insertBatch = append(insertBatch, v)
			insertBatchBytes += sqlBytes
			insertCount++
		case "UPDATE":
			if len(updateBatch) > 0 && (len(updateBatch) >= stageBatchStmt || updateBatchBytes+sqlBytes > stageBatchBytes) {
				flushUpdate()
			}
			updateBatch = append(updateBatch, v)
			updateBatchBytes += sqlBytes
			updateCount++
		}
	}
	flushDelete()
	flushInsert()
	flushUpdate()

	if deleteCount > 0 || insertCount > 0 || updateCount > 0 {
		vlog = fmt.Sprintf("(%d) Repair statements generated for %s.%s: DELETE=%d, INSERT=%d, UPDATE=%d",
			logThreadSeq, sp.schema, sp.table, deleteCount, insertCount, updateCount)
		global.Wlog.Debug(vlog)
		sp.pods.DIFFS = "yes"
	}

	// columns 模式：如有未自动修复的差异行，生成 advisory 提示文件
	// 注意：source-only 行在 columns 模式下始终不生成 INSERT（全列未知），
	// 因此无论 datafixType 是否为 "table"，只要存在未自动修复的差异就需要 advisory。
	if sp.sourceOnlyAdvisory != nil {
		sp.sourceOnlyAdvisory.mu.Lock()
		srcOnly := sp.sourceOnlyAdvisory.sourceOnlyCount
		dstOnly := sp.sourceOnlyAdvisory.targetOnlyCount
		sp.sourceOnlyAdvisory.mu.Unlock()
		if srcOnly > 0 || dstOnly > 0 {
			sp.writeColumnsModeAdvisory(srcOnly, dstOnly, logThreadSeq)
		}
	}

	// 无论是否有差异，都添加到结果中
	logStageMemory("fixsql-write-end", logThreadSeq, sp.schema, sp.table)
	measuredDataPods = append(measuredDataPods, *sp.pods)
}

func detectFixSQLType(sql string) string {
	sqlTrim := strings.TrimSpace(strings.ToUpper(sql))
	if strings.HasPrefix(sqlTrim, "DELETE") {
		return "DELETE"
	}
	if strings.HasPrefix(sqlTrim, "INSERT") {
		return "INSERT"
	}
	if strings.HasPrefix(sqlTrim, "UPDATE") {
		return "UPDATE"
	}
	return ""
}

// writeColumnsModeAdvisory 在 columns 模式下，当存在未自动生成修复 SQL 的差异行时，
// 写入一个纯注释的 advisory 文件，提示人工介入。涵盖两种情形：
//   - source-only：源端有目标端无，columns 模式不生成 INSERT（全列未知）
//   - target-only：目标端有源端无，extraRowsSyncToSource=OFF，不生成 DELETE
//
// 文件命名：<fixFileDir>/columns-advisory.<schema>.<table>.sql
// 文件内容全部为 SQL 注释（-- 开头），不含任何可执行语句，不会被误执行。
func (sp *SchedulePlan) writeColumnsModeAdvisory(sourceOnlyCount, targetOnlyCount int, logThreadSeq int64) {
	adv := sp.sourceOnlyAdvisory
	if adv == nil || (sourceOnlyCount == 0 && targetOnlyCount == 0) {
		return
	}
	const advisoryDrive = "mysql"

	// sp.datafixSql 仅在 datafix=file 时才会被初始化；datafix=table/yes 时为空字符串。
	// advisory 文件使用独立的目录：优先复用 datafixSql，否则回退到默认目录 "fixsql"。
	advisoryDir := sp.datafixSql
	if advisoryDir == "" {
		advisoryDir = "fixsql"
		// 记录回退时的实际绝对路径，避免因进程工作目录不同导致文件落盘位置不明确。
		if absDir, err := filepath.Abs(advisoryDir); err == nil {
			global.Wlog.Info(fmt.Sprintf("(%d) [columns-advisory] datafix dir not set, using default advisory dir: %s", logThreadSeq, absDir))
		}
	}
	if err := os.MkdirAll(advisoryDir, 0755); err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) [columns-advisory] Failed to create advisory directory %q: %v", logThreadSeq, advisoryDir, err))
		return
	}

	filePath := fmt.Sprintf("%s/columns-advisory.%s.%s.sql",
		advisoryDir, fixFileNameEncode(adv.schema), fixFileNameEncode(adv.table))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) [columns-advisory] Failed to create advisory file %q: %v", logThreadSeq, filePath, err))
		return
	}
	defer f.Close()

	quotedSourceCols := make([]string, len(sp.columnPlanSourceCols))
	for i, c := range sp.columnPlanSourceCols {
		quotedSourceCols[i] = quoteIdentifierByDrive(c, advisoryDrive)
	}
	cols := strings.Join(quotedSourceCols, ", ")

	quotedIndexCols := make([]string, len(adv.indexCols))
	for i, c := range adv.indexCols {
		quotedIndexCols[i] = quoteIdentifierByDrive(c, advisoryDrive)
	}
	pkCols := strings.Join(quotedIndexCols, ", ")
	firstPKCol := adv.indexCols[0]
	firstPKRef := quoteIdentifierByDrive(firstPKCol, advisoryDrive)

	// 表映射场景下目标表名可能与源表名不同
	destTableName := adv.destTable
	if destTableName == "" {
		destTableName = adv.table
	}
	destSchemaName := adv.destSchema
	if destSchemaName == "" {
		destSchemaName = adv.schema
	}
	sourceRef := qualifiedTableByDrive(adv.schema, adv.table, advisoryDrive)
	targetRef := qualifiedTableByDrive(destSchemaName, destTableName, advisoryDrive)

	var sb strings.Builder
	sb.WriteString("-- ===========================================================================\n")
	sb.WriteString("-- [MANUAL ACTION REQUIRED] gt-checksum columns-mode advisory\n")
	sb.WriteString("-- ===========================================================================\n")
	sb.WriteString("--\n")
	sb.WriteString(fmt.Sprintf("-- Source table       : %s\n", sourceRef))
	sb.WriteString(fmt.Sprintf("-- Target table       : %s\n", targetRef))
	sb.WriteString(fmt.Sprintf("-- Columns checked    : %s\n", cols))
	sb.WriteString(fmt.Sprintf("-- Primary/unique key : %s\n", pkCols))
	sb.WriteString("--\n")

	if sourceOnlyCount > 0 {
		sb.WriteString(fmt.Sprintf("-- Source-only rows (exist in source, absent in target) : %d\n", sourceOnlyCount))
		sb.WriteString("--   In columns mode, INSERT statements are NOT auto-generated because\n")
		sb.WriteString("--   only partial columns were compared and the full row content is unknown.\n")
		sb.WriteString("--   Query to locate these rows:\n")
		sb.WriteString(fmt.Sprintf("--     SELECT src.* FROM %s src\n", sourceRef))
		sb.WriteString(fmt.Sprintf("--     LEFT JOIN %s dst USING (%s)\n", targetRef, pkCols))
		sb.WriteString(fmt.Sprintf("--     WHERE dst.%s IS NULL;\n", firstPKRef))
		sb.WriteString("--\n")
	}

	if targetOnlyCount > 0 {
		sb.WriteString(fmt.Sprintf("-- Target-only rows (exist in target, absent in source) : %d\n", targetOnlyCount))
		sb.WriteString("--   extraRowsSyncToSource=OFF, DELETE statements are NOT auto-generated.\n")
		sb.WriteString("--   Set extraRowsSyncToSource=ON to auto-generate DELETE fix SQL, or\n")
		sb.WriteString("--   manually review and delete these rows if appropriate.\n")
		sb.WriteString("--   Query to locate these rows:\n")
		sb.WriteString(fmt.Sprintf("--     SELECT dst.* FROM %s dst\n", targetRef))
		sb.WriteString(fmt.Sprintf("--     LEFT JOIN %s src USING (%s)\n", sourceRef, pkCols))
		sb.WriteString(fmt.Sprintf("--     WHERE src.%s IS NULL;\n", firstPKRef))
		sb.WriteString("--\n")
	}

	sb.WriteString("-- RECOMMENDATION:\n")
	sb.WriteString("--   Review the rows above, then manually INSERT / DELETE as needed.\n")
	sb.WriteString("--   After remediation, re-run gt-checksum to verify consistency.\n")
	sb.WriteString("--\n")
	sb.WriteString("-- ===========================================================================\n")

	if _, err := f.WriteString(sb.String()); err != nil {
		global.Wlog.Error(fmt.Sprintf("(%d) [columns-advisory] Failed to write advisory file %q: %v", logThreadSeq, filePath, err))
		return
	}
	global.Wlog.Info(fmt.Sprintf("(%d) [columns-advisory] Written advisory file: %s (source-only=%d, target-only=%d)",
		logThreadSeq, filePath, sourceOnlyCount, targetOnlyCount))
}

type sqlRollingWriter struct {
	datafixType string
	ddrive      string
	djdbc       string
	logThread   int64
	fixTrxNum   int

	maxStmt  int
	maxBytes int64

	pathFunc func(fileSeq int) (string, bool)

	fileSeq    int
	current    *os.File
	currentCnt int
	currentB   int64
}

func (w *sqlRollingWriter) ensureFile() error {
	if w.current != nil {
		return nil
	}
	w.fileSeq++
	path, _ := w.pathFunc(w.fileSeq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.current = f
	return nil
}

func (w *sqlRollingWriter) rotate() error {
	if w.current != nil {
		if err := w.current.Close(); err != nil {
			return err
		}
	}
	w.current = nil
	w.currentCnt = 0
	w.currentB = 0
	return nil
}

func (w *sqlRollingWriter) close() error {
	return w.rotate()
}

func (w *sqlRollingWriter) writableSQLCount(sqls []string) int {
	if len(sqls) == 0 {
		return 0
	}
	limit := len(sqls)
	if w.maxStmt > 0 {
		remainStmt := w.maxStmt - w.currentCnt
		if remainStmt <= 0 {
			return 0
		}
		if remainStmt < limit {
			limit = remainStmt
		}
	}
	if w.maxBytes > 0 {
		remainBytes := w.maxBytes - w.currentB
		if remainBytes <= 0 {
			return 0
		}
		var (
			sum int64
			cnt int
		)
		for ; cnt < len(sqls) && cnt < limit; cnt++ {
			sz := int64(len(sqls[cnt]) + 1)
			if cnt > 0 && sum+sz > remainBytes {
				break
			}
			sum += sz
			if cnt == 0 && sz > remainBytes {
				// 单条SQL超过文件阈值时，仍允许写入，避免卡死
				cnt = 1
				break
			}
		}
		if cnt < limit {
			limit = cnt
		}
	}
	return limit
}

func (w *sqlRollingWriter) write(sqls []string) error {
	for len(sqls) > 0 {
		if err := w.ensureFile(); err != nil {
			return err
		}
		n := w.writableSQLCount(sqls)
		if n <= 0 {
			if err := w.rotate(); err != nil {
				return err
			}
			continue
		}
		part := sqls[:n]
		writeOptimizedSqlChunk(part, w.datafixType, w.current, w.ddrive, w.djdbc, w.logThread, w.fixTrxNum)
		w.currentCnt += len(part)
		w.currentB += estimateSqlBytes(part)
		sqls = sqls[n:]

		if (w.maxStmt > 0 && w.currentCnt >= w.maxStmt) || (w.maxBytes > 0 && w.currentB >= w.maxBytes) {
			if err := w.rotate(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sp *SchedulePlan) newSQLRollingWriter(sqlType string, maxStmtPerFile int, maxFileSizeBytes int64, logThreadSeq int64) *sqlRollingWriter {
	pathFunc := func(fileSeq int) (string, bool) {
		fixSchema := sp.destSchema
		if fixSchema == "" {
			fixSchema = sp.schema
		}
		fixTable := sp.getDestTableName()
		if sqlType == "DELETE" {
			return fmt.Sprintf("%s/table.%s.%s-DELETE-%d.sql",
				sp.datafixSql, fixFileNameEncode(fixSchema), fixFileNameEncode(fixTable), fileSeq), false
		}
		return fmt.Sprintf("%s/table.%s.%s-%d.sql",
			sp.datafixSql, fixFileNameEncode(fixSchema), fixFileNameEncode(fixTable), fileSeq), false
	}
	return &sqlRollingWriter{
		datafixType: sp.datafixType,
		ddrive:      sp.ddrive,
		djdbc:       sp.djdbc,
		logThread:   logThreadSeq,
		fixTrxNum:   sp.fixTrxNum,
		maxStmt:     maxStmtPerFile,
		maxBytes:    maxFileSizeBytes,
		pathFunc:    pathFunc,
	}
}

func processSQLStageFile(stagePath string, maxStmt int, maxBytes int64, handler func([]string) error) error {
	file, err := os.Open(stagePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if maxStmt <= 0 {
		maxStmt = 1000
	}
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024
	}

	reader := bufio.NewReaderSize(file, 4*1024*1024)

	var (
		batch      []string
		batchBytes int64
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := handler(batch); err != nil {
			return err
		}
		batch = nil
		batchBytes = 0
		return nil
	}

	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		sqlLine := strings.TrimSpace(line)
		if sqlLine == "" {
			if readErr == io.EOF {
				break
			}
			continue
		}
		sqlBytes := int64(len(sqlLine) + 1)
		if len(batch) > 0 && (len(batch) >= maxStmt || batchBytes+sqlBytes > maxBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, sqlLine)
		batchBytes += sqlBytes
		if readErr == io.EOF {
			break
		}
	}
	return flush()
}

func estimateSqlBytes(sqls []string) int64 {
	var total int64
	for _, sql := range sqls {
		total += int64(len(sql) + 1)
	}
	return total
}

func fitSqlChunk(sqls []string, maxStmtPerFile int, maxFileSizeBytes int64) bool {
	if len(sqls) == 0 {
		return true
	}
	if maxStmtPerFile > 0 && len(sqls) > maxStmtPerFile {
		return false
	}
	if maxFileSizeBytes > 0 && estimateSqlBytes(sqls) > maxFileSizeBytes {
		return false
	}
	return true
}

func splitSqlByConstraints(sqls []string, maxStmtPerFile int, maxFileSizeBytes int64) [][]string {
	if len(sqls) == 0 {
		return nil
	}
	if maxStmtPerFile <= 0 {
		maxStmtPerFile = len(sqls)
	}
	if maxFileSizeBytes <= 0 {
		maxFileSizeBytes = 4 * 1024 * 1024
	}

	var (
		result    [][]string
		current   []string
		currBytes int64
	)
	for _, sql := range sqls {
		sqlBytes := int64(len(sql) + 1)
		if len(current) > 0 && (len(current) >= maxStmtPerFile || currBytes+sqlBytes > maxFileSizeBytes) {
			result = append(result, current)
			current = nil
			currBytes = 0
		}
		current = append(current, sql)
		currBytes += sqlBytes
		if len(current) >= maxStmtPerFile || currBytes >= maxFileSizeBytes {
			result = append(result, current)
			current = nil
			currBytes = 0
		}
	}
	if len(current) > 0 {
		result = append(result, current)
	}
	return result
}

func optimizeSqlStatements(sqls []string, fixTrxNum int, isUniqueKey bool, deleteSqlSize int, insertSqlSize int) []string {
	if len(sqls) == 0 {
		return nil
	}
	var deleteSqls []string
	var insertSqls []string
	for _, sql := range sqls {
		sqlTrim := strings.TrimSpace(strings.ToUpper(sql))
		if strings.HasPrefix(sqlTrim, "DELETE") {
			deleteSqls = append(deleteSqls, sql)
		} else if strings.HasPrefix(sqlTrim, "INSERT") {
			insertSqls = append(insertSqls, sql)
		}
	}

	optFixTrxNum := fixTrxNum
	if optFixTrxNum <= 0 {
		optFixTrxNum = 1000
	}
	if isUniqueKey && len(deleteSqls) > 0 {
		deleteSqls = OptimizeDeleteSqls(deleteSqls, deleteSqlSize, optFixTrxNum)
	}
	if len(insertSqls) > 1 {
		insertSqls = OptimizeInsertSqls(insertSqls, insertSqlSize, optFixTrxNum)
	}

	var finalSqls []string
	finalSqls = append(finalSqls, deleteSqls...)
	finalSqls = append(finalSqls, insertSqls...)
	return finalSqls
}

func writeOptimizedSqlChunk(sqls []string, datafixType string, sfile *os.File, ddrive string, djdbc string, logThreadSeq int64, fixTrxNum int) {
	if len(sqls) == 0 {
		return
	}
	ApplyDataFixWithTrxNumOptimizedInput(sqls, datafixType, sfile, ddrive, djdbc, logThreadSeq, fixTrxNum)
}

// processBatch 批量处理SQL语句，根据类型排序后写入文件
func processBatch(sqls []string, datafixType string, sfile *os.File, ddrive string, djdbc string, logThreadSeq int64, fixTrxNum int, isUniqueKey bool, deleteSqlSize int, insertSqlSize int) {
	if len(sqls) == 0 {
		return
	}
	finalSqls := optimizeSqlStatements(sqls, fixTrxNum, isUniqueKey, deleteSqlSize, insertSqlSize)
	writeOptimizedSqlChunk(finalSqls, datafixType, sfile, ddrive, djdbc, logThreadSeq, fixTrxNum)
	global.Wlog.Debugf("DEBUG_BATCH_WRITE_%d: Wrote %d SQL statements to file, DELETE=%d, INSERT=%d\n",
		logThreadSeq, len(finalSqls), len(sqls), len(finalSqls))
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (sp SchedulePlan) getDestTableName() string {
	if strings.TrimSpace(sp.destTable) != "" {
		return sp.destTable
	}
	return sp.table
}

/*
处理有索引表的数据校验
*/
func (sp SchedulePlan) doIndexDataCheck() {
	queueDepth := sp.mqQueueDepth
	if queueDepth > sp.concurrency*2 {
		queueDepth = sp.concurrency * 2
	}
	if queueDepth < 1 {
		queueDepth = 1
	}
	var (
		sqlWhere            = make(chanString, queueDepth)
		diffQueryData       = make(chanDiffDataS, queueDepth)
		fixSQL              = make(chanString, queueDepth)
		tableColumn         = sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]
		selectColumnStringM = make(map[string]map[string]string)
	)

	var idxc, idxcDest dbExec.IndexColumnStruct
	logThreadSeq := rand.Int63()
	destTable := sp.getDestTableName()
	destColInfo := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.destSchema, sp.table)].DColumnInfo
	if mappedDestCols, ok := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.destSchema, destTable)]; ok && len(mappedDestCols.DColumnInfo) > 0 {
		destColInfo = mappedDestCols.DColumnInfo
	}
	idxc = dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName,
		ColData: sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.sourceSchema, sp.table)].SColumnInfo}
	selectColumnStringM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)
	idxcDest = dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: destTable, ColumnName: sp.columnName,
		ChanrowCount: sp.chanrowCount, Drivce: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName,
		ColData: destColInfo}
	selectColumnStringM[sp.ddrive] = idxcDest.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

	// 设置Pod结构体，包括映射关系信息
	mappingInfo := ""
	if sp.sourceSchema != sp.destSchema || sp.table != destTable {
		mappingInfo = fmt.Sprintf("Schema: %s.%s:%s.%s", sp.sourceSchema, sp.table, sp.destSchema, destTable)
	}

	sp.pods = &Pod{
		Schema:      sp.schema,
		Table:       sp.table,
		IndexColumn: strings.TrimLeft(strings.Join(sp.columnName, ","), ","),
		CheckObject: sp.checkObject, // 添加CheckObject字段
		DIFFS:       "no",
		Datafix:     sp.datafixType,
		MappingInfo: mappingInfo,
		ColumnsInfo: sp.buildColumnsInfo(),
	}

	// 关键检查：验证索引列在目标端是否存在
	// MySQL 8.0 GIPK (Generated Invisible Primary Key) 可能仅存在于源端
	// 如果索引列在目标端不存在，数据比较将会失败，需要提前标记DDL不一致
	ddbCheck := sp.ddbPool.Get(logThreadSeq)
	for _, colName := range sp.columnName {
		if colName == "" {
			continue
		}
		checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
			sp.destSchema, destTable, colName)
		var colCount int
		err := ddbCheck.QueryRow(checkSQL).Scan(&colCount)
		if err != nil || colCount == 0 {
			sp.ddbPool.Put(ddbCheck, logThreadSeq)
			vlog := fmt.Sprintf("[doIndexDataCheck] Index column '%s' does not exist in target table %s.%s (possible GIPK/INVISIBLE column mismatch). Setting Diffs=yes.",
				colName, sp.destSchema, destTable)
			global.Wlog.Warn(vlog)
			fmt.Printf("\n[WARNING] Index column '%s' exists in source %s.%s but NOT in target %s.%s (DDL mismatch)\n",
				colName, sp.sourceSchema, sp.table, sp.destSchema, destTable)

			// 获取行数用于报告
			idxc = dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, Drivce: sp.sdrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
			sdb := sp.sdbPool.Get(logThreadSeq)
			srcRows, _ := idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
			sp.sdbPool.Put(sdb, logThreadSeq)

			idxcDest := dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: destTable, Drivce: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
			ddb := sp.ddbPool.Get(logThreadSeq)
			destRows, _ := idxcDest.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
			sp.ddbPool.Put(ddb, logThreadSeq)

			sp.pods.DIFFS = "DDL-yes"
			sp.pods.Rows = fmt.Sprintf("%d,%d", srcRows, destRows)
			measuredDataPods = append(measuredDataPods, *sp.pods)
			return
		}
	}
	sp.ddbPool.Put(ddbCheck, logThreadSeq)

	// 确保使用正确的源表和目标表的Schema
	idxc = dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, Drivce: sp.sdrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
	sdb := sp.sdbPool.Get(logThreadSeq)
	var vlog string
	vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Querying source table rows for %s.%s", logThreadSeq, sp.sourceSchema, sp.table)
	global.Wlog.Debug(vlog)
	A, err := idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, logThreadSeq)
	if err != nil {
		vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get source table rows for %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
		global.Wlog.Error(vlog)
		return
	}

	idxcDest = dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: destTable, Drivce: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
	ddb := sp.ddbPool.Get(logThreadSeq)
	vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Querying destination table rows for %s.%s", logThreadSeq, sp.destSchema, destTable)
	global.Wlog.Debug(vlog)
	B, err := idxcDest.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	if err != nil {
		vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get destination table rows for %s.%s: %v", logThreadSeq, sp.destSchema, destTable, err)
		global.Wlog.Error(vlog)
		return
	}
	sp.ddbPool.Put(ddb, logThreadSeq)
	if A >= B {
		sp.tableMaxRows = A
	} else {
		sp.tableMaxRows = B
	}
	// 重新查询精确行数
	sourceExactCount, sourceCountExact := sp.getExactRowCount(sp.sdbPool, sp.sourceSchema, sp.table, logThreadSeq)
	targetExactCount, targetCountExact := sp.getExactRowCount(sp.ddbPool, sp.destSchema, destTable, logThreadSeq)
	sp.pods.Rows = fmt.Sprintf("%d,%d", sourceExactCount, targetExactCount)

	// 仅在两端都拿到精确计数时，才用行数差异做提前判定。
	// 元数据估算值仅用于展示，不应影响一致性判定逻辑。
	if sourceCountExact && targetCountExact && sourceExactCount != targetExactCount {
		vlog = fmt.Sprintf("Row count mismatch detected for %s.%s: source=%d, target=%d, diff=%d", sp.sourceSchema, sp.table, sourceExactCount, targetExactCount, abs(int64(sourceExactCount)-int64(targetExactCount)))
		global.Wlog.Info(vlog)
		sp.pods.DIFFS = "yes"

		// 如果源端行数大于目标端，记录日志，让正常的数据比较流程来处理
		if sourceExactCount > targetExactCount {
			vlog = fmt.Sprintf("Source has more rows than target for %s.%s, will handle missing rows through normal data comparison process", sp.sourceSchema, sp.table)
			global.Wlog.Info(vlog)
		}
	}

	// columns 模式：初始化 source-only advisory 收集器
	if len(sp.columnPlanSourceCols) > 0 {
		sp.sourceOnlyAdvisory = &columnsModeSourceOnlyAdvisory{
			schema:     sp.sourceSchema,
			table:      sp.table,
			destSchema: sp.destSchema,
			destTable:  sp.getDestTableName(),
			indexCols:  append([]string(nil), sp.columnName...),
		}
	}

	// 创建独立的channel用于源端和目标端查询SQL
	sourceSelectSql := make(chanMap, queueDepth)
	destSelectSql := make(chanMap, queueDepth)

	var scheduleCount = make(chan int64, 1)
	go sp.indexColumnDispos(sqlWhere, selectColumnStringM)

	// 调用分离的查询函数
	go sp.queryTableSqlSeparate(sqlWhere, sourceSelectSql, destSelectSql, tableColumn, scheduleCount, logThreadSeq)
	go sp.queryTableDataSeparate(sourceSelectSql, destSelectSql, diffQueryData, tableColumn, scheduleCount, logThreadSeq)

	go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
	sp.DataFixDispos(fixSQL, logThreadSeq)
}

// 新的函数处理分离的源端和目标端查询
func (sp *SchedulePlan) queryTableSqlSeparate(sqlWhere chanString, sourceSelectSql chanMap, destSelectSql chanMap, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	destTable := sp.getDestTableName()
	for c := range sqlWhere {
		// 源端查询SQL
		sourceWhere := strings.Replace(c, fmt.Sprintf("%s.%s", sp.destSchema, destTable), fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), -1)
		sourceWhere = strings.Replace(sourceWhere, fmt.Sprintf("`%s`.`%s`", sp.destSchema, destTable), fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), -1)
		sourceWhere = adaptWhereForDrive(sourceWhere, sp.sdrive)

		idxc := dbExec.IndexColumnStruct{
			Schema:                  sp.sourceSchema,
			Table:                   sp.table,
			Drivce:                  sp.sdrive,
			CaseSensitiveObjectName: sp.caseSensitiveObjectName,
			TableColumn:             cc1.SColumnInfo,
			Sqlwhere:                sourceWhere,
			ColData:                 cc1.SColumnInfo,
			CompareColumns:          sp.columnPlanSourceCols, // nil = 全列模式
		}
		sdb := sp.sdbPool.Get(logThreadSeq)
		sourceSql, err := idxc.TableIndexColumn().GeneratingQuerySql(sdb, logThreadSeq)
		sp.sdbPool.Put(sdb, logThreadSeq)
		if err != nil {
			continue
		}

		// 目标端查询SQL
		destWhere := strings.Replace(c, fmt.Sprintf("%s.%s", sp.sourceSchema, sp.table), fmt.Sprintf("%s.%s", sp.destSchema, destTable), -1)
		destWhere = strings.Replace(destWhere, fmt.Sprintf("`%s`.`%s`", sp.sourceSchema, sp.table), fmt.Sprintf("`%s`.`%s`", sp.destSchema, destTable), -1)
		destWhere = adaptWhereForDrive(destWhere, sp.ddrive)

		idxcDest := dbExec.IndexColumnStruct{
			Schema:                  sp.destSchema,
			Table:                   destTable,
			Drivce:                  sp.ddrive,
			CaseSensitiveObjectName: sp.caseSensitiveObjectName,
			TableColumn:             cc1.DColumnInfo,
			Sqlwhere:                destWhere,
			ColData:                 cc1.DColumnInfo,
			CompareColumns:          sp.columnPlanTargetCols, // nil = 全列模式
		}
		ddb := sp.ddbPool.Get(logThreadSeq)
		destSqlStr, err := idxcDest.TableIndexColumn().GeneratingQuerySql(ddb, logThreadSeq)
		sp.ddbPool.Put(ddb, logThreadSeq)
		if err != nil {
			continue
		}

		// 关键修复：只有源端和目标端SQL都生成成功后，才同时发送到各自的channel
		// 防止因某一端失败导致channel不同步，造成后续所有chunk配对错误
		sourceSelectSql <- map[string]string{sp.sdrive: sourceSql}
		destSelectSql <- map[string]string{sp.ddrive: destSqlStr}
	}
	close(sourceSelectSql)
	close(destSelectSql)
}

func (sp *SchedulePlan) queryTableDataSeparate(sourceSelectSql chanMap, destSelectSql chanMap, diffQueryData chanDiffDataS, cc1 global.TableAllColumnInfoS, sc chan int64, logThreadSeq int64) {
	var (
		curry     = make(chanStruct, sp.concurrency)
		autoSeq   = int64(0) // 任务计数器
		total     = int64(0)
		startTime = time.Now().UnixMilli() // 开始时间
		allClosed = false
	)

	// 重新初始化进度条为100，用于显示百分比进度
	sp.bar = &Bar{}
	sp.bar.NewOption(0, 100, "Processing")
	logStageMemory("chunk-query-start", logThreadSeq, sp.schema, sp.table)

	for {
		// 检查是否所有工作都已完成
		if allClosed {
			// 等待所有协程完成
			if len(curry) == 0 {
				// 完成进度条显示
				sp.bar.Finish()
				logStageMemory("chunk-query-end", logThreadSeq, sp.schema, sp.table)
				close(diffQueryData)
				return
			}
			// 继续循环，等待协程完成
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case d, ok := <-sc:
			if ok && d > 0 {
				total = d
				global.Wlog.Debugf("DEBUG_PROGRESS_%d: Total tasks received=%d at time=%.2fs\n",
					logThreadSeq, d, float64(time.Now().UnixMilli()-startTime)/1000)
			}
		case sourceSql, ok := <-sourceSelectSql:
			if !ok {
				// 源通道关闭，检查目标通道
				select {
				case _, destOk := <-destSelectSql:
					if !destOk {
						// 目标通道也关闭了
						allClosed = true
					}
				default:
					// 目标通道可能还有数据，继续处理
				}
				continue
			}

			// 从目标通道读取数据，检查是否已关闭
			destSql, destOk := <-destSelectSql
			if !destOk {
				allClosed = true
				continue
			}

			autoSeq++

			// 计算当前完成百分比并更新进度条
			var displayProgress int64
			if total > 0 {
				// 计算当前完成的百分比，映射到100的刻度上
				percent := float64(autoSeq) / float64(total)
				displayProgress = int64(percent * 100)
				if displayProgress > 100 {
					displayProgress = 100
				}
			} else {
				// 没有总数时，使用更平滑的进度估算
				var estimatedTotal int64
				if autoSeq <= 50 {
					estimatedTotal = 100 // 前50个任务时，估算总共100个
				} else if autoSeq <= 100 {
					estimatedTotal = autoSeq * 2 // 51-100个任务时，估算为当前的2倍
				} else if autoSeq <= 300 {
					estimatedTotal = autoSeq + autoSeq/2 // 101-300个任务时，估算再需要50%的任务
				} else {
					estimatedTotal = autoSeq + 150 // 超过300个任务时，估算再需要150个
				}

				percent := float64(autoSeq) / float64(estimatedTotal)
				displayProgress = int64(percent * 100)

				// 限制进度显示，避免过早达到100%
				if displayProgress > 95 {
					displayProgress = 95 // 最多显示95%，给最终完成留空间
				}
			}

			// DEBUG: 记录进度更新
			//currentTime := time.Now().UnixMilli()
			//global.Wlog.Debug("DEBUG_PROGRESS_UPDATE_%d: autoSeq=%d, total=%d, displayProgress=%d, time=%.2fs, curry_len=%d\n", logThreadSeq, autoSeq, total, displayProgress, float64(currentTime-startTime)/1000, len(curry))

			// 更新进度条
			sp.bar.Play(displayProgress)
			// 强制刷新缓冲区确保实时显示
			fmt.Fprint(os.Stdout, "")

			waitForMemoryBudget(0.90)
			curry <- struct{}{}
			go func(currentSeq int64, sourceSql, destSql map[string]string) {
				defer func() {
					<-curry
				}()

				// DEBUG: 记录任务开始处理
				//taskStartTime := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_TASK_START_%d: currentSeq=%d, autoSeq=%d, total=%d, time=%.2fs\n", logThreadSeq, currentSeq, autoSeq, total, float64(taskStartTime-startTime)/1000)

				// 源端查询
				sdb := sp.sdbPool.Get(logThreadSeq)
				sourceChecksum, err := queryRowsChecksumBySQL(sdb, sourceSql[sp.sdrive], sp.sdrive, logThreadSeq)
				sp.sdbPool.Put(sdb, logThreadSeq)
				if err != nil {
					global.Wlog.Warn(fmt.Sprintf("QUERY_ERROR: source checksum query failed for seq=%d, fallback to full row compare, sql=%s, err=%v", currentSeq, sourceSql[sp.sdrive], err))
					diffQueryData <- DifferencesDataStruct{
						Schema:          sp.schema,
						Table:           sp.table,
						SqlWhere:        map[string]string{"src": sourceSql[sp.sdrive], "dst": destSql[sp.ddrive]},
						TableColumnInfo: cc1,
					}
					return
				}

				// 目标端查询
				ddb := sp.ddbPool.Get(logThreadSeq)
				destChecksum, err := queryRowsChecksumBySQL(ddb, destSql[sp.ddrive], sp.ddrive, logThreadSeq)
				sp.ddbPool.Put(ddb, logThreadSeq)
				if err != nil {
					global.Wlog.Warn(fmt.Sprintf("QUERY_ERROR: dest checksum query failed for seq=%d, fallback to full row compare, sql=%s, err=%v", currentSeq, destSql[sp.ddrive], err))
					diffQueryData <- DifferencesDataStruct{
						Schema:          sp.schema,
						Table:           sp.table,
						SqlWhere:        map[string]string{"src": sourceSql[sp.sdrive], "dst": destSql[sp.ddrive]},
						TableColumnInfo: cc1,
					}
					return
				}

				// 比较结果
				if sourceChecksum != destChecksum {
					differencesData := DifferencesDataStruct{
						Schema:          sp.schema,
						Table:           sp.table,
						SqlWhere:        map[string]string{"src": sourceSql[sp.sdrive], "dst": destSql[sp.ddrive]},
						TableColumnInfo: cc1,
					}
					diffQueryData <- differencesData
				}

				// DEBUG: 记录任务完成时间
				//taskEndTime := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_TASK_END_%d: currentSeq=%d, autoSeq=%d, total=%d, totalTaskTime=%.2fs, timeFromStart=%.2fs\n", logThreadSeq, currentSeq, autoSeq, total, float64(taskEndTime-taskStartTime)/1000, float64(taskEndTime-startTime)/1000)

				// DEBUG: 记录任务完成（不更新进度条，避免跳动）
				//currentTime := time.Now().UnixMilli()
				//global.Wlog.Debug("DEBUG_TASK_COMPLETE_%d: currentSeq=%d, autoSeq=%d, total=%d, time=%.2fs, curry_len=%d\n", logThreadSeq, currentSeq, autoSeq, total, float64(currentTime-startTime)/1000, len(curry))
			}(autoSeq, sourceSql, destSql)
		}
	}
}

// 辅助函数：计算绝对值
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func logStageMemory(stage string, logThreadSeq int64, schema string, table string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	updateTableMemoryPeak(fmt.Sprintf("%s.%s", schema, table), tableMemoryPeak{
		Stage:       stage,
		AllocMB:     m.Alloc / 1024 / 1024,
		HeapInuseMB: m.HeapInuse / 1024 / 1024,
		HeapObjects: m.HeapObjects,
		NumGC:       m.NumGC,
	})
	global.Wlog.Info(fmt.Sprintf("(%d) MEM_STAGE=%s table=%s.%s Alloc=%dMB HeapInuse=%dMB HeapObjects=%d NumGC=%d",
		logThreadSeq,
		stage,
		schema,
		table,
		m.Alloc/1024/1024,
		m.HeapInuse/1024/1024,
		m.HeapObjects,
		m.NumGC,
	))
}

func dataDisposDBTypeByDrive(drive string) string {
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		return "Oracle"
	}
	return "MySQL"
}

func normalizedDrive(drive string) string {
	if strings.EqualFold(drive, "godror") || strings.EqualFold(drive, "oracle") {
		return "godror"
	}
	return "mysql"
}

func queryRowsDataBySQL(db *sql.DB, query string, drive string, logThreadSeq int64) ([]string, error) {
	dispos := dataDispos.DBdataDispos{
		DBType:       dataDisposDBTypeByDrive(drive),
		LogThreadSeq: logThreadSeq,
		Event:        "Q_Table_Data",
		DB:           db,
	}
	rows, err := dispos.DBSQLforExec(query)
	if err != nil {
		return nil, err
	}
	dispos.SqlRows = rows
	data, err := dispos.DataRowsDispos(make([]string, 0, 1024))
	rows.Close()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func queryRowsChecksumBySQL(db *sql.DB, query string, drive string, logThreadSeq int64) (string, error) {
	dispos := dataDispos.DBdataDispos{
		DBType:       dataDisposDBTypeByDrive(drive),
		LogThreadSeq: logThreadSeq,
		Event:        "Q_Table_Data",
		DB:           db,
	}
	rows, err := dispos.DBSQLforExec(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	rowSep := "/*go actions rowData*/"
	colSep := "/*go actions columnData*/"
	driver := normalizedDrive(drive)
	rowDigestCounts := make(map[string]uint64, 128)

	valuePtrs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for rows.Next() {
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", err
		}
		rowHasher := md5.New()
		for i := 0; i < len(columns); i++ {
			if i > 0 {
				_, _ = io.WriteString(rowHasher, colSep)
			}
			s := normalizeChecksumValue(values[i], driver)
			_, _ = io.WriteString(rowHasher, s)
		}
		rowDigest := hex.EncodeToString(rowHasher.Sum(nil))
		rowDigestCounts[rowDigest]++
	}

	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(rowDigestCounts) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(rowDigestCounts))
	for digest := range rowDigestCounts {
		keys = append(keys, digest)
	}
	sort.Strings(keys)
	finalHasher := md5.New()
	for i, digest := range keys {
		if i > 0 {
			_, _ = io.WriteString(finalHasher, rowSep)
		}
		_, _ = io.WriteString(finalHasher, digest)
		_, _ = io.WriteString(finalHasher, ":")
		_, _ = io.WriteString(finalHasher, strconv.FormatUint(rowDigestCounts[digest], 10))
	}
	return hex.EncodeToString(finalHasher.Sum(nil)), nil
}

func normalizeChecksumValue(val interface{}, driver string) string {
	switch normalizedDrive(driver) {
	case "godror":
		return dataDispos.NormalizeValueForComparison(val, "Oracle")
	case "mysql":
		return dataDispos.NormalizeValueForComparison(val, "MySQL")
	default:
		return dataDispos.NormalizeValueForComparison(val, "")
	}
}

func hashRowsIgnoringOrder(rows []string) string {
	if len(rows) == 0 {
		return ""
	}
	counts := make(map[string]uint64, len(rows))
	for _, row := range rows {
		counts[row]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := md5.New()
	for i, key := range keys {
		if i > 0 {
			_, _ = io.WriteString(h, "/*go actions rowData*/")
		}
		_, _ = io.WriteString(h, key)
		_, _ = io.WriteString(h, ":")
		_, _ = io.WriteString(h, strconv.FormatUint(counts[key], 10))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func waitForMemoryBudget(highWatermark float64) {
	globalConfig := inputArg.GetGlobalConfig()
	if globalConfig == nil {
		return
	}
	limitMB := globalConfig.SecondaryL.RulesV.MemoryLimit
	if limitMB <= 0 {
		return
	}
	if highWatermark <= 0 || highWatermark >= 1 {
		highWatermark = 0.90
	}
	if highWatermark < 0.70 {
		highWatermark = 0.70
	}
	if highWatermark > 0.98 {
		highWatermark = 0.98
	}
	threshold := int(float64(limitMB) * highWatermark)
	hardThreshold := int(float64(limitMB) * minFloat64(0.98, highWatermark+0.06))
	start := time.Now()
	sleepStep := 20 * time.Millisecond
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		currentMB := int(m.Alloc / 1024 / 1024)
		heapInuseMB := int(m.HeapInuse / 1024 / 1024)
		if heapInuseMB > currentMB {
			currentMB = heapInuseMB
		}
		if currentMB < threshold {
			return
		}

		// Force GC only when memory is near hard limit and only at a throttled cadence.
		if currentMB >= hardThreshold {
			tryForceGC(1500 * time.Millisecond)
		}

		// Avoid long producer stalls; the function is called frequently at hot points.
		if time.Since(start) > 2*time.Second {
			return
		}
		time.Sleep(sleepStep)
		if sleepStep < 120*time.Millisecond {
			sleepStep += 20 * time.Millisecond
		}
	}
}

func tryForceGC(minInterval time.Duration) {
	forcedGCMutex.Lock()
	now := time.Now()
	if !lastForcedGCAt.IsZero() && now.Sub(lastForcedGCAt) < minInterval {
		forcedGCMutex.Unlock()
		return
	}
	lastForcedGCAt = now
	forcedGCMutex.Unlock()
	runtime.GC()
}

func minFloat64(a float64, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func isFloatingColumnType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	if t == "" {
		return false
	}
	// Cover common Oracle/MySQL floating types.
	return strings.HasPrefix(t, "FLOAT") ||
		strings.HasPrefix(t, "DOUBLE") ||
		strings.HasPrefix(t, "REAL") ||
		strings.HasPrefix(t, "BINARY_FLOAT") ||
		strings.HasPrefix(t, "BINARY_DOUBLE")
}

func parseNumericScale(dataType string) (int, bool) {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	left := strings.Index(t, "(")
	right := strings.LastIndex(t, ")")
	if left == -1 || right <= left+1 {
		return 0, false
	}
	args := strings.TrimSpace(t[left+1 : right])
	parts := strings.Split(args, ",")
	if len(parts) < 2 {
		return 0, false
	}
	scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || scale < 0 {
		return 0, false
	}
	return scale, true
}

func resolveFloatComparisonScale(sourceType, destType string) int {
	sourceFloat := isFloatComparisonType(sourceType)
	destFloat := isFloatComparisonType(destType)
	if !sourceFloat && !destFloat {
		return -1
	}
	sourceScale, sourceOK := parseNumericScale(sourceType)
	destScale, destOK := parseNumericScale(destType)
	if sourceOK && destOK {
		if sourceScale < destScale {
			return sourceScale
		}
		return destScale
	}
	if sourceOK {
		return sourceScale
	}
	if destOK {
		return destScale
	}
	// 当两端均为 FLOAT/BINARY_FLOAT 且无显式小数精度时，
	// 使用 float32 精度哨兵规范化：消除 Oracle 精确十进制（123.45）
	// 与 MySQL 二进制浮点（123.449997）的字符串差异，两者在 float32 层面实际相同。
	if isFloatComparisonType(sourceType) && isFloatComparisonType(destType) {
		return floatSinglePrecisionSentinel
	}
	return 6
}

func isFloatComparisonType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return strings.HasPrefix(t, "FLOAT") || strings.HasPrefix(t, "BINARY_FLOAT")
}

func buildFloatComparisonScales(sourceCols, destCols []map[string]string) []int {
	colCount := len(sourceCols)
	if len(destCols) < colCount {
		colCount = len(destCols)
	}
	if colCount == 0 {
		return nil
	}
	scales := make([]int, colCount)
	hasFloatColumn := false
	for i := 0; i < colCount; i++ {
		scale := resolveFloatComparisonScale(sourceCols[i]["dataType"], destCols[i]["dataType"])
		scales[i] = scale
		if scale >= 0 {
			hasFloatColumn = true
		}
	}
	if !hasFloatColumn {
		return nil
	}
	return scales
}

// floatSinglePrecisionSentinel 作为 scale 哨兵值，指示使用 float32 精度规范化。
// 用于处理 Oracle FLOAT（以精确十进制存储，如 123.45）与
// MySQL FLOAT（IEEE 754 单精度，返回 123.449997）的字符串表示不一致问题。
const floatSinglePrecisionSentinel = -3

func normalizeFloatComparisonValue(raw string, scale int) string {
	if scale == -1 {
		return raw
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == dataDispos.ValueNullPlaceholder || trimmed == dataDispos.ValueEmptyPlaceholder {
		return raw
	}
	if scale == floatSinglePrecisionSentinel {
		// 使用 float32 精度规范化：将两端值都下转为 float32 再格式化，
		// 消除 Oracle 精确十进制与 MySQL 二进制浮点的字符串表示差异。
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return raw
		}
		f32 := float32(f)
		result := strconv.FormatFloat(float64(f32), 'g', -1, 32)
		return result
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return raw
	}
	if scale > 15 {
		scale = 15
	}
	factor := math.Pow10(scale)
	rounded := math.Round(f*factor) / factor
	normalized := strconv.FormatFloat(rounded, 'f', scale, 64)
	normalized = strings.TrimRight(strings.TrimRight(normalized, "0"), ".")
	if normalized == "" || normalized == "-0" {
		normalized = "0"
	}
	return normalized
}

func normalizeRowsForFloatComparison(rows []string, scales []int) []string {
	if len(rows) == 0 || len(scales) == 0 {
		return rows
	}
	const columnSep = "/*go actions columnData*/"
	normalizedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		parts := strings.Split(row, columnSep)
		limit := len(scales)
		if len(parts) < limit {
			limit = len(parts)
		}
		changed := false
		for colIdx := 0; colIdx < limit; colIdx++ {
			if scales[colIdx] == -1 {
				continue
			}
			normalized := normalizeFloatComparisonValue(parts[colIdx], scales[colIdx])
			if normalized != parts[colIdx] {
				parts[colIdx] = normalized
				changed = true
			}
		}
		if changed {
			normalizedRows[rowIdx] = strings.Join(parts, columnSep)
		} else {
			normalizedRows[rowIdx] = row
		}
	}
	return normalizedRows
}

// remapDelToOriginalDest 将 Arrcmp 返回的 del 行（来自归一化后的 cleanDestData）
// 映射回归一化前的目标端原始行（origDest），确保 DELETE WHERE 使用 MySQL 实际存储值。
//
// 原理：normalizeRowsForFloatComparison / normalizeRowsForTemporalComparison 按位置
// 修改每行字段，因此 normalizedDest[i] 与 origDest[i] 一一对应。
// 本函数通过 "归一化行→原始行队列" 映射表逐行消费；对多条目标行在归一化后字面相同
// 的"歧义"场景（例如多行 float 归一化到同值且与主键无关）提供 FIFO 兜底并显式告警，
// 让运维可借助日志人工复核，避免把错误原始行写入 DELETE WHERE。
func remapDelToOriginalDest(normalizedDel, normalizedDest, origDest []string) []string {
	if len(normalizedDest) != len(origDest) {
		// 长度不一致时保守地返回原 del，避免越界
		return normalizedDel
	}
	// 构建 normalizedRow → []originalRow 队列映射
	normToOrig := make(map[string][]string, len(normalizedDest))
	ambiguous := make(map[string]bool)
	for i, norm := range normalizedDest {
		if queue, exists := normToOrig[norm]; exists {
			// 多条原始行归一化后字面相同：结果可能依赖 Arrcmp 顺序（潜在错位）
			if len(queue) > 0 && queue[0] != origDest[i] {
				ambiguous[norm] = true
			}
		}
		normToOrig[norm] = append(normToOrig[norm], origDest[i])
	}
	result := make([]string, 0, len(normalizedDel))
	for _, normRow := range normalizedDel {
		if queue, ok := normToOrig[normRow]; ok && len(queue) > 0 {
			if ambiguous[normRow] {
				global.Wlog.Warnf("remapDelToOriginalDest: normalized dest row matches %d distinct originals (float/time collapse); "+
					"FIFO selecting first remaining — verify generated DELETE WHERE targets the intended row", len(queue))
			}
			result = append(result, queue[0])
			normToOrig[normRow] = queue[1:]
		} else {
			// 找不到对应原始行时回退使用归一化行（不应发生）
			global.Wlog.Warnf("remapDelToOriginalDest: normalized del row not found in normalizedDest; falling back to normalized form")
			result = append(result, normRow)
		}
	}
	return result
}

// buildCharTrimFlags 检查源端（Oracle）列类型，对 CHAR/NCHAR 列返回 true，
// 指示在 Arrcmp 前需要对该列的值执行尾部空格裁剪。
// Oracle CHAR/NCHAR 存储时以空格填充至列定义长度（如 'A         '），
// 而 MySQL CHAR SELECT 时自动去除尾部空格（返回 'A'），
// 若不归一化则字符串不等，导致无限 diff 循环。
func buildCharTrimFlags(sourceCols []map[string]string) []bool {
	if len(sourceCols) == 0 {
		return nil
	}
	flags := make([]bool, len(sourceCols))
	hasChar := false
	for i, col := range sourceCols {
		t := strings.ToUpper(strings.TrimSpace(col["dataType"]))
		if strings.HasPrefix(t, "NCHAR") || strings.HasPrefix(t, "CHAR") {
			flags[i] = true
			hasChar = true
		}
	}
	if !hasChar {
		return nil
	}
	return flags
}

// normalizeRowsForCharComparison 对 Oracle CHAR/NCHAR 列的值裁剪尾部空格，
// 使其与 MySQL 自动去除尾部空格后的值一致。
func normalizeRowsForCharComparison(rows []string, flags []bool) []string {
	if len(rows) == 0 || len(flags) == 0 {
		return rows
	}
	const columnSep = "/*go actions columnData*/"
	normalizedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		parts := strings.Split(row, columnSep)
		limit := len(flags)
		if len(parts) < limit {
			limit = len(parts)
		}
		changed := false
		for colIdx := 0; colIdx < limit; colIdx++ {
			if !flags[colIdx] {
				continue
			}
			if parts[colIdx] == dataDispos.ValueNullPlaceholder || parts[colIdx] == dataDispos.ValueEmptyPlaceholder {
				continue
			}
			trimmed := strings.TrimRight(parts[colIdx], " ")
			if trimmed != parts[colIdx] {
				parts[colIdx] = trimmed
				changed = true
			}
		}
		if changed {
			normalizedRows[rowIdx] = strings.Join(parts, columnSep)
		} else {
			normalizedRows[rowIdx] = row
		}
	}
	return normalizedRows
}

func isTemporalComparableType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return t == "DATE" ||
		strings.Contains(t, "TIMESTAMP") ||
		strings.HasPrefix(t, "DATETIME") ||
		strings.HasPrefix(t, "TIME") ||
		strings.HasPrefix(t, "INTERVAL DAY") ||
		strings.HasPrefix(t, "YEAR")
}

func isTimeOnlyType(dataType string) bool {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	return t == "TIME" || strings.HasPrefix(t, "TIME(")
}

func classifyTemporalCompareKind(sourceType, destType string) string {
	s := strings.ToUpper(strings.TrimSpace(sourceType))
	d := strings.ToUpper(strings.TrimSpace(destType))

	if strings.HasPrefix(s, "INTERVAL DAY") && strings.HasPrefix(d, "TIME") {
		return "time"
	}
	if strings.HasPrefix(d, "INTERVAL DAY") && strings.HasPrefix(s, "TIME") {
		return "time"
	}
	if isTimeOnlyType(s) && isTimeOnlyType(d) {
		return "time"
	}
	if strings.HasPrefix(s, "YEAR") || strings.HasPrefix(d, "YEAR") {
		return "year"
	}
	if isTemporalComparableType(s) && isTemporalComparableType(d) {
		return "datetime"
	}
	return ""
}

func buildTemporalCompareKinds(sourceCols, destCols []map[string]string) []string {
	colCount := len(sourceCols)
	if len(destCols) < colCount {
		colCount = len(destCols)
	}
	if colCount == 0 {
		return nil
	}
	kinds := make([]string, colCount)
	hasTemporal := false
	for i := 0; i < colCount; i++ {
		kind := classifyTemporalCompareKind(sourceCols[i]["dataType"], destCols[i]["dataType"])
		kinds[i] = kind
		if kind != "" {
			hasTemporal = true
		}
	}
	if !hasTemporal {
		return nil
	}
	return kinds
}

func normalizeTemporalValue(raw, kind string) string {
	if kind == "" {
		return raw
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == dataDispos.ValueNullPlaceholder || trimmed == dataDispos.ValueEmptyPlaceholder {
		return raw
	}

	switch kind {
	case "year":
		i, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return raw
		}
		return strconv.FormatInt(i, 10)
	case "time":
		// Handle datetime-like string first to avoid misclassifying "YYYY-MM-DD HH:MM:SS" as interval.
		if m := temporalDatetimePrefixRe.FindStringSubmatch(trimmed); len(m) == 3 {
			return m[2]
		}

		// Oracle interval fallback path: value may be total seconds text.
		if temporalNumericSecondsRe.MatchString(trimmed) {
			secondsFloat, secErr := strconv.ParseFloat(trimmed, 64)
			if secErr == nil {
				// Defensive compatibility: some Oracle drivers expose INTERVAL as
				// duration nanoseconds integer. Convert to seconds before HH:MM:SS.
				if math.Abs(secondsFloat) >= 1e10 {
					secondsFloat = secondsFloat / 1e9
				}
				totalSeconds := int64(math.Round(secondsFloat))
				sign := ""
				if totalSeconds < 0 {
					sign = "-"
					totalSeconds = -totalSeconds
				}
				hours := totalSeconds / 3600
				minutes := (totalSeconds % 3600) / 60
				seconds := totalSeconds % 60
				return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
			}
		}

		if m := temporalIntervalRe.FindStringSubmatch(trimmed); len(m) == 5 {
			dayPart, dayErr := strconv.Atoi(m[1])
			hourPart, hourErr := strconv.Atoi(m[2])
			if dayErr == nil && hourErr == nil {
				totalHours := dayPart*24 + hourPart
				sign := ""
				if totalHours < 0 {
					sign = "-"
					totalHours = -totalHours
				}
				return fmt.Sprintf("%s%02d:%s:%s", sign, totalHours, m[3], m[4])
			}
		}

		if m := temporalTimeTokenRe.FindStringSubmatch(trimmed); len(m) == 2 {
			return m[1]
		}
		// Go duration style fallback, e.g. "12h30m29s".
		if m := temporalGoDurationRe.FindStringSubmatch(trimmed); len(m) == 4 {
			hours, hErr := strconv.Atoi(m[1])
			minutes, mErr := strconv.Atoi(m[2])
			secondsFloat, sErr := strconv.ParseFloat(m[3], 64)
			if hErr == nil && mErr == nil && sErr == nil {
				seconds := int(math.Round(secondsFloat))
				sign := ""
				if hours < 0 {
					sign = "-"
					hours = -hours
				}
				return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
			}
		}
		if len(trimmed) >= 19 && trimmed[10] == ' ' {
			return trimmed[11:19]
		}
		return raw
	case "datetime":
		if temporalDateOnlyRe.MatchString(trimmed) {
			return trimmed + " 00:00:00"
		}
		if m := temporalDateTimeRe.FindStringSubmatch(trimmed); len(m) == 3 {
			return m[1] + " " + m[2]
		}
		return raw
	default:
		return raw
	}
}

func normalizeRowsForTemporalComparison(rows []string, kinds []string) []string {
	if len(rows) == 0 || len(kinds) == 0 {
		return rows
	}
	const columnSep = "/*go actions columnData*/"
	normalizedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		parts := strings.Split(row, columnSep)
		limit := len(kinds)
		if len(parts) < limit {
			limit = len(parts)
		}
		changed := false
		for colIdx := 0; colIdx < limit; colIdx++ {
			kind := kinds[colIdx]
			if kind == "" {
				continue
			}
			normalized := normalizeTemporalValue(parts[colIdx], kind)
			if normalized != parts[colIdx] {
				parts[colIdx] = normalized
				changed = true
			}
		}
		if changed {
			normalizedRows[rowIdx] = strings.Join(parts, columnSep)
		} else {
			normalizedRows[rowIdx] = row
		}
	}
	return normalizedRows
}

func reconcileTemporalNullArtifacts(addRows, delRows []string, kinds []string, sourceCols, destCols []map[string]string) ([]string, []string, int) {
	if len(addRows) == 0 || len(delRows) == 0 || len(kinds) == 0 {
		return addRows, delRows, 0
	}
	artifactCols := buildIntervalTimeArtifactColumns(sourceCols, destCols, kinds)
	if len(artifactCols) == 0 {
		return addRows, delRows, 0
	}
	const colSep = "/*go actions columnData*/"
	delUsed := make([]bool, len(delRows))
	delParts := make([][]string, len(delRows))
	delBuckets := make(map[string][]int, len(delRows))
	for idx, delRow := range delRows {
		parts := strings.Split(delRow, colSep)
		delParts[idx] = parts
		key := buildTemporalReconcileKey(parts, artifactCols)
		delBuckets[key] = append(delBuckets[key], idx)
	}
	keepAdd := make([]string, 0, len(addRows))
	healed := 0

	for _, addRow := range addRows {
		addParts := strings.Split(addRow, colSep)
		key := buildTemporalReconcileKey(addParts, artifactCols)
		matched := -1
		for _, idx := range delBuckets[key] {
			if delUsed[idx] {
				continue
			}
			if rowsEqualWithTemporalArtifact(addParts, delParts[idx], artifactCols) {
				matched = idx
				break
			}
		}
		if matched >= 0 {
			delUsed[matched] = true
			healed++
			continue
		}
		keepAdd = append(keepAdd, addRow)
	}

	keepDel := make([]string, 0, len(delRows))
	for i, delRow := range delRows {
		if !delUsed[i] {
			keepDel = append(keepDel, delRow)
		}
	}
	return keepAdd, keepDel, healed
}

func buildTemporalReconcileKey(parts []string, artifactCols map[int]struct{}) string {
	var b strings.Builder
	for i, part := range parts {
		if _, isArtifactCol := artifactCols[i]; isArtifactCol {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("|")
		}
		b.WriteString(strconv.Itoa(i))
		b.WriteString("=")
		b.WriteString(part)
	}
	return b.String()
}

func buildIntervalTimeArtifactColumns(sourceCols, destCols []map[string]string, kinds []string) map[int]struct{} {
	cols := make(map[int]struct{})
	limit := len(kinds)
	if len(sourceCols) < limit {
		limit = len(sourceCols)
	}
	if len(destCols) < limit {
		limit = len(destCols)
	}
	for i := 0; i < limit; i++ {
		if kinds[i] != "time" {
			continue
		}
		sType := strings.ToUpper(strings.TrimSpace(sourceCols[i]["dataType"]))
		dType := strings.ToUpper(strings.TrimSpace(destCols[i]["dataType"]))
		if strings.HasPrefix(sType, "INTERVAL DAY") && isTimeOnlyType(dType) {
			cols[i] = struct{}{}
			continue
		}
		if strings.HasPrefix(dType, "INTERVAL DAY") && isTimeOnlyType(sType) {
			cols[i] = struct{}{}
		}
	}
	return cols
}

func rowsEqualWithTemporalArtifact(addParts, delParts []string, artifactCols map[int]struct{}) bool {
	if len(addParts) != len(delParts) {
		return false
	}
	for i := range addParts {
		a := addParts[i]
		d := delParts[i]
		if a == d {
			continue
		}
		if _, ok := artifactCols[i]; ok {
			aNull := isNullPlaceholderValue(a)
			dNull := isNullPlaceholderValue(d)
			if aNull || dNull {
				if aNull && dNull {
					continue
				}
				return false
			}
			if normalizeTemporalValue(a, "time") == normalizeTemporalValue(d, "time") {
				continue
			}
		}
		return false
	}
	return true
}

func isNullPlaceholderValue(v string) bool {
	return v == dataDispos.ValueNullPlaceholder || strings.EqualFold(strings.TrimSpace(v), "null")
}

func (sp *SchedulePlan) isFloatingIndexColumn(columnName string) bool {
	colInfo, ok := sp.tableAllCol[fmt.Sprintf("%s_gtchecksum_%s", sp.schema, sp.table)]
	if !ok {
		return false
	}
	for _, col := range colInfo.SColumnInfo {
		if strings.EqualFold(col["columnName"], columnName) {
			return isFloatingColumnType(col["dataType"])
		}
	}
	for _, col := range colInfo.DColumnInfo {
		if strings.EqualFold(col["columnName"], columnName) {
			return isFloatingColumnType(col["dataType"])
		}
	}
	return false
}

// columnsModeFilteredCols returns a column info slice for columns-mode SELECT queries,
// keeping only PK columns (pkCols) and user-selected compare columns (compareCols),
// in the original table column order from allCols.
// columnsModeFilteredCols builds a column-info slice for columns mode.
// Output order: PK columns first (in pkCols order), then compare columns in compareCols order.
// This mirrors the SELECT column order produced by GeneratingQuerySql so that row-string
// positions are aligned between source and target (Pairs semantics).
func columnsModeFilteredCols(allCols []map[string]string, compareCols []string, pkCols []string) []map[string]string {
	// Case-insensitive lookup to match MySQL/MariaDB column name semantics.
	// TrimSpace is applied in addition to ToLower so that metadata with incidental
	// surrounding whitespace (rare but possible) is handled consistently with
	// normalizeColumnLookupKey in my_query_table_data.go.
	colByName := make(map[string]map[string]string, len(allCols))
	for _, col := range allCols {
		if name, ok := col["columnName"]; ok {
			colByName[strings.ToLower(strings.TrimSpace(name))] = col
		}
	}
	pkSet := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		pkSet[strings.ToLower(strings.TrimSpace(c))] = true
	}
	// PK columns first, in pkCols order
	var result []map[string]string
	for _, pkCol := range pkCols {
		if c, ok := colByName[strings.ToLower(strings.TrimSpace(pkCol))]; ok {
			result = append(result, c)
		}
	}
	// Compare columns in compareCols order (skip any that are also PK)
	for _, cmpCol := range compareCols {
		if pkSet[strings.ToLower(strings.TrimSpace(cmpCol))] {
			continue
		}
		if c, ok := colByName[strings.ToLower(strings.TrimSpace(cmpCol))]; ok {
			result = append(result, c)
		}
	}
	return result
}

// columnsModeSplitPKAndCompare aligns PK detection with the rest of the columns-mode
// pipeline by treating column names case-insensitively (ToLower + TrimSpace, consistent
// with normalizeColumnLookupKey in my_query_table_data.go).
//
// compareColNames preserves the original case of col["columnName"] from filteredCols;
// callers (FixUpdateSqlExec, orderColumnsForCompare) perform their own ToLower lookup,
// so the original case is safe to retain here.
func columnsModeSplitPKAndCompare(filteredCols []map[string]string, pkCols []string) ([]int, []string) {
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, c := range pkCols {
		pkSet[strings.ToLower(strings.TrimSpace(c))] = struct{}{}
	}

	pkPositions := make([]int, 0, len(pkCols))
	compareColNames := make([]string, 0, len(filteredCols))
	for i, col := range filteredCols {
		name := strings.ToLower(strings.TrimSpace(col["columnName"]))
		if _, ok := pkSet[name]; ok {
			pkPositions = append(pkPositions, i)
			continue
		}
		compareColNames = append(compareColNames, col["columnName"])
	}
	return pkPositions, compareColNames
}

// pkKeyMissingMarker 是 columnsModeExtractPKKey 在 pos 越界时使用的哨兵值。
// \x01 (SOH) 不会出现在正常 MySQL 文本数据中，可与合法空字符串 PK 值严格区分。
const pkKeyMissingMarker = "\x01<MISSING>\x01"

// columnsModeExtractPKKey extracts a composite PK key string from a row data string.
// pkPositions are the 0-based column positions of PK columns within the filtered column list.
// Values are joined with a NUL byte to avoid collisions with normal data.
//
// Out-of-bounds positions are represented with pkKeyMissingMarker (not empty string) so
// a legitimately-empty-string PK value and a missing PK component produce different keys.
// A WARN log is emitted when out-of-bounds occurs, because it means the row data is corrupted.
func columnsModeExtractPKKey(rowData string, pkPositions []int) string {
	parts := strings.Split(rowData, "/*go actions columnData*/")
	vals := make([]string, len(pkPositions))
	for i, pos := range pkPositions {
		if pos < len(parts) {
			vals[i] = parts[pos]
		} else {
			vals[i] = pkKeyMissingMarker
			if global.Wlog != nil {
				global.Wlog.Warnf("columnsModeExtractPKKey: pos %d out of bounds (parts=%d), row data may be corrupted", pos, len(parts))
			}
		}
	}
	return strings.Join(vals, "\x00")
}
