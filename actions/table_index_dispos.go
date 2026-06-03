package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"hash/fnv"
	"math/rand"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type (
	chanString      chan string
	chanFixSQLItem  chan fixSQLItem
	chanMap         chan map[string]string
	chanBool        chan bool
	chanDiffDataS   chan DifferencesDataStruct
	chanSliceString chan []string
	chanStruct      chan struct{}
)

type fixSQLItem struct {
	ChunkSeq int64
	SQL      string
	Done     bool
}

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
func (sp *SchedulePlan) doIndexDataCheck() {
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
		fixSQL              = make(chanFixSQLItem, queueDepth)
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

	// Resume 模式：若 fixsql 目录已存在该表的修复文件（上次运行产生），
	// 则初始化 DIFFS=yes，避免本次 resume 仅处理剩余 chunk 无差异时错误报告 Diffs=no。
	if sp.datafixSql != "" && sp.resumeFixFileSeqs != nil {
		fixSchema := fixFileNameEncode(sp.destSchema)
		fixTable := fixFileNameEncode(destTable)
		pattern := filepath.Join(sp.datafixSql, fmt.Sprintf("table.%s.%s-*.sql", fixSchema, fixTable))
		if matches, _ := filepath.Glob(pattern); len(matches) > 0 {
			sp.pods.DIFFS = "yes"
			global.Wlog.Info(fmt.Sprintf("[RESUME] Found %d existing fixsql file(s) for %s.%s, initializing Diffs=yes",
				len(matches), sp.schema, sp.table))
		}
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
			idxcDest := dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: destTable, Drivce: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
			srcRows, destRows, srcRowsErr, destRowsErr := sp.querySourceTargetTableRows(idxc, idxcDest, logThreadSeq)
			if srcRowsErr != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get source table rows for %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, srcRowsErr))
			}
			if destRowsErr != nil {
				global.Wlog.Error(fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get destination table rows for %s.%s: %v", logThreadSeq, sp.destSchema, destTable, destRowsErr))
			}

			sp.pods.DIFFS = "DDL-yes"
			sp.pods.Rows = fmt.Sprintf("%d,%d", srcRows, destRows)
			measuredDataPods = append(measuredDataPods, *sp.pods)
			return
		}
	}
	sp.ddbPool.Put(ddbCheck, logThreadSeq)

	// 确保使用正确的源表和目标表的Schema
	idxc = dbExec.IndexColumnStruct{Schema: sp.sourceSchema, Table: sp.table, Drivce: sp.sdrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}
	var vlog string
	idxcDest = dbExec.IndexColumnStruct{Schema: sp.destSchema, Table: destTable, Drivce: sp.ddrive, CaseSensitiveObjectName: sp.caseSensitiveObjectName}

	schemaTable := fmt.Sprintf("%s.%s", sp.schema, sp.table)
	var A, B uint64

	// resume 模式：优先从 progress 缓存读取 source/dest 行数，避免重复查询
	rowStatsCached := false
	if sp.ChecksumProgress != nil {
		if cachedA, cachedB, ok := sp.ChecksumProgress.GetTableRowStats(schemaTable); ok {
			A, B = cachedA, cachedB
			rowStatsCached = true
			global.Wlog.Info(fmt.Sprintf("(%d) [RESUME] Using cached row stats for %s: source=%d, dest=%d", logThreadSeq, schemaTable, A, B))
		}
	}
	if !rowStatsCached {
		vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Querying source table rows for %s.%s", logThreadSeq, sp.sourceSchema, sp.table)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Querying destination table rows for %s.%s", logThreadSeq, sp.destSchema, destTable)
		global.Wlog.Debug(vlog)
		var err, destErr error
		A, B, err, destErr = sp.querySourceTargetTableRows(idxc, idxcDest, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get source table rows for %s.%s: %v", logThreadSeq, sp.sourceSchema, sp.table, err)
			global.Wlog.Error(vlog)
			return
		}
		if destErr != nil {
			vlog = fmt.Sprintf("(%d) [doIndexDataCheck] Failed to get destination table rows for %s.%s: %v", logThreadSeq, sp.destSchema, destTable, destErr)
			global.Wlog.Error(vlog)
			return
		}
		if sp.ChecksumProgress != nil {
			_ = sp.ChecksumProgress.SetTableRowStats(schemaTable, A, B)
		}
	}

	if A >= B {
		sp.tableMaxRows = A
	} else {
		sp.tableMaxRows = B
	}
	// 记录 index 表开始处理，写入进度文件（供断点续传可见性使用）
	if sp.ChecksumProgress != nil {
		totalChunks := int64(sp.tableMaxRows / uint64(sp.chanrowCount))
		if sp.tableMaxRows%uint64(sp.chanrowCount) > 0 {
			totalChunks++
		}
		if totalChunks < 1 {
			totalChunks = 1
		}
		_ = sp.ChecksumProgress.SetTableTotalRows(schemaTable, totalChunks, 1)
	}

	// 查询精确行数：resume 模式下优先使用缓存，避免重复 COUNT(*) 扫描
	var sourceExactCount, targetExactCount int64
	var sourceCountExact, targetCountExact bool
	exactCached := false
	if sp.ChecksumProgress != nil {
		if cSrc, cDst, sExact, dExact, ok := sp.ChecksumProgress.GetTableExactRowStats(schemaTable); ok {
			sourceExactCount, targetExactCount = cSrc, cDst
			sourceCountExact, targetCountExact = sExact, dExact
			exactCached = true
			global.Wlog.Info(fmt.Sprintf("(%d) [RESUME] Using cached exact row stats for %s: source=%d, dest=%d", logThreadSeq, schemaTable, sourceExactCount, targetExactCount))
		}
	}
	if !exactCached {
		sourceExactCount, sourceCountExact, targetExactCount, targetCountExact = sp.getExactRowCountsParallel(sp.sourceSchema, sp.table, sp.destSchema, destTable, logThreadSeq)
		if sp.ChecksumProgress != nil {
			_ = sp.ChecksumProgress.SetTableExactRowStats(schemaTable, sourceExactCount, targetExactCount, sourceCountExact, targetCountExact)
		}
	}
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

	// 必须在 AbnormalDataDispos goroutine 启动之前设置 rollCC，
	// 否则 goroutine 内部读取 sp.rollCC 时可能看到 nil（竞态）。
	rollDone := sp.startRollbackDispos(queueDepth, logThreadSeq)
	go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
	sp.DataFixDispos(fixSQL, logThreadSeq)
	if rollDone != nil {
		<-rollDone
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
