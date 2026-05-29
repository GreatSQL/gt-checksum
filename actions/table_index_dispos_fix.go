package actions

import (
	"fmt"
	"gt-checksum/global"
	"os"
	"sort"
	"strings"
	"sync"
)

func (sp *SchedulePlan) DataFixDispos(fixSQL chanFixSQLItem, logThreadSeq int64) {
	var (
		vlog        string
		deleteCount int
		insertCount int
	)
	pendingSQLByChunk := make(map[int64]int)
	doneChunks := make(map[int64]bool)
	chunkFileSeqs := make(map[int64]map[string]map[int]struct{})
	schemaTable := fmt.Sprintf("%s.%s", sp.schema, sp.table)
	markChunkIfSafe := func(chunkSeq int64) {
		if sp.ChecksumProgress == nil || chunkSeq < 0 || !doneChunks[chunkSeq] || pendingSQLByChunk[chunkSeq] > 0 {
			return
		}
		fileMapping := buildFileMappingForChunk(chunkFileSeqs, chunkSeq)
		if len(fileMapping) > 0 {
			if err := sp.ChecksumProgress.MarkChunkFixSQLCompletedWithFiles(schemaTable, chunkSeq, fileMapping); err != nil {
				global.Wlog.Warn(fmt.Sprintf("(%d) [RESUME] Failed to mark chunk %d fixsql completed for %s: %v", logThreadSeq, chunkSeq, schemaTable, err))
			}
		} else {
			if err := sp.ChecksumProgress.MarkChunkFixSQLCompleted(schemaTable, chunkSeq); err != nil {
				global.Wlog.Warn(fmt.Sprintf("(%d) [RESUME] Failed to mark chunk %d fixsql completed for %s: %v", logThreadSeq, chunkSeq, schemaTable, err))
			}
		}
		delete(doneChunks, chunkSeq)
		delete(pendingSQLByChunk, chunkSeq)
		delete(chunkFileSeqs, chunkSeq)
	}
	markItemsWritten := func(items []fixSQLItem) {
		for _, item := range items {
			if item.ChunkSeq < 0 {
				continue
			}
			if pendingSQLByChunk[item.ChunkSeq] > 0 {
				pendingSQLByChunk[item.ChunkSeq]--
			}
			markChunkIfSafe(item.ChunkSeq)
		}
	}
	itemsToSQL := func(items []fixSQLItem) []string {
		sqls := make([]string, 0, len(items))
		for _, item := range items {
			if item.SQL != "" {
				sqls = append(sqls, item.SQL)
			}
		}
		return sqls
	}

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

	processDeleteBatch := func(batch []fixSQLItem) error {
		optimized := optimizeSqlStatements(itemsToSQL(batch), sp.fixTrxNum, isUniqueKey, sp.deleteSqlSize, sp.insertSqlSize)
		if len(optimized) == 0 {
			markItemsWritten(batch)
			return nil
		}
		if sp.datafixType == "table" {
			writeOptimizedSqlChunk(optimized, sp.datafixType, nil, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			markItemsWritten(batch)
			return nil
		}
		beforeSeq := deleteWriter.FileSeq()
		if err := deleteWriter.write(optimized); err != nil {
			return err
		}
		afterSeq := deleteWriter.FileSeq()
		recordBatchChunkFileSeqs(chunkFileSeqs, batch, "DELETE", beforeSeq, afterSeq)
		markItemsWritten(batch)
		return nil
	}
	processInsertBatch := func(batch []fixSQLItem) error {
		optimized := optimizeSqlStatements(itemsToSQL(batch), sp.fixTrxNum, false, sp.deleteSqlSize, sp.insertSqlSize)
		if len(optimized) == 0 {
			markItemsWritten(batch)
			return nil
		}
		if sp.datafixType == "table" {
			writeOptimizedSqlChunk(optimized, sp.datafixType, nil, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			markItemsWritten(batch)
			return nil
		}
		beforeSeq := insertWriter.FileSeq()
		if err := insertWriter.write(optimized); err != nil {
			return err
		}
		afterSeq := insertWriter.FileSeq()
		recordBatchChunkFileSeqs(chunkFileSeqs, batch, "INSERT", beforeSeq, afterSeq)
		markItemsWritten(batch)
		return nil
	}
	processUpdateBatch := func(batch []fixSQLItem) error {
		sqls := itemsToSQL(batch)
		if len(sqls) == 0 {
			markItemsWritten(batch)
			return nil
		}
		if sp.datafixType == "table" {
			writeOptimizedSqlChunk(sqls, sp.datafixType, nil, sp.ddrive, sp.djdbc, logThreadSeq, sp.fixTrxNum)
			markItemsWritten(batch)
			return nil
		}
		if err := updateWriter.write(sqls); err != nil {
			return err
		}
		markItemsWritten(batch)
		return nil
	}

	global.Wlog.Info(fmt.Sprintf("(%d) Writing per-object fixsql for %s.%s",
		logThreadSeq, sp.schema, sp.table))
	deleteBatch := make([]fixSQLItem, 0, stageBatchStmt)
	insertBatch := make([]fixSQLItem, 0, stageBatchStmt)
	updateBatch := make([]fixSQLItem, 0, stageBatchStmt)
	var deleteBatchBytes int64
	var insertBatchBytes int64
	var updateBatchBytes int64
	var updateCount int

	flushDelete := func() {
		if len(deleteBatch) == 0 {
			return
		}
		batch := append([]fixSQLItem(nil), deleteBatch...)
		if err := processDeleteBatch(batch); err != nil {
			sp.getErr(fmt.Sprintf("Failed streaming DELETE fixsql for %s.%s", sp.schema, sp.table), err)
		}
		deleteBatch = deleteBatch[:0]
		deleteBatchBytes = 0
	}
	flushInsert := func() {
		if len(insertBatch) == 0 {
			return
		}
		batch := append([]fixSQLItem(nil), insertBatch...)
		if err := processInsertBatch(batch); err != nil {
			sp.getErr(fmt.Sprintf("Failed streaming INSERT fixsql for %s.%s", sp.schema, sp.table), err)
		}
		insertBatch = insertBatch[:0]
		insertBatchBytes = 0
	}
	flushUpdate := func() {
		if len(updateBatch) == 0 {
			return
		}
		batch := append([]fixSQLItem(nil), updateBatch...)
		if err := processUpdateBatch(batch); err != nil {
			sp.getErr(fmt.Sprintf("Failed streaming UPDATE fixsql for %s.%s", sp.schema, sp.table), err)
		}
		updateBatch = updateBatch[:0]
		updateBatchBytes = 0
	}

	for item := range fixSQL {
		if item.Done {
			doneChunks[item.ChunkSeq] = true
			markChunkIfSafe(item.ChunkSeq)
			continue
		}
		sqlType := detectFixSQLType(item.SQL)
		if sqlType == "" {
			continue
		}
		pendingSQLByChunk[item.ChunkSeq]++
		sp.pods.DIFFS = "yes"
		sqlBytes := int64(len(item.SQL) + 1)
		switch sqlType {
		case "DELETE":
			if len(deleteBatch) > 0 && (len(deleteBatch) >= stageBatchStmt || deleteBatchBytes+sqlBytes > stageBatchBytes) {
				flushDelete()
			}
			deleteBatch = append(deleteBatch, item)
			deleteBatchBytes += sqlBytes
			deleteCount++
		case "INSERT":
			if len(insertBatch) > 0 && (len(insertBatch) >= stageBatchStmt || insertBatchBytes+sqlBytes > stageBatchBytes) {
				flushInsert()
			}
			insertBatch = append(insertBatch, item)
			insertBatchBytes += sqlBytes
			insertCount++
		case "UPDATE":
			if len(updateBatch) > 0 && (len(updateBatch) >= stageBatchStmt || updateBatchBytes+sqlBytes > stageBatchBytes) {
				flushUpdate()
			}
			updateBatch = append(updateBatch, item)
			updateBatchBytes += sqlBytes
			updateCount++
		}
	}
	flushDelete()
	flushInsert()
	flushUpdate()
	for chunkSeq := range doneChunks {
		markChunkIfSafe(chunkSeq)
	}

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
	if strings.HasPrefix(sqlTrim, "TRUNCATE") {
		return "TRUNCATE"
	}
	return ""
}

func recordBatchChunkFileSeqs(m map[int64]map[string]map[int]struct{}, batch []fixSQLItem, sqlType string, beforeSeq, afterSeq int) {
	startSeq := beforeSeq
	if startSeq == 0 {
		startSeq = afterSeq
	}
	if startSeq == 0 {
		return
	}
	for _, item := range batch {
		if item.ChunkSeq < 0 {
			continue
		}
		if m[item.ChunkSeq] == nil {
			m[item.ChunkSeq] = make(map[string]map[int]struct{})
		}
		if m[item.ChunkSeq][sqlType] == nil {
			m[item.ChunkSeq][sqlType] = make(map[int]struct{})
		}
		for seq := startSeq; seq <= afterSeq; seq++ {
			if seq > 0 {
				m[item.ChunkSeq][sqlType][seq] = struct{}{}
			}
		}
	}
}

func buildFileMappingForChunk(m map[int64]map[string]map[int]struct{}, chunkSeq int64) map[string][]int {
	entry, ok := m[chunkSeq]
	if !ok || len(entry) == 0 {
		return nil
	}
	result := make(map[string][]int, len(entry))
	for sqlType, seqSet := range entry {
		seqs := make([]int, 0, len(seqSet))
		for seq := range seqSet {
			seqs = append(seqs, seq)
		}
		sort.Ints(seqs)
		result[sqlType] = seqs
	}
	return result
}

// RollbackDispos consumes rollback SQL from rollCC and writes them to rollback SQL files.
// It mirrors DataFixDispos but targets sp.rollSqlDir.
// TRUNCATE statements are written directly to a dedicated file; INSERT/DELETE use rolling writers.
func (sp *SchedulePlan) RollbackDispos(rollCC chanString, logThreadSeq int64) {
	vlog := fmt.Sprintf("(%d) Writing rollback SQL for %s.%s", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)

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
	if stageBatchBytes > 32*1024*1024 {
		stageBatchBytes = 32 * 1024 * 1024
	}

	// 判断是否有主键或唯一索引，决定是否可以合并 DELETE 语句
	isUniqueKey := strings.HasPrefix(sp.indexColumnType, "pri_") || strings.HasPrefix(sp.indexColumnType, "uni_")

	// INSERT writer: rollback for DELETE fix (i.e. re-insert deleted rows)
	insertWriter := sp.newRollbackSQLRollingWriter("INSERT", maxStmtPerFile, maxFileSizeBytes, logThreadSeq)
	// DELETE writer: rollback for INSERT fix (i.e. delete inserted rows)
	deleteWriter := sp.newRollbackSQLRollingWriter("DELETE", maxStmtPerFile, maxFileSizeBytes, logThreadSeq)
	defer insertWriter.close()
	defer deleteWriter.close()

	// 批量收集和优化回滚 SQL
	deleteBatch := make([]string, 0, stageBatchStmt)
	insertBatch := make([]string, 0, stageBatchStmt)
	var deleteBatchBytes, insertBatchBytes int64

	processDeleteBatch := func(batch []string) error {
		toProcess := batch
		if !isUniqueKey {
			// 无主键/唯一键表：合并相同 WHERE 条件的 DELETE LIMIT 语句，累加 LIMIT 值
			toProcess = mergeDuplicateDeleteLimits(batch)
		}
		optimized := optimizeSqlStatements(toProcess, sp.fixTrxNum, isUniqueKey, sp.deleteSqlSize, sp.insertSqlSize)
		if len(optimized) == 0 {
			return nil
		}
		return deleteWriter.write(optimized)
	}

	processInsertBatch := func(batch []string) error {
		optimized := optimizeSqlStatements(batch, sp.fixTrxNum, false, sp.deleteSqlSize, sp.insertSqlSize)
		if len(optimized) == 0 {
			return nil
		}
		return insertWriter.write(optimized)
	}

	flushDelete := func() {
		if len(deleteBatch) == 0 {
			return
		}
		if err := processDeleteBatch(deleteBatch); err != nil {
			sp.getErr(fmt.Sprintf("Failed writing rollback DELETE for %s.%s", sp.schema, sp.table), err)
		}
		deleteBatch = deleteBatch[:0]
		deleteBatchBytes = 0
	}

	flushInsert := func() {
		if len(insertBatch) == 0 {
			return
		}
		if err := processInsertBatch(insertBatch); err != nil {
			sp.getErr(fmt.Sprintf("Failed writing rollback INSERT for %s.%s", sp.schema, sp.table), err)
		}
		insertBatch = insertBatch[:0]
		insertBatchBytes = 0
	}

	var deleteCount, insertCount, truncateCount int

	for v := range rollCC {
		sqlType := detectFixSQLType(v)
		sqlBytes := int64(len(v) + 1)
		switch sqlType {
		case "TRUNCATE":
			// Write TRUNCATE to a dedicated file (one-shot, not rolling)
			truncatePath := fmt.Sprintf("%s/table.%s.%s.rollback-TRUNCATE-1.sql",
				sp.rollSqlDir,
				fixFileNameEncode(func() string {
					if sp.destSchema != "" {
						return sp.destSchema
					}
					return sp.schema
				}()),
				fixFileNameEncode(sp.getDestTableName()))
			f, err := os.OpenFile(truncatePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				sp.getErr(fmt.Sprintf("Failed to open rollback TRUNCATE file for %s.%s", sp.schema, sp.table), err)
				continue
			}
			fmt.Fprintln(f, v)
			f.Close()
			truncateCount++
		case "INSERT":
			if len(insertBatch) > 0 && (len(insertBatch) >= stageBatchStmt || insertBatchBytes+sqlBytes > stageBatchBytes) {
				flushInsert()
			}
			insertBatch = append(insertBatch, v)
			insertBatchBytes += sqlBytes
			insertCount++
		case "DELETE":
			if len(deleteBatch) > 0 && (len(deleteBatch) >= stageBatchStmt || deleteBatchBytes+sqlBytes > stageBatchBytes) {
				flushDelete()
			}
			deleteBatch = append(deleteBatch, v)
			deleteBatchBytes += sqlBytes
			deleteCount++
		}
	}
	flushDelete()
	flushInsert()

	if deleteCount > 0 || insertCount > 0 || truncateCount > 0 {
		global.Wlog.Infof("(%d) Rollback SQL written for %s.%s: DELETE=%d, INSERT=%d, TRUNCATE=%d",
			logThreadSeq, sp.schema, sp.table, deleteCount, insertCount, truncateCount)
	}
}
