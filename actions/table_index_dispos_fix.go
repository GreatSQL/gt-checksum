package actions

import (
	"fmt"
	"gt-checksum/global"
	"os"
	"strings"
	"sync"
)

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
	if strings.HasPrefix(sqlTrim, "TRUNCATE") {
		return "TRUNCATE"
	}
	return ""
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
