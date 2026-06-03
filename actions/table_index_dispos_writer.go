package actions

import (
	"bufio"
	"fmt"
	"gt-checksum/global"
	"io"
	"os"
	"strings"
)

type sqlRollingWriter struct {
	datafixType string
	ddrive      string
	djdbc       string
	logThread   int64
	fixTrxNum   int

	maxStmt  int
	maxBytes int64

	pathFunc func(fileSeq int) (string, bool)

	// startFileSeq 用于 resume 模式：writer 从此序号之后开始编号，
	// 跳过已完成 chunk 写入的文件，避免覆盖或追加到已有完整文件。
	startFileSeq int
	fileSeq      int
	current      *os.File
	currentCnt   int
	currentB     int64
}

func (w *sqlRollingWriter) ensureFile() error {
	if w.current != nil {
		return nil
	}
	if w.fileSeq == 0 && w.startFileSeq > 0 {
		w.fileSeq = w.startFileSeq
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

func (w *sqlRollingWriter) FileSeq() int {
	return w.fileSeq
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
		if err := writeOptimizedSqlChunk(part, w.datafixType, w.current, w.ddrive, w.djdbc, w.logThread, w.fixTrxNum); err != nil {
			return err
		}
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
	startSeq := 0
	if sp.resumeFixFileSeqs != nil {
		if sqlType == "DELETE" {
			startSeq = sp.resumeFixFileSeqs["DELETE"]
		} else {
			startSeq = sp.resumeFixFileSeqs["INSERT"]
		}
	}
	return &sqlRollingWriter{
		datafixType:  sp.datafixType,
		ddrive:       sp.ddrive,
		djdbc:        sp.djdbc,
		logThread:    logThreadSeq,
		fixTrxNum:    sp.fixTrxNum,
		maxStmt:      maxStmtPerFile,
		maxBytes:     maxFileSizeBytes,
		pathFunc:     pathFunc,
		startFileSeq: startSeq,
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

func writeOptimizedSqlChunk(sqls []string, datafixType string, sfile *os.File, ddrive string, djdbc string, logThreadSeq int64, fixTrxNum int) error {
	if len(sqls) == 0 {
		return nil
	}
	return ApplyDataFixWithTrxNumOptimizedInput(sqls, datafixType, sfile, ddrive, djdbc, logThreadSeq, fixTrxNum)
}

// processBatch 批量处理SQL语句，根据类型排序后写入文件
func processBatch(sqls []string, datafixType string, sfile *os.File, ddrive string, djdbc string, logThreadSeq int64, fixTrxNum int, isUniqueKey bool, deleteSqlSize int, insertSqlSize int) {
	if len(sqls) == 0 {
		return
	}
	finalSqls := optimizeSqlStatements(sqls, fixTrxNum, isUniqueKey, deleteSqlSize, insertSqlSize)
	if err := writeOptimizedSqlChunk(finalSqls, datafixType, sfile, ddrive, djdbc, logThreadSeq, fixTrxNum); err != nil {
		global.Wlog.Error(fmt.Sprintf("DEBUG_BATCH_WRITE_%d: failed to write SQL batch: %v", logThreadSeq, err))
	}
	global.Wlog.Debugf("DEBUG_BATCH_WRITE_%d: Wrote %d SQL statements to file, DELETE=%d, INSERT=%d\n",
		logThreadSeq, len(finalSqls), len(sqls), len(finalSqls))
}

// newRollbackSQLRollingWriter creates a rolling writer for rollback SQL files.
// sqlType is "INSERT" (rollback for DELETE fix) or "DELETE" (rollback for INSERT fix).
// File naming: rollsql/table.schema.table.rollback-TYPE-seq.sql
func (sp *SchedulePlan) newRollbackSQLRollingWriter(sqlType string, maxStmtPerFile int, maxFileSizeBytes int64, logThreadSeq int64) *sqlRollingWriter {
	pathFunc := func(fileSeq int) (string, bool) {
		fixSchema := sp.destSchema
		if fixSchema == "" {
			fixSchema = sp.schema
		}
		fixTable := sp.getDestTableName()
		return fmt.Sprintf("%s/table.%s.%s.rollback-%s-%d.sql",
			sp.rollSqlDir, fixFileNameEncode(fixSchema), fixFileNameEncode(fixTable), sqlType, fileSeq), false
	}
	startSeq := 0
	if sp.resumeFixFileSeqs != nil {
		startSeq = sp.resumeFixFileSeqs["rollback-"+sqlType]
	}
	return &sqlRollingWriter{
		datafixType:  "file",
		ddrive:       sp.ddrive,
		djdbc:        sp.djdbc,
		logThread:    logThreadSeq,
		fixTrxNum:    sp.fixTrxNum,
		maxStmt:      maxStmtPerFile,
		maxBytes:     maxFileSizeBytes,
		pathFunc:     pathFunc,
		startFileSeq: startSeq,
	}
}
