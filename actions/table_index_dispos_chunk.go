package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"regexp"
	"strconv"
	"strings"
)

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
