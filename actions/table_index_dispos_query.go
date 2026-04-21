package actions

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/utils"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)


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
