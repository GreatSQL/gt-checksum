package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"math/rand"
	"os"
	"strings"
)

func (sp *SchedulePlan) getErr(msg string, err error) {
	if err != nil {
		fmt.Println(err, ":", msg)
		os.Exit(1)
	}
}

/*
使用count(1)的方式进行校验
*/
func (sp *SchedulePlan) DoCountDataCheck() {
	var (
		sourceSchema, sourceTable      string
		destSchema, destTable          string
		stmpTableCount, dtmpTableCount uint64
		err                            error
		vlog                           string
	)
	logThreadSeq := rand.Int63()
	vlog = fmt.Sprintf("(%d) Starting table row count checksum", logThreadSeq)
	global.Wlog.Info(vlog)

	// 添加调试日志，显示表索引映射
	vlog = fmt.Sprintf("Table index column mapping options: %v", sp.tableIndexColumnMap)
	global.Wlog.Debug(vlog)

	for k, v := range sp.tableIndexColumnMap {
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}

		// 解析键值，提取源表和目标表信息
		sourceSchema = ""
		sourceTable = ""
		destSchema = ""
		destTable = ""

		// 检查是否包含映射信息
		if strings.Contains(k, "/*mapping*/") {
			// 新格式: "sourceSchema/*gtchecksumSchemaTable*/sourceTable/*mapping*/destSchema/*mappingTable*/destTable"
			// 或者: "sourceSchema/*gtchecksumSchemaTable*/sourceTable/*indexColumnType*/indexType/*mapping*/destSchema/*mappingTable*/destTable"

			ki := strings.Split(k, "/*indexColumnType*/")[0]
			if strings.Contains(ki, "/*gtchecksumSchemaTable*/") {
				sourceSchema = strings.Split(ki, "/*gtchecksumSchemaTable*/")[0]
				remainingPart := strings.Split(ki, "/*gtchecksumSchemaTable*/")[1]

				if strings.Contains(remainingPart, "/*mapping*/") {
					sourceTable = strings.Split(remainingPart, "/*mapping*/")[0]
					mappingPart := strings.Split(remainingPart, "/*mapping*/")[1]

					if strings.Contains(mappingPart, "/*mappingTable*/") {
						destSchema = strings.Split(mappingPart, "/*mappingTable*/")[0]
						destTable = strings.Split(mappingPart, "/*mappingTable*/")[1]
					}
				}
			}
		} else {
			// 旧格式处理
			ki := strings.Split(k, "/*indexColumnType*/")[0]
			if strings.Contains(ki, "/*gtchecksumSchemaTable*/") {
				sourceSchema = strings.Split(ki, "/*gtchecksumSchemaTable*/")[0]
				sourceTable = strings.Split(ki, "/*gtchecksumSchemaTable*/")[1]
				destSchema = sourceSchema
				destTable = sourceTable
			}
		}

		// 如果解析失败，跳过此项
		if sourceSchema == "" || sourceTable == "" {
			vlog = fmt.Sprintf("(%d) Unable to parse table information from key: %s", logThreadSeq, k)
			global.Wlog.Warn(vlog)
			continue
		}

		// 如果目标表信息为空，使用源表信息
		if destSchema == "" || destTable == "" {
			destSchema = sourceSchema
			destTable = sourceTable
		}

		// 构建显示名称，包含映射关系
		displayTableName := sourceSchema + "." + sourceTable
		if sourceSchema != destSchema || sourceTable != destTable {
			displayTableName = fmt.Sprintf("%s.%s:%s.%s", sourceSchema, sourceTable, destSchema, destTable)
		}

		vlog = fmt.Sprintf("(%d) Initializing row count checksum for table %s", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)

		sdb := sp.sdbPool.Get(logThreadSeq)
		//查询源端的表总行数
		idxc := dbExec.IndexColumnStruct{Schema: sourceSchema, Table: sourceTable, ColumnName: sp.columnName, Drivce: sp.sdrive}
		stmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(sdb, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Failed to retrieve source table row count: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}
		sp.sdbPool.Put(sdb, logThreadSeq)

		ddb := sp.ddbPool.Get(logThreadSeq)
		//查询目标端的表总行数
		idxcDest := dbExec.IndexColumnStruct{Schema: destSchema, Table: destTable, ColumnName: sp.columnName, Drivce: sp.ddrive}
		dtmpTableCount, err = idxcDest.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Failed to retrieve target table row count: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return
		}
		sp.ddbPool.Put(ddb, logThreadSeq)

		//输出校验结果信息
		var pods = Pod{
			Schema:      sourceSchema,
			Table:       sourceTable,
			CheckObject: sp.checkObject,
		}

		// 如果存在映射关系，将映射信息添加到表名中
		if sourceSchema != destSchema || sourceTable != destTable {
			pods.Table = fmt.Sprintf("%s:%s.%s", pods.Table, destSchema, destTable)
		}

		vlog = fmt.Sprintf("(%d) Verifying row counts for table %s", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)

		if stmpTableCount == dtmpTableCount {
			vlog = fmt.Sprintf("(%d) Row counts match for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			pods.DIFFS = "no"
			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		} else {
			vlog = fmt.Sprintf("(%d) Row counts differ for table %s", logThreadSeq, displayTableName)
			global.Wlog.Debug(vlog)
			pods.DIFFS = "yes"
			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		}

		measuredDataPods = append(measuredDataPods, pods)
		vlog = fmt.Sprintf("(%d) Row count checksum completed for table %s", logThreadSeq, displayTableName)
		global.Wlog.Debug(vlog)
	}

	vlog = fmt.Sprintf("(%d) All table row counts checksum completed", logThreadSeq)
	global.Wlog.Info(vlog)
}
