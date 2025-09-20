package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"strings"
)

/*
检查当前用户对该库表是否有响应的权限（权限包括：查询权限，flush_tables,session_variables_admin）
*/
func (stcls *schemaTable) GlobalAccessPriCheck(logThreadSeq, logThreadSeq2 int64) bool {
	var (
		vlog                   string
		err                    error
		StableList, DtableList bool
	)
	vlog = fmt.Sprintf("(%d) Retrieving global privileges for source and target databases", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Datafix: stcls.datefix}
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for srcDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().GlobalAccessPri(stcls.sourceDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) Source database global privileges checksum result: %v", logThreadSeq, StableList)
	global.Wlog.Debug(vlog)
	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)

	if DtableList, err = tc.Query().GlobalAccessPri(stcls.destDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) Target database global privileges checksum result: %v", logThreadSeq, DtableList)
	global.Wlog.Debug(vlog)
	if StableList && DtableList {
		vlog = fmt.Sprintf("(%d) Global privileges checksum completed for both databases", logThreadSeq)
		global.Wlog.Info(vlog)
		return true
	}
	vlog = fmt.Sprintf("(%d) Insufficient global privileges detected, operation terminated", logThreadSeq)
	global.Wlog.Error(vlog)
	return false
}
func (stcls *schemaTable) TableAccessPriCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		err                                  error
		StableList, DtableList               map[string]int
		newCheckTableList, abnormalTableList []string
	)
	vlog = fmt.Sprintf("(%d) Retrieving table access privileges for both databases", logThreadSeq)
	global.Wlog.Info(vlog)

	// 添加调试日志，显示传入的表列表
	vlog = fmt.Sprintf("Table access check options received: %v", checkTableList)
	global.Wlog.Debug(vlog)

	// 处理映射关系的表列表
	var processedTableList []string
	for _, tableEntry := range checkTableList {
		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(tableEntry, ":") {
			parts := strings.Split(tableEntry, ":")
			if len(parts) == 2 {
				// 只使用源端表名进行权限检查
				processedTableList = append(processedTableList, parts[0])
			} else {
				processedTableList = append(processedTableList, tableEntry)
			}
		} else {
			processedTableList = append(processedTableList, tableEntry)
		}
	}

	vlog = fmt.Sprintf("Processed table list for access checksum: %v", processedTableList)
	global.Wlog.Debug(vlog)

	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for srcDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().TableAccessPriCheck(stcls.sourceDB, processedTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(StableList) == 0 {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for srcDB check failed: {%v}.", logThreadSeq, StableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Source database table access checksum completed: %v", logThreadSeq, StableList)
		global.Wlog.Debug(vlog)
	}

	// 处理目标端表名
	var destTableList []string
	for _, tableEntry := range checkTableList {
		// 检查是否包含映射关系（格式为 sourceSchema.sourceTable:destSchema.destTable）
		if strings.Contains(tableEntry, ":") {
			parts := strings.Split(tableEntry, ":")
			if len(parts) == 2 {
				// 使用目标端表名进行权限检查
				destTableList = append(destTableList, parts[1])
			} else {
				destTableList = append(destTableList, tableEntry)
			}
		} else {
			destTableList = append(destTableList, tableEntry)
		}
	}

	vlog = fmt.Sprintf("Destination table list for permission check: %v", destTableList)
	global.Wlog.Debug(vlog)

	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if DtableList, err = tc.Query().TableAccessPriCheck(stcls.destDB, destTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(DtableList) == 0 {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for dstDB check failed: {%v}.", logThreadSeq, DtableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Target database table access checksum completed: %v", logThreadSeq, DtableList)
		global.Wlog.Debug(vlog)
	}

	vlog = fmt.Sprintf("(%d) Start checking the differences between the tables in srcDB and dstDB", logThreadSeq)
	global.Wlog.Debug(vlog)

	// 创建映射关系表，用于将源表名映射到目标表名
	tableMapping := make(map[string]string)
	for _, tableEntry := range checkTableList {
		if strings.Contains(tableEntry, ":") {
			parts := strings.Split(tableEntry, ":")
			if len(parts) == 2 {
				tableMapping[parts[0]] = parts[1]
			}
		}
	}

	// 检查权限并保持映射关系
	for k, _ := range StableList {
		// 查找对应的目标表名
		destTableName := k
		if mappedName, exists := tableMapping[k]; exists {
			destTableName = mappedName
		}

		if _, ok := DtableList[destTableName]; ok {
			// 保持原始的映射关系
			originalEntry := k
			for _, entry := range checkTableList {
				if strings.HasPrefix(entry, k+":") || entry == k {
					originalEntry = entry
					break
				}
			}
			newCheckTableList = append(newCheckTableList, originalEntry)
		} else {
			abnormalTableList = append(abnormalTableList, k)
		}
	}

	vlog = fmt.Sprintf("(%d) Table access checksum completed - Consistent tables: %d (%s), Inconsistent tables: %d (%s)", logThreadSeq, len(newCheckTableList), newCheckTableList, len(abnormalTableList), abnormalTableList)
	global.Wlog.Info(vlog)
	return newCheckTableList, abnormalTableList, nil
}
