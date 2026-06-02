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
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Datafix: stcls.datafix}
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for srcDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().GlobalAccessPri(stcls.sourceDB, "source", logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) Source database global privileges checksum result: %v", logThreadSeq, StableList)
	global.Wlog.Debug(vlog)
	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)

	if DtableList, err = tc.Query().GlobalAccessPri(stcls.destDB, "dest", logThreadSeq2); err != nil {
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

func accessPriSplitMappedTableEntry(tableEntry string) (string, string) {
	tableEntry = strings.TrimSpace(tableEntry)
	parts := strings.SplitN(tableEntry, ":", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return tableEntry, tableEntry
}

func accessPriSplitSchemaTable(tableName string) (string, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(tableName), ".", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func accessPriSchemaWildcard(pattern string) (string, bool) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasSuffix(pattern, ".*") {
		return "", false
	}
	schema := strings.TrimSpace(strings.TrimSuffix(pattern, ".*"))
	if schema == "" || strings.ContainsAny(schema, "*%") {
		return "", false
	}
	return schema, true
}

func (stcls *schemaTable) accessPriTablePatterns() string {
	if strings.TrimSpace(stcls.rawTables) != "" {
		return stcls.rawTables
	}
	return stcls.table
}

func (stcls *schemaTable) sourceWildcardAccessSchemas() map[string]int {
	wildcardSchemas := make(map[string]int)
	for _, tablePattern := range strings.Split(stcls.accessPriTablePatterns(), ",") {
		tablePattern = strings.TrimSpace(tablePattern)
		if tablePattern == "" {
			continue
		}
		if strings.Contains(tablePattern, ":") {
			parts := strings.SplitN(tablePattern, ":", 2)
			if sourceSchema, ok := accessPriSchemaWildcard(parts[0]); ok {
				wildcardSchemas[sourceSchema]++
			}
			continue
		}
		if sourceSchema, ok := accessPriSchemaWildcard(tablePattern); ok {
			wildcardSchemas[sourceSchema]++
		}
	}
	return wildcardSchemas
}

func (stcls *schemaTable) destWildcardAccessSchemas() map[string]int {
	wildcardSchemas := make(map[string]int)
	for _, tablePattern := range strings.Split(stcls.accessPriTablePatterns(), ",") {
		tablePattern = strings.TrimSpace(tablePattern)
		if tablePattern == "" {
			continue
		}
		if strings.Contains(tablePattern, ":") {
			parts := strings.SplitN(tablePattern, ":", 2)
			if destSchema, ok := accessPriSchemaWildcard(parts[1]); ok {
				wildcardSchemas[destSchema]++
			}
			continue
		}
		if destSchema, ok := accessPriSchemaWildcard(tablePattern); ok {
			wildcardSchemas[destSchema]++
		}
	}
	return wildcardSchemas
}

func (stcls *schemaTable) accessPriSchemaSelected(schemaSet map[string]int, schema string, dest bool) bool {
	for selectedSchema := range schemaSet {
		if dest {
			if stcls.destObjectNameEqual(selectedSchema, schema) {
				return true
			}
			continue
		}
		if stcls.sourceObjectNameEqual(selectedSchema, schema) {
			return true
		}
	}
	return false
}

func (stcls *schemaTable) accessPriTableNameEqual(a, b string, dest bool) bool {
	aSchema, aTable, aOK := accessPriSplitSchemaTable(a)
	bSchema, bTable, bOK := accessPriSplitSchemaTable(b)
	if aOK && bOK {
		if dest {
			return stcls.destObjectNameEqual(aSchema, bSchema) && stcls.destObjectNameEqual(aTable, bTable)
		}
		return stcls.sourceObjectNameEqual(aSchema, bSchema) && stcls.sourceObjectNameEqual(aTable, bTable)
	}
	if dest {
		return stcls.destObjectNameEqual(a, b)
	}
	return stcls.sourceObjectNameEqual(a, b)
}

func (stcls *schemaTable) appendAccessPriTableOnce(tableList []string, tableName string, dest bool) []string {
	for _, existing := range tableList {
		if stcls.accessPriTableNameEqual(existing, tableName, dest) {
			return tableList
		}
	}
	return append(tableList, tableName)
}

func (stcls *schemaTable) compressAccessCheckListForWildcardSchemas(tableList []string, wildcardSchemas map[string]int, dest bool) []string {
	if len(wildcardSchemas) == 0 {
		return tableList
	}
	compressedTableList := make([]string, 0, len(tableList))
	for _, tableName := range tableList {
		schemaName, _, ok := accessPriSplitSchemaTable(tableName)
		if ok && stcls.accessPriSchemaSelected(wildcardSchemas, schemaName, dest) {
			compressedTableList = stcls.appendAccessPriTableOnce(compressedTableList, fmt.Sprintf("%s.*", schemaName), dest)
			continue
		}
		compressedTableList = stcls.appendAccessPriTableOnce(compressedTableList, tableName, dest)
	}
	return compressedTableList
}

func (stcls *schemaTable) privilegeMapCoversTable(privilegeMap map[string]int, tableName string, dest bool) bool {
	if len(privilegeMap) == 0 {
		return false
	}
	schemaName, _, hasSchema := accessPriSplitSchemaTable(tableName)
	wildcardTableName := ""
	if hasSchema {
		wildcardTableName = fmt.Sprintf("%s.*", schemaName)
	}
	for grantedTable := range privilegeMap {
		if stcls.accessPriTableNameEqual(grantedTable, tableName, dest) {
			return true
		}
		if wildcardTableName != "" && stcls.accessPriTableNameEqual(grantedTable, wildcardTableName, dest) {
			return true
		}
	}
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

	sourcePrivilegeCheckList := stcls.compressAccessCheckListForWildcardSchemas(processedTableList, stcls.sourceWildcardAccessSchemas(), false)
	vlog = fmt.Sprintf("Source table list for permission check: %v", sourcePrivilegeCheckList)
	global.Wlog.Debug(vlog)

	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for srcDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().TableAccessPriCheck(stcls.sourceDB, sourcePrivilegeCheckList, stcls.checkRules.CheckObject, stcls.datafix, "source", logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(StableList) == 0 {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for srcDB check failed: {%v}. See Q_Table_Access_Pri logs above for required privileges, missing privileges and suggested GRANT statements.", logThreadSeq, StableList)
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

	destPrivilegeCheckList := stcls.compressAccessCheckListForWildcardSchemas(destTableList, stcls.destWildcardAccessSchemas(), true)

	vlog = fmt.Sprintf("Destination table list for permission check: %v", destPrivilegeCheckList)
	global.Wlog.Debug(vlog)

	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if DtableList, err = tc.Query().TableAccessPriCheck(stcls.destDB, destPrivilegeCheckList, stcls.checkRules.CheckObject, stcls.datafix, "dest", logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(DtableList) == 0 {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for dstDB check failed: {%v}. See Q_Table_Access_Pri logs above for required privileges, missing privileges and suggested GRANT statements.", logThreadSeq, DtableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Target database table access checksum completed: %v", logThreadSeq, DtableList)
		global.Wlog.Debug(vlog)
	}

	vlog = fmt.Sprintf("(%d) Start checking the differences between the tables in srcDB and dstDB", logThreadSeq)
	global.Wlog.Debug(vlog)

	// 按原始候选表顺序检查权限并保持映射关系，避免 map 遍历导致顺序不稳定。
	for _, tableEntry := range checkTableList {
		sourceTableName, destTableName := accessPriSplitMappedTableEntry(tableEntry)
		if !stcls.privilegeMapCoversTable(StableList, sourceTableName, false) {
			abnormalTableList = append(abnormalTableList, sourceTableName)
			continue
		}
		if !stcls.privilegeMapCoversTable(DtableList, destTableName, true) {
			abnormalTableList = append(abnormalTableList, destTableName)
			continue
		}
		newCheckTableList = append(newCheckTableList, tableEntry)
	}

	vlog = fmt.Sprintf("(%d) Table access checksum completed - Consistent tables: %d (%s), Inconsistent tables: %d (%s)", logThreadSeq, len(newCheckTableList), newCheckTableList, len(abnormalTableList), abnormalTableList)
	global.Wlog.Info(vlog)
	return newCheckTableList, abnormalTableList, nil
}
