package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
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
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for both the srcDB and dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Datafix: stcls.datefix}
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for srcDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().GlobalAccessPri(stcls.sourceDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) The global privileges for srcDB check completed: {%v}.", logThreadSeq, StableList)
	global.Wlog.Debug(vlog)
	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Obtain the global privileges for dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)

	if DtableList, err = tc.Query().GlobalAccessPri(stcls.destDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) The global privileges for dstDB check completed: {%v}.", logThreadSeq, DtableList)
	global.Wlog.Debug(vlog)
	if StableList && DtableList {
		vlog = fmt.Sprintf("(%d) The global privileges for both srcDB and dstDB are check completed", logThreadSeq)
		global.Wlog.Info(vlog)
		return true
	}
	vlog = fmt.Sprintf("(%d) Insufficient global privileges for srcDB or dstDB, unable to continue", logThreadSeq)
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
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for both the srcDB and dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for srcDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().TableAccessPriCheck(stcls.sourceDB, checkTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(StableList) == 0 {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for srcDB check failed: {%v}.", logThreadSeq, StableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for srcDB check completed: {%v}.", logThreadSeq, StableList)
		global.Wlog.Debug(vlog)
	}

	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Obtain the privileges for tables access for dstDB, and check that they are set correctly", logThreadSeq)
	global.Wlog.Debug(vlog)
	if DtableList, err = tc.Query().TableAccessPriCheck(stcls.destDB, checkTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(DtableList) == 0 {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for dstDB check failed: {%v}.", logThreadSeq, DtableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) The privileges for tables access for dstDB check completed: {%v}.", logThreadSeq, DtableList)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) Start checking the differences between the tables in srcDB and dstDB", logThreadSeq)
	global.Wlog.Debug(vlog)
	for k, _ := range StableList {
		if _, ok := DtableList[k]; ok {
			newCheckTableList = append(newCheckTableList, k)
		} else {
			abnormalTableList = append(abnormalTableList, k)
		}
	}
	vlog = fmt.Sprintf("(%d) The checksum of srcDB and dstDB tables is complete. The [%d] consistent tables are: {%s}, and the [%d] inconsistent tables are: {%s}", logThreadSeq, len(newCheckTableList), newCheckTableList, len(abnormalTableList), abnormalTableList)
	global.Wlog.Info(vlog)
	return newCheckTableList, abnormalTableList, nil
}
