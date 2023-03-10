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
	vlog = fmt.Sprintf("(%d) Start to get the source and target Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Datafix: stcls.datefix}
	vlog = fmt.Sprintf("(%d) Start to get the source Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().GlobalAccessPri(stcls.sourceDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) The Global Access Permission verification of the source DB is completed, and the status of the global access permission is {%v}.", logThreadSeq, StableList)
	global.Wlog.Debug(vlog)
	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Start to get the dest Global Access Permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)

	if DtableList, err = tc.Query().GlobalAccessPri(stcls.destDB, logThreadSeq2); err != nil {
		return false
	}
	vlog = fmt.Sprintf("(%d) The Global Access Permission verification of the dest DB is completed, and the status of the global access permission is {%v}.", logThreadSeq, DtableList)
	global.Wlog.Debug(vlog)
	if StableList && DtableList {
		vlog = fmt.Sprintf("(%d) The verification of the global access permission of the source and destination is completed", logThreadSeq)
		global.Wlog.Info(vlog)
		return true
	}
	vlog = fmt.Sprintf("(%d) Some global access permissions are missing at the source and destination, and verification cannot continue.", logThreadSeq)
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
	vlog = fmt.Sprintf("(%d) Start to get the source and target table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Info(vlog)
	tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
	vlog = fmt.Sprintf("(%d) Start to get the source table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)
	if StableList, err = tc.Query().TableAccessPriCheck(stcls.sourceDB, checkTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(StableList) == 0 {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, StableList)
		global.Wlog.Debug(vlog)
	}

	tc.Drive = stcls.destDrive
	vlog = fmt.Sprintf("(%d) Start to get the dest table access permissions information and check whether they are consistent", logThreadSeq)
	global.Wlog.Debug(vlog)
	if DtableList, err = tc.Query().TableAccessPriCheck(stcls.destDB, checkTableList, stcls.datefix, logThreadSeq2); err != nil {
		return nil, nil, err
	}
	if len(DtableList) == 0 {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, DtableList)
		global.Wlog.Error(vlog)
	} else {
		vlog = fmt.Sprintf("(%d) Complete the verification table permission verification of the source DB, the current verification table with permission is {%v}.", logThreadSeq, DtableList)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) Start processing the difference of the table to be checked at the source and target.", logThreadSeq)
	global.Wlog.Debug(vlog)
	for k, _ := range StableList {
		if _, ok := DtableList[k]; ok {
			newCheckTableList = append(newCheckTableList, k)
		} else {
			abnormalTableList = append(abnormalTableList, k)
		}
	}
	vlog = fmt.Sprintf("(%d) The difference processing of the table to be checked at the source and target ends is completed. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, newCheckTableList, len(newCheckTableList), abnormalTableList, len(abnormalTableList))
	global.Wlog.Info(vlog)
	return newCheckTableList, abnormalTableList, nil
}
