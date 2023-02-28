package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"math/rand"
	"os"
	"strings"
	"time"
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
		schema, table                  string
		stmpTableCount, dtmpTableCount uint64
		err                            error
		vlog                           string
	)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	vlog = fmt.Sprintf("(%d) Start the table validation for the total number of rows ...", logThreadSeq)
	global.Wlog.Info(vlog)
	for k, v := range sp.tableIndexColumnMap {
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		ki := strings.Split(k, "/*indexColumnType*/")[0]
		if strings.Contains(ki, "/*greatdbSchemaTable*/") {
			schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
			table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
		}
		vlog = fmt.Sprintf("(%d) Check table %s.%s initialization single check row number.", logThreadSeq, schema, table)
		global.Wlog.Debug(vlog)

		sdb := sp.sdbPool.Get(logThreadSeq)
		//查询原目标端的表总行数，并生成调度计划
		idxc := dbExec.IndexColumnStruct{Schema: schema, Table: table, ColumnName: sp.columnName, Drivce: sp.sdrive}
		stmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(sdb, logThreadSeq)
		if err != nil {
			return
		}
		sp.sdbPool.Put(sdb, logThreadSeq)

		ddb := sp.ddbPool.Get(logThreadSeq)
		idxc.Drivce = sp.ddrive
		dtmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
		if err != nil {
			return
		}
		sp.ddbPool.Put(ddb, logThreadSeq)

		//输出校验结果信息
		var pods = Pod{
			Schema:      schema,
			Table:       table,
			CheckObject: sp.checkObject,
			CheckMod:    sp.checkMod,
		}
		vlog = fmt.Sprintf("(%d) Start to verify the total number of rows of table %s.%s source and target ...", logThreadSeq, schema, table)
		global.Wlog.Debug(vlog)
		if stmpTableCount == dtmpTableCount {
			vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is consistent", logThreadSeq, schema, table)
			global.Wlog.Debug(vlog)
			pods.Differences = "no"
			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		} else {
			vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is inconsistent.", logThreadSeq, schema, table)
			global.Wlog.Debug(vlog)
			pods.Differences = "yes"
			pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		}
		measuredDataPods = append(measuredDataPods, pods)
		vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, schema, table)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The total number of rows in the check table has been checked !!!", logThreadSeq)
	global.Wlog.Info(vlog)
}
