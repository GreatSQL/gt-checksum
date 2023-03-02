package oracle

import (
	"database/sql"
	"fmt"
	"gt-checksum/dataDispos"
	"gt-checksum/global"
	"strconv"
	"strings"
)

/*
	查询Oracle库下指定表的索引统计信息
*/

func (or *QueryTable) QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error) {
	var (
		Event = "Q_Index_Statistics"
	)
	strsql = fmt.Sprintf("select c.COLUMN_NAME as \"columnName\",decode(c.DATA_TYPE,'DATE',c.data_type,c.DATA_TYPE || '(' || c.data_LENGTH || ')') as \"columnType\", decode(co.constraint_type, 'P','1','0') as \"columnKey\",i.UNIQUENESS as \"nonUnique\", ic.INDEX_NAME as \"indexName\", ic.COLUMN_POSITION as \"IndexSeq\", c.COLUMN_ID as \"columnSeq\" from all_tab_cols c inner join all_ind_columns ic on c.TABLE_NAME = ic.TABLE_NAME and c.OWNER = ic.INDEX_OWNER and c.COLUMN_NAME = ic.COLUMN_NAME inner join all_indexes i on ic.INDEX_OWNER = i.OWNER and ic.INDEX_NAME = i.INDEX_NAME and ic.TABLE_NAME = i.TABLE_NAME left join all_constraints co on co.owner = c.owner and co.table_name = c.table_name and co.index_name = i.index_name where c.OWNER = '%s' and c.TABLE_NAME = '%s' ORDER BY I.INDEX_NAME, ic.COLUMN_POSITION", strings.ToUpper(or.Schema), or.Table)
	vlog = fmt.Sprintf("(%d) [%s] Generate a sql statement to query the index statistics of table %s.%s under the %s database.sql messige is {%s}", logThreadSeq, Event, or.Schema, or.Table, DBType, strsql)
	global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	vlog = fmt.Sprintf("(%d) [%s] The index statistics query of table %s.%s under the %s database is completed. index statistics is {%v}", logThreadSeq, Event, or.Schema, or.Table, DBType, tableData)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tableData, err
}

/*
	根据Oracle库下指定表的索引信息，筛选主键索引、唯一索引、普通索引
*/
func (or *QueryTable) IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) (map[string][]string, map[string][]string, map[string][]string) {
	var (
		nultiseriateIndexColumnMap            = make(map[string][]string)
		multiseriateIndexColumnMap            = make(map[string][]string)
		priIndexColumnMap                     = make(map[string][]string)
		PriIndexCol, uniIndexCol, mulIndexCol []string
		indexName                             string
		currIndexName                         string
		Event                                 = "E_Index_Filter"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to filter the primary key index, unique index, and common index based on the index information of the specified table %s.%s under the %s library", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		currIndexName = strings.ToUpper(v["indexName"].(string))
		//判断唯一索引（包含主键索引和普通索引）
		if v["nonUnique"].(string) == "UNIQUE" {
			if v["columnKey"].(string) == "1" {
				if currIndexName != indexName {
					indexName = currIndexName
				}
				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
				priIndexColumnMap["pri"] = PriIndexCol
			} else {
				if currIndexName != indexName {
					indexName = currIndexName
					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				} else {
					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
				}
			}
		}
		//处理普通索引
		if v["nonUnique"].(string) == "NONUNIQUE" {
			if currIndexName != indexName {
				indexName = currIndexName
				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			} else {
				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
			}
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] The index information screening of the specified table %s.%s under the %s library is completed", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return priIndexColumnMap, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
}

/*
	查询表，输出索引列数据的字符串长度，判断是否有null或空
*/
func (or *QueryTable) TmpTableIndexColumnSelectDispos(logThreadSeq int64) map[string]string {
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	var (
		columnSelect = make(map[string]string)
		columnName   = or.ColumnName
		Event        = "D_Index_Length"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the length of the query index column in table %s.%s in the specified %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	//根据索引列的多少，生成select 列条件，并生成列长度，为判断列是否为null或为空做判断
	if len(columnName) == 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "")
		columnSelect["selectColumnLength"] = fmt.Sprintf("LENGTH(trim(%s)) as %s_length", strings.Join(columnName, ""), strings.Join(columnName, ""))
		columnSelect["selectColumnLengthSlice"] = fmt.Sprintf("%s_length", strings.Join(columnName, ""))
		columnSelect["selectColumnNull"] = fmt.Sprintf("%s is null ", strings.Join(columnName, ""))
		columnSelect["selectColumnEmpty"] = fmt.Sprintf("%s = '' ", strings.Join(columnName, ""))
		columnSelect["selectColumnData"] = fmt.Sprintf("%s != '' and %s is not null ", strings.Join(columnName, ""), strings.Join(columnName, ""))
	} else if len(columnName) > 1 {
		columnSelect["selectColumnName"] = strings.Join(columnName, "/*column*/")
		var aa, bb, cc, dd []string
		for i := range columnName {
			aa = append(aa, fmt.Sprintf("LENGTH(trim(%s)) as %s_length", columnName[i], columnName[i]))
			bb = append(bb, fmt.Sprintf("%s_length", columnName[i]))
			cc = append(cc, fmt.Sprintf("%s is null ", columnName[i]))
			dd = append(dd, fmt.Sprintf("%s = '' ", columnName[i]))
		}
		columnSelect["selectColumnLength"] = strings.Join(aa, "/*column*/")
		columnSelect["selectColumnLengthSlice"] = strings.Join(bb, "/*column*/")
		columnSelect["selectColumnNull"] = strings.Join(cc, "/*column*/")
		columnSelect["selectColumnEmpty"] = strings.Join(dd, "/*column*/")
	}
	vlog = fmt.Sprintf("(%d) [%s] The length of the query index column of table %s.%s in the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return columnSelect
}

/*
  Oracle 查询有索引表的总行数
*/
func (or *QueryTable) TmpTableIndexColumnRowsCount(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		tmpTableCount uint64
		Event         = "Q_Index_Table_Count"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the total number of rows in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("select count(1) as \"sum\" from \"%s\".\"%s\"", or.Schema, or.Table)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return 0, err
	}
	if tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{}); err != nil {
		return 0, err
	} else {
		for _, i := range tableData {
			d, _ := strconv.ParseUint(fmt.Sprintf("%s", i["sum"]), 10, 64)
			tmpTableCount += d
		}
	}
	vlog = fmt.Sprintf("(%d) [%s] The query of the total number of rows in the following table %s.%s of the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	defer dispos.SqlRows.Close()
	return tmpTableCount, nil
}

/*
	Oracle库下查询表的索引列数据，并进行去重排序
*/
func (or *QueryTable) TmpTableColumnGroupDataDispos(db *sql.DB, where string, columnName string, logThreadSeq int64) (chan map[string]interface{}, error) {
	var (
		Event      = "Q_Index_ColumnData"
		whereExist string
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the index column data of the following table %s.%s in the %s database and de-reorder the data.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	whereExist = where
	if where != "" {
		whereExist = fmt.Sprintf("where %s ", where)
	}
	strsql = fmt.Sprintf("select %s as \"columnName\",count(1) as \"count\" from \"%s\".\"%s\" %s group by %s order by %s", columnName, or.Schema, or.Table, whereExist, columnName, columnName)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return nil, err
	}
	C := dispos.DataChanDispos()
	vlog = fmt.Sprintf("(%d) [%s] The index column data query of the following table %s.%s in the %s database is completed.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return C, nil
}

/*
	MySQL 查询表的统计信息中行数
*/
func (or *QueryTable) TableRows(db *sql.DB, logThreadSeq int64) (uint64, error) {
	var (
		Event = "Q_I_S_tableRows"
	)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	vlog = fmt.Sprintf("(%d) [%s] Start querying the statistical information of table %s.%s in the %s database and get the number of rows in the table", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	strsql = fmt.Sprintf("exec dbms_stats.gather_table_stats('%s','%s');", or.Schema, or.Table)
	dispos.DBSQLforExec(strsql)
	strsql = fmt.Sprintf("select num_rows as \"tableRows\" from dba_tables where owner='%s' and table_name='%s'", or.Schema, or.Table)
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return 0, err
	}
	tableData, err := dispos.DataRowsAndColumnSliceDispos([]map[string]interface{}{})
	if err != nil {
		return 0, err
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] The number of rows in table %s.%s in the %s database has been obtained.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return strconv.ParseUint(fmt.Sprintf("%s", tableData[0]["tableRows"]), 10, 64)
}

//处理无索引表查询select的order by列，防止原目标端查询的段不一致情况
func (or *QueryTable) NoIndexOrderBySingerColumn(orderCol []map[string]string) []string {
	//处理order by column
	var selectC []string
	for _, v := range orderCol {
		selectC = append(selectC, v["columnName"])
		//if strings.HasPrefix(v["dataType"], "NUMBER") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "DATE") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "CHAR") {
		//	return v["columnName"]
		//}
		//if strings.HasPrefix(v["dataType"], "VARCHAR2") {
		//	return v["columnName"]
		//}
	}
	return selectC
}

func (or *QueryTable) NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq uint64, chanrowCount int, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "Q_table_Data"
	)
	//处理oracle查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for _, i := range or.TableColumn {
		mu := "9"
		nu := "0"
		var tmpcolumnName string
		tmpcolumnName = i["columnName"]
		if strings.ToUpper(i["dataType"]) == "DATE" {
			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", i["columnName"])
		}
		if strings.Contains(strings.ToUpper(i["dataType"]), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", i["columnName"])
		}
		if strings.HasPrefix(strings.ToUpper(i["dataType"]), "NUMBER(") {
			dianAfter := strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(i["dataType"], ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			var tmpa, tmpb []string
			for ii := 0; ii < bb; ii++ {
				tmpa = append(tmpa, nu)
			}
			for ii := 1; ii < bbc-bb; ii++ {
				tmpb = append(tmpb, mu)
			}
			if bb == 0 {
				tmpcolumnName = fmt.Sprintf("to_char(%s,'FM%s0')", i["columnName"], strings.Join(tmpb, ""))
			} else {
				tmpcolumnName = fmt.Sprintf("to_char(%s,'FM%s0.%s')", i["columnName"], strings.Join(tmpb, ""), strings.Join(tmpa, ""))
			}
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}
	strsql = fmt.Sprintf("SELECT %s FROM ( SELECT A.*, ROWNUM RN FROM (SELECT * FROM \"%s\".\"%s\") A WHERE ROWNUM <= %d) WHERE RN > %d", strings.Join(columnNameSeq, ","), strings.ToUpper(or.Schema), or.Table, beginSeq+uint64(chanrowCount), beginSeq)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(strsql); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsDispos([]string{})
	if err != nil {
		return "", err
	}
	defer dispos.SqlRows.Close()
	return strings.Join(tableData, "/*go actions rowData*/"), nil
}

/*
	Oracle 通过where条件查询表的分段数据（查询数据生成带有greatdbCheck标识的数据块）
*/
func (or *QueryTable) GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		Event = "Q_Table_Data"
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to query the segmented data of the following table %s.%s in the %s database through the where condition.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	dispos := dataDispos.DBdataDispos{DBType: DBType, LogThreadSeq: logThreadSeq, Event: Event, DB: db}
	if dispos.SqlRows, err = dispos.DBSQLforExec(or.Sqlwhere); err != nil {
		return "", err
	}
	tableData, err := dispos.DataRowsDispos([]string{})
	if err != nil {
		return "", err
	}
	defer dispos.SqlRows.Close()
	vlog = fmt.Sprintf("(%d) [%s] Complete the data in the following table %s.%s of the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	return strings.Join(tableData, "/*go actions rowData*/"), nil
}

/*
	Oracle 生成查询数据的sql语句
*/
func (or *QueryTable) GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error) {
	var (
		columnNameSeq []string
		Event         = "E_Table_SQL"
		selectSql     string
	)
	vlog = fmt.Sprintf("(%d) [%s] Start to generate the data query sql of table %s.%s in the %s database", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	//处理oracle查询时间列时数据带时区问题  2021-01-23 10:16:29 +0800 CST
	for _, i := range or.TableColumn {
		mu := "9"
		nu := "0"
		var tmpcolumnName string
		tmpcolumnName = i["columnName"]
		if strings.ToUpper(i["dataType"]) == "DATE" {
			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", i["columnName"])
		}
		if strings.Contains(strings.ToUpper(i["dataType"]), "TIMESTAMP") {
			tmpcolumnName = fmt.Sprintf("to_char(%s,'YYYY-MM-DD HH24:MI:SS')", i["columnName"])
		}
		if strings.HasPrefix(strings.ToUpper(i["dataType"]), "NUMBER(") {
			dianAfter := strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")
			bb, _ := strconv.Atoi(dianAfter)
			dianBefer := strings.Split(strings.Split(i["dataType"], ",")[0], "(")[1]
			bbc, _ := strconv.Atoi(dianBefer)
			var tmpa, tmpb []string
			for ii := 0; ii < bb; ii++ {
				tmpa = append(tmpa, nu)
			}
			for ii := 1; ii < bbc-bb; ii++ {
				tmpb = append(tmpb, mu)
			}
			if bb == 0 {
				tmpcolumnName = fmt.Sprintf("to_char(%s,'FM%s0')", i["columnName"], strings.Join(tmpb, ""))
			} else {
				tmpcolumnName = fmt.Sprintf("to_char(%s,'FM%s0.%s')", i["columnName"], strings.Join(tmpb, ""), strings.Join(tmpa, ""))
			}
		}
		columnNameSeq = append(columnNameSeq, tmpcolumnName)
	}
	queryColumn := strings.Join(columnNameSeq, ",")
	//sqlstr := fmt.Sprintf("select %s from \"%s\".\"%s\" as of scn %s where %s", queryColumn, schema, table, oracleScn, sqlWhere)
	//fmt.Println(fmt.Sprintf("select %s from \"%s\".\"%s\" where %s", queryColumn, strings.ToUpper(or.Schema), or.Table, or.Sqlwhere))
	selectSql = fmt.Sprintf("select %s from \"%s\".\"%s\" where %s", queryColumn, strings.ToUpper(or.Schema), or.Table, or.Sqlwhere)
	vlog = fmt.Sprintf("(%d) [%s] Complete the data query sql of table %s.%s in the %s database.", logThreadSeq, Event, or.Schema, or.Table, DBType)
	global.Wlog.Debug(vlog)
	return selectSql, nil
}
