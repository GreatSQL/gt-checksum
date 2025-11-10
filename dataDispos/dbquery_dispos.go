package dataDispos

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"sort"
	"strconv"
	"strings"
	"time"
)

type DBdataDispos struct {
	SqlRows         *sql.Rows
	Event           string
	Schema          string
	Table           string
	LogThreadSeq    int64
	TableColumnType []map[string]string
	DB              *sql.DB
	DBType          string
}

// 处理单列数据类型
func (dbpos *DBdataDispos) ColumnTypeDispos(columnName string) (string, int, bool) {
	var (
		vlog              string
		indexColumnType   string //索引列的数据类型
		indexColumnIsNull bool   //索引列数据类型是否允许为null
		office            int    //浮点类型的偏移量
	)
	vlog = fmt.Sprintf("(%d) Table [%v.%v] column info: %v", dbpos.LogThreadSeq, dbpos.Schema, dbpos.Table, dbpos.TableColumnType)
	global.Wlog.Debug(vlog)
	IntType := []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"}
	floatType := []string{"FLOAT", "DOUBLE", "DECIMAL"}
	for _, i := range dbpos.TableColumnType {
		if i["columnName"] == columnName {
			ct := strings.Split(strings.ToUpper(i["dataType"]), "(")[0]
			if strings.Contains(strings.Join(IntType, ","), ct) {
				indexColumnType = "int"
			} else if strings.Contains(strings.Join(floatType, ","), ct) {
				office, _ = strconv.Atoi(strings.TrimSpace(strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")))
				indexColumnType = "float"
			} else {
				indexColumnType = "char"
			}
			if i["isNull"] == "YES" { //判断当前索引列是否允许为null
				indexColumnIsNull = true
			}
		}
	}
	vlog = fmt.Sprintf("(%d) Table [%v.%v] column [%v] type: %v, nullable: %v", dbpos.LogThreadSeq, dbpos.Schema, dbpos.Table, columnName, indexColumnType, indexColumnIsNull)
	global.Wlog.Debug(vlog)
	vlog = fmt.Sprintf("(%d) Completed getting column [%v] metadata for table [%v.%v]", dbpos.LogThreadSeq, columnName, dbpos.Schema, dbpos.Table)
	global.Wlog.Info(vlog)
	return indexColumnType, office, indexColumnIsNull
}

func (dbpos *DBdataDispos) DataAscSort(data map[string]interface{}, columnName string) []map[string]string {
	var (
		vlog               string
		indexColumnUniqueS []map[string]string
		z                  = make(map[string]int)         //源目标端索引列数据的集合（无序的）
		zint               []int                          //int类型的索引列集合，用于正序排序
		zfloat             []float64                      //float类型的索引列集合，用于正序排序
		zchar              []string                       //char类型的索引列集合，用于正序排序
		znull              = make(map[string]interface{}) //针对null的值的一个处理
		office             int                            //浮点类型的偏移量
	)
	//原目标端索引列数据去重，并按照索引列数据类型进行分类合并索引列
	vlog = fmt.Sprintf("(%d) Start merging the data of index column [%v] of source target segment table [%v.%v]...", dbpos.LogThreadSeq, columnName, dbpos.Schema, dbpos.Table)
	global.Wlog.Info(vlog)
	indexColumnType, office, indexColumnIsNull := dbpos.ColumnTypeDispos(columnName)
	for k, v := range data {
		switch indexColumnType {
		case "int":
			if indexColumnIsNull {
				if k != "<nil>" {
					zc, _ := strconv.Atoi(k)
					zint = append(zint, zc)
				} else {
					znull["<nil>"] = v
				}
			} else {
				zc, _ := strconv.Atoi(k)
				zint = append(zint, zc)
			}
		case "float":
			//处理null值
			if indexColumnIsNull {
				if k != "<nil>" {
					zc, _ := strconv.ParseFloat(k, office)
					zfloat = append(zfloat, zc)
				} else {
					znull["<nil>"] = v
				}
			} else {
				zc, _ := strconv.ParseFloat(k, office)
				zfloat = append(zfloat, zc)
			}
		case "char":
			if indexColumnIsNull {
				if k != "<nil>" {
					zchar = append(zchar, k)
				} else {
					znull["<nil>"] = v
				}
			} else {
				zchar = append(zchar, k)
			}
		}
	}
	vlog = fmt.Sprintf("(%d) Data merge completed for index column [%v] in table [%v.%v]", dbpos.LogThreadSeq, columnName, dbpos.Schema, dbpos.Table)
	global.Wlog.Info(vlog)

	vlog = fmt.Sprintf("(%d) Start sorting the merged data of index column [%v] of source target segment table [%v.%v] in positive order...", dbpos.LogThreadSeq, columnName, dbpos.Schema, dbpos.Table)
	global.Wlog.Info(vlog)

	//针对索引类数据进行正序排序
	switch indexColumnType {
	case "int":
		vlog = fmt.Sprintf("(%d) Start sorting index column data of type int,The index column data that needs to be sorted is [%v] ...", dbpos.LogThreadSeq, zint)
		global.Wlog.Debug(vlog)
		sort.Ints(zint)
		vlog = fmt.Sprintf("(%d) Sorted int type index column data: [%v]", dbpos.LogThreadSeq, zint)
		global.Wlog.Debug(vlog)
		if _, ok := znull["<nil>"]; ok {
			vlog = fmt.Sprintf("(%d) The index column data of int type and index column data is null values.", dbpos.LogThreadSeq)
			global.Wlog.Debug(vlog)
			indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", "<nil>"), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", "<nil>")])})
		} else {
			for _, i := range zint {
				indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{fmt.Sprintf("%v", i): fmt.Sprintf("%v", data[fmt.Sprintf("%v", i)])})
			}
		}
		zint, z = nil, nil

	case "float":
		vlog = fmt.Sprintf("(%d) Start sorting index column data of type float,The index column data that needs to be sorted is [%v] ...", dbpos.LogThreadSeq, zfloat)
		global.Wlog.Debug(vlog)
		sort.Float64s(zfloat)
		vlog = fmt.Sprintf("(%d) Sorted float type index column data: [%v]", dbpos.LogThreadSeq, zfloat)
		global.Wlog.Debug(vlog)
		if _, ok := znull["<nil>"]; ok {
			vlog = fmt.Sprintf("(%d) The index column data of float type and index column data is null values.", dbpos.LogThreadSeq)
			global.Wlog.Debug(vlog)
			indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", "<nil>"), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", "<nil>")])})
		} else {
			for _, i := range zfloat {
				ii := strconv.FormatFloat(i, 'f', 2, 64)
				indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{ii: fmt.Sprintf("%v", data[ii])})
			}
		}
		zfloat, z = nil, nil
	case "char":
		vlog = fmt.Sprintf("(%d) Start sorting index column data of type char,The index column data that needs to be sorted is [%v] ...", dbpos.LogThreadSeq, zchar)
		global.Wlog.Debug(vlog)
		sort.Strings(zchar)
		vlog = fmt.Sprintf("(%d) Sorted char type index column data: [%v]", dbpos.LogThreadSeq, zchar)
		global.Wlog.Debug(vlog)
		if _, ok := znull["<nil>"]; ok {
			vlog = fmt.Sprintf("(%d) The index column data of char type and index column data is null values.", dbpos.LogThreadSeq)
			global.Wlog.Debug(vlog)
			indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", "<nil>"), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", "<nil>")])})
		} else {
			for _, i := range zchar {
				indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{i: fmt.Sprintf("%v", data[i])})
			}
		}
		zchar, z = nil, nil
	}
	vlog = fmt.Sprintf("(%d) Completed sorting index column [%v] data for table [%v.%v]", dbpos.LogThreadSeq, columnName, dbpos.Schema, dbpos.Table)
	global.Wlog.Info(vlog)
	return indexColumnUniqueS
}

// 处理行数据的null值
func (dbpos *DBdataDispos) RowsdataNullDispos(i map[string]interface{}) map[string]interface{} {
	var (
		znull = make(map[string]interface{}) //源目标端索引列数据的集合（无序的） //针对null的值的一个处理
	)
	if fmt.Sprintf("%v", i["columnName"]) == "<nil>" {
		if _, ok := i["count"]; ok {
			c, _ := strconv.ParseUint(fmt.Sprintf("%v", i["count"]), 10, 64)
			znull["<nil>"] = c
		}
	} else {
		znull[fmt.Sprintf("%v", i["columnName"])] = i["count"]
	}

	return znull
}

// 表查询使用chan来做流式处理
func (dbpos *DBdataDispos) DataChanDispos() chan map[string]interface{} {
	var chanEntry = make(chan map[string]interface{}, 1000)
	go func() {
		columns, err := dbpos.SqlRows.Columns()
		if err != nil {
			errInfo := fmt.Sprintf("(%d) %s DB Get the column fail. Error Info: %s", dbpos.DBType, dbpos.LogThreadSeq, err)
			global.Wlog.Error(errInfo)
			return
		}
		// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
		//values := make([]sql.RawBytes,len(columns))
		//定义一个切片，元素类型是interface{}接口
		//scanArgs := make([]interface{},len(values))
		valuePtrs := make([]interface{}, len(columns))
		values := make([]interface{}, len(columns))
		for dbpos.SqlRows.Next() {
			for i := 0; i < len(columns); i++ {
				valuePtrs[i] = &values[i]
			}
			dbpos.SqlRows.Scan(valuePtrs...)
			entry := make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					// 对于字符串类型，去除尾部空格
					v = strings.TrimRight(string(b), " ")
				} else {
					v = val
				}
				if v == nil {
					v = "<nil>"
				}
				if v == "" {
					v = "<entry>"
				}
				entry[col] = v
			}
			chanEntry <- dbpos.RowsdataNullDispos(entry)
		}
		close(chanEntry)
		dbpos.SqlRows.Close()
	}()
	// 获取列名
	return chanEntry
}

// 表查询使用slice来做聚合处理
func (dbpos *DBdataDispos) DataRowsAndColumnSliceDispos(tableData []map[string]interface{}) ([]map[string]interface{}, error) {
	// 获取列名
	columns, err := dbpos.SqlRows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("(%d) [%s] Failed to obtain %s database column data information. Error Info: %s", dbpos.LogThreadSeq, dbpos.Event, dbpos.DBType, err)
		global.Wlog.Error(errInfo)
		return nil, err
	}
	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
	//values := make([]sql.RawBytes,len(columns))
	//定义一个切片，元素类型是interface{}接口
	//scanArgs := make([]interface{},len(values))
	valuePtrs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for dbpos.SqlRows.Next() {
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		dbpos.SqlRows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				// 对于字符串类型，去除尾部空格
				// 特殊处理ROUTINE_DEFINITION列，不去除尾部空格，保留原始格式
				if col == "ROUTINE_DEFINITION" {
					v = string(b)
				} else {
					v = strings.TrimRight(string(b), " ")
				}
			} else {
				v = val
			}
			if v == nil {
				// 特殊处理ROUTINE_DEFINITION列，如果为nil，设置为空字符串而不是"<nil>"
				if col == "ROUTINE_DEFINITION" {
					v = ""
				} else {
					v = "<nil>"
				}
			}
			if v == "" && col != "ROUTINE_DEFINITION" {
				v = "<entry>"
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	return tableData, nil
}

/*
返回行数据切片和列数据切片，多用于无索引表的对比去重
*/
func (dbpos *DBdataDispos) DataRowsDispos(tableData []string) ([]string, error) {
	// 获取列名
	columns, err := dbpos.SqlRows.Columns()
	if err != nil {
		errInfo := fmt.Sprintf("(%d) %s DB Get the column fail. Error Info: %s", dbpos.DBType, dbpos.LogThreadSeq, err)
		global.Wlog.Error(errInfo)
		return nil, err
	}
	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
	//values := make([]sql.RawBytes,len(columns))
	//定义一个切片，元素类型是interface{}接口
	//scanArgs := make([]interface{},len(values))
	valuePtrs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for dbpos.SqlRows.Next() {
		var tmpaaS []string
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		dbpos.SqlRows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				// 对于字符串类型，去除尾部空格
				v = strings.TrimRight(string(b), " ")
			} else {
				v = val
			}
			if v == nil {
				v = "<nil>"
			}
			//oracle只有null没有空值
			if dbpos.DBType == "Oracle" {
				if v == "" {
					v = "<nil>"
				}
			}
			if dbpos.DBType == "MySQL" {
				if v == "" {
					v = "<entry>"
				}
			}
			entry[col] = v
			tmpaaS = append(tmpaaS, fmt.Sprintf("%v", v))
		}
		tableData = append(tableData, strings.Join(tmpaaS, "/*go actions columnData*/"))
	}
	return tableData, nil
}

/*
连接数据库执行sql语句，尝试执行次数
*/
func (dbpos *DBdataDispos) DBSQLforExec(strsql string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
		vlog string
	)
	
	// 检查是否是查询表行数的SQL，如果是且表不存在，记录跳过的表
	if strings.Contains(strsql, "SELECT COUNT(*) AS tableRows FROM") && dbpos.Schema != "" && dbpos.Table != "" {
		// 检查表是否存在
		tableExistsQuery := fmt.Sprintf("SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbpos.Schema, dbpos.Table)
		var exists int
		tableExistsErr := dbpos.DB.QueryRow(tableExistsQuery).Scan(&exists)
		if tableExistsErr == sql.ErrNoRows {
			// 表不存在，记录跳过的表
			global.AddSkippedTable(dbpos.Schema, dbpos.Table, "data", "table does not exist")
			vlog = fmt.Sprintf("(%d) [%s] Table %s.%s does not exist, skipping", dbpos.LogThreadSeq, dbpos.Event, dbpos.Schema, dbpos.Table)
			global.Wlog.Warn(vlog)
			return nil, fmt.Errorf("table %s.%s does not exist", dbpos.Schema, dbpos.Table)
		}
	}
	
	for i := 1; i < 4; i++ {
		rows, err = dbpos.DB.Query(strsql)
		if err != nil {
			// 特殊处理表不存在的错误，不进行重试
			errMsg := err.Error()
			if strings.Contains(errMsg, "Table") && strings.Contains(errMsg, "doesn't exist") && dbpos.Schema != "" && dbpos.Table != "" {
				global.AddSkippedTable(dbpos.Schema, dbpos.Table, "data", "table does not exist")
				vlog = fmt.Sprintf("(%d) [%s] Table %s.%s does not exist, skipping", dbpos.LogThreadSeq, dbpos.Event, dbpos.Schema, dbpos.Table)
				global.Wlog.Warn(vlog)
				return nil, fmt.Errorf("table %s.%s does not exist", dbpos.Schema, dbpos.Table)
			}
			switch i {
			case 1:
				vlog = fmt.Sprintf("(%d) [%s] The first connection to the %s database failed to execute the sql statement.sql message is {%s} Error info is {%s}", dbpos.LogThreadSeq, dbpos.Event, dbpos.DBType, strsql, err)
			case 2:
				vlog = fmt.Sprintf("(%d) [%s] The second connection to the %s database failed to execute the sql statement.sql message is {%s} Error info is {%s}", dbpos.LogThreadSeq, dbpos.Event, dbpos.DBType, strsql, err)
			case 3:
				vlog = fmt.Sprintf("(%d) [%s] The third connection to the %s database failed to execute the sql statement.sql message is {%s} Error info is {%s}", dbpos.LogThreadSeq, dbpos.Event, dbpos.DBType, strsql, err)
			}
			global.Wlog.Error(vlog)
			if i == 3 {
				vlog = fmt.Sprintf("(%d) [%s] Failed to connect to the %s database, unable to execute the sql statement.", dbpos.LogThreadSeq, dbpos.Event, dbpos.DBType)
				global.Wlog.Error(vlog)
				return nil, err
			}
			// yejr存疑，反复查询失败的话，每次时间间隔可以改成1秒，5秒看起来有点大了
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	return rows, nil
}
