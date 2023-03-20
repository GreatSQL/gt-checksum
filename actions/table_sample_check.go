package actions

import (
	"database/sql"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"math/rand"
	"strings"
	"time"
)

/*
	递归查询索引列数据，并按照单次校验块的大小来切割索引列数据，生成查询的where条件
*/
func (sp SchedulePlan) SampRecursiveIndexColumn(sqlWhere chanString, sdb, ddb *sql.DB, level, queryNum int, where string, selectColumn map[string]map[string]string, logThreadSeq int64) {
	//var (
	//	sqlwhere           string                   //查询sql的where条件
	//	indexColumnUniqueS []map[string]string      //源目标端索引列数据的集合（有序的）
	//	indexColumnType    string                   //索引列的数据类型
	//	indexColumnIsNull  bool                     //索引列数据类型是否允许为null
	//	z                  = make(map[string]int)   //源目标端索引列数据的集合（无序的）
	//	zint               []int                    //int类型的索引列集合，用于正序排序
	//	zfloat             []float64                //float类型的索引列集合，用于正序排序
	//	zchar              []string                 //char类型的索引列集合，用于正序排序
	//	znull              = make(map[string]int)   //针对null的值的一个处理
	//	office             int                      //浮点类型的偏移量
	//	d                  int                      //索引列每一行group重复值的累加值，临时变量
	//	e, g               string                   //定义每个chunk的初始值和结尾值,e为起始值，g为数据查询的动态指针值
	//	vlog               string                   //日志输出变量
	//	sa, da             []map[string]interface{} //原目标端索引列数据
	//	err                error                    //错误日志
	//)
	////获取索引列的数据类型
	//vlog = fmt.Sprintf("(%d) Start to get the index column data type and null value constraint of table [%v.%v] index column [%v]...", logThreadSeq, sp.schema, sp.table, sp.columnName[level])
	//global.Wlog.Info(vlog)
	//a := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)].SColumnInfo
	//vlog = fmt.Sprintf("(%d) All column data information of table [%v.%v] is {%v}.", logThreadSeq, sp.schema, sp.table, a)
	//global.Wlog.Debug(vlog)
	//IntType := []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"}
	//floatType := []string{"FLOAT", "DOUBLE", "DECIMAL"}
	//
	//for _, i := range a {
	//	if i["columnName"] == sp.columnName[level] {
	//		ct := strings.Split(strings.ToUpper(i["dataType"]), "(")[0]
	//		if strings.Contains(strings.Join(IntType, ","), ct) {
	//			indexColumnType = "int"
	//		} else if strings.Contains(strings.Join(floatType, ","), ct) {
	//			office, _ = strconv.Atoi(strings.TrimSpace(strings.ReplaceAll(strings.Split(i["dataType"], ",")[1], ")", "")))
	//			indexColumnType = "float"
	//		} else {
	//			indexColumnType = "char"
	//		}
	//		if i["isNull"] == "YES" { //判断当前索引列是否允许为null
	//			indexColumnIsNull = true
	//		}
	//	}
	//}
	//vlog = fmt.Sprintf("(%d) The data type of index column [%v] of table [%v.%v] is {%v} and the null constraint is {%v}", logThreadSeq, sp.columnName[level], sp.schema, sp.table, indexColumnType, indexColumnIsNull)
	//global.Wlog.Debug(vlog)
	//vlog = fmt.Sprintf("(%d) The index column data type and null value constraint acquisition of index column [%v] of table [%v.%v] is completed!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	//
	////查询源目标端索引列数据
	//idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName,
	//	ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive, SelectColumn: selectColumn[sp.sdrive]}
	//vlog = fmt.Sprintf("(%d) Start to query the index column data of index column [%v] of source table [%v.%v]...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	//for i := 1; i < 4; i++ {
	//	sa, err = idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(sdb, where, sp.columnName[level], logThreadSeq)
	//	if err != nil {
	//		vlog = fmt.Sprintf("(%d) Failed to query the data of index column [%v] of source table [%v.%v] for the %v time.", logThreadSeq, sp.columnName[level], sp.schema, sp.table, i)
	//		global.Wlog.Error(vlog)
	//		if i == 3 {
	//			return
	//		}
	//		time.Sleep(5 * time.Second)
	//	} else {
	//		break
	//	}
	//}
	//vlog = fmt.Sprintf("(%d) The index column data of index column [%v] in source table [%v.%v] is {%v}", logThreadSeq, sp.columnName[level], sp.schema, sp.table, sa)
	//global.Wlog.Debug(vlog)
	//if len(sa) == 0 {
	//	vlog = fmt.Sprintf("(%d) The index column data of index column [%v] in source table [%v.%v] is empty.", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//	global.Wlog.Warn(vlog)
	//}
	//vlog = fmt.Sprintf("(%d) The index column data query of index column [%v] of source table [%v.%v] is completed!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//idxc.Drivce = sp.ddrive
	//idxc.SelectColumn = selectColumn[sp.ddrive]
	//vlog = fmt.Sprintf("(%d) Start to query the index column data of index column [%v] of dest table [%v.%v]...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//for i := 1; i < 4; i++ {
	//	da, err = idxc.TableIndexColumn().TmpTableColumnGroupDataDispos(ddb, where, sp.columnName[level], logThreadSeq)
	//	if err != nil {
	//		vlog = fmt.Sprintf("(%d) Failed to query the data of index column [%v] of dest table [%v.%v] for the %v time.", logThreadSeq, sp.columnName[level], sp.schema, sp.table, i)
	//		global.Wlog.Error(vlog)
	//		if i == 3 {
	//			return
	//		}
	//		time.Sleep(5 * time.Second)
	//	} else {
	//		break
	//	}
	//}
	//vlog = fmt.Sprintf("(%d) The index column data of index column [%v] in dest table [%v.%v] is {%v}", logThreadSeq, sp.columnName[level], sp.schema, sp.table, da)
	//global.Wlog.Debug(vlog)
	//if len(da) == 0 {
	//	vlog = fmt.Sprintf("(%d) The index column data of index column [%v] in dest table [%v.%v] is empty.", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//	global.Wlog.Warn(vlog)
	//}
	//vlog = fmt.Sprintf("(%d) The index column data query of index column [%v] of dest table [%v.%v] is completed!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//
	////原目标端索引列数据去重，并按照索引列数据类型进行分类合并索引列
	//vlog = fmt.Sprintf("(%d) Start merging the data of index column [%v] of source target segment table [%v.%v]...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	//for _, i := range sa {
	//	c, _ := strconv.Atoi(fmt.Sprintf("%v", i["count"]))
	//	z[fmt.Sprintf("%v", i["columnName"])] = c
	//	switch indexColumnType {
	//	case "int":
	//		if indexColumnIsNull {
	//			if fmt.Sprintf("%v", i["columnName"]) != "<nil>" {
	//				zc, _ := strconv.Atoi(fmt.Sprintf("%v", i["columnName"]))
	//				zint = append(zint, zc)
	//			} else {
	//				znull["<nil>"] = c
	//			}
	//		} else {
	//			zc, _ := strconv.Atoi(fmt.Sprintf("%v", i["columnName"]))
	//			zint = append(zint, zc)
	//		}
	//	case "float":
	//		//处理null值
	//		if indexColumnIsNull {
	//			if fmt.Sprintf("%v", i["columnName"]) != "<nil>" {
	//				zc, _ := strconv.ParseFloat(fmt.Sprintf("%v", i["columnName"]), office)
	//				zfloat = append(zfloat, zc)
	//			} else {
	//				znull["<nil>"] = c
	//			}
	//		} else {
	//			zc, _ := strconv.ParseFloat(fmt.Sprintf("%v", i["columnName"]), office)
	//			zfloat = append(zfloat, zc)
	//		}
	//	case "char":
	//		if indexColumnIsNull {
	//			if fmt.Sprintf("%v", i["columnName"]) != "<nil>" {
	//				zchar = append(zchar, fmt.Sprintf("%v", i["columnName"]))
	//			} else {
	//				znull["<nil>"] = c
	//			}
	//		} else {
	//			zchar = append(zchar, fmt.Sprintf("%v", i["columnName"]))
	//		}
	//	}
	//}
	//sa = nil
	//for _, i := range da {
	//	c, _ := strconv.Atoi(fmt.Sprintf("%v", i["count"]))
	//	if _, ok := z[fmt.Sprintf("%v", i["columnName"])]; ok {
	//		if c > z[fmt.Sprintf("%v", i["columnName"])] {
	//			z[fmt.Sprintf("%v", i["columnName"])] = c
	//		}
	//	} else {
	//		z[fmt.Sprintf("%v", i["columnName"])] = c
	//		switch indexColumnType {
	//		case "int":
	//			if indexColumnIsNull {
	//				if fmt.Sprintf("%v", i["columnName"]) != "<nil>" {
	//					zc, _ := strconv.Atoi(fmt.Sprintf("%v", i["columnName"]))
	//					zint = append(zint, zc)
	//				} else {
	//					znull["<nil>"] = c
	//				}
	//			} else {
	//				zc, _ := strconv.Atoi(fmt.Sprintf("%v", i["columnName"]))
	//				zint = append(zint, zc)
	//			}
	//		case "float":
	//			if indexColumnIsNull {
	//				if fmt.Sprintf("%v", i["columnName"]) != "<nil>" {
	//					zc, _ := strconv.ParseFloat(fmt.Sprintf("%v", i["columnName"]), office)
	//					zfloat = append(zfloat, zc)
	//				} else {
	//					znull["<nil>"] = c
	//				}
	//			} else {
	//				zc, _ := strconv.ParseFloat(fmt.Sprintf("%v", i["columnName"]), office)
	//				zfloat = append(zfloat, zc)
	//			}
	//		case "char":
	//			if indexColumnIsNull {
	//				if fmt.Sprintf("%v", i["columnName"]) != "<nil>" {
	//					zchar = append(zchar, fmt.Sprintf("%v", i["columnName"]))
	//				} else {
	//					znull["<nil>"] = c
	//				}
	//			} else {
	//				zchar = append(zchar, fmt.Sprintf("%v", i["columnName"]))
	//			}
	//		}
	//	}
	//}
	//da = nil
	//vlog = fmt.Sprintf("(%d) The data merge of the index column [%v] of the source target segment table [%v.%v] is completed!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	//
	//vlog = fmt.Sprintf("(%d) Start sorting the merged data of index column [%v] of source target segment table [%v.%v] in positive order...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	////针对索引类数据进行正序排序
	//switch indexColumnType {
	//case "int":
	//	vlog = fmt.Sprintf("(%d) Start sorting index column data of type int,The index column data that needs to be sorted is [%v] ...", logThreadSeq, zint)
	//	global.Wlog.Debug(vlog)
	//	sort.Ints(zint)
	//	vlog = fmt.Sprintf("(%d) The index column data of int type is sorted, and the data after sorting in positive order is [%v] !!!", logThreadSeq, zint)
	//	global.Wlog.Debug(vlog)
	//	if _, ok := znull["<nil>"]; ok {
	//		vlog = fmt.Sprintf("(%d) The index column data of int type and index column data is null values.", logThreadSeq)
	//		global.Wlog.Debug(vlog)
	//		indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", "<nil>"), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", "<nil>")])})
	//	}
	//	for _, i := range zint {
	//		indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", i), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", i)])})
	//	}
	//	zint, z = nil, nil
	//case "float":
	//	vlog = fmt.Sprintf("(%d) Start sorting index column data of type float,The index column data that needs to be sorted is [%v] ...", logThreadSeq, zfloat)
	//	global.Wlog.Debug(vlog)
	//	sort.Float64s(zfloat)
	//	vlog = fmt.Sprintf("(%d) The index column data of float type is sorted, and the data after sorting in positive order is [%v] !!!", logThreadSeq, zfloat)
	//	global.Wlog.Debug(vlog)
	//	if _, ok := znull["<nil>"]; ok {
	//		vlog = fmt.Sprintf("(%d) The index column data of float type and index column data is null values.", logThreadSeq)
	//		global.Wlog.Debug(vlog)
	//		indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", "<nil>"), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", "<nil>")])})
	//	}
	//	for _, i := range zfloat {
	//		ii := strconv.FormatFloat(i, 'f', 2, 64)
	//		indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": ii, "count": fmt.Sprintf("%v", z[ii])})
	//	}
	//	zfloat, z = nil, nil
	//case "char":
	//	vlog = fmt.Sprintf("(%d) Start sorting index column data of type char,The index column data that needs to be sorted is [%v] ...", logThreadSeq, zchar)
	//	global.Wlog.Debug(vlog)
	//	sort.Strings(zchar)
	//	vlog = fmt.Sprintf("(%d) The index column data of char type is sorted, and the data after sorting in positive order is [%v] !!!", logThreadSeq, zchar)
	//	global.Wlog.Debug(vlog)
	//	if _, ok := znull["<nil>"]; ok {
	//		vlog = fmt.Sprintf("(%d) The index column data of char type and index column data is null values.", logThreadSeq)
	//		global.Wlog.Debug(vlog)
	//		indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": fmt.Sprintf("%v", "<nil>"), "count": fmt.Sprintf("%v", z[fmt.Sprintf("%v", "<nil>")])})
	//	}
	//	for _, i := range zchar {
	//		indexColumnUniqueS = append(indexColumnUniqueS, map[string]string{"columnName": i, "count": fmt.Sprintf("%v", z[i])})
	//	}
	//	zchar, z = nil, nil
	//}
	//vlog = fmt.Sprintf("(%d) The positive sequence sorting of the merged data of the index column [%v] of the source target segment table [%v.%v] is completed!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	//
	//vlog = fmt.Sprintf("(%d) Start to recursively process the where condition of index column [%v] of table [%v.%v] according to the size of a single check block...", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
	////处理原目标端索引列数据的集合，并按照单次校验数据块大小来进行数据截取，如果是多列索引，则需要递归查询截取
	//var startRowBool = false
	//var firstRow int
	////次数
	//
	//for f, b := range indexColumnUniqueS {
	//	vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], the column value is [%v], and the number of repeated data in the column is [%v]", logThreadSeq, level, where, sp.columnName[level], f, b["columnName"], b["count"])
	//	global.Wlog.Debug(vlog)
	//	//处理null值
	//	if b["columnName"] == "<nil>" && f == 0 {
	//		vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], Start processing null value data...", logThreadSeq, level, where, sp.columnName[level], f)
	//		global.Wlog.Debug(vlog)
	//		if where != "" {
	//			sqlwhere = fmt.Sprintf(" %s %s is null ", where, sp.columnName[level])
	//		} else {
	//			sqlwhere = fmt.Sprintf(" %s is null ", sp.columnName[level])
	//		}
	//		vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], the query sql-where is [%v], Null value data processing is complete!!!", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//		global.Wlog.Debug(vlog)
	//		sqlWhere <- sqlwhere
	//		sqlwhere = ""
	//		startRowBool = true
	//		continue
	//	}
	//	if f == 0 && b["columnName"] != "<nil>" {
	//		startRowBool = true
	//	}
	//	//获取联合索引或单列索引的首值
	//	if startRowBool {
	//		e = fmt.Sprintf("%v", b["columnName"])
	//		vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], The starting value of the current index column is [%v].", logThreadSeq, level, where, sp.columnName[level], f, e)
	//		global.Wlog.Debug(vlog)
	//		firstRow = f
	//		startRowBool = false
	//	}
	//
	//	//获取每行的count值,并将count值记录及每次动态的值
	//	c, _ := strconv.Atoi(fmt.Sprintf("%v", b["count"]))
	//	g = fmt.Sprintf("%v", b["columnName"])
	//
	//	// group count(*)的值进行累加
	//	d = d + c
	//	vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], The accumulated value of the duplicate value of the current index column is [%v].", logThreadSeq, level, where, sp.columnName[level], f, d)
	//	global.Wlog.Debug(vlog)
	//	//判断行数累加值是否小于要校验的值，并且是最后一条索引列数据
	//	if d < queryNum && f == len(indexColumnUniqueS)-1 {
	//		vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], {end index column} {group index column cumulative value < single block checksum}.", logThreadSeq, level, where, sp.columnName[level], f)
	//		global.Wlog.Debug(vlog)
	//		if level == len(sp.columnName)-1 { //最后一列索引
	//			g = fmt.Sprintf("%v", b["columnName"])
	//			vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],{end index column} {end row data} start dispos...", logThreadSeq, level, where, sp.columnName[level], f)
	//			global.Wlog.Debug(vlog)
	//			if where != "" {
	//				if e == g { //只有一行值且小于块校验值
	//					sqlwhere = fmt.Sprintf(" %v and %v = %v ", where, sp.columnName[level], g)
	//					vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sql where is [%v],{Single row index column data} {group index column cumulative value < single block checksum}.", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//					global.Wlog.Debug(vlog)
	//				} else {
	//					sqlwhere = fmt.Sprintf(" %v and %v > '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//					vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sql where is [%v],{Multi-row index column data} {group index column cumulative value < single block checksum}.", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//					global.Wlog.Debug(vlog)
	//				}
	//			} else {
	//				sqlwhere = fmt.Sprintf(" %v > '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
	//			}
	//			vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sql where is [%v],{end index column} {end row data} dispos Finish!!!", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//			global.Wlog.Debug(vlog)
	//		} else { //非最后索引列，但是是数据的最后一行，且小于校验的行数
	//			if where != "" {
	//				sqlwhere = fmt.Sprintf(" %v and %v > '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//			} else {
	//				sqlwhere = fmt.Sprintf(" %v >= '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
	//			}
	//			vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sql where is [%v],{not end index column} {end row data}", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//			global.Wlog.Debug(vlog)
	//		}
	//		sqlWhere <- sqlwhere
	//	}
	//
	//	//判断行数累加值是否>=要校验的值
	//	if d >= queryNum {
	//		//判断联合索引列深度
	//		if level == len(sp.columnName)-1 { //单列索引或最后一列索引
	//			g = fmt.Sprintf("%s", b["columnName"])
	//			vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], {end index column} {group index column cumulative value > single block checksum}.", logThreadSeq, level, where, sp.columnName[level], f)
	//			global.Wlog.Debug(vlog)
	//			if where != "" { //递归的第二层或其他层
	//				if sqlwhere == "" { //首行数据
	//					sqlwhere = fmt.Sprintf(" %v and %v >= '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//				} else { //非首行数据
	//					sqlwhere = fmt.Sprintf(" %v and %v > '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//				}
	//				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], the query sqlwhere is [%v], {end index column} {group index column cumulative value > single block checksum}.", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//				global.Wlog.Debug(vlog)
	//			} else { //where条件为空（第一次调用）
	//				if sqlwhere == "" { //首行数据
	//					sqlwhere = fmt.Sprintf(" %v >= '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
	//				} else { //非首行数据
	//					sqlwhere = fmt.Sprintf(" %v > '%v' and %v <= '%v' ", sp.columnName[level], e, sp.columnName[level], g)
	//				}
	//				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], the query sqlwhere is [%v], {end index column} {group index column cumulative value > single block checksum}.", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//				global.Wlog.Debug(vlog)
	//			}
	//			e = fmt.Sprintf("%s", b["columnName"])
	//			sqlWhere <- sqlwhere
	//		} else { //非最后一列索引列
	//			//判断当前索引列的重复值是否是校验数据块大小的两倍
	//			if d/queryNum < 2 { //小于校验块的两倍，则直接输出当前索引列深度的条件
	//				//第一层
	//				if level == 0 {
	//					if f == firstRow {
	//						sqlwhere = fmt.Sprintf(" %v and %v >= '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//					} else {
	//						sqlwhere = fmt.Sprintf(" %v and %v > '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//					}
	//					vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sqlwhere is [%s], {not end index column} {group index column cumulative value / single block checksum < 2}.", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//					global.Wlog.Debug(vlog)
	//				}
	//				if level > 0 && level < len(sp.columnName)-1 {
	//					if f == firstRow {
	//						sqlwhere = fmt.Sprintf(" %v and %v >= '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//					} else {
	//						sqlwhere = fmt.Sprintf(" %v and %v > '%v' and %v <= '%v' ", where, sp.columnName[level], e, sp.columnName[level], g)
	//					}
	//					vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v],the query sqlwhere is [%s], {not end index column} {group index column cumulative value / single block checksum < 2}.", logThreadSeq, level, where, sp.columnName[level], f, sqlwhere)
	//					global.Wlog.Debug(vlog)
	//				}
	//				e = fmt.Sprintf("%s", b["columnName"])
	//				sqlWhere <- sqlwhere
	//			} else { //大于校验块的两倍，递归进入下一层索引列进行处理
	//				if where != "" {
	//					where = fmt.Sprintf(" %v and %v = '%v' ", where, sp.columnName[level], g)
	//				} else {
	//					where = fmt.Sprintf(" %v = '%v' ", sp.columnName[level], g)
	//				}
	//				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], {not end index column} {group index column cumulative value / single block checksum > 2}.", logThreadSeq, level, where, sp.columnName[level], f)
	//				global.Wlog.Debug(vlog)
	//				level++ //索引列层数递增
	//				//进入下一层的索引计算
	//				sp.recursiveIndexColumn(sqlWhere, sdb, ddb, level, queryNum, where, selectColumn, logThreadSeq)
	//				level-- //回到上一层
	//				//递归处理结束后，处理where条件，将下一层的索引列条件去掉
	//				if strings.Contains(strings.TrimSpace(where), sp.columnName[level]) {
	//					where = strings.TrimSpace(where[:strings.Index(where, sp.columnName[level])])
	//					if strings.HasSuffix(where, "and") {
	//						where = strings.TrimSpace(where[:strings.LastIndex(where, "and")])
	//					}
	//				}
	//				vlog = fmt.Sprintf("(%d) The current index column level is [%v],the where condition is [%v], the index column is [%v], the query sequence number is [%v], {not end index column} {group index column cumulative value / single block checksum > 2}.", logThreadSeq, level, where, sp.columnName[level], f)
	//				global.Wlog.Debug(vlog)
	//				e = fmt.Sprintf("%s", b["columnName"])
	//			}
	//		}
	//		d = 0 //累加值清0
	//	}
	//}
	//vlog = fmt.Sprintf("(%d) Recursively process the where condition of the index column [%v] of table [%v.%v] according to the size of the word check block!!!", logThreadSeq, sp.columnName[level], sp.schema, sp.table)
	//global.Wlog.Info(vlog)
}

/*
	计算源目标段表的最大行数
*/
//func (sp SchedulePlan) SampLimiterSeq(limitPag chan string, limitPagDone chan bool) { //定义变量
//	var (
//		schema                         = sp.schema
//		table                          = sp.table
//		columnName                     = sp.columnName
//		chanrowCount                   = sp.chanrowCount
//		maxTableCount                  uint64
//		schedulePlanCount              uint64
//		stmpTableCount, dtmpTableCount uint64
//		err                            error
//		vlog                           string
//	)
//	time.Sleep(time.Nanosecond * 2)
//	rand.Seed(time.Now().UnixNano())
//	logThreadSeq := rand.Int63()
//	vlog = fmt.Sprintf("(%d) Check table %s.%s and start generating query sequence.", logThreadSeq, schema, table)
//	global.Wlog.Info(vlog)
//
//	vlog = fmt.Sprintf("(%d) The current verification table %s.%s single verification row number is [%d]", logThreadSeq, schema, table, chanrowCount)
//	global.Wlog.Info(vlog)
//	sdb := sp.sdbPool.Get(logThreadSeq)
//	//查询原目标端的表总行数，并生成调度计划
//	idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, ChanrowCount: sp.chanrowCount, Drivce: sp.sdrive}
//	stmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(sdb, logThreadSeq)
//	if err != nil {
//		fmt.Println(err)
//	}
//	sp.sdbPool.Put(sdb, logThreadSeq)
//	idxc.Drivce = sp.ddrive
//	ddb := sp.ddbPool.Get(logThreadSeq)
//	dtmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
//	if err != nil {
//		fmt.Println(err)
//	}
//	sp.ddbPool.Put(ddb, logThreadSeq)
//	if stmpTableCount > dtmpTableCount || stmpTableCount == dtmpTableCount {
//		maxTableCount = stmpTableCount
//	} else {
//		maxTableCount = dtmpTableCount
//	}
//	//输出校验结果信息
//	pods := Pod{
//		Schema:      schema,
//		Table:       table,
//		IndexCol:    strings.TrimLeft(strings.Join(columnName, ","), ","),
//		CheckMod:    sp.checkMod,
//		Differences: "no",
//		Datafix:     sp.datafixType,
//	}
//	if stmpTableCount != dtmpTableCount {
//		pods.Rows = fmt.Sprintf("%d|%d", stmpTableCount, dtmpTableCount)
//		measuredDataPods = append(measuredDataPods, pods)
//	} else {
//		newMaxTableCount := maxTableCount //抽样比例后的总数值
//		if maxTableCount > uint64(chanrowCount) {
//			newMaxTableCount = maxTableCount * uint64(sp.ratio) / 100
//			if chanrowCount > sp.concurrency {
//				chanrowCount = chanrowCount / sp.concurrency
//			}
//		}
//		if newMaxTableCount%uint64(chanrowCount) != 0 {
//			schedulePlanCount = newMaxTableCount/uint64(chanrowCount) + 1
//		} else {
//			schedulePlanCount = newMaxTableCount / uint64(chanrowCount)
//		}
//		tlog := fmt.Sprintf("(%d) There is currently index table %s.%s, the number of rows to be verified at a time is %d, and the number of rows to be verified is %d times", logThreadSeq, schema, table, chanrowCount, schedulePlanCount)
//		global.Wlog.Info(tlog)
//		var beginSeq int64
//		nanotime := int64(time.Now().Nanosecond())
//		rand.Seed(nanotime)
//		for i := 0; i < int(schedulePlanCount); i++ {
//			if newMaxTableCount > uint64(chanrowCount) {
//				beginSeq = rand.Int63n(int64(maxTableCount))
//			}
//			xlog := fmt.Sprintf("(%d) Verify table %s.%s The query sequence is written to the mq queue for the %d time, and the written information is {%s}", logThreadSeq, schema, table, i, fmt.Sprintf("%d,%d", beginSeq, maxTableCount))
//			global.Wlog.Info(xlog)
//			limitPag <- fmt.Sprintf("%d,%d", beginSeq, newMaxTableCount)
//			beginSeq = beginSeq + int64(chanrowCount)
//		}
//		pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, newMaxTableCount)
//		measuredDataPods = append(measuredDataPods, pods)
//	}
//	limitPagDone <- true
//	ylog := fmt.Sprintf("(%d) Verify table %s.%s Close the mq queue that stores the query sequence.", logThreadSeq, schema, table)
//	global.Wlog.Info(ylog)
//	close(limitPag)
//	zlog := fmt.Sprintf("(%d) Verify that table %s.%s query sequence is generated. !!!", logThreadSeq, schema, table)
//	global.Wlog.Info(zlog)
//}

//func (sp SchedulePlan) SampIndexColumnDispos(sqlWhere chanString, selectColumn map[string]map[string]string, sqlWhereDone chanBool) {
//	var (
//		schema                           = sp.schema
//		table                            = sp.table
//		columnName                       = sp.columnName
//		chanrowCount                     = sp.chanrowCount
//		maxTableCount, schedulePlanCount int
//	)
//	time.Sleep(time.Nanosecond * 2)
//	rand.Seed(time.Now().UnixNano())
//	logThreadSeq := rand.Int63()
//	alog := fmt.Sprintf("(%d) Check table %s.%s and start generating query sequence.", logThreadSeq, schema, table)
//	global.Wlog.Info(alog)
//
//	clog := fmt.Sprintf("(%d) The current verification table %s.%s single verification row number is [%d]", logThreadSeq, schema, table, chanrowCount)
//	global.Wlog.Info(clog)
//
//	sdb := sp.sdbPool.Get(logThreadSeq)
//	ddb := sp.ddbPool.Get(logThreadSeq)
//	sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, "", selectColumn, logThreadSeq)
//	sp.sdbPool.Put(sdb, logThreadSeq)
//	sp.ddbPool.Put(ddb, logThreadSeq)
//
//	//输出校验结果信息
//	pods := Pod{
//		Schema:      schema,
//		Table:       table,
//		IndexCol:    strings.TrimLeft(strings.Join(columnName, ","), ","),
//		CheckMod:    sp.checkMod,
//		Differences: "no",
//		Datafix:     sp.datafixType,
//	}
//	if maxTableCount%chanrowCount != 0 {
//		schedulePlanCount = maxTableCount/chanrowCount + 1
//	} else {
//		schedulePlanCount = maxTableCount / chanrowCount
//	}
//	tlog := fmt.Sprintf("(%d) There is currently index table %s.%s, the number of rows to be verified at a time is %d, and the number of rows to be verified is %d times", logThreadSeq, schema, table, chanrowCount, schedulePlanCount)
//	global.Wlog.Info(tlog)
//	pods.Rows = fmt.Sprintf("%d,%d", maxTableCount, maxTableCount)
//	measuredDataPods = append(measuredDataPods, pods)
//	ylog := fmt.Sprintf("(%d) Verify table %s.%s Close the mq queue that stores the query sequence.", logThreadSeq, schema, table)
//	global.Wlog.Info(ylog)
//	zlog := fmt.Sprintf("(%d) Verify that table %s.%s query sequence is generated. !!!", logThreadSeq, schema, table)
//	global.Wlog.Info(zlog)
//	sqlWhereDone <- true
//	close(sqlWhere)
//}

/*
针对表的所有列的数据类型进行处理，将列类型转换成字符串，例如时间类型
*/
func (sp SchedulePlan) sampQueryTableSql(sqlWhere chanString, selectSql chanMap, sampDataGroupNumber uint64, cc1 global.TableAllColumnInfoS, logThreadSeq int64) {
	var (
		vlog       string
		curry      = make(chanStruct, sp.concurrency)
		count      uint64
		autoSeq    int64
		sampleList = make(map[int64]int)
		err        error
	)
	vlog = fmt.Sprintf("(%d) Start processing the block data verification query sql of the verification table ...", logThreadSeq)
	global.Wlog.Debug(vlog)

	for i := 0; i < int(sp.sampDataGroupNumber); i++ {
		rand.Seed(time.Now().UnixNano())
		c := rand.Int63n(int64(sp.tableMaxRows / uint64(sp.chanrowCount)))
		if _, ok := sampleList[c]; !ok {
			sampleList[c]++
		}
		time.Sleep(1 * time.Nanosecond)
	}
	for {
		select {
		case c, ok := <-sqlWhere:
			if !ok {
				if len(curry) == 0 {
					close(selectSql)
					return
				}
			} else {
				count++
				if _, ok1 := sampleList[int64(count)]; !ok1 {
					continue
				}
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
					//查询该表的列名和列信息
					idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, TableColumn: cc1.SColumnInfo, Sqlwhere: c1, Drivce: sp.sdrive}
					lock.Lock()
					selectSqlMap[sp.sdrive], err = idxc.TableIndexColumn().GeneratingQuerySql(sd, logThreadSeq)
					if err != nil {
						return
					}
					lock.Unlock()
					idxc.Drivce = sp.ddrive
					idxc.TableColumn = cc1.DColumnInfo
					lock.Lock()
					selectSqlMap[sp.ddrive], err = idxc.TableIndexColumn().GeneratingQuerySql(dd, logThreadSeq)
					if err != nil {
						return
					}
					lock.Unlock()
					vlog = fmt.Sprintf("(%d) The block data verification query sql processing of the verification table is completed. !!!", logThreadSeq)
					global.Wlog.Debug(vlog)
					selectSql <- selectSqlMap
				}(c, sdb, ddb, sp.sdbPool, sp.ddbPool)
			}
		}
	}
}

/*
	单表的数据循环校验
*/
func (sp *SchedulePlan) sampSingleTableCheckProcessing(chanrowCount int, sampDataGroupNumber uint64, logThreadSeq int64) {
	var (
		vlog          string
		beginSeq      uint64
		stt, dtt      string
		err           error
		Cycles        uint64 //循环次数
		maxTableCount uint64
		md5Chan       = make(chan map[string]string, sp.mqQueueDepth)
		dataFixC      = make(chan map[string]string, sp.mqQueueDepth)
		noIndexC      = make(chan struct{}, sp.concurrency)
		tableRow      = make(chan int, sp.mqQueueDepth)
		//uniqMD5C      = make(chan map[string]string, 1)
		rowEnd bool
		//sqlStrExec    = make(chan string, sp.mqQueueDepth)
	)
	fmt.Println(fmt.Sprintf("begin checkSum no index table %s.%s", sp.schema, sp.table))
	vlog = fmt.Sprintf("(%d) Start to verify the data of the original target end of the non-indexed table %s.%s ...", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	var A, B uint64
	idxc := dbExec.IndexColumnStruct{Drivce: sp.sdrive, Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, ChanrowCount: chanrowCount}
	sdb := sp.sdbPool.Get(int64(logThreadSeq))
	A, err = idxc.TableIndexColumn().TableRows(sdb, int64(logThreadSeq))
	sp.sdbPool.Put(sdb, int64(logThreadSeq))

	ddb := sp.ddbPool.Get(int64(logThreadSeq))
	B, err = idxc.TableIndexColumn().TableRows(ddb, int64(logThreadSeq))
	sp.ddbPool.Put(ddb, int64(logThreadSeq))
	//var barTableRow int64
	//if A >= B {
	//	barTableRow = int64(A)
	//} else {
	//	barTableRow = int64(B)
	//}
	//sp.bar.NewOption(0, barTableRow)
	fmt.Println(A, B)
	pods := Pod{Schema: sp.schema, Table: sp.table,
		IndexCol:    "noIndex",
		CheckMod:    sp.checkMod,
		Differences: "no",
		Datafix:     sp.datafixType,
	}

	uniqMD5C := sp.AbDataMd5Unique(md5Chan, int64(logThreadSeq))
	sqlStrExec := sp.DataFixSql(dataFixC, &pods, int64(logThreadSeq))
	//检测临时文件，并按照一定条件读取
	go func() {
		for {
			select {
			case ic := <-uniqMD5C:
				FileOperate{File: sp.file, BufSize: 1024, fileName: sp.TmpFileName}.ConcurrencyReadFile(ic, dataFixC)
				dataFixC <- map[string]string{"END": "end"}
				close(dataFixC)
				return
			}
		}
	}()

	//统计表的总行数
	go func() {
		var cc int
		for {
			if rowEnd && len(noIndexC) == 0 {
				return
			}
			select {
			case tr := <-tableRow:
				cc++
				maxTableCount += uint64(tr)
			}
		}
	}()
	FileOper := FileOperate{File: sp.file, BufSize: 1024 * 4 * 1024, fileName: sp.TmpFileName}
	for {
		if rowEnd && len(noIndexC) == 0 {
			md5Chan <- map[string]string{"END": "end"}
			break
		}
		if rowEnd {
			continue
		}
		if beginSeq%sampDataGroupNumber != 0 {
			continue
		}
		Cycles++
		noIndexC <- struct{}{}
		go func(a, beginSeq uint64) {
			defer func() {
				<-noIndexC
			}()
			vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s, and the %d md5 check of the data consistency of the original target is started.", logThreadSeq, sp.schema, sp.table, Cycles)
			global.Wlog.Debug(vlog)
			stt, dtt, err = sp.QueryTableData(beginSeq, Cycles, chanrowCount, int64(logThreadSeq))
			if err != nil {
				fmt.Println(err)
				return
			}
			slength := len(strings.Split(stt, "/*go actions rowData*/"))
			dlength := len(strings.Split(dtt, "/*go actions rowData*/"))
			if stt == dtt && stt == "" {
				rowEnd = true
				return
			}
			if slength >= dlength {
				tableRow <- slength
			} else {
				tableRow <- dlength
			}
			sp.QueryDataCheckSum(stt, dtt, md5Chan, FileOper, Cycles, logThreadSeq)
			vlog = fmt.Sprintf("(%d) There is currently no index table %s.%s The %d round of data cycle verification is complete.", logThreadSeq, sp.schema, sp.table, Cycles)
			global.Wlog.Debug(vlog)
		}(Cycles, beginSeq)
		beginSeq = beginSeq + uint64(chanrowCount)
		time.Sleep(500 * time.Millisecond)
		//if beginSeq < uint64(barTableRow) {
		//	sp.bar.Play(int64(beginSeq))
		//} else {
		//	sp.bar.Play(barTableRow)
		//}
	}
	//sp.bar.Finish()
	sp.FixSqlExec(sqlStrExec, int64(logThreadSeq))
	//输出校验结果信息
	pods.Rows = fmt.Sprintf("%v", maxTableCount)
	measuredDataPods = append(measuredDataPods, pods)
	vlog = fmt.Sprintf("(%d) No index table %s.%s The data consistency check of the original target end is completed", logThreadSeq, sp.schema, sp.table)
	global.Wlog.Info(vlog)
	fmt.Println(fmt.Sprintf("%s.%s 校验完成", sp.schema, sp.table))
}

/*
	做数据的抽样检查，先使用count*，针对count*数量一致的表在进行部分数据的抽样检查
*/
func (sp *SchedulePlan) DoSampleDataCheck() {
	var (
		stmpTableCount, dtmpTableCount uint64
		chanrowCount                   int
		err                            error
		vlog                           string
		queueDepth                     = sp.mqQueueDepth
		sqlWhere                       = make(chanString, queueDepth)
		selectSql                      = make(chanMap, queueDepth)
		diffQueryData                  = make(chanDiffDataS, queueDepth)
		fixSQL                         = make(chanString, queueDepth)
	)
	rand.Seed(time.Now().UnixNano())
	logThreadSeq := rand.Int63()
	vlog = fmt.Sprintf("(%d) Start the sampling data verification of the original target...", logThreadSeq)
	global.Wlog.Info(vlog)
	for k, v := range sp.tableIndexColumnMap {
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		//输出校验结果信息
		sp.pods = &Pod{
			CheckObject: sp.checkObject,
			CheckMod:    sp.checkMod,
			Differences: "no",
		}
		if strings.Contains(k, "/*indexColumnType*/") {
			ki := strings.Split(k, "/*indexColumnType*/")[0]
			sp.pods.IndexCol = strings.TrimLeft(strings.Join(v, ","), ",")
			sp.indexColumnType = strings.Split(k, "/*indexColumnType*/")[1]
			if strings.Contains(ki, "/*greatdbSchemaTable*/") {
				sp.schema = strings.Split(ki, "/*greatdbSchemaTable*/")[0]
				sp.table = strings.Split(ki, "/*greatdbSchemaTable*/")[1]
			}
		} else {
			sp.pods.IndexCol = "no"
			if strings.Contains(k, "/*greatdbSchemaTable*/") {
				sp.schema = strings.Split(k, "/*greatdbSchemaTable*/")[0]
				sp.table = strings.Split(k, "/*greatdbSchemaTable*/")[1]
			}
		}
		sp.pods.Schema = sp.schema
		sp.pods.Table = sp.table
		fmt.Println(fmt.Sprintf("begin checkSum table %s.%s", sp.schema, sp.table))
		tableColumn := sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)]
		//根据索引列数量觉得chanrowCount数
		if len(v) > 1 {
			sp.chanrowCount = sp.jointIndexChanRowCount
		} else if len(v) == 1 {
			sp.chanrowCount = sp.singleIndexChanRowCount
		} else {
			if sp.singleIndexChanRowCount <= sp.jointIndexChanRowCount {
				sp.chanrowCount = sp.singleIndexChanRowCount
			} else {
				sp.chanrowCount = sp.jointIndexChanRowCount
			}
		}
		sp.columnName = v
		//统计表的总行数
		sdb := sp.sdbPool.Get(logThreadSeq)
		//查询原目标端的表总行数，并生成调度计划
		idxc := dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName, Drivce: sp.sdrive}
		stmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(sdb, logThreadSeq)
		sp.sdbPool.Put(sdb, logThreadSeq)
		if err != nil {
			return
		}
		ddb := sp.ddbPool.Get(logThreadSeq)
		idxc.Drivce = sp.ddrive
		dtmpTableCount, err = idxc.TableIndexColumn().TmpTableIndexColumnRowsCount(ddb, logThreadSeq)
		if err != nil {
			return
		}
		sp.ddbPool.Put(ddb, logThreadSeq)

		vlog = fmt.Sprintf("(%d) Start to verify the total number of rows of table %s.%s source and target ...", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
		if stmpTableCount != dtmpTableCount {
			vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is inconsistent.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			sp.pods.Differences = "yes"
			sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
			measuredDataPods = append(measuredDataPods, *sp.pods)
			vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
			continue
		}
		vlog = fmt.Sprintf("(%d) Verify that the total number of rows at the source and destination of table %s.%s is consistent", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
		var sampDataGroupNumber, dataGroupNumber uint64
		var selectColumnStringM = make(map[string]map[string]string)
		dataGroupNumber = stmpTableCount / uint64(sp.chanrowCount)
		if (stmpTableCount/uint64(sp.chanrowCount))%uint64(sp.chanrowCount) > 0 {
			dataGroupNumber = dataGroupNumber + 1
		}
		sp.sampDataGroupNumber = int64(stmpTableCount / 100 * uint64(sp.ratio) / uint64(sp.chanrowCount))
		if (stmpTableCount/100*uint64(sp.ratio))%uint64(sp.chanrowCount) > 0 {
			sp.sampDataGroupNumber = sp.sampDataGroupNumber + 1
		}
		sp.tableMaxRows = stmpTableCount
		sp.pods.Rows = fmt.Sprintf("%d,%d", stmpTableCount, dtmpTableCount)
		sp.pods.Sample = fmt.Sprintf("%d,%d", stmpTableCount, stmpTableCount/100*uint64(sp.ratio))

		if len(v) == 0 {
			sp.pods.IndexCol = "noIndex"
			sp.sampSingleTableCheckProcessing(sp.chanrowCount, sampDataGroupNumber, logThreadSeq)
			//measuredDataPods = append(measuredDataPods, pods)
			fmt.Println()
			fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
			vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, sp.schema, sp.table)
			global.Wlog.Debug(vlog)
			continue
		}
		//开始校验有索引表
		sp.pods.IndexCol = strings.TrimLeft(strings.Join(sp.columnName, ","), ",")
		//获取索引列数据长度，处理索引列数据中有null或空字符串的问题
		idxc = dbExec.IndexColumnStruct{Schema: sp.schema, Table: sp.table, ColumnName: sp.columnName,
			ChanrowCount: chanrowCount, Drivce: sp.sdrive,
			ColData: sp.tableAllCol[fmt.Sprintf("%s_greatdbCheck_%s", sp.schema, sp.table)].SColumnInfo}
		selectColumnStringM[sp.sdrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)
		idxc.Drivce = sp.ddrive
		selectColumnStringM[sp.ddrive] = idxc.TableIndexColumn().TmpTableIndexColumnSelectDispos(logThreadSeq)

		var scheduleCount = make(chan int64, 1)
		go sp.recursiveIndexColumn(sqlWhere, sdb, ddb, 0, sp.chanrowCount, "", selectColumnStringM, logThreadSeq)
		go sp.sampQueryTableSql(sqlWhere, selectSql, sampDataGroupNumber, tableColumn, logThreadSeq)
		go sp.queryTableData(selectSql, diffQueryData, tableColumn, scheduleCount, logThreadSeq)
		go sp.AbnormalDataDispos(diffQueryData, fixSQL, logThreadSeq)
		sp.DataFixDispos(fixSQL, logThreadSeq)
		fmt.Println()
		fmt.Println(fmt.Sprintf("table %s.%s checksum complete", sp.schema, sp.table))
		vlog = fmt.Sprintf("(%d) Check table %s.%s The total number of rows at the source and target end has been checked.", logThreadSeq, sp.schema, sp.table)
		global.Wlog.Debug(vlog)
	}
	vlog = fmt.Sprintf("(%d) The sampling data verification of the original target is completed !!!", logThreadSeq)
	global.Wlog.Info(vlog)
}
