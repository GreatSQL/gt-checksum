package actions

import (
	"database/sql"
	"errors"
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"strings"
)

type schemaTable struct {
	schema              string
	table               string
	ignoreSchema        string
	ignoreTable         string
	sourceDrive         string
	destDrive           string
	sourceDB            *sql.DB
	destDB              *sql.DB
	lowerCaseTableNames string
	datefix             string
	sfile               *os.File
	djdbc               string
	checkMode           string //列的校验模式，分为宽松模式和严格模式
	structRul           inputArg.StructS
	checkRules          inputArg.RulesS
}

/*
   查询待校验表的列名
*/
func (stcls *schemaTable) tableColumnName(db *sql.DB, tc dbExec.TableColumnNameStruct, logThreadSeq, logThreadSeq2 int64) ([]map[string][]string, error) {
	var (
		col       []map[string][]string
		vlog      string
		CS        []string
		queryData []map[string]interface{}
		err       error
		Event     = "Q_table_columns"
		A         = make(map[string][]string)
		C         = func(c string) string {
			switch c {
			case "<nil>":
				return "null"
			case "<entry>":
				return "empty"
			default:
				return c
			}
		}
	)
	if queryData, err = tc.Query().TableColumnName(db, logThreadSeq2); err != nil {
		return col, err
	}
	vlog = fmt.Sprintf("(%d) [%s] start dispos DB query columns data. to dispos it...", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	for _, v := range queryData {
		if fmt.Sprintf("%v", v["columnName"]) != "" {
			A[fmt.Sprintf("%v", v["columnName"])] = []string{C(fmt.Sprintf("%v", v["columnType"])), C(fmt.Sprintf("%v", v["charset"])), C(fmt.Sprintf("%v", v["collationName"])), C(fmt.Sprintf("%v", v["isNull"])), C(fmt.Sprintf("%v", v["columnDefault"])), C(fmt.Sprintf("%v", v["columnComment"]))}
			CS = append(CS, fmt.Sprintf("%v", v["columnName"]))
		}
	}
	for _, v := range CS {
		col = append(col, map[string][]string{v: A[v]})
	}
	vlog = fmt.Sprintf("(%d) [%s] complete dispos DB query columns data.", logThreadSeq, Event)
	global.Wlog.Debug(vlog)
	return col, nil
}

/*
	针对表的列名进行校验
*/
func (stcls *schemaTable) TableColumnNameCheck(checkTableList []string, logThreadSeq, logThreadSeq2 int64) ([]string, []string, error) {
	var (
		vlog                                 string
		newCheckTableList, abnormalTableList []string
		aa                                   = &CheckSumTypeStruct{}
		err                                  error
		tableAbnormalBool                    = false
		event                                string
	)
	vlog = fmt.Sprintf("(%d) %s Start to check the consistency information of source and target table structure and column information ...", logThreadSeq, event)
	global.Wlog.Debug(vlog)
	for _, v := range checkTableList {
		vlog = fmt.Sprintf("(%d %s Start to check the table structure consistency of table %s.", logThreadSeq, event, v)
		global.Wlog.Debug(vlog)
		var sColumn, dColumn []map[string][]string
		stcls.schema = strings.Split(v, ".")[0]
		stcls.table = strings.Split(v, ".")[1]
		dbf := dbExec.DataAbnormalFixStruct{Schema: stcls.schema, Table: stcls.table, DestDevice: stcls.destDrive, DatafixType: stcls.datefix}
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		sColumn, err = stcls.tableColumnName(stcls.sourceDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the metadata information of table %s.%s in the source %s database failed, and the error message is {%s}", logThreadSeq, event, stcls.schema, stcls.table, stcls.sourceDrive, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s source DB %s table name [%s.%s] column name message is {%v} num [%d]", logThreadSeq, event, stcls.sourceDrive, stcls.schema, stcls.table, sColumn, len(sColumn))
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		dColumn, err = stcls.tableColumnName(stcls.destDB, tc, logThreadSeq, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the metadata information of table %s.%s in the source %s database failed, and the error message is {%s}", logThreadSeq, event, stcls.schema, stcls.table, stcls.destDrive, err)
			global.Wlog.Error(vlog)
			return nil, nil, err
		}
		vlog = fmt.Sprintf("(%d) %s dest DB %s table name [%s.%s] column name message is {%v} num [%d]", logThreadSeq, event, stcls.destDrive, stcls.schema, stcls.table, dColumn, len(dColumn))
		global.Wlog.Debug(vlog)

		alterSlice := []string{}
		var sourceColumnSlice, destColumnSlice []string
		var sourceColumnMap, destColumnMap = make(map[string][]string), make(map[string][]string)
		var sourceColumnSeq, destColumnSeq = make(map[string]int), make(map[string]int)
		for k1, v1 := range sColumn {
			v1k := ""
			v2 := []string{}
			for k, v22 := range v1 {
				v1k = k
				if stcls.lowerCaseTableNames == "no" {
					v1k = strings.ToUpper(k)
				}
				v2 = v22
			}
			sourceColumnMap[v1k] = v2
			sourceColumnSeq[v1k] = k1
			sourceColumnSlice = append(sourceColumnSlice, v1k)
		}
		for k1, v1 := range dColumn {
			v1k := ""
			v2 := []string{}
			for k, v22 := range v1 {
				v1k = k
				if stcls.lowerCaseTableNames == "no" {
					v1k = strings.ToUpper(k)
				}
				v2 = v22
			}
			destColumnMap[v1k] = v2
			destColumnSeq[v1k] = k1
			destColumnSlice = append(destColumnSlice, v1k)
		}
		addColumn, delColumn := aa.Arrcmp(sourceColumnSlice, destColumnSlice)
		if stcls.checkRules.CheckObject == "data" {
			if len(addColumn) == 0 && len(delColumn) == 0 {
				newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", stcls.schema, stcls.table))
			} else {
				vlog = fmt.Sprintf("(%d) %s The %s table structure of the current source and destination is inconsistent, please check whether the current table structure is consistent. add:{%v} del:{%v}", logThreadSeq, event, fmt.Sprintf("%s.%s", stcls.schema, stcls.table), addColumn, delColumn)
				global.Wlog.Error(vlog)
				abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", stcls.schema, stcls.table))
			}
			continue
		}
		if len(addColumn) == 0 && len(delColumn) == 0 {
		}
		vlog = fmt.Sprintf("(%d) %s The column that needs to be deleted in the target %s table %s.%s is {%v}", logThreadSeq, event, stcls.destDrive, stcls.schema, stcls.table, delColumn)
		global.Wlog.Debug(vlog)
		//先删除缺失的
		if len(delColumn) > 0 {
			for _, v1 := range delColumn {
				dropSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("drop", destColumnMap[v1], 1, "", v1, logThreadSeq)
				alterSlice = append(alterSlice, dropSql)
				delete(destColumnMap, v1)
			}
		}
		vlog = fmt.Sprintf("(%d) %s The statement to delete a column in %s table %s.%s on the target side is {%v}", logThreadSeq, event, stcls.destDrive, stcls.schema, stcls.table, alterSlice)
		global.Wlog.Debug(vlog)
		for k1, v1 := range sourceColumnSlice {
			lastcolumn := ""
			var alterColumnData []string
			if k1 == 0 {
				lastcolumn = sourceColumnSlice[k1]
			} else {
				lastcolumn = sourceColumnSlice[k1-1]
			}
			switch stcls.structRul.ScheckFixRule {
			case "src":
				alterColumnData = sourceColumnMap[v1]
			case "dst":
				alterColumnData = destColumnMap[v1]
			default:
				err = errors.New(fmt.Sprintf("unknown parameters"))
				vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
				global.Wlog.Error(vlog)
				return nil, nil, err
			}
			if _, ok := destColumnMap[v1]; ok {
				switch stcls.structRul.ScheckMod {
				case "loose":
					switch stcls.structRul.ScheckOrder {
					case "yes":
						if sourceColumnSeq[v1] != destColumnSeq[v1] {
							tableAbnormalBool = true
						} else {
							tableAbnormalBool = false
						}
					case "no":
						tableAbnormalBool = false
					default:
						err = errors.New(fmt.Sprintf("unknown parameters"))
						vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
						global.Wlog.Error(vlog)
						return nil, nil, err
					}
					if tableAbnormalBool {
						modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", alterColumnData, k1, lastcolumn, v1, logThreadSeq)
						vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, v1, stcls.schema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						alterSlice = append(alterSlice, modifySql)
					}

				case "strict":
					switch stcls.structRul.ScheckOrder {
					case "yes":
						if CheckSum().CheckMd5(strings.Join(sourceColumnMap[v1], "")) != CheckSum().CheckMd5(strings.Join(destColumnMap[v1], "")) || sourceColumnSeq[v1] != destColumnSeq[v1] {
							tableAbnormalBool = true
						} else {
							tableAbnormalBool = false
						}
					case "no":
						if CheckSum().CheckMd5(strings.Join(sourceColumnMap[v1], "")) != CheckSum().CheckMd5(strings.Join(destColumnMap[v1], "")) {
							tableAbnormalBool = true
						} else {
							tableAbnormalBool = false
						}
					default:
						err = errors.New(fmt.Sprintf("unknown parameters"))
						vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
						global.Wlog.Error(vlog)
						return nil, nil, err
					}
					if tableAbnormalBool {
						modifySql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("modify", alterColumnData, k1, lastcolumn, v1, logThreadSeq)
						vlog = fmt.Sprintf("(%d) %s The column name of column %s of the source and target table %s.%s is the same, but the definition of the column is inconsistent, and a modify statement is generated, and the modification statement is {%v}", logThreadSeq, v1, stcls.schema, stcls.table, modifySql)
						global.Wlog.Warn(vlog)
						alterSlice = append(alterSlice, modifySql)
					}
				default:
					err = errors.New(fmt.Sprintf("unknown parameters"))
					vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
					global.Wlog.Error(vlog)
					return nil, nil, err
				}
				delete(destColumnMap, v1)
			} else {
				switch stcls.structRul.ScheckOrder {
				case "yes":
					lastcolumn = lastcolumn
				case "no":
					lastcolumn = "alterNoAfter"
				default:
					err = errors.New(fmt.Sprintf("unknown parameters"))
					vlog = fmt.Sprintf("(%d) %s The validation mode of the correct table structure is not selected. error message is {%v}", logThreadSeq, event, err)
					global.Wlog.Error(vlog)
					return nil, nil, err
				}
				addSql := dbf.DataAbnormalFix().FixAlterColumnSqlDispos("add", sourceColumnMap[v1], k1, lastcolumn, v1, logThreadSeq)
				vlog = fmt.Sprintf("(%d) %s The column %s is missing in the %s table %s.%s on the target side, and the add statement is generated, and the add statement is {%v}", logThreadSeq, event, v1, stcls.destDrive, stcls.schema, stcls.table, addSql)
				global.Wlog.Warn(vlog)
				alterSlice = append(alterSlice, addSql)
				delete(destColumnMap, v1)
			}
		}
		if len(alterSlice) > 0 {
			abnormalTableList = append(abnormalTableList, fmt.Sprintf("%s.%s", stcls.schema, stcls.table))
		} else {
			newCheckTableList = append(newCheckTableList, fmt.Sprintf("%s.%s", stcls.schema, stcls.table))
		}
		sqlS := dbf.DataAbnormalFix().FixAlterColumnSqlGenerate(alterSlice, logThreadSeq)
		vlog = fmt.Sprintf("(%d) %s The table structure consistency check of table %s is completed.", logThreadSeq, event, v)
		global.Wlog.Debug(vlog)
		if len(sqlS) > 0 {
			vlog = fmt.Sprintf("(%d) %s Start to repair the statement in %s table %s on the target side according to the specified repair method. The repair statement is {%v}.", logThreadSeq, event, stcls.destDrive, v, sqlS)
			global.Wlog.Debug(vlog)
			if err = ApplyDataFix(sqlS, stcls.datefix, stcls.sfile, stcls.destDrive, stcls.djdbc, logThreadSeq); err != nil {
				return nil, nil, err
			}
			vlog = fmt.Sprintf("(%d) %s Target side %s table %s repair statement application is completed.", logThreadSeq, event, stcls.destDrive, v)
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) %s The consistency information check of the source and target table structure and column information is completed", logThreadSeq, event)
	global.Wlog.Info(vlog)

	return newCheckTableList, abnormalTableList, nil
}

/*
 该函数用于获取MySQL的表的索引信息,判断表是否存在索引，加入存在，获取索引的类型，以主键索引、唯一索引、普通索引及无索引，主键索引或唯一索引以自增id为优先
  缺少索引列为空或null的处理
*/
func (stcls *schemaTable) tableIndexAlgorithm(indexType map[string][]string) (string, []string) {
	if len(indexType) > 0 {
		//假如有单列主键索引，则选择单列主键索引
		if len(indexType["pri_single"]) > 0 {
			return "pri_single", indexType["pri_single"]
		}
		//假如没有单列主键索引，有多列主键索引，且有单列唯一索引，则选择单列唯一索引
		if len(indexType["uni_single"]) > 0 {
			return "uni_single", indexType["uni_single"]
		}

		//假如没有单列主键索引，有多列主键索引，没有单列唯一索引，则选择多列主键索引
		if len(indexType["pri_multiseriate"]) > 0 {
			return "pri_multiseriate", indexType["pri_multiseriate"]
		}

		//假如没有单列主键索引，有多列主键索引，没有单列唯一索引，有多列唯一索引， 则选择多列主键索引
		if len(indexType["uni_multiseriate"]) > 0 {
			return "uni_multiseriate", indexType["uni_multiseriate"]
		}

		//有单列索引存在
		if len(indexType["mui_single"]) >= 1 {
			return "mui_single", indexType["mui_single"]
		}

		//有无单列普通索引，和多列普通索引，选择多列普通索引
		if len(indexType["mui_multiseriate"]) > 1 {
			return "mui_multiseriate", indexType["mui_multiseriate"]
		}
	} else {
		var err = errors.New("Missing indexes")
		global.Wlog.Error("[check table index choose]GreatdbCheck Check table ", stcls.schema, ".", stcls.table, ", no indexed columns, checksum terminated", err)
		//return "noIndex", []string{""}
	}
	return "", nil
}

func (stcls *schemaTable) FuzzyMatchingDispos(dbCheckNameList map[string]int, Ftable string, logThreadSeq1 int64) map[string]int {
	var a, b, f = make(map[string]int), make(map[string]int), make(map[string]int)
	for k, _ := range dbCheckNameList {
		a[strings.Split(k, "/*schema&table*/")[0]]++
	}
	//处理*.*
	if Ftable == "*.*" {
		for k, _ := range dbCheckNameList {
			d := strings.Split(k, "/*schema&table*/")
			f[fmt.Sprintf("%s.%s", d[0], d[1])]++
		}
		return f
	}

	//处理库的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		if !strings.Contains(i, ".") {
			continue
		}
		schema := i[:strings.Index(i, ".")]
		if schema == "*" { //处理*.table
			b = dbCheckNameList
		} else if strings.HasPrefix(schema, "%") && !strings.HasSuffix(schema, "%") { //处理%schema.xxx
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range a {
				//获取该库对应下的表信息，以切片的方式
				if strings.HasSuffix(k, tmpschema) {
					for ki, _ := range dbCheckNameList {
						d := strings.Split(ki, "/*schema&table*/")
						if strings.EqualFold(d[0], k) {
							b[fmt.Sprintf("%s/*schema&table*/%s", k, d[1])]++
						}
					}
				}
			}
		} else if strings.HasSuffix(schema, "%") && !strings.HasPrefix(schema, "%") { //处理schema%.xxx
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range a {
				if strings.HasPrefix(k, tmpschema) {
					for ki, _ := range dbCheckNameList {
						d := strings.Split(ki, "/*schema&table*/")
						if strings.EqualFold(d[0], k) {
							b[fmt.Sprintf("%s/*schema&table*/%s", k, d[1])]++
						}
					}
				}
			}
		} else if strings.HasPrefix(schema, "%") && strings.HasSuffix(schema, "%") { //处理%schema%.xxx
			tmpschema := strings.ReplaceAll(schema, "%", "")
			for k, _ := range a {
				if strings.Contains(k, tmpschema) {
					for ki, _ := range dbCheckNameList {
						d := strings.Split(ki, "/*schema&table*/")
						if strings.EqualFold(d[0], k) {
							b[fmt.Sprintf("%s/*schema&table*/%s", k, d[1])]++
						}
					}
				}
			}
		} else { //处理schema.xxx
			if _, ok := a[schema]; ok {
				for ki, _ := range dbCheckNameList {
					d := strings.Split(ki, "/*schema&table*/")
					if strings.EqualFold(d[0], schema) {
						b[fmt.Sprintf("%s/*schema&table*/%s", schema, d[1])]++
					}
				}
			}
		}
	}
	//处理表的模糊查询
	for _, i := range strings.Split(Ftable, ",") {
		if !strings.Contains(i, ".") {
			continue
		}
		schema := strings.ReplaceAll(i[:strings.Index(i, ".")], "%", "")
		table := i[strings.Index(i, ".")+1:]
		for k, _ := range b {
			g := strings.Split(k, "/*schema&table*/")
			if strings.Contains(g[0], schema) || schema == "*" {
				if table == "*" { //处理schema.*
					f[fmt.Sprintf("%s.%s", g[0], g[1])]++
				} else if strings.HasPrefix(table, "%") && !strings.HasSuffix(table, "%") { //处理schema.%table
					tmptable := strings.ReplaceAll(table, "%", "")
					if strings.HasSuffix(g[1], tmptable) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				} else if strings.HasSuffix(table, "%") && !strings.HasPrefix(table, "%") { //处理schema.table%
					tmptable := strings.ReplaceAll(table, "%", "")
					if strings.HasPrefix(g[1], tmptable) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				} else if strings.HasPrefix(table, "%") && strings.HasSuffix(table, "%") { //处理schema.%table%
					tmptable := strings.ReplaceAll(table, "%", "")
					if strings.Contains(g[1], tmptable) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				} else { //处理schema.table
					if strings.EqualFold(g[1], table) {
						f[fmt.Sprintf("%s.%s", g[0], g[1])]++
					}
				}
			}
		}
	}
	return f
}

/*
	处理需要校验的库表
	将忽略的库表从校验列表中去除，如果校验列表为空则退出
*/
func (stcls *schemaTable) SchemaTableFilter(logThreadSeq1, logThreadSeq2 int64) ([]string, error) {
	var (
		vlog            string
		f               []string
		dbCheckNameList map[string]int
		err             error
	)
	fmt.Println("-- gt-checksum init check table name -- ")
	vlog = fmt.Sprintf("(%d) Start to init schema.table info.", logThreadSeq1)
	global.Wlog.Info(vlog)
	//获取当前数据库信息列表
	tc := dbExec.TableColumnNameStruct{Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB, IgnoreTable: stcls.ignoreTable, LowerCaseTableNames: stcls.lowerCaseTableNames}
	vlog = fmt.Sprintf("(%d) query check database list info.", logThreadSeq1)
	global.Wlog.Debug(vlog)
	if dbCheckNameList, err = tc.Query().DatabaseNameList(stcls.sourceDB, logThreadSeq2); err != nil {
		return f, err
	}
	vlog = fmt.Sprintf("(%d) checksum database list message is {%s}", logThreadSeq1, dbCheckNameList)
	global.Wlog.Debug(vlog)
	//判断校验的库是否为空，为空则退出
	if len(dbCheckNameList) == 0 {
		vlog = fmt.Sprintf("(%d) source %s query Schema list is empty", logThreadSeq1, stcls.sourceDrive)
		global.Wlog.Error(vlog)
		return f, nil
	}
	schema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.table, logThreadSeq1)
	if len(schema) == 0 {
		vlog = fmt.Sprintf("(%d) source %s check Schema list is empty,Please check whether the database parameter is enabled for the table case setting.", logThreadSeq1, stcls.sourceDrive)
		global.Wlog.Error(vlog)
		return f, nil
	}
	ignoreSchema := stcls.FuzzyMatchingDispos(dbCheckNameList, stcls.ignoreTable, logThreadSeq1)
	for k, _ := range ignoreSchema {
		if _, ok := schema[k]; ok {
			delete(schema, k)
		}
	}
	for k, _ := range schema {
		f = append(f, k)
	}
	vlog = fmt.Sprintf("(%d) schema.table {%s} init sccessfully, num [%d].", logThreadSeq1, f, len(f))
	global.Wlog.Info(vlog)
	return f, nil
}

/*
	库表的所有列信息
*/
func (stcls *schemaTable) SchemaTableAllCol(tableList []string, logThreadSeq, logThreadSeq2 int64) map[string]global.TableAllColumnInfoS {
	var (
		a, b           []map[string]interface{}
		err            error
		vlog           string
		tableCol       = make(map[string]global.TableAllColumnInfoS)
		interfToString = func(colData []map[string]interface{}) []map[string]string {
			kel := make([]map[string]string, 0)
			for i := range colData {
				ke := make(map[string]string)
				for ii, iv := range colData[i] {
					ke[ii] = fmt.Sprintf("%v", iv)
				}
				kel = append(kel, ke)
			}
			return kel
		}
	)
	vlog = fmt.Sprintf("(%d) Start to obtain the metadata information of the source-target verification table ...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range tableList {
		if strings.Contains(i, ".") {
			schema := strings.Split(i, ".")[0]
			table := strings.Split(i, ".")[1]
			vlog = fmt.Sprintf("(%d) Start to query all column information of source DB %s table %s.%s", logThreadSeq, stcls.sourceDrive, schema, table)
			global.Wlog.Debug(vlog)
			tc := dbExec.TableColumnNameStruct{Schema: schema, Table: table, Drive: stcls.sourceDrive}
			a, err = tc.Query().TableAllColumn(stcls.sourceDB, logThreadSeq2)
			if err != nil {
				return nil
			}
			vlog = fmt.Sprintf("(%d) All column information query of source DB %s table %s.%s is completed", logThreadSeq, stcls.sourceDrive, schema, table)
			global.Wlog.Debug(vlog)

			vlog = fmt.Sprintf("(%d) Start to query all column information of dest DB %s table %s.%s", logThreadSeq, stcls.destDrive, schema, table)
			global.Wlog.Debug(vlog)
			tc.Drive = stcls.destDrive
			b, err = tc.Query().TableAllColumn(stcls.destDB, logThreadSeq2)
			if err != nil {
				return nil
			}
			vlog = fmt.Sprintf("(%d) All column information query of dest DB %s table %s.%s is completed", logThreadSeq, stcls.destDrive, schema, table)
			global.Wlog.Debug(vlog)
			tableCol[fmt.Sprintf("%s_greatdbCheck_%s", schema, table)] = global.TableAllColumnInfoS{
				SColumnInfo: interfToString(a),
				DColumnInfo: interfToString(b),
			}
			vlog = fmt.Sprintf("(%d) all column information query of table %s.%s is completed. table column message is {source: %s, dest: %s}", logThreadSeq, schema, table, interfToString(a), interfToString(b))
			global.Wlog.Debug(vlog)
		}
	}
	vlog = fmt.Sprintf("(%d) The metadata information of the source target verification table has been obtained", logThreadSeq)
	global.Wlog.Info(vlog)
	return tableCol
}

/*
	获取校验表的索引列信息，包含是否有索引，列名，列序号
*/
func (stcls *schemaTable) TableIndexColumn(dtabS []string, logThreadSeq, logThreadSeq2 int64) map[string][]string {
	var (
		queryData           []map[string]interface{}
		err                 error
		vlog                string
		tableIndexColumnMap = make(map[string][]string)
	)
	vlog = fmt.Sprintf("(%d) Start to query the table index listing information and select the appropriate index ...", logThreadSeq)
	global.Wlog.Info(vlog)

	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start querying the index list information of table %s.%s.", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		queryData, err = idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			return nil
		}
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive, Db: stcls.sourceDB}
		indexType := tc.Query().TableIndexChoice(queryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) Table %s.%s index list information query completed. index list message is {%v}", logThreadSeq, stcls.schema, stcls.table, indexType)
		global.Wlog.Debug(vlog)
		if len(indexType) == 0 { //针对于表没有索引的，进行处理
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s", stcls.schema, stcls.table)
			tableIndexColumnMap[key] = []string{}
			vlog = fmt.Sprintf("(%d) The current table %s.%s has no index.", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Warn(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Start to perform index selection on table %s.%s according to the algorithm", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			ab, aa := stcls.tableIndexAlgorithm(indexType)
			key := fmt.Sprintf("%s/*greatdbSchemaTable*/%s/*indexColumnType*/%s", stcls.schema, stcls.table, ab)
			tableIndexColumnMap[key] = aa
			vlog = fmt.Sprintf("(%d) The index selection of table %s.%s is completed, and the selected index information is { keyName:%s keyColumn: %s}", logThreadSeq, stcls.schema, stcls.table, ab, aa)
			global.Wlog.Debug(vlog)
		}
	}
	zlog := fmt.Sprintf("(%d) Table index listing information and appropriate index completion", logThreadSeq)
	global.Wlog.Info(zlog)
	return tableIndexColumnMap
}

/*
	校验触发器
*/
func (stcls *schemaTable) Trigger(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog string
		tmpM = make(map[string]int)
		z    = make(map[string]int)
		c, d []string
		pods = Pod{
			Datafix:     "no",
			CheckObject: "trigger",
		}
		sourceTrigger, destTrigger map[string]string
		err                        error
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Trigger. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		i = strings.Split(i, ".")[0]
		if stcls.lowerCaseTableNames == "yes" {
			i = strings.ToUpper(i)
			z[i]++
		}
		if stcls.lowerCaseTableNames == "no" {
			z[i]++
		}
	}
	//校验触发器
	for i, _ := range z {
		pods.Schema = stcls.schema
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data databases %s Trigger. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: i, Drive: stcls.sourceDrive}
		if sourceTrigger, err = tc.Query().Trigger(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceTrigger)
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data databases %s Trigger data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destTrigger, err = tc.Query().Trigger(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destTrigger)
		global.Wlog.Debug(vlog)
		if len(sourceTrigger) == 0 && len(destTrigger) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Trigger. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceTrigger {
			tmpM[k]++
		}
		for k, _ := range destTrigger {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Trigger is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			pods.TriggerName = strings.ReplaceAll(strings.Split(k, ".")[1], "\"", "")
			if sourceTrigger[k] != destTrigger[k] {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				pods.Differences = "no"
				c = append(c, k)
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Trigger. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Trigger data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Trigger data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
	校验存储过程
*/
func (stcls *schemaTable) Proc(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog      string
		c, d      []string
		schemaMap = make(map[string]int)
		pods      = Pod{
			Datafix:     "no",
			CheckObject: "proc",
		}
		sourceProc, destProc map[string]string
		err                  error
		tmpM                 = make(map[string]int)
	)
	vlog = fmt.Sprintf("(%d) Start init check source and target DB Stored Procedure. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}

	for schema, _ := range schemaMap {
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		if sourceProc, err = tc.Query().Proc(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceProc)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s Stored Procedure data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destProc, err = tc.Query().Proc(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destProc)
		global.Wlog.Debug(vlog)
		if len(sourceProc) == 0 && len(destProc) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Warn(vlog)
			continue
		}

		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Procedure. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k]++
		}
		for k, _ := range destProc {
			if k == "DEFINER" {
				continue
			}
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Stored Procedure is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		pods.Schema = schema
		for k, v := range tmpM {
			if stcls.sourceDrive != stcls.destDrive {
				if v == 2 {
					pods.ProcName = k
					pods.Differences = "no"
					c = append(c, k)
				} else {
					pods.ProcName = k
					pods.Differences = "yes"
					d = append(d, k)
				}
			} else {
				if sourceProc[k] != destProc[k] {
					pods.ProcName = k
					pods.Differences = "yes"
					d = append(d, k)
				} else {
					pods.ProcName = k
					pods.Differences = "no"
					c = append(c, k)
				}
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Procedure. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Stored Procedure data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Procedure data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
	校验函数
*/
func (stcls *schemaTable) Func(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                 string
		sourceFunc, destFunc map[string]string
		tmpM                 = make(map[string]int)
		schemaMap            = make(map[string]int)
		pods                 = Pod{
			Datafix:     "no",
			CheckObject: "func",
		}
		err  error
		c, d []string
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Stored Function. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		schemaMap[strings.Split(i, ".")[0]] = +schemaMap[strings.Split(i, ".")[0]]
	}

	for schema, _ := range schemaMap {
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data databases %s Stored Function. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: schema, Drive: stcls.sourceDrive}
		if sourceFunc, err = tc.Query().Func(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data databases %s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, sourceFunc)
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s Stored Function data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destFunc, err = tc.Query().Func(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data databases %s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, destFunc)
		global.Wlog.Debug(vlog)

		if len(sourceFunc) == 0 && len(destFunc) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this databases %s will be skipped", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target databases %s Stored Function. to dispos it...", logThreadSeq, stcls.schema)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceFunc {
			tmpM[k]++
		}
		for k, _ := range destFunc {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Stored Function is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		pods.Schema = schema
		for k, v := range tmpM {
			var sv, dv string
			if stcls.sourceDrive != stcls.destDrive { //异构,只校验函数名
				if v == 2 {
					pods.FuncName = k
					pods.Differences = "no"
					c = append(c, k)
				} else {
					pods.FuncName = k
					pods.Differences = "yes"
					d = append(d, k)
				}
			} else { //相同架构，校验函数结构体
				sv, dv = sourceFunc[k], destFunc[k]
				if sv != dv {
					pods.FuncName = k
					pods.Differences = "yes"
					d = append(d, k)
				} else {
					pods.FuncName = k
					pods.Differences = "no"
					c = append(c, k)
				}
			}
			vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment databases %s Stored Function. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, stcls.schema, c, len(c), d, len(d))
			global.Wlog.Debug(vlog)
			vlog = fmt.Sprintf("(%d) The source target segment databases %s Stored Function data verification is completed", logThreadSeq, stcls.schema)
			global.Wlog.Debug(vlog)
			measuredDataPods = append(measuredDataPods, pods)
		}
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Stored Function data. normal databases message is {%s} num [%d] abnormal databases message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

/*
	校验函数
*/

//func (stcls *schemaTable) IndexDisposF(queryData []map[string]interface{}) ([]string, map[string][]string, map[string][]string) {
//	nultiseriateIndexColumnMap := make(map[string][]string)
//	multiseriateIndexColumnMap := make(map[string][]string)
//	var PriIndexCol, uniIndexCol, mulIndexCol []string
//	var indexName string
//	for _, v := range queryData {
//		var currIndexName = strings.ToUpper(v["indexName"].(string))
//		//判断唯一索引（包含主键索引和普通索引）
//		if v["nonUnique"].(string) == "0" || v["nonUnique"].(string) == "UNIQUE" {
//			if currIndexName == "PRIMARY" || v["columnKey"].(string) == "1" {
//				if currIndexName != indexName {
//					indexName = currIndexName
//				}
//				PriIndexCol = append(PriIndexCol, fmt.Sprintf("%s", v["columnName"]))
//			} else {
//				if currIndexName != indexName {
//					indexName = currIndexName
//					nultiseriateIndexColumnMap[indexName] = append(uniIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
//				} else {
//					nultiseriateIndexColumnMap[indexName] = append(nultiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
//				}
//			}
//		}
//		//处理普通索引
//		if v["nonUnique"].(string) == "1" || (v["nonUnique"].(string) == "NONUNIQUE" && v["columnKey"].(string) == "0") {
//			if currIndexName != indexName {
//				indexName = currIndexName
//				multiseriateIndexColumnMap[indexName] = append(mulIndexCol, fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
//			} else {
//				multiseriateIndexColumnMap[indexName] = append(multiseriateIndexColumnMap[indexName], fmt.Sprintf("%s /*actions Column Type*/ %s", v["columnName"], v["columnType"]))
//			}
//		}
//	}
//	return PriIndexCol, nultiseriateIndexColumnMap, multiseriateIndexColumnMap
//}

func (stcls *schemaTable) Foreign(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                       string
		sourceForeign, destForeign map[string]string
		tmpM                       = make(map[string]int)
		err                        error
		pods                       = Pod{
			Datafix:     "no",
			CheckObject: "Foreign",
		}
	)

	vlog = fmt.Sprintf("(%d) Start init check source and target DB Foreign. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	//校验外键
	var c, d []string
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		pods.Schema = stcls.schema
		pods.Table = stcls.table
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourceForeign, err = tc.Query().Foreign(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Source DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourceForeign)
		global.Wlog.Debug(vlog)

		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s Foreign. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc.Drive = stcls.destDrive
		if destForeign, err = tc.Query().Foreign(stcls.destDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destForeign)
		global.Wlog.Debug(vlog)
		if len(sourceForeign) == 0 && len(destForeign) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}
		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Foreign Name. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourceForeign {
			tmpM[k]++
		}
		for k, _ := range destForeign {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the Foreign table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			if sourceForeign[k] != destForeign[k] {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				pods.Differences = "no"
				c = append(c, k)
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s Foreign. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s Foreign data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table Foreign data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

//校验分区
func (stcls *schemaTable) Partitions(dtabS []string, logThreadSeq, logThreadSeq2 int64) {
	var (
		vlog                             string
		err                              error
		c, d                             []string
		sourcePartitions, destPartitions map[string]string
		pods                             = Pod{
			Datafix:     "no",
			CheckObject: "Partitions",
		}
		tmpM = make(map[string]int)
	)
	vlog = fmt.Sprintf("(%d) Start init check source and target DB partition table. to check it...", logThreadSeq)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		vlog = fmt.Sprintf("(%d) Start processing source DB %s data table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		tc := dbExec.TableColumnNameStruct{Schema: stcls.schema, Table: stcls.table, Drive: stcls.sourceDrive}
		if sourcePartitions, err = tc.Query().Partitions(stcls.sourceDB, logThreadSeq2); err != nil {
			return
		}

		vlog = fmt.Sprintf("(%d) Source DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.sourceDrive, stcls.schema, stcls.table, sourcePartitions)
		global.Wlog.Debug(vlog)

		tc.Drive = stcls.destDrive
		vlog = fmt.Sprintf("(%d) Start processing dest DB %s data table %s.%s partitions data. to dispos it...", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		if destPartitions, err = tc.Query().Partitions(stcls.destDB, logThreadSeq2); err != nil {
			return
		}
		vlog = fmt.Sprintf("(%d) Dest DB %s data table %s.%s message is {%s}", logThreadSeq, stcls.destDrive, stcls.schema, stcls.table, destPartitions)
		global.Wlog.Debug(vlog)

		pods.Schema = stcls.schema
		pods.Table = stcls.table
		if len(sourcePartitions) == 0 && len(destPartitions) == 0 {
			vlog = fmt.Sprintf("(%d) The current original target data is empty, and the verification of this table %s.%s will be skipped", logThreadSeq, stcls.schema, stcls.table)
			global.Wlog.Debug(vlog)
			continue
		}

		tmpM = nil
		vlog = fmt.Sprintf("(%d) Start seeking the union of the source and target table %s.%s Partitions Column. to dispos it...", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		for k, _ := range sourcePartitions {
			tmpM[k]++
		}
		for k, _ := range destPartitions {
			tmpM[k]++
		}
		vlog = fmt.Sprintf("(%d) Start to compare whether the partitions table is consistent.", logThreadSeq)
		global.Wlog.Debug(vlog)
		for k, _ := range tmpM {
			if strings.Join(strings.Fields(sourcePartitions[k]), "") != strings.Join(strings.Fields(destPartitions[k]), "") {
				pods.Differences = "yes"
				d = append(d, k)
			} else {
				c = append(c, k)
				pods.Differences = "no"
			}
		}
		vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table %s.%s partitions. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, stcls.schema, stcls.table, c, len(c), d, len(d))
		global.Wlog.Debug(vlog)
		vlog = fmt.Sprintf("(%d) The source target segment table %s.%s partitions data verification is completed", logThreadSeq, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		measuredDataPods = append(measuredDataPods, pods)
	}
	vlog = fmt.Sprintf("(%d) Complete the consistency check of the source target segment table partitions data. normal table message is {%s} num [%d] abnormal table message is {%s} num [%d]", logThreadSeq, c, len(c), d, len(d))
	global.Wlog.Info(vlog)
}

func (stcls *schemaTable) Index(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	var (
		vlog          string
		sqlS          []string
		aa            = &CheckSumTypeStruct{}
		event         string
		indexGenerate = func(smu, dmu map[string][]string, a *CheckSumTypeStruct, indexType string) []string {
			var cc, c, d []string
			for k, _ := range smu {
				c = append(c, k)
			}
			for k, _ := range dmu {
				d = append(d, k)
			}
			if a.CheckMd5(strings.Join(c, ",")) != a.CheckMd5(strings.Join(d, ",")) {
				e, f := a.Arrcmp(c, d)
				dbf := dbExec.DataAbnormalFixStruct{Schema: stcls.schema, Table: stcls.table, SourceDevice: stcls.sourceDrive, DestDevice: stcls.destDrive, IndexType: indexType, DatafixType: stcls.datefix}
				cc = dbf.DataAbnormalFix().FixAlterIndexSqlExec(e, f, smu, stcls.sourceDrive, logThreadSeq)
			}
			return cc
		}
	)
	fmt.Println("-- gt-checksum checksum table index info -- ")
	event = fmt.Sprintf("[%s]", "check_table_index")
	//校验索引
	vlog = fmt.Sprintf("(%d) %s start init check source and target DB index Column. to check it...", logThreadSeq, event)
	global.Wlog.Info(vlog)
	for _, i := range dtabS {
		stcls.schema = strings.Split(i, ".")[0]
		stcls.table = strings.Split(i, ".")[1]
		idxc := dbExec.IndexColumnStruct{Schema: stcls.schema, Table: stcls.table, Drivce: stcls.sourceDrive}
		vlog = fmt.Sprintf("(%d) %s Start processing source DB %s data table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.sourceDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		squeryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.sourceDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of source %s database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.sourceDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		spri, suni, smul := idxc.TableIndexColumn().IndexDisposF(squeryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) %s The index column data of the source %s database table %s is {primary:%v,unique key:%v,index key:%v}", logThreadSeq, event, stcls.sourceDrive, i, spri, suni, smul)
		global.Wlog.Debug(vlog)

		idxc.Drivce = stcls.destDrive
		vlog = fmt.Sprintf("(%d) %s Start processing dest DB %s data table %s.%s index column data. to dispos it...", logThreadSeq, event, stcls.destDrive, stcls.schema, stcls.table)
		global.Wlog.Debug(vlog)
		dqueryData, err := idxc.TableIndexColumn().QueryTableIndexColumnInfo(stcls.destDB, logThreadSeq2)
		if err != nil {
			vlog = fmt.Sprintf("(%d) %s Querying the index column data of dest %s database table %s failed, and the error message is {%v}", logThreadSeq, event, stcls.destDrive, i, err)
			global.Wlog.Error(vlog)
			return err
		}
		dpri, duni, dmul := idxc.TableIndexColumn().IndexDisposF(dqueryData, logThreadSeq2)
		vlog = fmt.Sprintf("(%d) %s The index column data of the source %s database table %s is {primary:%v,unique key:%v,index key:%v}", logThreadSeq, event, stcls.destDrive, i, dpri, duni, dmul)
		global.Wlog.Debug(vlog)

		var pods = Pod{
			Datafix:     stcls.datefix,
			CheckObject: "Index",
			Differences: "no",
			Schema:      stcls.schema,
			Table:       stcls.table,
		}
		//先比较主键索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the primary key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(spri, dpri, aa, "pri")...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the primary key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		//再比较唯一索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(suni, duni, aa, "uni")...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Info(vlog)
		//后比较普通索引
		vlog = fmt.Sprintf("(%d) %s Start to compare whether the no-unique key index is consistent.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		sqlS = append(sqlS, indexGenerate(smul, dmul, aa, "mul")...)
		vlog = fmt.Sprintf("(%d) %s Compare whether the no-unique key index is consistent and verified.", logThreadSeq, event)
		global.Wlog.Debug(vlog)
		if len(sqlS) > 0 {
			pods.Differences = "yes"
		}
		if err = ApplyDataFix(sqlS, stcls.datefix, stcls.sfile, stcls.destDrive, stcls.djdbc, logThreadSeq); err != nil {
			return err
		}
		measuredDataPods = append(measuredDataPods, pods)
		vlog = fmt.Sprintf("(%d) %s The source target segment table %s.%s index column data verification is completed", logThreadSeq, event, stcls.schema, stcls.table)
		global.Wlog.Info(vlog)
	}
	fmt.Println("-- gt-checksum report: Table index verification completed -- ")
	return nil
}

/*
	校验表结构是否正确
*/
func (stcls *schemaTable) Struct(dtabS []string, logThreadSeq, logThreadSeq2 int64) error {
	//校验列名
	var (
		vlog  string
		event string
	)
	event = fmt.Sprintf("[check_table_columns]")
	fmt.Println("-- gt-checksum checksum table strcut info -- ")
	vlog = fmt.Sprintf("(%d) %s begin check source and target struct. check object is {%v} num[%d]", logThreadSeq, event, dtabS, len(dtabS))
	global.Wlog.Info(vlog)
	normal, abnormal, err := stcls.TableColumnNameCheck(dtabS, logThreadSeq, logThreadSeq2)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) %s Complete the data consistency check of the source target segment table structure column. normal table message is {%s} num [%d], abnormal table message is {%s} num [%d].", logThreadSeq, event, normal, len(normal), abnormal, len(abnormal))
	global.Wlog.Debug(vlog)
	//输出校验结果信息
	var pods = Pod{
		Datafix:     stcls.datefix,
		CheckObject: "Struct",
	}
	for _, i := range normal {
		aa := strings.Split(i, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.Differences = "no"
		measuredDataPods = append(measuredDataPods, pods)
	}
	for _, i := range abnormal {
		aa := strings.Split(i, ".")
		pods.Schema = aa[0]
		pods.Table = aa[1]
		pods.Differences = "yes"
		measuredDataPods = append(measuredDataPods, pods)
	}
	fmt.Println("-- gt-checksum report Table structure verification completed -- ")
	vlog = fmt.Sprintf("(%d) %s check source and target DB table struct complete", logThreadSeq, event)
	global.Wlog.Info(vlog)
	return nil
}

/*
	用于测试db链接串是否正确，是否可以连接
*/
func dbOpenTest(drive, jdbc string) *sql.DB {
	p := dbExec.DBexec()
	p.JDBC = jdbc
	p.DBDevice = drive
	db, err := p.OpenDB()
	if err != nil {
		fmt.Println("")
		os.Exit(1)
	}
	err1 := db.Ping()
	if err1 != nil {
		os.Exit(1)
	}
	return db
}

/*
	库表的初始化
*/
func SchemaTableInit(m *inputArg.ConfigParameter) *schemaTable {
	sdb := dbOpenTest(m.SecondaryL.DsnsV.SrcDrive, m.SecondaryL.DsnsV.SrcJdbc)
	ddb := dbOpenTest(m.SecondaryL.DsnsV.DestDrive, m.SecondaryL.DsnsV.DestJdbc)
	return &schemaTable{
		ignoreTable:         m.SecondaryL.SchemaV.IgnoreTables,
		table:               m.SecondaryL.SchemaV.Tables,
		sourceDrive:         m.SecondaryL.DsnsV.SrcDrive,
		destDrive:           m.SecondaryL.DsnsV.DestDrive,
		sourceDB:            sdb,
		destDB:              ddb,
		lowerCaseTableNames: m.SecondaryL.SchemaV.LowerCaseTableNames,
		datefix:             m.SecondaryL.RepairV.Datafix,
		sfile:               m.SecondaryL.RepairV.FixFileFINE,
		djdbc:               m.SecondaryL.DsnsV.DestJdbc,
		structRul:           m.SecondaryL.StructV,
		checkRules:          m.SecondaryL.RulesV,
	}
}
