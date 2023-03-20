package actions

import (
	"fmt"
	"sort"
	"strings"
)

//解析binlog event生成回滚的sql语句
var rollbackSQL = func(sl []string) []string {
	var newDelS []string
	for _, i := range sl {
		if strings.HasPrefix(i, "insert") {
			ii := strings.Replace(strings.Replace(i, "insert into", "delete from", 1), "values", "where", 1)
			newDelS = append(newDelS, ii)
		}
		if strings.HasPrefix(i, "update") {
			schemaTable := strings.TrimSpace(strings.Split(strings.Split(i, "where")[0], "update")[1])
			e := strings.Split(strings.Split(i, "where")[1], "/*columnModify*/")
			oldrow := strings.Replace(e[0], "(", "", 1)
			newrow := strings.Replace(e[1], ");", "", 1)
			delSql := fmt.Sprintf("delete from %s where %s;", schemaTable, newrow)
			addSql := fmt.Sprintf("insert into %s values (%s);", schemaTable, oldrow)
			newDelS = append(newDelS, delSql, addSql)
		}
		if strings.HasPrefix(i, "delete") {
			ii := strings.Replace(strings.Replace(i, "delete from", "insert into", 1), "where", "values", 1)
			newDelS = append(newDelS, ii)
		}
	}
	return newDelS
}

//解析binlog event生成正序的sql语句
var positiveSequenceSQL = func(sl []string) []string {
	var newDelS []string
	for _, i := range sl {
		if i != "" && strings.HasPrefix(i, "insert into") {
			newDelS = append(newDelS, i)
		}
		if i != "" && strings.HasPrefix(i, "delete") {
			newDelS = append(newDelS, i)
		}
		if i != "" && strings.HasPrefix(i, "update") {
			schemaTable := strings.TrimSpace(strings.Split(strings.Split(i, "where")[0], "update")[1])
			e := strings.Split(i, "/*columnModify*/")
			delSql := fmt.Sprintf("delete from %s);", strings.Replace(e[0], "update ", "", 1))
			newDelS = append(newDelS, delSql)
			addSql := fmt.Sprintf("insert into %s values (%s", schemaTable, e[1])
			newDelS = append(newDelS, addSql)
		}
	}
	return newDelS
}

/*
	针对全量、增量数据的差异做处理，生成add和delete
*/
func DifferencesDataDispos(SourceItemAbnormalDataChan chan SourceItemAbnormalDataStruct, addChan chan string, delChan chan string) {
	for {
		select {
		case aa := <-SourceItemAbnormalDataChan:
			addS, delS := CheckSum().Arrcmp(aa.sourceSqlGather, aa.destSqlGather)
			if len(addS) == 0 && len(delS) > 0 {
				sort.Slice(delS, func(i, j int) bool {
					return delS[i] > delS[j]
				})
				dels := rollbackSQL(delS)
				fmt.Println(dels)
			} else if len(addS) > 0 && len(delS) > 0 { //针对目标端需要删除的事务进行回滚，针对事务生成回滚sql
				//此处需要将多余参数按照事务的方式进行倒叙
				sort.Slice(delS, func(i, j int) bool {
					return delS[i] > delS[j]
				})
				dels := rollbackSQL(delS)
				fmt.Println(dels)
				adds := positiveSequenceSQL(addS)
				fmt.Println(adds)

			} else if len(addS) > 0 && len(delS) == 0 {
				fmt.Println("--1--", addS)
				adds := positiveSequenceSQL(addS)
				fmt.Println(adds)
			}
		}
	}
}

/*
	针对差异数据，生成修复语句，并根据修复方式进行处理,通过对字符串做hash值，使用map进行group by去重
*/
func DataFixSql(addChan chan string, delChan chan string) {
	var (
		delHashMap = make(map[string]string)
		addHashMap = make(map[string]string)
	)

	for {
		select {
		case del := <-delChan:
			delStr := CheckSum().CheckSha1(del)
			if _, ok := delHashMap[delStr]; !ok {
				delHashMap[delStr] = ""
			}
		case add := <-addChan:
			addStr := CheckSum().CheckSha1(add)
			if _, ok := delHashMap[addStr]; !ok {
				addHashMap[addStr] = ""
			}
		}
	}
}
