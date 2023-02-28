package dbExec

import (
	"database/sql"
	"gt-checksum/MySQL"
	"gt-checksum/Oracle"
)

type TableColumnNameStruct struct {
	Schema              string
	Table               string
	IgnoreTable         string
	Drive               string
	Db                  *sql.DB
	Datafix             string
	LowerCaseTableNames string
}

type QueryTableColumnNameInterface interface {
	TableColumnName(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	GlobalAccessPri(db *sql.DB, logThreadSeq int64) (bool, error)
	TableAccessPriCheck(db *sql.DB, checkTableList []string, datefix string, logThreadSeq int64) (map[string]int, error)
	DatabaseNameList(db *sql.DB, logThreadSeq int64) (map[string]int, error)
	TableAllColumn(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	TableIndexChoice(queryData []map[string]interface{}, logThreadSeq int64) map[string][]string
	Trigger(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Proc(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Func(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Struct(db *sql.DB) (map[string]string, error)
	Foreign(db *sql.DB, logThreadSeq int64) (map[string]string, error)
	Partitions(db *sql.DB, logThreadSeq int64) (map[string]string, error)
}

func (tcns *TableColumnNameStruct) Query() QueryTableColumnNameInterface {
	var aa QueryTableColumnNameInterface
	if tcns.Drive == "mysql" {
		aa = &mysql.QueryTable{
			Schema:              tcns.Schema,
			Table:               tcns.Table,
			Db:                  tcns.Db,
			LowerCaseTableNames: tcns.LowerCaseTableNames,
		}
	}
	if tcns.Drive == "godror" {
		aa = &oracle.QueryTable{
			Schema:              tcns.Schema,
			Table:               tcns.Table,
			Db:                  tcns.Db,
			LowerCaseTableNames: tcns.LowerCaseTableNames,
		}
	}
	return aa
}
