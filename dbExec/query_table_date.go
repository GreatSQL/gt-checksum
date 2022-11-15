package dbExec

import (
	"database/sql"
	mysql "greatdbCheck/MySQL"
	oracle "greatdbCheck/Oracle"
)

type IndexColumnStruct struct {
	Drivce           string
	Schema           string
	Table            string
	TmpTableFileName string
	ColumnName       []string
	ChanrowCount     int
	TableColumn      []map[string]string
	//TableColumn global.TableAllColumnInfoS
	Sqlwhere string
	ColData  []map[string]string
}

type TableIndexColumner interface {
	QueryTableIndexColumnInfo(db *sql.DB) ([]map[string]interface{}, error)
	TmpTableRowsCount(db *sql.DB) (int, error)
	TmpTableIndexColumnDataDispos(db *sql.DB, threadId int, selectColumnString, lengthTrim string, columnLengthAs, columnName []string, beginSeq, rowDataCh int64) ([]string, error)
	TmpTableIndexColumnDataLength() (string, []string, string)
	NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq, chanrowCount int) (string, error)
	GeneratingQuerySql() string
	GeneratingQueryCriteria(db *sql.DB) (string, error)
}

func (qticis *IndexColumnStruct) TableIndexColumn() TableIndexColumner {
	var aa TableIndexColumner
	if qticis.Drivce == "mysql" {
		aa = &mysql.QueryTableDate{
			Schema:       qticis.Schema,
			Table:        qticis.Table,
			ColumnName:   qticis.ColumnName,
			ChanrowCount: qticis.ChanrowCount,
			TableColumn:  qticis.TableColumn,
			Sqlwhere:     qticis.Sqlwhere,
			ColData:      qticis.ColData,
		}
	}
	if qticis.Drivce == "godror" {
		aa = &oracle.QueryTableDate{
			Schema:       qticis.Schema,
			Table:        qticis.Table,
			ColumnName:   qticis.ColumnName,
			ChanrowCount: qticis.ChanrowCount,
			TableColumn:  qticis.TableColumn,
			Sqlwhere:     qticis.Sqlwhere,
			ColData:      qticis.ColData,
		}
	}
	return aa
}
