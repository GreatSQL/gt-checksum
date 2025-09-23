package dbExec

import (
	"database/sql"
	mysql "gt-checksum/MySQL"
	oracle "gt-checksum/Oracle"
)

type IndexColumnStruct struct {
	Drivce           string
	Schema           string
	Table            string
	TmpTableFileName string
	ColumnName       []string
	ChanrowCount     int
	TableColumn      []map[string]string
	Sqlwhere         string
	ColData          []map[string]string
	BeginSeq         string
	RowDataCh        int64
	SelectColumn     map[string]string
}

type TableIndexColumner interface {
	TmpTableIndexColumnSelectDispos(logThreadSeq int64) map[string]string
	TmpTableIndexColumnRowsCount(db *sql.DB, logThreadSeq int64) (uint64, error)
	TmpTableColumnGroupDataDispos(db *sql.DB, where string, columnName string, logThreadSeq int64) (chan map[string]interface{}, error)
	TableRows(db *sql.DB, logThreadSeq int64) (uint64, error)
	QueryTableIndexColumnInfo(db *sql.DB, logThreadSeq int64) ([]map[string]interface{}, error)
	IndexDisposF(queryData []map[string]interface{}, logThreadSeq int64) (map[string][]string, map[string][]string, map[string][]string)
	NoIndexOrderBySingerColumn(orderCol []map[string]string) []string
	NoIndexGeneratingQueryCriteria(db *sql.DB, beginSeq uint64, chanrowCount int, logThreadSeq int64) (string, error)
	GeneratingQuerySql(db *sql.DB, logThreadSeq int64) (string, error)
	GeneratingQueryCriteria(db *sql.DB, logThreadSeq int64) (string, error)
}

func (qticis *IndexColumnStruct) TableIndexColumn() TableIndexColumner {
	var aa TableIndexColumner
	if qticis.Drivce == "mysql" {
		aa = &mysql.QueryTable{
			Schema:       qticis.Schema,
			Table:        qticis.Table,
			ColumnName:   qticis.ColumnName,
			ChanrowCount: qticis.ChanrowCount,
			TableColumn:  qticis.TableColumn,
			Sqlwhere:     qticis.Sqlwhere,
			ColData:      qticis.ColData,
			SelectColumn: qticis.SelectColumn,
			BeginSeq:     qticis.BeginSeq,
			RowDataCh:    qticis.RowDataCh,
		}
	}
	if qticis.Drivce == "godror" {
		aa = &oracle.QueryTable{
			Schema:       qticis.Schema,
			Table:        qticis.Table,
			ColumnName:   qticis.ColumnName,
			ChanrowCount: qticis.ChanrowCount,
			TableColumn:  qticis.TableColumn,
			Sqlwhere:     qticis.Sqlwhere,
			ColData:      qticis.ColData,
			SelectColumn: qticis.SelectColumn,
			BeginSeq:     qticis.BeginSeq,
			RowDataCh:    qticis.RowDataCh,
		}
	}
	return aa
}
