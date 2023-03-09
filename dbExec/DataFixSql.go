package dbExec

import (
	"database/sql"
	mysql "gt-checksum/MySQL"
	oracle "gt-checksum/Oracle"
)

type DataAbnormalFixStruct struct {
	Schema          string
	Table           string
	RowData         string
	SourceDevice    string
	DestDevice      string
	Sqlwhere        string
	IndexColumnType string
	ColData         []map[string]string
	IndexType       string
	IndexColumn     []string
	DatafixType     string
}
type DataAbnormalFixInterface interface {
	FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error)
	FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error)
	FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string
	FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string
	FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string
}

func (dafs DataAbnormalFixStruct) DataAbnormalFix() DataAbnormalFixInterface {
	var tqaci DataAbnormalFixInterface
	if dafs.DestDevice == "mysql" {
		tqaci = &mysql.MysqlDataAbnormalFixStruct{
			Schema:          dafs.Schema,
			Table:           dafs.Table,
			Sqlwhere:        dafs.Sqlwhere,
			RowData:         dafs.RowData,
			SourceDevice:    dafs.SourceDevice,
			IndexColumnType: dafs.IndexColumnType,
			ColData:         dafs.ColData,
			IndexType:       dafs.IndexType,
			IndexColumn:     dafs.IndexColumn,
		}
	}
	if dafs.DestDevice == "godror" {
		tqaci = &oracle.OracleDataAbnormalFixStruct{
			Schema:          dafs.Schema,
			Table:           dafs.Table,
			Sqlwhere:        dafs.Sqlwhere,
			RowData:         dafs.RowData,
			SourceDevice:    dafs.SourceDevice,
			IndexColumnType: dafs.IndexColumnType,
			ColData:         dafs.ColData,
			IndexType:       dafs.IndexType,
			IndexColumn:     dafs.IndexColumn,
			DatafixType:     dafs.DatafixType,
		}
	}
	return tqaci
}
