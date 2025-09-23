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
	SourceSchema    string // 源端schema，用于处理数据库映射关系
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
			DatafixType:     dafs.DatafixType,
			SourceSchema:    dafs.SourceSchema, // 传递源端schema信息
		}
	}
	if dafs.DestDevice == "godror" {
		// 创建Oracle数据修复结构体
		// 注意：Oracle结构体不支持SourceSchema字段，所以我们只传递基本字段
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
