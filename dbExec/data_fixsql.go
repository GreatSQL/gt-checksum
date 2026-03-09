package dbExec

import (
	"database/sql"
	"fmt"
	mysql "gt-checksum/MySQL"
	oracle "gt-checksum/Oracle"
	"gt-checksum/global"
)

type DataAbnormalFixStruct struct {
	Schema                  string
	Table                   string
	RowData                 string
	SourceDevice            string
	DestDevice              string
	Sqlwhere                string
	IndexColumnType         string
	ColData                 []map[string]string
	IndexType               string
	IndexColumn             []string
	DatafixType             string
	SourceSchema            string            // 源端schema，用于处理数据库映射关系
	CaseSensitiveObjectName string            // 是否区分对象名大小写
	IndexVisibilityMap      map[string]string // 索引可见性信息
}
type DataAbnormalFixInterface interface {
	FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error)
	FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error)
	FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string
	FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string
	FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string
	FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations []string, logThreadSeq int64) []string
	FixAlterIndexSqlGenerate(indexOperations []string, logThreadSeq int64) []string
	FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string
	FixTableAutoIncrementSqlGenerate(nextValue int64, logThreadSeq int64) []string
}

type unsupportedDataAbnormalFix struct {
	destDevice string
}

func (u *unsupportedDataAbnormalFix) unsupportedErr(action string) error {
	err := fmt.Errorf("unsupported destination device [%s] for %s", u.destDevice, action)
	if global.Wlog != nil {
		global.Wlog.Error(err.Error())
	}
	return err
}

func (u *unsupportedDataAbnormalFix) FixInsertSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	return "", u.unsupportedErr("FixInsertSqlExec")
}

func (u *unsupportedDataAbnormalFix) FixDeleteSqlExec(db *sql.DB, sourceDrive string, logThreadSeq int64) (string, error) {
	return "", u.unsupportedErr("FixDeleteSqlExec")
}

func (u *unsupportedDataAbnormalFix) FixAlterIndexSqlExec(e, f []string, si map[string][]string, sourceDrive string, logThreadSeq int64) []string {
	_ = u.unsupportedErr("FixAlterIndexSqlExec")
	return nil
}

func (u *unsupportedDataAbnormalFix) FixAlterColumnSqlDispos(alterType string, columnDataType []string, columnSeq int, lastColumn, curryColumn string, logThreadSeq int64) string {
	_ = u.unsupportedErr("FixAlterColumnSqlDispos")
	return ""
}

func (u *unsupportedDataAbnormalFix) FixAlterColumnSqlGenerate(modifyColumn []string, logThreadSeq int64) []string {
	_ = u.unsupportedErr("FixAlterColumnSqlGenerate")
	return nil
}

func (u *unsupportedDataAbnormalFix) FixAlterColumnAndIndexSqlGenerate(columnOperations, indexOperations []string, logThreadSeq int64) []string {
	_ = u.unsupportedErr("FixAlterColumnAndIndexSqlGenerate")
	return nil
}

func (u *unsupportedDataAbnormalFix) FixAlterIndexSqlGenerate(indexOperations []string, logThreadSeq int64) []string {
	_ = u.unsupportedErr("FixAlterIndexSqlGenerate")
	return nil
}

func (u *unsupportedDataAbnormalFix) FixTableCharsetSqlGenerate(charset, collation string, logThreadSeq int64) []string {
	_ = u.unsupportedErr("FixTableCharsetSqlGenerate")
	return nil
}

func (u *unsupportedDataAbnormalFix) FixTableAutoIncrementSqlGenerate(nextValue int64, logThreadSeq int64) []string {
	_ = u.unsupportedErr("FixTableAutoIncrementSqlGenerate")
	return nil
}

func (dafs DataAbnormalFixStruct) DataAbnormalFix() DataAbnormalFixInterface {
	var tqaci DataAbnormalFixInterface
	if dafs.DestDevice == "mysql" {
		tqaci = &mysql.MysqlDataAbnormalFixStruct{
			Schema:                  dafs.Schema,
			Table:                   dafs.Table,
			Sqlwhere:                dafs.Sqlwhere,
			RowData:                 dafs.RowData,
			SourceDevice:            dafs.SourceDevice,
			IndexColumnType:         dafs.IndexColumnType,
			ColData:                 dafs.ColData,
			IndexType:               dafs.IndexType,
			IndexColumn:             dafs.IndexColumn,
			DatafixType:             dafs.DatafixType,
			SourceSchema:            dafs.SourceSchema,            // 传递源端schema信息
			CaseSensitiveObjectName: dafs.CaseSensitiveObjectName, // 传递是否区分对象名大小写
			IndexVisibilityMap:      dafs.IndexVisibilityMap,      // 传递索引可见性信息
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
	if tqaci == nil {
		tqaci = &unsupportedDataAbnormalFix{destDevice: dafs.DestDevice}
	}
	return tqaci
}
