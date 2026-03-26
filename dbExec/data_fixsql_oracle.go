//go:build cgo

package dbExec

import oracle "gt-checksum/Oracle"

func newOracleDataAbnormalFix(dafs DataAbnormalFixStruct) DataAbnormalFixInterface {
	return &oracle.OracleDataAbnormalFixStruct{
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
		SourceSchema:    dafs.SourceSchema,
	}
}
