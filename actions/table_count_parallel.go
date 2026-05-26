package actions

import (
	"fmt"
	"gt-checksum/dbExec"
	"gt-checksum/global"
)

type tableRowsCountResult struct {
	rows uint64
	err  error
}

type exactRowsCountResult struct {
	rows  int64
	exact bool
}

func queryTmpTableRowsFromPool(pool *global.Pool, idxc dbExec.IndexColumnStruct, logThreadSeq int64) (uint64, error) {
	db := pool.Get(logThreadSeq)
	if db == nil {
		return 0, fmt.Errorf("failed to get database connection")
	}
	defer pool.Put(db, logThreadSeq)

	tableIndexColumn := idxc.TableIndexColumn()
	if tableIndexColumn == nil {
		return 0, fmt.Errorf("unsupported database drive %q", idxc.Drivce)
	}
	return tableIndexColumn.TmpTableIndexColumnRowsCount(db, logThreadSeq)
}

func queryTableRowsFromPool(pool *global.Pool, idxc dbExec.IndexColumnStruct, logThreadSeq int64) (uint64, error) {
	db := pool.Get(logThreadSeq)
	if db == nil {
		return 0, fmt.Errorf("failed to get database connection")
	}
	defer pool.Put(db, logThreadSeq)

	tableIndexColumn := idxc.TableIndexColumn()
	if tableIndexColumn == nil {
		return 0, fmt.Errorf("unsupported database drive %q", idxc.Drivce)
	}
	return tableIndexColumn.TableRows(db, logThreadSeq)
}

func (sp *SchedulePlan) querySourceTargetTmpTableRows(sourceIdxc, destIdxc dbExec.IndexColumnStruct, logThreadSeq int64) (uint64, uint64, error, error) {
	sourceCh := make(chan tableRowsCountResult, 1)
	targetCh := make(chan tableRowsCountResult, 1)

	go func() {
		rows, err := queryTmpTableRowsFromPool(sp.sdbPool, sourceIdxc, logThreadSeq)
		sourceCh <- tableRowsCountResult{rows: rows, err: err}
	}()
	go func() {
		rows, err := queryTmpTableRowsFromPool(sp.ddbPool, destIdxc, logThreadSeq)
		targetCh <- tableRowsCountResult{rows: rows, err: err}
	}()

	sourceResult := <-sourceCh
	targetResult := <-targetCh
	return sourceResult.rows, targetResult.rows, sourceResult.err, targetResult.err
}

func (sp *SchedulePlan) querySourceTargetTableRows(sourceIdxc, destIdxc dbExec.IndexColumnStruct, logThreadSeq int64) (uint64, uint64, error, error) {
	sourceCh := make(chan tableRowsCountResult, 1)
	targetCh := make(chan tableRowsCountResult, 1)

	go func() {
		rows, err := queryTableRowsFromPool(sp.sdbPool, sourceIdxc, logThreadSeq)
		sourceCh <- tableRowsCountResult{rows: rows, err: err}
	}()
	go func() {
		rows, err := queryTableRowsFromPool(sp.ddbPool, destIdxc, logThreadSeq)
		targetCh <- tableRowsCountResult{rows: rows, err: err}
	}()

	sourceResult := <-sourceCh
	targetResult := <-targetCh
	return sourceResult.rows, targetResult.rows, sourceResult.err, targetResult.err
}

func (sp *SchedulePlan) getExactRowCountsParallel(sourceSchema, sourceTable, targetSchema, targetTable string, logThreadSeq int64) (int64, bool, int64, bool) {
	sourceCh := make(chan exactRowsCountResult, 1)
	targetCh := make(chan exactRowsCountResult, 1)

	go func() {
		rows, exact := sp.getExactRowCount(sp.sdbPool, sourceSchema, sourceTable, logThreadSeq)
		sourceCh <- exactRowsCountResult{rows: rows, exact: exact}
	}()
	go func() {
		rows, exact := sp.getExactRowCount(sp.ddbPool, targetSchema, targetTable, logThreadSeq)
		targetCh <- exactRowsCountResult{rows: rows, exact: exact}
	}()

	sourceResult := <-sourceCh
	targetResult := <-targetCh
	return sourceResult.rows, sourceResult.exact, targetResult.rows, targetResult.exact
}
