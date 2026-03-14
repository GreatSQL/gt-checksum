//go:build cgo

package dbExec

import oracle "gt-checksum/Oracle"

func newOracleGlobalCS(poolMin int, jdbc, dbDevice string) DBGlobalCS {
	return &oracle.GlobalCS{
		Jdbc:        jdbc,
		ConnPoolMin: poolMin,
		Drive:       dbDevice,
	}
}
