//go:build !cgo

package dbExec

func newOracleDataAbnormalFix(dafs DataAbnormalFixStruct) DataAbnormalFixInterface {
	// Keep Oracle repair support out of the CGO-disabled regression baseline.
	return nil
}
