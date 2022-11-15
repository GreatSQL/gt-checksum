package dbExec

import (
	mysql "greatdbCheck/MySQL"
	oracle "greatdbCheck/Oracle"
	"greatdbCheck/global"
)

type GlobalSNStruct struct {
	mysql  DBGlobalCS
	oracle DBGlobalCS
}

/*
   全局一致性接口，初始化连接池、获取全局一致性位点
*/
type DBGlobalCS interface {
	GlobalCN() (map[string]string, error) //全局一致性位点
	NewConnPool() (*global.Pool, bool)    //连接池
}

func (gs GlobalSNStruct) GcnObject(poolMin, poolMax int, jdbc, dbDevice string) DBGlobalCS {
	var dbcs DBGlobalCS
	if dbDevice == "mysql" {
		dbcs = &mysql.GlobalCS{
			Jdbc:        jdbc,
			ConnPoolMax: poolMax,
			ConnPoolMin: poolMin,
			Drive:       dbDevice,
		}
	}
	if dbDevice == "godror" {
		dbcs = &oracle.GlobalCS{
			Jdbc:        jdbc,
			ConnPoolMax: poolMax,
			ConnPoolMin: poolMin,
			Drive:       dbDevice,
		}
	}
	return dbcs

}

func GCN() *GlobalSNStruct {
	return &GlobalSNStruct{}
}
