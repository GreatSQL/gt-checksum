package dbExec

import (
	mysql "gt-checksum/MySQL"
	oracle "gt-checksum/Oracle"
	"gt-checksum/global"
)

type GlobalSNStruct struct {
	mysql  DBGlobalCS
	oracle DBGlobalCS
}

/*
   全局一致性接口，初始化连接池、获取全局一致性位点
*/
type DBGlobalCS interface {
	GlobalCN(logThreadSeq int) (map[string]string, error) //全局一致性位点
	NewConnPool(logThreadSeq int) (*global.Pool, bool)    //连接池
}

func (gs GlobalSNStruct) GcnObject(poolMin int, jdbc, dbDevice string) DBGlobalCS {
	var dbcs DBGlobalCS
	if dbDevice == "mysql" {
		dbcs = &mysql.GlobalCS{
			Jdbc:        jdbc,
			ConnPoolMin: poolMin,
			Drive:       dbDevice,
		}
	}
	if dbDevice == "godror" {
		dbcs = &oracle.GlobalCS{
			Jdbc:        jdbc,
			ConnPoolMin: poolMin,
			Drive:       dbDevice,
		}
	}
	return dbcs

}

func GCN() *GlobalSNStruct {
	return &GlobalSNStruct{}
}
