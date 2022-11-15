package oracle

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"time"
)

type GlobalCS struct {
	Jdbc            string
	Drive           string
	ConnPoolMin     int
	ConnPoolMax     int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

func (or *GlobalCS) flushTable(db *sql.DB) error {
	sqlstr := fmt.Sprintf("alter system checkpoint")
	global.Wlog.Info("[exec global Snapshot] exec oracle sql info :", sqlstr)
	if _, err := db.Exec(sqlstr); err != nil {
		global.Wlog.Error("[exec global Snapshot] exec oracle sql fail. sql: ", sqlstr, "error info: ", err)
		return err
	}
	return nil
}

/*
   添加全局一致性读锁，防止数据写入
*/
func (or *GlobalCS) fushTableReadLock(db *sql.DB) error {
	//sqlstr := fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
	//global.Wlog.Info("[exec global Snapshot] exec mysql sql info :", sqlstr)
	//if _, err := db.Exec(sqlstr); err != nil {
	//	global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
	//	return err
	//}
	return nil
}

/*
   创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
*/
func (or *GlobalCS) sessionRR() ([]*sql.DB, error) {
	var cisoRRsession []*sql.DB //设置有全局一致性事务的事务快照的db连接id管道
	global.Wlog.Info("[create session conn Pool] init database conn Pool")
	for i := 1; i <= or.ConnPoolMin; i++ {
		db1, err := sql.Open(or.Drive, or.Jdbc)
		if err != nil {
			fmt.Println("database open fail. Error Info: ", err)
			return nil, err
		}
		if err = db1.Ping(); err != nil {
			fmt.Println("database connection fail. conn jdbc: ", or.Jdbc, "Error Info: ", err)
			return nil, err
		}
		db1.SetMaxIdleConns(1000)
		db1.SetMaxOpenConns(1000)
		db1.SetConnMaxLifetime(-1)
		//db1.SetConnMaxIdleTime(time.Second * 86400)
		db1.SetConnMaxIdleTime(-1)
		tx, err2 := db1.Begin()
		if err2 != nil {
			global.Wlog.Error("[create session conn Pool] database create session connection fail. Error Info: ", err)
		}
		//strsql := "set session wait_timeout=86400;"
		//if _, err = tx.Exec(strsql); err != nil {
		//	global.Wlog.Error(fmt.Sprintf("exec sql %s fail. error info: %s", strsql, err))
		//	return nil, err
		//}
		//strsql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
		//if _, err = tx.Exec(strsql); err != nil {
		//	global.Wlog.Error(fmt.Sprintf("exec sql %s fail. error info: %s", strsql, err))
		//	return nil, err
		//}
		//strsql = "SET session sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
		//if _, err = tx.Exec(strsql); err != nil {
		//	global.Wlog.Error(fmt.Sprintf("exec sql %s fail. error info: %s", strsql, err))
		//	return nil, err
		//}
		tx.Commit()
		cisoRRsession = append(cisoRRsession, db1)
	}
	return cisoRRsession, nil
}

/*
  获取全局一致性位点
*/
func (or *GlobalCS) globalConsistencyPoint(db *sql.DB) (map[string]string, error) {
	var position string
	var rows *sql.Rows
	var globalPoint = make(map[string]string)
	sqlstr := fmt.Sprintf("select current_scn as \"globalScn\" from v$database")
	global.Wlog.Info("[exec global Snapshot] exec oracle sql info :", sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&position)
	}
	defer rows.Close()
	globalPoint["position"] = position
	infostr := fmt.Sprintf("[exec global Snapshot] The current global scn of oracle is SCN: : %s ", position)
	global.Wlog.Info(infostr)
	return globalPoint, nil
}

/*
   解锁
*/
func (or *GlobalCS) unlock(db *sql.DB) error {
	//sqlstr := fmt.Sprintf("UNLOCK TABLES")
	//global.Wlog.Info("[exec global Snapshot] exec mysql sql info :", sqlstr)
	//if _, err := db.Exec(sqlstr); err != nil {
	//	global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
	//	return err
	//}
	return nil
}

func (or *GlobalCS) GlobalCN() (map[string]string, error) {
	var GCNMap map[string]string
	defer func() {
		if err := recover(); err != nil {
		}
	}()

	db, err := sql.Open(or.Drive, or.Jdbc)
	if err != nil {
		fmt.Println(err)
	}
	if err = db.Ping(); err != nil {
		fmt.Println(err)
	}
	if err = or.flushTable(db); err != nil {
		return nil, err
	}
	if err = or.fushTableReadLock(db); err != nil {
		return nil, err
	}
	if GCNMap, err = or.globalConsistencyPoint(db); err != nil {
		return nil, err
	}
	if err = or.unlock(db); err != nil {
		return nil, err
	}
	db.Close()
	return GCNMap, nil
}

func (or *GlobalCS) NewConnPool() (*global.Pool, bool) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	db, err := sql.Open(or.Drive, or.Jdbc)
	if err != nil {
		fmt.Println(err)
	}
	if err = db.Ping(); err != nil {
		fmt.Println(err)
	}
	or.ConnMaxLifetime = -1
	or.ConnMaxIdleTime = -1
	if err = or.flushTable(db); err != nil {
		return nil, false
	}
	session, err := or.sessionRR()
	if err != nil {
		or.unlock(db)
		return nil, false
	}
	if err = or.unlock(db); err != nil {
		return nil, false
	}
	db.Close()
	return global.NewPool(or.ConnPoolMin, or.ConnPoolMax, session), true
}
