package mysql

import (
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"time"
)

//mysql  全局一致性点
type GlobalCS struct {
	Jdbc            string
	Drive           string
	ConnPoolMin     int
	ConnPoolMax     int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

/*
   刷新表，将内存中已经修改的表而未来的及刷脏的数据进行刷脏
*/
func (my *GlobalCS) flushTable(db *sql.DB) error {
	sqlstr := fmt.Sprintf("FLUSH /*!40101 LOCAL */ TABLES")
	global.Wlog.Info("[exec global Snapshot] exec mysql sql info :", sqlstr)
	if _, err := db.Exec(sqlstr); err != nil {
		global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
		return err
	}
	return nil
}

/*
   添加全局一致性读锁，防止数据写入
*/
func (my *GlobalCS) fushTableReadLock(db *sql.DB) error {
	sqlstr := fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
	global.Wlog.Info("[exec global Snapshot] exec mysql sql info :", sqlstr)
	if _, err := db.Exec(sqlstr); err != nil {
		global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
		return err
	}
	return nil
}

/*
   创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
*/
func (my *GlobalCS) sessionRR() ([]*sql.DB, error) {
	var cisoRRsession []*sql.DB //设置有全局一致性事务的事务快照的db连接id管道
	global.Wlog.Info("[create session conn Pool] init database conn Pool")
	for i := 1; i <= my.ConnPoolMin; i++ {
		db1, err := sql.Open(my.Drive, my.Jdbc)
		if err != nil {
			fmt.Println("database open fail. Error Info: ", err)
			return nil, err
		}
		if err = db1.Ping(); err != nil {
			fmt.Println("database connection fail. conn jdbc: ", my.Jdbc, "Error Info: ", err)
			return nil, err
		}
		db1.SetMaxIdleConns(1000)
		db1.SetMaxOpenConns(1000)
		db1.SetConnMaxLifetime(-1)
		db1.SetConnMaxIdleTime(-1)
		tx, err2 := db1.Begin()
		if err2 != nil {
			global.Wlog.Error("[create session conn Pool] database create session connection fail. Error Info: ", err)
		}
		strsql := "set session wait_timeout=86400;"
		if _, err = tx.Exec(strsql); err != nil {
			global.Wlog.Error(fmt.Sprintf("exec sql %s fail. error info: %s", strsql, err))
			return nil, err
		}
		strsql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
		if _, err = tx.Exec(strsql); err != nil {
			global.Wlog.Error(fmt.Sprintf("exec sql %s fail. error info: %s", strsql, err))
			return nil, err
		}
		strsql = "SET session sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
		if _, err = tx.Exec(strsql); err != nil {
			global.Wlog.Error(fmt.Sprintf("exec sql %s fail. error info: %s", strsql, err))
			return nil, err
		}
		tx.Commit()
		cisoRRsession = append(cisoRRsession, db1)
	}
	return cisoRRsession, nil
}

/*
  获取全局一致性位点
*/
func (my *GlobalCS) globalConsistencyPoint(db *sql.DB) (map[string]string, error) {
	var file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set string
	var rows *sql.Rows
	var globalPoint = make(map[string]string)
	sqlstr := fmt.Sprintf("SHOW MASTER STATUS")
	global.Wlog.Info("[exec global Snapshot] exec mysql sql info :", sqlstr)
	rows, err := db.Query(sqlstr)
	if err != nil {
		global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&file, &position, &binlog_Do_DB, &binlog_Ignore_DB, &executed_Gtid_Set)
	}
	defer rows.Close()
	globalPoint["file"] = file
	globalPoint["position"] = position
	globalPoint["Point"] = executed_Gtid_Set
	infostr := fmt.Sprintf("[exec global Snapshot] mysql master status info: binlogFile: %s, binlogPos: %s, binlog_do_db: %s, binlog_ignore_db: %s, executed_gtid_set: %s", file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set)
	global.Wlog.Info(infostr)
	return globalPoint, nil
}

/*
   解锁
*/
func (my *GlobalCS) unlock(db *sql.DB) error {
	sqlstr := fmt.Sprintf("UNLOCK TABLES")
	global.Wlog.Info("[exec global Snapshot] exec mysql sql info :", sqlstr)
	if _, err := db.Exec(sqlstr); err != nil {
		global.Wlog.Error("[exec global Snapshot] exec mysql sql fail. sql: ", sqlstr, "error info: ", err)
		return err
	}
	return nil
}

func (my *GlobalCS) GlobalCN() (map[string]string, error) {
	var GCNMap map[string]string
	defer func() {
		if err := recover(); err != nil {
		}
	}()

	db, err := sql.Open(my.Drive, my.Jdbc)
	if err != nil {
		fmt.Println(err)
	}
	if err = db.Ping(); err != nil {
		fmt.Println(err)
	}
	if err = my.flushTable(db); err != nil {
		return nil, err
	}
	if err = my.fushTableReadLock(db); err != nil {
		return nil, err
	}
	if GCNMap, err = my.globalConsistencyPoint(db); err != nil {
		return nil, err
	}
	if err = my.unlock(db); err != nil {
		return nil, err
	}
	db.Close()
	return GCNMap, nil
}

func (my *GlobalCS) NewConnPool() (*global.Pool, bool) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	db, err := sql.Open(my.Drive, my.Jdbc)
	if err != nil {
		fmt.Println(err)
	}
	if err = db.Ping(); err != nil {
		fmt.Println(err)
	}
	my.ConnMaxLifetime = -1
	my.ConnMaxIdleTime = -1
	if err = my.flushTable(db); err != nil {
		return nil, false
	}
	session, err := my.sessionRR()
	if err != nil {
		my.unlock(db)
		return nil, false
	}
	if err = my.unlock(db); err != nil {
		return nil, false
	}
	db.Close()
	return global.NewPool(my.ConnPoolMin, my.ConnPoolMax, session), true
}
