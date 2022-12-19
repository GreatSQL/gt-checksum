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
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

/*
   刷新表，将内存中已经修改的表而未来的及刷脏的数据进行刷脏
*/
func (my *GlobalCS) flushTable(db *sql.DB, logThreadSeq int) error {
	sqlstr := fmt.Sprintf("FLUSH /*!40101 LOCAL */ TABLES")
	alog := fmt.Sprintf("(%d) MySQL DB start flush tables...", logThreadSeq)
	global.Wlog.Info(alog)
	if _, err := db.Exec(sqlstr); err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return err
	}
	return nil
}

/*
   添加全局一致性读锁，防止数据写入
*/
func (my *GlobalCS) fushTableReadLock(db *sql.DB, logThreadSeq int) error {
	sqlstr := fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
	alog := fmt.Sprintf("(%d) MySQL DB start flush tables with read lock...", logThreadSeq)
	global.Wlog.Info(alog)
	if _, err := db.Exec(sqlstr); err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return err
	}
	return nil
}

/*
   创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
*/
func (my *GlobalCS) sessionRR(logThreadSeq int) ([]*sql.DB, error) {
	var cisoRRsession []*sql.DB //设置有全局一致性事务的事务快照的db连接id管道
	alog := fmt.Sprintf("(%d) MySQL DB init database conn Pool ...", logThreadSeq)
	global.Wlog.Info(alog)
	for i := 1; i <= my.ConnPoolMin; i++ {
		db1, err := sql.Open(my.Drive, my.Jdbc)
		if err != nil {
			blog := fmt.Sprintf("(%d) MySQL DB database open fail. Error Info is {%s}.", logThreadSeq, err)
			global.Wlog.Error(blog)
			return nil, err
		}
		if err = db1.Ping(); err != nil {
			clog := fmt.Sprintf("(%d) MySQL DB database connection fail. conn jdbc is {%s} Error Info is {%s}.", logThreadSeq, my.Jdbc, err)
			global.Wlog.Error(clog)
			return nil, err
		}
		db1.SetMaxIdleConns(1000)
		db1.SetMaxOpenConns(1000)
		db1.SetConnMaxLifetime(-1)
		db1.SetConnMaxIdleTime(-1)
		hlog := fmt.Sprintf("(%d) MySQL DB starts a transaction", logThreadSeq)
		global.Wlog.Info(hlog)
		tx, err2 := db1.Begin()
		if err2 != nil {
			dlog := fmt.Sprintf("(%d) MySQL DB database create session connection fail. conn jdbc is {%s} Error Info is {%s}.", logThreadSeq, my.Jdbc, err)
			global.Wlog.Error(dlog)
			return nil, err
		}
		strsql := "set session wait_timeout=86400;"
		if _, err = tx.Exec(strsql); err != nil {
			elog := fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
			global.Wlog.Error(elog)
			return nil, err
		}
		strsql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
		if _, err = tx.Exec(strsql); err != nil {
			flog := fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
			global.Wlog.Error(flog)
			return nil, err
		}
		strsql = "SET session sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
		if _, err = tx.Exec(strsql); err != nil {
			glog := fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
			global.Wlog.Error(glog)
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			ilog := fmt.Sprintf("(%d) MySQL DB commit a transaction fail.", logThreadSeq)
			global.Wlog.Error(ilog)
			tx.Rollback()
		} else {
			ilog := fmt.Sprintf("(%d) MySQL DB commit a transaction successful.", logThreadSeq)
			global.Wlog.Info(ilog)
		}

		cisoRRsession = append(cisoRRsession, db1)
	}
	return cisoRRsession, nil
}

/*
  获取全局一致性位点
*/
func (my *GlobalCS) globalConsistencyPoint(db *sql.DB, logThreadSeq int) (map[string]string, error) {
	var file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set string
	var rows *sql.Rows
	var globalPoint = make(map[string]string)
	sqlstr := fmt.Sprintf("SHOW MASTER STATUS")
	alog := fmt.Sprintf("(%d) MySQL DB start show master status...")
	global.Wlog.Info(alog)
	rows, err := db.Query(sqlstr)
	if err != nil {
		glog := fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(glog)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&file, &position, &binlog_Do_DB, &binlog_Ignore_DB, &executed_Gtid_Set)
	}
	defer rows.Close()
	globalPoint["file"] = file
	globalPoint["position"] = position
	globalPoint["Point"] = executed_Gtid_Set
	zlog := fmt.Sprintf("(%d) MySQL DB master status info: binlogFile: %s, binlogPos: %s, binlog_do_db: %s, binlog_ignore_db: %s, executed_gtid_set: %s", logThreadSeq, file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set)
	global.Wlog.Info(zlog)
	return globalPoint, nil
}

/*
   解锁
*/
func (my *GlobalCS) unlock(db *sql.DB, logThreadSeq int) error {
	sqlstr := fmt.Sprintf("UNLOCK TABLES")
	alog := fmt.Sprintf("(%d) MySQL DB unlock tables.", logThreadSeq)
	global.Wlog.Info(alog)
	if _, err := db.Exec(sqlstr); err != nil {
		glog := fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(glog)
		return err
	}
	return nil
}

func (my *GlobalCS) GlobalCN(logThreadSeq int) (map[string]string, error) {
	var GCNMap map[string]string
	defer func() {
		if err := recover(); err != nil {
		}
	}()

	db, err := sql.Open(my.Drive, my.Jdbc)
	if err != nil {
		alog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(alog)
		return nil, err
	}
	if err = db.Ping(); err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	if err = my.flushTable(db, logThreadSeq); err != nil {
		return nil, err
	}
	if err = my.fushTableReadLock(db, logThreadSeq); err != nil {
		return nil, err
	}
	if GCNMap, err = my.globalConsistencyPoint(db, logThreadSeq); err != nil {
		return nil, err
	}
	if err = my.unlock(db, logThreadSeq); err != nil {
		return nil, err
	}
	db.Close()
	return GCNMap, nil
}

func (my *GlobalCS) NewConnPool(logThreadSeq int) (*global.Pool, bool) {
	//defer func() {
	//	if err := recover(); err != nil {
	//	}
	//}()
	db, err := sql.Open(my.Drive, my.Jdbc)
	if err != nil {
		alog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(alog)
		return nil, false
	}
	if err = db.Ping(); err != nil {
		blog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return nil, false
	}
	my.ConnMaxLifetime = -1
	my.ConnMaxIdleTime = -1
	//if err = my.flushTable(db); err != nil {
	//	return nil, false
	//}
	session, err := my.sessionRR(logThreadSeq)
	if err != nil {
		my.unlock(db, logThreadSeq)
		return nil, false
	}
	if err = my.unlock(db, logThreadSeq); err != nil {
		return nil, false
	}
	db.Close()
	return global.NewPool(my.ConnPoolMin, session, logThreadSeq, "MySQL"), true
}
