package mysql

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"time"
)

// mysql  全局一致性点
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
	vlog := fmt.Sprintf("(%d) Flushing MySQL tables", logThreadSeq)
	global.Wlog.Info(vlog)
	if _, err := db.Exec(sqlstr); err != nil {
		vlog := fmt.Sprintf("(%d) MySQL connection failed: %v", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return err
	}
	return nil
}

/*
添加全局一致性读锁，防止数据写入
*/
func (my *GlobalCS) fushTableReadLock(db *sql.DB, logThreadSeq int) error {
	sqlstr := fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
	vlog := fmt.Sprintf("(%d) Flushing MySQL tables with read lock", logThreadSeq)
	global.Wlog.Info(vlog)
	if _, err := db.Exec(sqlstr); err != nil {
		vlog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return err
	}
	return nil
}

/*
创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
*/
func (my *GlobalCS) sessionRR(logThreadSeq int) ([]*sql.DB, error) {
	var (
		vlog          string
		cisoRRsession []*sql.DB //设置有全局一致性事务的事务快照的db连接id管道
	)

	vlog = fmt.Sprintf("(%d) Initializing MySQL connection pool", logThreadSeq)
	global.Wlog.Debug(vlog)
	for i := 1; i <= my.ConnPoolMin; i++ {
		db1, err := sql.Open(my.Drive, my.Jdbc)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Failed to open MySQL database: %v", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		if err = db1.Ping(); err != nil {
			vlog = fmt.Sprintf("(%d) MySQL connection failed (JDBC: %s): %v", logThreadSeq, my.Jdbc, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		db1.SetMaxIdleConns(1000)
		db1.SetMaxOpenConns(1000)
		db1.SetConnMaxLifetime(-1)
		db1.SetConnMaxIdleTime(-1)
		vlog = fmt.Sprintf("(%d) Starting MySQL transaction", logThreadSeq)
		//global.Wlog.Debug(vlog)
		tx, err2 := db1.Begin()
		if err2 != nil {
			vlog = fmt.Sprintf("(%d) Failed to create MySQL session (JDBC: %s): %v", logThreadSeq, my.Jdbc, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		strsql := "SET SESSION wait_timeout=86400;"
		if _, err = tx.Exec(strsql); err != nil {
			vlog = fmt.Sprintf("(%d) Failed to execute SQL: %s - Error: %v", logThreadSeq, strsql, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		strsql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
		if _, err = tx.Exec(strsql); err != nil {
			vlog = fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		strsql = "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
		if _, err = tx.Exec(strsql); err != nil {
			vlog = fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			vlog = fmt.Sprintf("(%d) MySQL transaction commit failed", logThreadSeq)
			global.Wlog.Error(vlog)
			tx.Rollback()
		} else {
			vlog = fmt.Sprintf("(%d) MySQL transaction committed", logThreadSeq)
			//global.Wlog.Debug(vlog)
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
	vlog := fmt.Sprintf("(%d) Checking MySQL master status", logThreadSeq)
	global.Wlog.Info(vlog)
	rows, err := db.Query(sqlstr)
	if err != nil {
		vlog := fmt.Sprintf("(%d) Failed to execute SHOW MASTER STATUS: %v", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&file, &position, &binlog_Do_DB, &binlog_Ignore_DB, &executed_Gtid_Set)
	}
	defer rows.Close()
	globalPoint["file"] = file
	globalPoint["position"] = position
	globalPoint["Point"] = executed_Gtid_Set
	vlog = fmt.Sprintf("(%d) MySQL master status - File: %s, Position: %s, DoDB: %s, IgnoreDB: %s, GTID: %s", logThreadSeq, file, position, binlog_Do_DB, binlog_Ignore_DB, executed_Gtid_Set)
	global.Wlog.Info(vlog)
	return globalPoint, nil
}

/*
解锁
*/
func (my *GlobalCS) unlock(db *sql.DB, logThreadSeq int) error {
	var (
		vlog   string
		sqlstr string
	)
	sqlstr = fmt.Sprintf("UNLOCK TABLES")
	vlog = fmt.Sprintf("(%d) Unlocking MySQL tables", logThreadSeq)
	global.Wlog.Debug(vlog)
	if _, err := db.Exec(sqlstr); err != nil {
		vlog = fmt.Sprintf("(%d) MySQL DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(vlog)
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
		vlog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return nil, err
	}
	if err = db.Ping(); err != nil {
		vlog := fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
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
	var (
		vlog string
		err  error
		db   *sql.DB
	)
	vlog = fmt.Sprintf("(%d) Initializing target connection pool", logThreadSeq)
	global.Wlog.Info(vlog)

	db, err = sql.Open(my.Drive, my.Jdbc)
	if err != nil {
		vlog = fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return nil, false
	}
	if err = db.Ping(); err != nil {
		vlog = fmt.Sprintf("(%d) MySQL DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return nil, false
	}
	my.ConnMaxLifetime = -1
	my.ConnMaxIdleTime = -1

	session, err := my.sessionRR(logThreadSeq)
	if err != nil {
		my.unlock(db, logThreadSeq)
		return nil, false
	}
	if err = my.unlock(db, logThreadSeq); err != nil {
		return nil, false
	}
	db.Close()
	C := global.NewPool(my.ConnPoolMin, session, logThreadSeq, "MySQL")
	vlog = fmt.Sprintf("(%d) MySQL target connection pool initialized", logThreadSeq)
	global.Wlog.Info(vlog)
	return C, true
}
