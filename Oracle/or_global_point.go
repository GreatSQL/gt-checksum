package oracle

import (
	"database/sql"
	"fmt"
	"gt-checksum/global"
	"time"
)

type GlobalCS struct {
	Jdbc            string
	Drive           string
	ConnPoolMin     int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

func (or *GlobalCS) flushTable(db *sql.DB, logThreadSeq int) error {
	sqlstr := fmt.Sprintf("alter system checkpoint")
	alog := fmt.Sprintf("(%d) Oracle DB alter system checkpoint...", logThreadSeq)
	global.Wlog.Info(alog)
	if _, err := db.Exec(sqlstr); err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return err
	}
	return nil
}

/*
   添加全局一致性读锁，防止数据写入
*/
func (or *GlobalCS) fushTableReadLock(db *sql.DB, logThreadSeq int) error {
	//sqlstr := fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
	//alog := fmt.Sprintf("(%d) MySQL DB start flush tables with read lock...",logThreadSeq)
	//global.Wlog.Info(alog)
	//if _, err := db.Exec(sqlstr); err != nil {
	//	blog := fmt.Sprintf("(%d) Oracle DB connection failed. The error message is {%s}", logThreadSeq, err)
	//	global.Wlog.Error(blog)
	//	return err
	//}
	return nil
}

/*
   创建源、目并发查询数据时需要的 快照会话，防止数据修改查询数据不对
*/
func (or *GlobalCS) sessionRR(logThreadSeq int) ([]*sql.DB, error) {
	var (
		vlog          string
		cisoRRsession []*sql.DB //设置有全局一致性事务的事务快照的db连接id管道
	)
	vlog = fmt.Sprintf("(%d) Oracle DB init database conn Pool ...", logThreadSeq)
	global.Wlog.Debug(vlog)
	for i := 1; i <= or.ConnPoolMin; i++ {
		db1, err := sql.Open(or.Drive, or.Jdbc)
		if err != nil {
			vlog = fmt.Sprintf("(%d) Oracle DB database open fail. Error Info is {%s}.", logThreadSeq, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		if err = db1.Ping(); err != nil {
			vlog = fmt.Sprintf("(%d) Oracle DB database connection fail. conn jdbc is {%s} Error Info is {%s}.", logThreadSeq, or.Jdbc, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		db1.SetMaxIdleConns(1000)
		db1.SetMaxOpenConns(1000)
		db1.SetConnMaxLifetime(-1)
		db1.SetConnMaxIdleTime(-1)
		vlog = fmt.Sprintf("(%d) Oracle DB starts a transaction", logThreadSeq)
		global.Wlog.Debug(vlog)
		tx, err2 := db1.Begin()
		if err2 != nil {
			vlog = fmt.Sprintf("(%d) Oracle DB database create session connection fail. conn jdbc is {%s} Error Info is {%s}.", logThreadSeq, or.Jdbc, err)
			global.Wlog.Error(vlog)
		}
		//strsql := "set session wait_timeout=86400;"
		//if _, err = tx.Exec(strsql); err != nil {
		//	elog := fmt.Sprintf("(%d) Oracle DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
		//	global.Wlog.Error(elog)
		//	return nil, err
		//}
		//strsql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
		//if _, err = tx.Exec(strsql); err != nil {
		//	flog := fmt.Sprintf("(%d) Oracle DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
		//	global.Wlog.Error(flog)
		//	return nil, err
		//}
		//strsql = "SET session sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
		//if _, err = tx.Exec(strsql); err != nil {
		//	glog := fmt.Sprintf("(%d) Oracle DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, strsql, err)
		//	global.Wlog.Error(glog)
		//	return nil, err
		//}
		if err = tx.Commit(); err != nil {
			vlog = fmt.Sprintf("(%d) MySQL DB commit a transaction fail.", logThreadSeq)
			global.Wlog.Error(vlog)
		} else {
			vlog = fmt.Sprintf("(%d) Oracle DB commit a transaction", logThreadSeq)
			global.Wlog.Debug(vlog)
		}

		cisoRRsession = append(cisoRRsession, db1)
	}
	return cisoRRsession, nil
}

/*
  获取全局一致性位点
*/
func (or *GlobalCS) globalConsistencyPoint(db *sql.DB, logThreadSeq int) (map[string]string, error) {
	var position string
	var rows *sql.Rows
	var globalPoint = make(map[string]string)
	sqlstr := fmt.Sprintf("select current_scn as \"globalScn\" from v$database")
	alog := fmt.Sprintf("(%d) Oracle DB start select current_scn from v$database...", logThreadSeq)
	global.Wlog.Info(alog)
	rows, err := db.Query(sqlstr)
	if err != nil {
		glog := fmt.Sprintf("(%d) Oracle DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(glog)
		return nil, err
	}
	for rows.Next() {
		rows.Scan(&position)
	}
	defer rows.Close()
	globalPoint["position"] = position
	zlog := fmt.Sprintf("(%d) The current global scn of Oracle DB is SCN: %s", logThreadSeq, position)
	global.Wlog.Info(zlog)
	return globalPoint, nil
}

/*
   解锁
*/
func (or *GlobalCS) unlock(db *sql.DB, logThreadSeq int) error {
	//sqlstr := fmt.Sprintf("UNLOCK TABLES")
	//alog := fmt.Sprintf("(%d) Oracle DB unlock tables.",logThreadSeq)
	//global.Wlog.Info(alog)
	//if _, err := db.Exec(sqlstr); err != nil {
	//	glog := fmt.Sprintf("(%d) Oracle DB exec sql %s fail. Error Info is {%s}.", logThreadSeq, sqlstr, err)
	//	global.Wlog.Error(glog)
	//	return err
	//}
	return nil
}

func (or *GlobalCS) GlobalCN(logThreadSeq int) (map[string]string, error) {
	var GCNMap map[string]string
	defer func() {
		if err := recover(); err != nil {
		}
	}()

	db, err := sql.Open(or.Drive, or.Jdbc)
	if err != nil {
		alog := fmt.Sprintf("(%d) Oracle DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(alog)
		return nil, err
	}
	if err = db.Ping(); err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return nil, err
	}
	if err = or.flushTable(db, logThreadSeq); err != nil {
		return nil, err
	}
	if err = or.fushTableReadLock(db, logThreadSeq); err != nil {
		return nil, err
	}
	if GCNMap, err = or.globalConsistencyPoint(db, logThreadSeq); err != nil {
		return nil, err
	}
	if err = or.unlock(db, logThreadSeq); err != nil {
		return nil, err
	}
	db.Close()
	return GCNMap, nil
}

func (or *GlobalCS) NewConnPool(logThreadSeq int) (*global.Pool, bool) {
	//defer func() {
	//	if err := recover(); err != nil {
	//	}
	//}()
	var (
		vlog string
		err  error
		db   *sql.DB
	)
	vlog = fmt.Sprintf("(%d) Start to initialize the connection pool of the original target...", logThreadSeq)
	global.Wlog.Info(vlog)

	db, err = sql.Open(or.Drive, or.Jdbc)
	if err != nil {
		vlog = fmt.Sprintf("(%d) Oracle DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return nil, false
	}
	if err = db.Ping(); err != nil {
		blog := fmt.Sprintf("(%d) Oracle DB connection failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(blog)
		return nil, false
	}
	or.ConnMaxLifetime = -1
	or.ConnMaxIdleTime = -1
	//if err = or.flushTable(db); err != nil {
	//	return nil, false
	//}
	session, err := or.sessionRR(logThreadSeq)
	if err != nil {
		or.unlock(db, logThreadSeq)
		return nil, false
	}
	if err = or.unlock(db, logThreadSeq); err != nil {
		return nil, false
	}
	db.Close()
	C := global.NewPool(or.ConnPoolMin, session, logThreadSeq, "Oracle")
	vlog = fmt.Sprintf("(%d) The connection pool initialization of the Oracle target is completed !!!", logThreadSeq)
	global.Wlog.Info(vlog)
	return C, true
}
