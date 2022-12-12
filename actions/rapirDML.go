package actions

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"greatdbCheck/global"
	"os"
)

type rapirSqlStruct struct{}

func isFile(file string) *os.File {
	sfile, err := os.Open(file)
	if err != nil && os.IsNotExist(err) {
		sfile, err = os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	} else {
		os.Remove(file)
		sfile, err = os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0666)
	}
	if err != nil {
		fmt.Println("actions open datafix file fail. error msg is :", err)
		global.Wlog.Error("actions open datafix file fail. error msg is :", err)
		os.Exit(1)
	}
	return sfile
}

/*
   向目标端执行修复sql语句
*/
func (rs rapirSqlStruct) execRapirSql(db *sql.DB, sqlstr, dbType string, logThreadSeq int64) error {
	//执行sql语句不记录binlog
	alog := fmt.Sprintf("(%d) Execute the repair statement on the target side for the current table.", logThreadSeq)
	global.Wlog.Info(alog)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		blog := fmt.Sprintf("(%d) database create session connection fail. Error Info: %s", logThreadSeq, err)
		global.Wlog.Error(blog)
		return err
	}
	defer conn.Close()
	if dbType == "mysql" {
		sql1 := "set session sql_log_bin=off"
		if _, err1 := conn.ExecContext(ctx, sql1); err1 != nil {
			clog := fmt.Sprintf("(%d) actions prepare dataFix SQL fail. sql is:{%s}, error info is : {%s}", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(clog)
			return err1
		}
	}
	if _, err = conn.ExecContext(ctx, sqlstr); err != nil {
		dlog := fmt.Sprintf("(%d) prepare dataFix SQL fail. sql is {%s}, error info is {%s}.", logThreadSeq, sqlstr, err)
		global.Wlog.Error(dlog)
		elog := fmt.Sprintf("(%d) prepare dataFix SQL fail. start rollback. !", logThreadSeq)
		global.Wlog.Info(elog)
		conn.ExecContext(ctx, "rollback")
		return err
	} else {
		flog := fmt.Sprintf("(%d) prepare dataFix SQL successfule. sql is {%s}.", logThreadSeq, sqlstr)
		global.Wlog.Info(flog)
		glog := fmt.Sprintf("(%d) start commit dataFix SQL. sql is {%s}.", logThreadSeq, sqlstr)
		global.Wlog.Info(glog)
		if _, err = conn.ExecContext(ctx, "commit"); err != nil {
			hlog := fmt.Sprintf("(%d) commit dataFix SQL fail. sql is {%s}. error info is {%s}", logThreadSeq, sqlstr)
			global.Wlog.Error(hlog)
		}
	}
	return nil
}

/*
   生成修复sql语句，并写入到文件中
*/
func (rs rapirSqlStruct) SqlFile(sfile *os.File, sql string, logThreadSeq int64) error { //在/tmp/下创建数据修复文件，将在目标端数据修复的语句写入到文件中
	//alog := fmt.Sprintf("(%d) Write the repair statement to the repair file for the current table.", logThreadSeq)
	//global.Wlog.Info(alog)

	//延迟关闭文件：在函数return前执行的程序
	//defer sfile.Close()
	//写入数据
	write := bufio.NewWriter(sfile)
	dlog := fmt.Sprintf("(%d) Start writing repair statements to the repair file.", logThreadSeq)
	global.Wlog.Info(dlog)
	_, err := write.WriteString(sql)
	if err != nil {
		elog := fmt.Sprintf("(%d) Failed to write repair statement to repair file {%s}.The sql message is {%s} The error message is {%s}", logThreadSeq, sql, err)
		global.Wlog.Error(elog)
		return err
	}
	_, err = write.WriteString("\n")
	if err != nil {
		elog := fmt.Sprintf("(%d) Failed to write repair statement to repair file {%s}.The sql message is {%s} The error message is {%s}", logThreadSeq, "\n", err)
		global.Wlog.Error(elog)
		return err
	}
	err = write.Flush()
	if err != nil {
		glog := fmt.Sprintf("(%d) Flush file buffer to repair file {%s} failed. The error message is {%s}", logThreadSeq, err)
		global.Wlog.Error(glog)
		return err
	}
	hlog := fmt.Sprintf("(%d) Write the repair statement to the repair file successfully.sql info is {%s}", logThreadSeq, sql)
	global.Wlog.Info(hlog)
	return nil
}
func ApplyDataFix(fixSql string, db *sql.DB, datafixType string, sfile *os.File, ddrive string, logThreadSeq int64) error {
	var rapirdml = rapirSqlStruct{}
	var err error
	if datafixType == "file" {
		err = rapirdml.SqlFile(sfile, fixSql, logThreadSeq)
		if err != nil {
			return nil
		}
	}
	if datafixType == "table" {
		err = rapirdml.execRapirSql(db, fixSql, ddrive, logThreadSeq)
		if err != nil {
			return nil
		}
	}
	return nil
}
