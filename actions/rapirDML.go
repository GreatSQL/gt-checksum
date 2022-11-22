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
func (rs rapirSqlStruct) execRapirSql(db *sql.DB, sqlstr, dbType string) error {
	//执行sql语句不记录binlog
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		global.Wlog.Error("[db conn] database create session connection fail. Error Info: ", err)
		return err
	}
	defer conn.Close()
	if dbType == "mysql" {
		sql1 := "set session sql_log_bin=off"
		if _, err1 := conn.ExecContext(ctx, sql1); err1 != nil {
			global.Wlog.Error("actions prepare dataFix SQL fail. sql is:", "\"set session sql_log_bin=off\"", " error msg: ", err)
			return err1
		}
	}
	if _, err = conn.ExecContext(ctx, sqlstr); err != nil {
		global.Wlog.Error(fmt.Sprintf("prepare dataFix SQL fail. sql is: %s,  error msg: %s", sqlstr, err))
		conn.ExecContext(ctx, "rollback")
		return err
	} else {
		if _, err = conn.ExecContext(ctx, "commit"); err == nil {
			global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
		}
	}
	return nil
}

/*
   生成修复sql语句，并写入到文件中
*/
func (rs rapirSqlStruct) SqlFile(sqlfile, sql string) error { //在/tmp/下创建数据修复文件，将在目标端数据修复的语句写入到文件中
	sfile, err := os.OpenFile(sqlfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	//延迟关闭文件：在函数return前执行的程序
	defer sfile.Close()
	//写入数据
	write := bufio.NewWriter(sfile)
	_, err = write.WriteString(sql)
	if err != nil {
		return err
	}
	_, err = write.WriteString("\n")
	if err != nil {
		return err
	}
	err = write.Flush()
	if err != nil {
		return err
	}
	return nil
}
func ApplyDataFix(fixSql string, db *sql.DB, datafixType, fixfile string, ddrive string) error {
	var rapirdml = rapirSqlStruct{}
	var err error
	if datafixType == "file" {
		err = rapirdml.SqlFile(fixfile, fixSql)
		if err != nil {
			return nil
		}
	}
	if datafixType == "table" {
		err = rapirdml.execRapirSql(db, fixSql, ddrive)
		if err != nil {
			return nil
		}
	}
	return nil
}
