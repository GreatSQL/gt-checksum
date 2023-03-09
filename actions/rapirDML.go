package actions

import (
	"context"
	"fmt"
	"gt-checksum/global"
	"os"
	"strings"
)

type rapirSqlStruct struct {
	Drive string
	JDBC  string
}

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
func (rs rapirSqlStruct) execRapirSql(sqlstr []string, dbType string, logThreadSeq int64) error {
	//执行sql语句不记录binlog
	var (
		vlog string
	)
	vlog = fmt.Sprintf("(%d) Execute the repair statement on the target side for the current table.", logThreadSeq)
	global.Wlog.Info(vlog)
	db := dbOpenTest(rs.Drive, rs.JDBC)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		vlog = fmt.Sprintf("(%d) database create session connection fail. Error Info: %s", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return err
	}
	defer conn.Close()
	if dbType == "mysql" {
		sql1 := "set session sql_log_bin=off"
		if _, err1 := conn.ExecContext(ctx, sql1); err1 != nil {
			vlog = fmt.Sprintf("(%d) actions prepare dataFix SQL fail. sql is:{%s}, error info is : {%s}", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(vlog)
			return err1
		}
		sql2 := "set autocommit = 0;"
		if _, err1 := conn.ExecContext(ctx, sql2); err1 != nil {
			vlog = fmt.Sprintf("(%d) actions prepare dataFix SQL fail. sql is:{%s}, error info is : {%s}", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(vlog)
			return err1
		}

	}
	for _, i := range sqlstr {
		if strings.HasPrefix(strings.ToUpper(i), "ALTER TABLE") {
			if _, err = db.Exec(i); err != nil {
				vlog = fmt.Sprintf("(%d) commit dataFix SQL fail. error info is {%s}", logThreadSeq, err)
				global.Wlog.Error(vlog)
				return err
			}
		} else {
			if _, err = conn.ExecContext(ctx, i); err != nil {
				vlog = fmt.Sprintf("(%d) prepare dataFix SQL fail.start rollback! sql is {%s}, error info is {%s}.", logThreadSeq, i, err)
				global.Wlog.Error(vlog)
				conn.ExecContext(ctx, "rollback")
				return err
			}
		}

	}
	vlog = fmt.Sprintf("(%d) start commit dataFix SQL.", logThreadSeq)
	global.Wlog.Info(vlog)
	if _, err = conn.ExecContext(ctx, "commit"); err != nil {
		vlog = fmt.Sprintf("(%d) commit dataFix SQL fail. error info is {%s}", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return err
	}
	defer db.Close()
	return nil
}

/*
生成修复sql语句，并写入到文件中
*/
func (rs rapirSqlStruct) SqlFile(sfile *os.File, sql []string, logThreadSeq int64) error { //在/tmp/下创建数据修复文件，将在目标端数据修复的语句写入到文件中
	var (
		vlog      string
		sqlCommit []string
	)
	vlog = fmt.Sprintf("(%d) Start writing repair statements to the repair file.", logThreadSeq)
	global.Wlog.Info(vlog)
	if strings.HasPrefix(strings.ToUpper(strings.Join(sql, ";")), "ALTER TABLE") {
		sqlCommit = sql
	} else {
		sqlCommit = []string{"begin;"}
		sqlCommit = append(sqlCommit, sql...)
		sqlCommit = append(sqlCommit, "commit;")
	}
	_, err := FileOperate{File: sfile, BufSize: 1024 * 4 * 1024, SqlType: "sql"}.ConcurrencyWriteFile(sqlCommit)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) Write the repair statement to the repair file successfully.", logThreadSeq)
	global.Wlog.Info(vlog)
	return nil
}
func ApplyDataFix(fixSql []string, datafixType string, sfile *os.File, ddrive, jdbc string, logThreadSeq int64) error {
	var (
		err      error
		rapirdml = rapirSqlStruct{
			Drive: ddrive,
			JDBC:  jdbc,
		}
	)
	if datafixType == "file" {
		if err = rapirdml.SqlFile(sfile, fixSql, logThreadSeq); err != nil {
			return err
		}
	}
	if datafixType == "table" {
		if err = rapirdml.execRapirSql(fixSql, ddrive, logThreadSeq); err != nil {
			return err
		}
	}
	return nil
}
