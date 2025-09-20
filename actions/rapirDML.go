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
		fmt.Println("Failed to open datafix file. Error:", err)
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
	vlog = fmt.Sprintf("(%d) Executing repair statement on target table", logThreadSeq)
	global.Wlog.Info(vlog)
	db := dbOpenTest(rs.Drive, rs.JDBC)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		vlog = fmt.Sprintf("(%d) Failed to create database session. Error: %s", logThreadSeq, err)
		global.Wlog.Error(vlog)
		return err
	}
	defer conn.Close()
	if dbType == "mysql" {
		sql1 := "SET SESSION sql_log_bin=OFF"
		if _, err1 := conn.ExecContext(ctx, sql1); err1 != nil {
			vlog = fmt.Sprintf("(%d) Failed to prepare dataFix SQL: %s. Error: %s", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(vlog)
			return err1
		}
		sql2 := "SET autocommit=0;"
		if _, err1 := conn.ExecContext(ctx, sql2); err1 != nil {
			vlog = fmt.Sprintf("(%d) actions prepare dataFix SQL fail. sql is:{%s}, error info is : {%s}", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(vlog)
			return err1
		}

	}
	for _, i := range sqlstr {
		if strings.HasPrefix(strings.ToUpper(i), "ALTER TABLE") {
			if _, err = db.Exec(i); err != nil {
				vlog = fmt.Sprintf("(%d) Failed to commit dataFix SQL. Error: %s", logThreadSeq, err)
				global.Wlog.Error(vlog)
				return err
			}
		} else {
			if _, err = conn.ExecContext(ctx, i); err != nil {
				vlog = fmt.Sprintf("(%d) Failed to prepare dataFix SQL: %s. Error: %s. Starting rollback", logThreadSeq, i, err)
				global.Wlog.Error(vlog)
				conn.ExecContext(ctx, "ROLLBACK")
				return err
			}
		}

	}
	vlog = fmt.Sprintf("(%d) Starting dataFix SQL commit", logThreadSeq)
	global.Wlog.Info(vlog)
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
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
	vlog = fmt.Sprintf("(%d) Writing repair statements to file", logThreadSeq)
	global.Wlog.Debug(vlog)
	if strings.HasPrefix(strings.ToUpper(strings.Join(sql, ";")), "ALTER TABLE") {
		sqlCommit = sql
	} else {
		sqlCommit = []string{"BEGIN;"}
		sqlCommit = append(sqlCommit, sql...)
		sqlCommit = append(sqlCommit, "COMMIT;")
	}
	_, err := FileOperate{File: sfile, BufSize: 1024 * 4 * 1024, SqlType: "sql"}.ConcurrencyWriteFile(sqlCommit)
	if err != nil {
		return err
	}
	vlog = fmt.Sprintf("(%d) Repair statements written to file successfully", logThreadSeq)
	global.Wlog.Debug(vlog)
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
