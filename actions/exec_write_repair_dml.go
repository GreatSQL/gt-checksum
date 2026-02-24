package actions

import (
	"context"
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"os"
	"strings"
	"sync"
)

type repairSqlStruct struct {
	Drive     string
	JDBC      string
	FixTrxNum int
}

// 包级变量，用于存储已写入文件的SQL语句，实现跨函数调用的去重
var writtenSqlMap sync.Map

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
func (rs repairSqlStruct) execRepairSql(sqlstr []string, dbType string, logThreadSeq int64) error {
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
		// 首先设置不记录binlog
		sql1 := "SET SESSION sql_log_bin=OFF"
		if _, err1 := conn.ExecContext(ctx, sql1); err1 != nil {
			vlog = fmt.Sprintf("(%d) Failed to prepare dataFix SQL: %s. Error: %s", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(vlog)
			return err1
		}

		// 关闭自动提交
		sql2 := "SET autocommit=0;"
		if _, err1 := conn.ExecContext(ctx, sql2); err1 != nil {
			vlog = fmt.Sprintf("(%d) actions prepare dataFix SQL fail. sql is:{%s}, error info is : {%s}", logThreadSeq, "set session sql_log_bin=off", err1)
			global.Wlog.Error(vlog)
			return err1
		}

		// 添加必要的前置语句
		// 从rs.JDBC（即dstDSN）中获取charset值
		charset := global.ExtractCharsetFromDSN(rs.JDBC)

		preSqls := []string{
			fmt.Sprintf("SET NAMES %s;", charset),
			"SET FOREIGN_KEY_CHECKS=0;",
			"SET UNIQUE_CHECKS=0;",
			"SET INNODB_LOCK_WAIT_TIMEOUT=1073741824;",
		}

		for _, preSql := range preSqls {
			if _, err1 := conn.ExecContext(ctx, preSql); err1 != nil {
				vlog = fmt.Sprintf("(%d) Failed to execute prep statement: %s. Error: %s", logThreadSeq, preSql, err1)
				global.Wlog.Error(vlog)
				return err1
			}
		}

		vlog = fmt.Sprintf("(%d) Executed necessary SET statements before datafix", logThreadSeq)
		global.Wlog.Debug(vlog)
	}

	// 根据fixTrxNum参数控制事务提交频率
	if rs.FixTrxNum <= 0 {
		// 如果fixTrxNum <= 0，则所有SQL放在一个事务中（默认行为）
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
	} else {
		// 根据fixTrxNum拆分成多个事务执行
		totalSql := len(sqlstr)
		for i := 0; i < totalSql; i += rs.FixTrxNum {
			end := i + rs.FixTrxNum
			if end > totalSql {
				end = totalSql
			}

			vlog = fmt.Sprintf("(%d) Starting transaction batch %d-%d (total %d SQL statements)",
				logThreadSeq, i+1, end, end-i)
			global.Wlog.Debug(vlog)

			// 执行当前批次的SQL语句
			for j := i; j < end; j++ {
				sql := sqlstr[j]
				if strings.HasPrefix(strings.ToUpper(sql), "ALTER TABLE") {
					if _, err = db.Exec(sql); err != nil {
						vlog = fmt.Sprintf("(%d) Failed to execute ALTER TABLE statement: %s. Error: %s", logThreadSeq, sql, err)
						global.Wlog.Error(vlog)
						return err
					}
				} else {
					if _, err = conn.ExecContext(ctx, sql); err != nil {
						vlog = fmt.Sprintf("(%d) Failed to execute repair SQL: %s. Error: %s. Starting rollback for batch %d-%d", logThreadSeq, sql, err, i+1, end)
						global.Wlog.Error(vlog)
						conn.ExecContext(ctx, "ROLLBACK")
						return err
					}
				}
			}

			// 提交当前批次的事务
			vlog = fmt.Sprintf("(%d) Committing transaction batch %d-%d", logThreadSeq, i+1, end)
			global.Wlog.Debug(vlog)
			if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
				vlog = fmt.Sprintf("(%d) Failed to commit transaction batch %d-%d. Error: %s", logThreadSeq, i+1, end, err)
				global.Wlog.Error(vlog)
				return err
			}
		}
	}

	defer db.Close()
	return nil
}

/*
生成修复sql语句，并写入到文件中
*/
func (rs repairSqlStruct) SqlFile(sfile *os.File, sql []string, logThreadSeq int64) error { //在/tmp/下创建数据修复文件，将在目标端数据修复的语句写入到文件中
	var (
		vlog string
		err  error
	)
	vlog = fmt.Sprintf("(%d) Writing repair statements to file", logThreadSeq)
	global.Wlog.Debug(vlog)

	// 检查文件是否为空，为空则添加必要的前置语句
	fileInfo, err := sfile.Stat()
	if err == nil && fileInfo.Size() == 0 {
		// 从rs.JDBC（即dstDSN）中获取charset值
		charset := global.ExtractCharsetFromDSN(rs.JDBC)

		// 添加必要的前置语句（所有文件都添加）
		preSqls := []string{
			fmt.Sprintf("SET NAMES %s;", charset),
			"SET FOREIGN_KEY_CHECKS=0;",
			"SET UNIQUE_CHECKS=0;",
			"SET INNODB_LOCK_WAIT_TIMEOUT=1073741824;",
		}

		for _, preSql := range preSqls {
			// 不使用全局去重，确保每个文件都有SET语句
			if _, err := sfile.WriteString(preSql + "\n"); err != nil {
				return err
			}
		}

		vlog = fmt.Sprintf("(%d) Added necessary SET statements to fix SQL file", logThreadSeq)
		global.Wlog.Debug(vlog)
	}

	if strings.HasPrefix(strings.ToUpper(strings.Join(sql, ";")), "ALTER TABLE") {
		// ALTER TABLE语句不需要事务包装
		// 先去重
		var uniqueSqls []string
		for _, s := range sql {
			trimmedSql := strings.TrimSpace(s)
			if trimmedSql == "" {
				continue
			}
			if _, loaded := writtenSqlMap.LoadOrStore(trimmedSql, true); !loaded {
				uniqueSqls = append(uniqueSqls, s)
			}
		}
		if len(uniqueSqls) > 0 {
			_, err = FileOperate{File: sfile, BufSize: 1024 * 4 * 1024, SqlType: "sql"}.ConcurrencyWriteFile(uniqueSqls)
			if err != nil {
				return err
			}
		}
	} else {
		// 不对INSERT语句进行去重，保留所有需要的记录
		var uniqueSqls []string
		for _, s := range sql {
			trimmedSql := strings.TrimSpace(s)
			if trimmedSql == "" {
				continue
			}
			// 对于INSERT语句，不进行去重，确保所有需要的记录都被插入
			// 对于其他类型的语句，仍然进行去重
			if strings.HasPrefix(strings.ToUpper(trimmedSql), "INSERT INTO") {
				uniqueSqls = append(uniqueSqls, s)
			} else {
				// 使用全局writtenSqlMap进行去重，确保跨调用去重
				if _, loaded := writtenSqlMap.LoadOrStore(trimmedSql, true); !loaded {
					uniqueSqls = append(uniqueSqls, s)
				}
			}
		}

		// 如果去重后没有SQL语句，直接返回
		if len(uniqueSqls) == 0 {
			vlog = fmt.Sprintf("(%d) No unique repair statements to write after deduplication", logThreadSeq)
			global.Wlog.Debug(vlog)
			return nil
		}

		// 根据fixTrxNum参数拆分事务
		if rs.FixTrxNum <= 0 {
			// 如果fixTrxNum <= 0，则所有SQL放在一个事务中（默认行为）
			sqlCommit := []string{"BEGIN;"}
			sqlCommit = append(sqlCommit, uniqueSqls...)
			sqlCommit = append(sqlCommit, "COMMIT;")

			_, err = FileOperate{File: sfile, BufSize: 1024 * 4 * 1024, SqlType: "sql"}.ConcurrencyWriteFile(sqlCommit)
			if err != nil {
				return err
			}
		} else {
			// 根据fixTrxNum拆分成多个事务
			totalSql := len(uniqueSqls)
			for i := 0; i < totalSql; i += rs.FixTrxNum {
				end := i + rs.FixTrxNum
				if end > totalSql {
					end = totalSql
				}

				// 构建一个事务的SQL语句
				batchSql := []string{"BEGIN;"}
				currentBatch := uniqueSqls[i:end]
				batchSql = append(batchSql, currentBatch...)
				batchSql = append(batchSql, "COMMIT;")

				_, err = FileOperate{File: sfile, BufSize: 1024 * 4 * 1024, SqlType: "sql"}.ConcurrencyWriteFile(batchSql)
				if err != nil {
					return err
				}

				vlog = fmt.Sprintf("(%d) Written transaction batch %d-%d (total %d SQL statements)",
					logThreadSeq, i+1, end, end-i)
				global.Wlog.Debug(vlog)
			}
		}
	}

	vlog = fmt.Sprintf("(%d) Repair statements written to file successfully", logThreadSeq)
	global.Wlog.Debug(vlog)
	return nil
}
func ApplyDataFix(fixSql []string, datafixType string, sfile *os.File, ddrive, jdbc string, logThreadSeq int64) error {
	return ApplyDataFixWithTrxNum(fixSql, datafixType, sfile, ddrive, jdbc, logThreadSeq, 0)
}

func ApplyDataFixWithTrxNum(fixSql []string, datafixType string, sfile *os.File, ddrive, jdbc string, logThreadSeq int64, fixTrxNum int) error {
	var (
		err       error
		repairdml = repairSqlStruct{
			Drive:     ddrive,
			JDBC:      jdbc,
			FixTrxNum: fixTrxNum,
		}
	)

	// 优化INSERT语句：合并相同表的多条INSERT为单条VALUES多组数据格式
	if len(fixSql) > 1 {
		var deleteSqls []string
		var insertSqls []string

		for _, sql := range fixSql {
			sqlTrim := strings.TrimSpace(strings.ToUpper(sql))
			if strings.HasPrefix(sqlTrim, "DELETE") {
				deleteSqls = append(deleteSqls, sql)
			} else if strings.HasPrefix(sqlTrim, "INSERT") {
				insertSqls = append(insertSqls, sql)
			}
		}

		insertSqlSize := inputArg.GetGlobalConfig().SecondaryL.RepairV.InsertSqlSize * 1024

		// 对INSERT语句进行合并优化
		if len(insertSqls) > 1 {
			optFixTrxNum := fixTrxNum
			if optFixTrxNum <= 0 {
				optFixTrxNum = 1000 // 默认值
			}
			insertSqls = OptimizeInsertSqls(insertSqls, insertSqlSize, optFixTrxNum)
		}

		// 重新组合SQL语句
		fixSql = append(deleteSqls, insertSqls...)
	}

	// 修复：当datafixType为"yes"时，将修复SQL写入文件
	if datafixType == "yes" || datafixType == "file" {
		if err = repairdml.SqlFile(sfile, fixSql, logThreadSeq); err != nil {
			return err
		}
	}
	if datafixType == "table" {
		if err = repairdml.execRepairSql(fixSql, ddrive, logThreadSeq); err != nil {
			return err
		}
	}
	return nil
}
