package mysql

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"strconv"
	"strings"
	"time"
)

type BinlogPrepareStruct struct {
	dmlSqlType       string   //dml sql 语句类型
	dmlSqlCollection []string //dml sql 语句集合
	dmlTableName     string   //dml sql 操作的表
}

// 一个事务完整的binlog信息包含 时间、server_id，xid、binlog文件信息，sql语句等
type binlogTrxPrepareStruct struct {
	timestamp    uint32                //时间戳
	serverID     uint32                //server id
	binlogFile   string                //binlog文件名
	logPos       uint32                //binlog pos
	trxBegin     string                //trx begin
	strSqlGather []BinlogPrepareStruct //事务集合
	trxType      string                //dml sql的类型，是insert还是update、还是delete
	tableName    string                //操作的表名 schema.table
	trxCommit    string                //trx commit
	xid          uint64                //xid信息
	gtid         gtidInfoStruct        //gtid信息
}

type xaTrxPrepareStruct struct {
	timestamp    uint32                //时间戳
	serverID     uint32                //server id
	binlogFile   string                //binlog文件名
	logPos       uint32                //binlog pos
	xaStart      string                //XA START
	strSqlGather []BinlogPrepareStruct //事务集合
	xaCommit     string                //trx commit
	gtid         gtidInfoStruct        //gtid信息
	xaName       string
}

/*
	将binlog生成的事务语句转换成对应语句
*/
func (my MySQLIncDataBinlogPrepareStruct) binlogSqlTransition(a []BinlogPrepareStruct) []string {
	var sql []string
	for i := range a {
		schema := strings.Split(a[i].dmlTableName, "/*SchemaTable*/")[0]
		table := strings.Split(a[i].dmlTableName, "/*SchemaTable*/")[1]
		switch a[i].dmlSqlType {
		case "insert":
			tmpSql := fmt.Sprintf("insert into `%s`.`%s` values (%s);", schema, table, strings.Join(a[i].dmlSqlCollection, "),("))
			sql = append(sql, tmpSql)
		case "update":
			tmpSql := fmt.Sprintf("update `%s`.`%s` where (%s);", schema, table, strings.Join(a[i].dmlSqlCollection, "),("))
			sql = append(sql, tmpSql)
		case "delete":
			tmpSql := fmt.Sprintf("delete from `%s`.`%s` where (%s);", schema, table, strings.Join(a[i].dmlSqlCollection, "),("))
			sql = append(sql, tmpSql)
		}
	}
	return sql
}

/*
	解析binlog中的ddl语句
*/
func (my MySQLIncDataBinlogPrepareStruct) binlogQuerySql(ev *replication.BinlogEvent) map[string]string {
	var querySqlMap = make(map[string]string, 1)
	if ev.Header.EventType == replication.QUERY_EVENT {
		qEvent := ev.Event.(*replication.QueryEvent)
		//fmt.Println(fmt.Sprintf("Slave proxy ID: %d", qEvent.SlaveProxyID))
		//fmt.Println(fmt.Sprintf("Execution time: %d", qEvent.ExecutionTime))
		//fmt.Println(fmt.Sprintf("Error code: %d", qEvent.ErrorCode))
		//fmt.Println(fmt.Sprintf("Schame: %s", string(qEvent.Schema)))
		querySql := strings.TrimSpace(string(qEvent.Query))
		//create 语句
		if strings.HasPrefix(strings.ToUpper(querySql), "CREATE") {
			if strings.HasPrefix(strings.ToUpper(querySql), "CREATE DATABASE") {
				querySqlMap["createDatabase"] = querySql
				//fmt.Println("create database query info: ", querySql)
			}
			if strings.HasPrefix(strings.ToUpper(querySql), "CREATE TABLE") {
				querySqlMap["createTable"] = querySql
				//fmt.Println("create table query info: ", querySql)
			}
			if strings.HasPrefix(strings.ToUpper(querySql), "CREATE INDEX") {
				querySqlMap["createIndex"] = querySql
				//fmt.Println("create index query info: ", querySql)
			}
		}
		//drop 语句
		if strings.HasPrefix(strings.ToUpper(querySql), "DROP ") {
			if strings.HasPrefix(strings.ToUpper(querySql), "DROP DATABASE") {
				querySqlMap["dropDatabase"] = querySql
				//fmt.Println("drop database query info: ", querySql)
			}
			if strings.HasPrefix(strings.ToUpper(querySql), "DROP TABLE") {
				querySqlMap["dropTable"] = querySql
				//fmt.Println("drop table query info: ", querySql)
			}
			if strings.HasPrefix(strings.ToUpper(querySql), "DROP INDEX") {
				querySqlMap["dropIndex"] = querySql
				//fmt.Println("drop index query info: ", querySql)
			}
		}
		//alter 语句
		if strings.HasPrefix(strings.ToUpper(querySql), "ALTER") {
			if strings.HasPrefix(strings.ToUpper(querySql), "ALTER DATABASE") {
				querySqlMap["alterDatabase"] = querySql
				//fmt.Println("alter database query info: ", querySql)
			}
			if strings.HasPrefix(strings.ToUpper(querySql), "ALTER EVENT") {
				querySqlMap["alterEvent"] = querySql
				//fmt.Println("alter event query info: ", querySql)
			}
			if strings.HasPrefix(strings.ToUpper(querySql), "ALTER TABLE") {
				if strings.Contains(strings.ToUpper(querySql), "COLUMN") {
					querySqlMap["alterTable"] = querySql
					//fmt.Println("alter table query info: ", querySql)
				}
				if strings.Contains(strings.ToUpper(querySql), "MODIFY") {
					querySqlMap["alterTable"] = querySql
					//fmt.Println("alter table query info: ", querySql)
				}
				//if strings.Contains(strings.ToUpper(querySql), "PARTITION") {
				//	fmt.Println("alter table query info: ", querySql)
				//}
				if strings.Contains(strings.ToUpper(querySql), "CHANGE") {
					querySqlMap["alterTable"] = querySql
					//fmt.Println("alter table query info: ", querySql)
				}
				if strings.Contains(strings.ToUpper(querySql), "INDEX") {
					querySqlMap["alterTable"] = querySql
					//fmt.Println("alter table query info: ", querySql)
				}
				if strings.Contains(strings.ToUpper(querySql), "KEY") {
					querySqlMap["alterTable"] = querySql
					//fmt.Println("alter table query info: ", querySql)
				}
			}
		}

		//truncate 语句
		if strings.HasPrefix(strings.ToUpper(querySql), "TRUNCATE") {
			querySqlMap["truncateTable"] = querySql
			//fmt.Println("truncate database query info: ", querySql)
		}
		//rename 语句
		if strings.HasPrefix(strings.ToUpper(querySql), "RENAME") {
			querySqlMap["renameTable"] = querySql
			//fmt.Println("rename database query info: ", querySql)
		}
		if strings.HasPrefix(strings.ToUpper(querySql), "BEGIN") {
			querySqlMap["begin"] = querySql
			//return querySql
		}
		if strings.HasPrefix(strings.ToUpper(querySql), "XA START") {
			querySqlMap["xaStart"] = strings.ReplaceAll(querySql, "XA START ", "")
		}
		if strings.HasPrefix(strings.ToUpper(querySql), "XA END") {
			querySqlMap["xaEnd"] = strings.ReplaceAll(querySql, "XA END ", "")
		}
		if strings.HasPrefix(strings.ToUpper(querySql), "XA COMMIT") {
			var xaid string
			if strings.Contains(strings.ToUpper(querySql), "XA COMMIT") {
				xaid = strings.ReplaceAll(querySql, "XA COMMIT ", "")
			}
			if strings.Contains(strings.ToUpper(xaid), "ONE PHASE") {
				xaid = strings.ReplaceAll(xaid, " ONE PHASE", "")
			}
			querySqlMap["xaCommit"] = xaid
		}
	}
	return querySqlMap
}

/*
	输出xid信息，xid为事务执行完成准备提交的id信息
*/
func (my MySQLIncDataBinlogPrepareStruct) binlogXid(ev *replication.BinlogEvent) uint64 {
	var xid uint64
	if ev.Header.EventType == replication.XID_EVENT {
		xid = ev.Event.(*replication.XIDEvent).XID
	}
	return xid
}

/*
	解析binlog事件的dml语句
*/
func (my MySQLIncDataBinlogPrepareStruct) binlogDmlPrepare(ev *replication.BinlogEvent) BinlogPrepareStruct {
	var binlogPrepare = BinlogPrepareStruct{}
	//insert 、 delete event
	var insertDeleteEventDispos = func(wrEvent *replication.RowsEvent) (string, []string) {
		var columnSlice []string
		db := string(wrEvent.Table.Schema)
		tb := string(wrEvent.Table.Table)
		if _, ok := my.TableList[fmt.Sprintf("%s.%s", db, tb)]; ok {
			columnCount := wrEvent.ColumnCount
			for i := range wrEvent.Rows {
				var columnVal = make([]string, 0, columnCount)
				for v := range wrEvent.Rows[i] {
					columnVal = append(columnVal, fmt.Sprintf("%v", wrEvent.Rows[i][v]))
				}
				colummnValString := fmt.Sprintf("'%s'", strings.Join(columnVal, "','"))
				columnSlice = append(columnSlice, colummnValString)
			}
		}
		return fmt.Sprintf("%s/*SchemaTable*/%s", db, tb), columnSlice
	}
	//update event
	var updateEventDispos = func(wrEvent *replication.RowsEvent) (string, []string) {
		upEvent := ev.Event.(*replication.RowsEvent)
		db := string(upEvent.Table.Schema)
		tb := string(upEvent.Table.Table)
		var columnSlice []string
		if _, ok := my.TableList[fmt.Sprintf("%s.%s", db, tb)]; ok {
			upRows := upEvent.Rows
			columnCount := upEvent.ColumnCount
			for i := 0; i < len(upRows); i += 2 {
				var updateSql string
				//old row data
				var columnVal = make([]string, 0, columnCount)
				for v := range upRows[i] {
					columnVal = append(columnVal, fmt.Sprintf("%v", upRows[i][v]))
				}
				colummnValOldString := fmt.Sprintf("'%s'", strings.Join(columnVal, "','"))
				//new row data
				columnVal = []string{}
				for v := range upRows[i+1] {
					columnVal = append(columnVal, fmt.Sprintf("%v", upRows[i+1][v]))
				}
				colummnValUpdateString := fmt.Sprintf("'%s'", strings.Join(columnVal, "','"))

				//odl new row data
				updateSql = fmt.Sprintf("%s/*columnModify*/%s", colummnValOldString, colummnValUpdateString)
				columnSlice = append(columnSlice, updateSql)
			}
		}
		return fmt.Sprintf("%s/*SchemaTable*/%s", db, tb), columnSlice
	}
	//insert
	if ev.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
		wrEvent := ev.Event.(*replication.RowsEvent)
		table, sql := insertDeleteEventDispos(wrEvent)
		binlogPrepare.dmlTableName = table
		binlogPrepare.dmlSqlType = "insert"
		binlogPrepare.dmlSqlCollection = sql
	}

	//delete
	if ev.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
		deEvent := ev.Event.(*replication.RowsEvent)
		table, sql := insertDeleteEventDispos(deEvent)
		binlogPrepare.dmlTableName = table
		binlogPrepare.dmlSqlType = "delete"
		binlogPrepare.dmlSqlCollection = sql
	}
	//update
	if ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		upEvent := ev.Event.(*replication.RowsEvent)
		table, sql := updateEventDispos(upEvent)
		binlogPrepare.dmlTableName = table
		binlogPrepare.dmlSqlType = "update"
		binlogPrepare.dmlSqlCollection = sql
	}
	return binlogPrepare
}

/*
	返回一个事务包含的所有sql语句
*/
func (my MySQLIncDataBinlogPrepareStruct) OneEventSql(block chan struct{}, trxEvent chan map[string][]string, dQ chan struct{}) {
	var ad = &binlogTrxPrepareStruct{}
	var ac = &xaTrxPrepareStruct{}
	var curritySites = make(map[string]string)

	var trxMap = make(map[string][]string)
	var currityTrxType string

	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: my.ServerId,
		Flavor:   my.Mytype,
		Host:     my.Host,
		Port:     my.Port,
		User:     my.User,
		Password: my.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"
	//streamer, _ := syncer.StartSync(mysql.Position{my.BinlogFile, my.Pos})
	streamer, _ := syncer.StartSync(mysql.Position{my.BinlogFile, 4})
	startPos := my.Pos
	binlogFile := my.BinlogFile
	var strSqlGather []BinlogPrepareStruct
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		ev, err := streamer.GetEvent(ctx)
		if err == context.DeadlineExceeded {
			if len(dQ) == 1 {
				var e1 = make(map[string][]string)
				cancel()
				syncer.Close()
				e1["quit"] = []string{}
				trxEvent <- e1
				break
			}
		} else {
			//检查轮转日志
			if ev.Header.EventType == replication.ROTATE_EVENT {
				rotateEvent := ev.Event.(*replication.RotateEvent)
				my.BinlogFile = string(rotateEvent.NextLogName)
			}

			gtid := my.binlogGtid(ev)
			if len(gtid.gtidVal) > 0 {
				ad.gtid, ac.gtid = gtid, gtid
				curritySites["Point"] = gtid.gtidVal
			}

			queryType := my.binlogQuerySql(ev)
			if _, ok := queryType["begin"]; ok {
				currityTrxType = "beginCommit"
				ad.timestamp = ev.Header.Timestamp
				ad.serverID = ev.Header.ServerID
				ad.logPos = ev.Header.LogPos
				curritySites["position"] = strconv.Itoa(int(ev.Header.LogPos))
				ad.trxBegin = "begin"
			}
			if _, ok := queryType["xaStart"]; ok {
				currityTrxType = "xaTxr"
				ac.timestamp = ev.Header.Timestamp
				ac.serverID = ev.Header.ServerID
				ac.logPos = ev.Header.LogPos
				ac.xaStart = "xaStart"
				ac.xaName = queryType["xaStart"]
			}

			dmlSql := my.binlogDmlPrepare(ev)
			if len(dmlSql.dmlSqlCollection) > 0 {
				switch currityTrxType {
				case "beginCommit":
					if len(dmlSql.dmlSqlCollection) > 0 { //普通事务处理
						strSqlGather = append(strSqlGather, dmlSql)
					}
				case "xaTxr":
					if len(dmlSql.dmlSqlCollection) > 0 { //xa事务处理
						strSqlGather = append(strSqlGather, dmlSql)
					}
				}
			}

			if _, ok := queryType["xaEnd"]; ok {
				ac.timestamp = ev.Header.Timestamp
				ac.serverID = ev.Header.ServerID
				ac.logPos = ev.Header.LogPos
				ac.xaCommit = "xaEnd"
				trxMap[ac.xaName] = my.binlogSqlTransition(strSqlGather)
				strSqlGather = []BinlogPrepareStruct{}
			}

			if xaid, ok := queryType["xaCommit"]; ok {
				if eg, ok1 := trxMap[xaid]; ok1 {
					ac.timestamp = ev.Header.Timestamp
					ac.serverID = ev.Header.ServerID
					ac.logPos = ev.Header.LogPos
					var g = make(map[string][]string, 1)
					if ac.logPos < startPos && binlogFile == my.BinlogFile {
						ac = &xaTrxPrepareStruct{}
						delete(queryType, xaid)
						delete(trxMap, xaid)
						continue
					} else {
						g["xaCommit"] = eg
						block <- struct{}{}
						trxEvent <- g
						ac = &xaTrxPrepareStruct{}
						delete(queryType, xaid)
						delete(trxMap, xaid)
					}
				}
			}
			if xid := my.binlogXid(ev); xid != 0 {
				ad.timestamp = ev.Header.Timestamp
				ad.serverID = ev.Header.ServerID
				ad.logPos = ev.Header.LogPos
				ad.xid = xid
				ad.trxCommit = "commit"
				ad.strSqlGather = strSqlGather
				strSqlGather = []BinlogPrepareStruct{}
			}
			if ad.trxBegin == "begin" && ad.trxCommit == "commit" && ad.xid != 0 && len(ad.strSqlGather) > 0 {
				curritySites["position"] = strconv.Itoa(int(ad.logPos))
				var e = make(map[string][]string, 1)
				if ad.logPos < startPos && binlogFile == my.BinlogFile {
					ad = &binlogTrxPrepareStruct{}
					continue
				} else {
					e["beginCommit"] = my.binlogSqlTransition(ad.strSqlGather)
					block <- struct{}{}
					trxEvent <- e
					ad = &binlogTrxPrepareStruct{}
					delete(queryType, "beginCommit")
					strSqlGather = []BinlogPrepareStruct{}
				}

			}
		}
	}
}
