package dbExec

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	"gt-checksum/global"
	"os"
	"strings"
	"time"
)

type DBConnStruct struct {
	DBDevice        string
	JDBC            string
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

/*
   连接数据库，返回连接内存地址
*/
func (dbs *DBConnStruct) openDb() (*sql.DB, error) {
	db, err := sql.Open(dbs.DBDevice, dbs.JDBC)
	if err != nil {
		global.Wlog.Info("(0) database open fail. Error Info: ", err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		global.Wlog.Error("(0) database connection fail. Error Info: ", err)
		return nil, err
	}
	db.SetMaxIdleConns(dbs.MaxIdleConns)
	db.SetMaxOpenConns(dbs.MaxOpenConns)
	db.SetConnMaxLifetime(-1)
	db.SetConnMaxIdleTime(-1)
	return db, nil
}

func (dbs *DBConnStruct) OpenDB() (*sql.DB, error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("database create session connection fail, Check the database connection information.")
			os.Exit(0)
		}
	}()
	return dbs.openDb()
}
func (dbs *DBConnStruct) QPrepareRow(db *sql.DB, sqlStr string) (*sql.Rows, error) {
	global.Wlog.Info("begin prepare sql \"", sqlStr, "\"")
	var sqlRows *sql.Rows
	stmt, err := db.Prepare(sqlStr)
	if err != err {
		global.Wlog.Error("sql prepare fail. sql: ", sqlStr, " Error info: ", err)
		return nil, err
	}
	if strings.HasPrefix(strings.ToUpper(sqlStr), "SELECT") {
		sqlRows, err = stmt.Query()
		if err != nil {
			global.Wlog.Error("select sql exec fail. sql: ", sqlStr, " Error info: ", err)
			return nil, err
		}
	} else {
		if _, err = stmt.Exec(); err != nil {
			global.Wlog.Error("transaction sql exec fail. sql: ", sqlStr, " Error info: ", err)
			return nil, err
		}
	}
	stmt.Close()
	global.Wlog.Info("sql exec successful. sql info: ", sqlStr)
	return sqlRows, nil
}

/*
   查询数据库，返回数据库接口切片，或返回json（包含列名）
*/
func (dbs *DBConnStruct) QMapData(db *sql.DB, sqlStr string) ([]map[string]interface{}, error) {
	var (
		sqlRows *sql.Rows
		err     error
	)
	if sqlRows, err = dbs.QPrepareRow(db, sqlStr); err != nil {
		return nil, err
	}
	// 获取列名
	columns, err := sqlRows.Columns()

	if err != nil {
		errInfo := fmt.Sprintf("get database table column name fail. Error info: %s.", err)
		global.Wlog.Error(errInfo)
		return nil, err
	}
	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
	//values := make([]sql.RawBytes,len(columns))
	//定义一个切片，元素类型是interface{}接口
	//scanArgs := make([]interface{},len(values))
	valuePtrs := make([]interface{}, len(columns))
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, len(columns))
	for sqlRows.Next() {
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		sqlRows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	return tableData, nil
}

func GetDBexec(jdbcurl, dbDevice string) *DBConnStruct {
	return &DBConnStruct{
		JDBC:            jdbcurl,
		DBDevice:        dbDevice,
		MaxOpenConns:    1000,
		MaxIdleConns:    100,
		ConnMaxIdleTime: 100,
		ConnMaxLifetime: 0,
	}
}

/*
   长事务会话执行
*/
func (dbs *DBConnStruct) LongSessionExec(db *sql.DB, sqlstr string) error {
	global.Wlog.Debug("GreatdbCheck executes \"", sqlstr, "\" at the MySQL")
	_, err := db.Exec(sqlstr)
	if err != nil {
		global.Wlog.Error("exec sql fail. sql: ", sqlstr, "error info: ", err)
		return err
	}
	return nil
}

/*
长会话连接查询、返回单行int类型,用于查询数据建库数值列，并返回值
*/
func (dbs *DBConnStruct) LSQInt(db *sql.DB, sqlstr string) (int, error) {
	var tmpTableCount int
	global.Wlog.Debug("GreatdbCheck prepare sql: \"", sqlstr, "\" at the MySQL")
	stamt, err := db.Prepare(sqlstr)
	if err != nil {
		global.Wlog.Error("GreatdbCheck parpare sql fail. sql: ", sqlstr, "error info: ", err)
		return 0, err
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
	rows, err := stamt.Query()
	if err != nil {
		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
		return 0, err
	}
	for rows.Next() {
		rows.Scan(&tmpTableCount)
	}
	return tmpTableCount, nil
}

/*
长会话连接查询、返回多行string类型,用于查询数据建库数值列，并返回值
*/
func (dbs *DBConnStruct) LSQSEInt(db *sql.DB, sqlstr string) ([]string, error) {
	var tmpTableCount []string
	global.Wlog.Debug("GreatdbCheck prepare sql: \"", sqlstr, "\" at the MySQL")
	stmat, err := db.Prepare(sqlstr)
	if err != err {
		global.Wlog.Error("GreatdbCheck parpare sql fail. sql: ", sqlstr, "error info: ", err)
		return tmpTableCount, err
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
	rows, err := stmat.Query()
	var num string
	if err != err {
		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
		return tmpTableCount, err
	} else {
		for rows.Next() {
			rows.Scan(&num)
			tmpTableCount = append(tmpTableCount, num)
		}
	}
	return tmpTableCount, nil
}

func (dbs *DBConnStruct) DbSqlExecString(db *sql.DB, sqlstr string) (string, error) {
	var (
		rows          *sql.Rows
		rowDataString []string
		err           error
		columns       []string
	)
	global.Wlog.Debug("GreatdbCheck prepare sql: \"", sqlstr, "\" at the MySQL")
	//stmt, err = db.Prepare(sqlstr)
	//if err != nil {
	//	global.Wlog.Error("GreatdbCheck parpare sql fail. sql: ", sqlstr, "error info: ", err)
	//}
	//rows, err = stmt.Query()
	rows, err = db.Query(sqlstr)
	if err != nil {
		fmt.Println(err)
		rows, err = db.Query(sqlstr)
	}
	global.Wlog.Debug("GreatdbCheck exec sql: \"", sqlstr, "\" at the MySQL")
	columns, err = rows.Columns()
	if err != nil {
		global.Wlog.Error("GreatdbCheck exec sql fail. sql: ", sqlstr, "error info: ", err)
		return "", err
	}
	valuePtrs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for rows.Next() {
		var tmpaaS []string
		for i := 0; i < len(columns); i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		for i := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			tmpaaS = append(tmpaaS, fmt.Sprintf("%v", v))
		}
		tmpaa := strings.Join(tmpaaS, "/*go actions columnData*/")
		rowDataString = append(rowDataString, tmpaa)
	}
	rows.Close()
	return strings.Join(rowDataString, "/*go actions rowData*/"), nil
}

func DBexec() *DBConnStruct {
	return &DBConnStruct{}
}
