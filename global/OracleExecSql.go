package global
//
//import (
//	"database/sql"
//	"fmt"
//	_ "github.com/go-sql-driver/mysql"
//	"log"
//	"os"
//	"strings"
//	"time"
//)
//
//type Connection struct {
//	DriverName, DataSourceName string
//	MaxIdleConns,MaxOpenConns int
//	ConnMaxLifetime,ConnMaxIdleTime time.Duration
//}
//
//type SummaryInfo struct{
//	Database,Tablename,StrSql,IgnoreTable string
//	RowCount,ChunkSize,JobNums,TableRows int
//	TableIndexQue []byte
//	ColumnPRI,TableFirstIndexVal string
//	MySQLSelectColumn,OracleSelectColumn string
//	TableList []string
//	Database,Tablename,StrSql string
//}
//
//type DB interface {
//	GetConnection() *sql.DB
//}
//
//type SqlExec interface {
//	DB
//	SQLColumnsNum(o *SummaryInfo) ([]byte,[]byte)
//	SQLTableNum(o *SummaryInfo) ([]byte,bool)
//	SQLTablePRIColumn(strSql string,o *SummaryInfo) string
//	SQLTableRows(strSql string,o *SummaryInfo) int
//	SQLTablePoint(strSql string,o *SummaryInfo) []string
//	SQLTableCheckSum(strSql string,o *SummaryInfo) ([]string, []byte)
//	SQLTablePRIColumn(o *SummaryInfo) (string,bool)
//}
//
//
//
//func (con *Connection) GetConnection() *sql.DB {
//	db,err:=sql.Open(con.DriverName,con.DataSourceName)
//	if err != nil {
//		fmt.Errorf("Failed to open to %s database with",con.DataSourceName)
//		return nil
//	}
//	db.SetMaxIdleConns(con.MaxIdleConns)
//	db.SetMaxOpenConns(con.MaxOpenConns)
//	db.SetConnMaxLifetime(con.ConnMaxLifetime)
//	db.SetConnMaxIdleTime(con.ConnMaxIdleTime)
//	return db
//}
//
//func (con *Connection) SQLColumnsNum(strSql string,o *SummaryInfo) ([]byte,[]byte){ //获取每个表的列信息
//
//	var columnsList []byte
//	var columnsInfo []byte
//	dbconn:=con.GetConnection()
//	defer dbconn.Close()
//	stmt,err := dbconn.Prepare(strSql)
//	rows,err := stmt.Query()
//	if err != nil {
//		fmt.Printf("Failed to get column information for the current table %s under the databases %s !\n",o.Database,o.Tablename)
//		os.Exit(1)
//	}
//	columnsList = append(columnsList,o.Tablename...)
//	columnsList = append(columnsList,":"...)
//
//	stmt,err := dbconn.Prepare(o.StrSql)
//	rows,err := stmt.Query()
//	if err != nil {
//		fmt.Printf("Failed to get column information for the current table %s under the databases %s !The information is as follows:%s\n",err)
//		os.Exit(1)
//	}
//
//	for rows.Next(){
//		var columns string
//		var colDataType string
//		var numericScale string
//		rows.Scan(&columns,&colDataType,&numericScale)
//
//		columns = strings.ToUpper(columns)
//		colDataType = strings.ToUpper(colDataType)
//		numericScale = strings.ToUpper(numericScale)
//		if len(numericScale) == 0  {
//			numericScale = "9999999999"
//		}
//		columnsList = append(columnsList,columns...)
//		columnsList = append(columnsList,"@"...)
//		columnsInfo = append(columnsInfo,columns...)
//		columnsInfo = append(columnsInfo,":"...)
//		columnsInfo = append(columnsInfo,colDataType...)
//		columnsInfo = append(columnsInfo,":"...)
//		columnsInfo = append(columnsInfo,numericScale...)
//		columnsInfo = append(columnsInfo,"@"...)
//	}
//	defer rows.Close()
//	return columnsList,columnsInfo
//}
//
//func (m *Connection) SQLTableNum(strSql string,o * SummaryInfo) []byte{ //获取库下表和列的信息
//		var tableList []byte
//		dbconn := m.GetConnection()
//		defer dbconn.Close()
//		stmt, err := dbconn.Prepare(strSql)
//		rows, err := stmt.Query()
//		if err != nil{
//		fmt.Println("获取数据库%s的表信息失败！详细信息如下：%s", o.Database, err)
//	}
//	return
//	}
//func (m *Connection) SQLTableNum(o *SummaryInfo) ([]byte,bool) { //获取库下表和列的信息
//	var tableList []byte
//	var status bool = true
//
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	//	strSql := "show tables from " + o.Database
//	stmt, err := dbconn.Prepare(o.StrSql)
//	rows, err := stmt.Query()
//	if err != nil {
//		fmt.Println("获取数据库%s的表信息失败！详细信息如下：%s", o.Database, err)
//		status = false
//	}
//	for rows.Next() {
//		var tablename string
//		rows.Scan(&tablename)
//		tablename = strings.ToUpper(tablename)
//		tableList = append(tableList, tablename...)
//		tableList = append(tableList, ";"...)
//	}
//	return tableList
//}
//
//func (m *Connection) SQLTablePRIColumn(strSql string) string{ //初始化数据库，获取当前库下每个表是否有int类型主键
//	// 获取当前主键信息
//	var PRIcolumn string
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	stmt,err := dbconn.Prepare(strSql)
//	err = stmt.QueryRow().Scan(&PRIcolumn)
//	if err != nil {
//		fmt.Println("Failed to get primary key columns information")
//	}
//	return PRIcolumn
//}
//
//func (m *Connection) SQLTableRows(strSql string,o * SummaryInfo) string{
//	var rowCount string
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	stmt,err := dbconn.Prepare(strSql)
//	err = stmt.QueryRow().Scan(&rowCount)
//	if err != nil{
//		fmt.Printf("[error]: Failed to query total %s rows for table under current databases %s.The error message is : %s.\n",o.Tablename,o.Database,err)
//		os.Exit(1)
//	}
//	return rowCount
//}
//
//func (m *Connection) SQLTableStartVal(strSql string,o * SummaryInfo) string { //查询源表的当前数据行总数，以源表的信息为准
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	var firstIndexPoint string
//	stmt,err := dbconn.Prepare(strSql)
//	err = stmt.QueryRow().Scan(&firstIndexPoint)
//	if err != nil {
//		fmt.Printf("[error]: Failed to query the table %s in the current databases %s for primary key information. Please check whether the table has data or primary key information of type int.\n",o.Tablename,o.Database)
//		os.Exit(1)
//	}
//	return firstIndexPoint
//}
//
//func (m *Connection) SQLTablePoint(strSql string,o * SummaryInfo) []string{ //对所有chunk的开头、结尾索引节点进行处理，生成字节数组，返回数组 //目前只支持主键为int类型的
//	var d []string
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	stmt,err := dbconn.Prepare(strSql)
//	rows, err := stmt.Query()
//	if err != nil {
//		fmt.Printf("[error]: Failed to query the Chunk beginning and end nodes of the table %s under the current database %s.The error message is: %s.\n",  o.Tablename,o.Database,err)
//	}
//	for rows.Next() {
//		var b string
//		rows.Scan(&b)
//		d = append(d,b)
//
//	}
//	return d
//}
//
//
//
//func (m *Connection) SQLTableCheckSum(strSql string,o * SummaryInfo) ([]string, []byte) { //根据每个chunk的起始节点，生成查询语句，去数据库查询数据，返回数据的字节数组
//	var result []byte
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	stmt,err := dbconn.Prepare(strSql)
//	rows, err := stmt.Query()
//	if err != nil {
//		log.Fatal("[error]: Failed to query date from Source MySQL database. Please check current database status！", err)
//	}
//	// 获取列名
//	columns,_ := rows.Columns()
//	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
//	values := make([]sql.RawBytes,len(columns))
//	//定义一个切片，元素类型是interface{}接口
//	scanArgs := make([]interface{},len(values))
//	for i := range values{
//		//把sql.RawBytes类型的地址存进来
//		scanArgs[i] = &values[i]
//	}
//	for rows.Next(){
//		rows.Scan(scanArgs...)
//		for k,col := range values {
//			result = append(result,columns[k]...)
//			result = append(result,"&:"...)
//			result = append(result,col...)
//			result = append(result,"&@"...)
//		}
//		result = append(result,"&,"...)
//	}
//
//	defer rows.Close()
//	return columns,result
//}
//
//func (m *Connection) SqlExec(strSql string,o * SummaryInfo){ //执行目标端数据修复语句
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//	stmt,err := dbconn.Prepare(strSql)
//	if err != nil {
//		fmt.Printf("[error]: Failed to query the Chunk beginning and end nodes of the table %s under the current database %s.The error message is: %s.\n",  o.Tablename,o.Database,err)
//	}
//	_,err = stmt.Exec()
//	if err != nil{
//		fmt.Printf("exec failed, err:%v\n", err)
//		os.Exit(1)
//	}
//		o.Tablename = tablename
//		columns, _ := m.SQLColumnsNum(o)
//
//		tableList = append(tableList, tablename...)
//		tableList = append(tableList, ":"...)
//		tableList = append(tableList, columns...)
//		tableList = append(tableList, ";"...)
//	}
//	return tableList, status
//}
//
//func (m *Connection) QueryMySQLTablePRIColumn(o *SummaryInfo) (string,bool){ //初始化数据库，获取当前库下每个表是否有int类型主键
//	// 获取当前主键信息
//	var status bool = true
//	var PRIcolumn string
//
//	dbconn := m.GetConnection()
//	defer dbconn.Close()
//
//	//strSql := "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema='" + o.Database + "' and table_name = '" + o.Tablename +"' and COLUMN_KEY='PRI' and COLUMN_TYPE like '%int%';"
//	stmt,err := dbconn.Prepare(o.StrSql)
//	err = stmt.QueryRow().Scan(&PRIcolumn)
//	if err != nil {
//		status = false
//	}
//	return PRIcolumn,status
//}