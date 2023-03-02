package mysql

import (
	"encoding/hex"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	uuid "github.com/satori/go.uuid"
)

const (
	TimeFormat string = "2006-01-02 15:04:05"
)

type MySQLIncDataBinlogPrepareStruct struct {
	Mytype     string         //mysql体系的类型 是MySQL或miriadb
	Host       string         //数据库连接地址
	User       string         //数据库user
	Password   string         //数据库password
	Port       uint16         //数据库的端口
	ServerId   uint32         //伪装slave的server id
	BinlogFile string         //读取的增量binlog file文件
	Pos        uint32         //读取的增量binlog pos点
	TableList  map[string]int //校验的表列表
}

type binlogTableInfoStruct struct {
	TableID     uint64 //table id
	Flags       uint16 //flages
	table       string
	ColumnCount uint64 //列个数
}

type gtidInfoStruct struct {
	lastCommitted            int64  //上一个事务提交状态
	sequenceNumber           int64  //事务提交的序号
	immediateCommitTimestamp uint64 //立即提交的时间戳
	originalCommitTimestamp  uint64 //原始时间戳
	transactionLength        uint64 //事务长度
	immediateServerVersion   uint32 //立即提交的server 版本
	originalServerVersion    uint32 //原始提交的server 版本
	gtidVal                  string //当前事务的gtid值
	CommitFlag               uint8  //事务提交标志   //xa事务为1，普通事务为0
}

/*
	解析binlog的table信息
*/
func (my MySQLIncDataBinlogPrepareStruct) incBinlogTableInfo(ev *replication.BinlogEvent) {
	fmt.Println("++++ table Map info +++++")
	if ev.Header.EventType == replication.TABLE_MAP_EVENT {
		tmEvent := ev.Event.(*replication.TableMapEvent)
		db := string(tmEvent.Schema)
		tb := string(tmEvent.Table)
		if _, ok := my.TableList[fmt.Sprintf("%s.%s", db, tb)]; ok {
			fmt.Println(fmt.Sprintf("TableID: %d\n", tmEvent.TableID))
			fmt.Println(fmt.Sprintf("Flags: %d\n", tmEvent.Flags))
			fmt.Println(fmt.Sprintf("Schema: %s\n", tmEvent.Schema))
			fmt.Println(fmt.Sprintf("Table: %s\n", tmEvent.Table))
			fmt.Println(fmt.Sprintf("Column count: %d\n", tmEvent.ColumnCount))
			fmt.Println(fmt.Sprintf("Column type: \n%s", hex.Dump(tmEvent.ColumnType)))
			fmt.Println(fmt.Sprintf("NULL bitmap: \n%s", hex.Dump(tmEvent.NullBitmap)))
			fmt.Println(fmt.Sprintf("Signedness bitmap: \n%s", hex.Dump(tmEvent.SignednessBitmap)))
			fmt.Println(fmt.Sprintf("Default charset: %v\n", tmEvent.DefaultCharset))
			fmt.Println(fmt.Sprintf("Column charset: %v\n", tmEvent.ColumnCharset))
			fmt.Println(fmt.Sprintf("Set str value: %v\n", tmEvent.SetStrValueString()))
			fmt.Println(fmt.Sprintf("Enum str value: %v\n", tmEvent.EnumStrValueString()))
			fmt.Println(fmt.Sprintf("Column name: %v\n", tmEvent.ColumnNameString()))
			fmt.Println(fmt.Sprintf("Geometry type: %v\n", tmEvent.GeometryType))
			fmt.Println(fmt.Sprintf("Primary key: %v\n", tmEvent.PrimaryKey))
			fmt.Println(fmt.Sprintf("Primary key prefix: %v\n", tmEvent.PrimaryKeyPrefix))
			fmt.Println(fmt.Sprintf("Enum/set default charset: %v\n", tmEvent.EnumSetDefaultCharset))
			fmt.Println(fmt.Sprintf("Enum/set column charset: %v\n", tmEvent.EnumSetColumnCharset))

			unsignedMap := tmEvent.UnsignedMap()
			fmt.Println(fmt.Sprintf("UnsignedMap: %#v\n", unsignedMap))

			collationMap := tmEvent.CollationMap()
			fmt.Println(fmt.Sprintf("CollationMap: %#v\n", collationMap))

			enumSetCollationMap := tmEvent.EnumSetCollationMap()
			fmt.Println(fmt.Sprintf("EnumSetCollationMap: %#v\n", enumSetCollationMap))

			enumStrValueMap := tmEvent.EnumStrValueMap()
			fmt.Println(fmt.Sprintf("EnumStrValueMap: %#v\n", enumStrValueMap))

			setStrValueMap := tmEvent.SetStrValueMap()
			fmt.Println(fmt.Sprintf("SetStrValueMap: %#v\n", setStrValueMap))

			geometryTypeMap := tmEvent.GeometryTypeMap()
			fmt.Println(fmt.Sprintf("GeometryTypeMap: %#v\n", geometryTypeMap))

			nameMaxLen := 0
			for _, name := range tmEvent.ColumnName {
				if len(name) > nameMaxLen {
					nameMaxLen = len(name)
				}
			}
			nameFmt := "  %s"
			if nameMaxLen > 0 {
				nameFmt = fmt.Sprintf("  %%-%ds", nameMaxLen)
			}

			primaryKey := map[int]struct{}{}
			for _, pk := range tmEvent.PrimaryKey {
				primaryKey[int(pk)] = struct{}{}
			}

			fmt.Println(fmt.Sprintf("Columns: \n"))
			for i := 0; i < int(tmEvent.ColumnCount); i++ {
				if len(tmEvent.ColumnName) == 0 {
					fmt.Println(fmt.Sprintf(nameFmt, "<n/a>"))
				} else {
					fmt.Println(fmt.Sprintf(nameFmt, tmEvent.ColumnName[i]))
				}

				//fmt.Println(fmt.Sprintf("  type=%-3d", tmEvtmEvent.realType(i)))

				if tmEvent.IsNumericColumn(i) {
					if len(unsignedMap) == 0 {
						fmt.Println(fmt.Sprintf("  unsigned=<n/a>"))
					} else if unsignedMap[i] {
						fmt.Println(fmt.Sprintf("  unsigned=yes"))
					} else {
						fmt.Println(fmt.Sprintf("  unsigned=no "))
					}
				}
				if tmEvent.IsCharacterColumn(i) {
					if len(collationMap) == 0 {
						fmt.Println(fmt.Sprintf("  collation=<n/a>"))
					} else {
						fmt.Println(fmt.Sprintf("  collation=%d ", collationMap[i]))
					}
				}
				if tmEvent.IsEnumColumn(i) {
					if len(enumSetCollationMap) == 0 {
						fmt.Println(fmt.Sprintf("  enum_collation=<n/a>"))
					} else {
						fmt.Println(fmt.Sprintf("  enum_collation=%d", enumSetCollationMap[i]))
					}

					if len(enumStrValueMap) == 0 {
						fmt.Println(fmt.Sprintf("  enum=<n/a>"))
					} else {
						fmt.Println(fmt.Sprintf("  enum=%v", enumStrValueMap[i]))
					}
				}
				if tmEvent.IsSetColumn(i) {
					if len(enumSetCollationMap) == 0 {
						fmt.Println(fmt.Sprintf("  set_collation=<n/a>"))
					} else {
						fmt.Println(fmt.Sprintf("  set_collation=%d", enumSetCollationMap[i]))
					}

					if len(setStrValueMap) == 0 {
						fmt.Println(fmt.Sprintf("  set=<n/a>"))
					} else {
						fmt.Println(fmt.Sprintf("  set=%v", setStrValueMap[i]))
					}
				}
				if tmEvent.IsGeometryColumn(i) {
					if len(geometryTypeMap) == 0 {
						fmt.Println(fmt.Sprintf("  geometry_type=<n/a>"))
					} else {
						fmt.Println(fmt.Sprintf("  geometry_type=%v", geometryTypeMap[i]))
					}
				}
				available, nullable := tmEvent.Nullable(i)
				if !available {
					fmt.Println(fmt.Sprintf("  null=<n/a>"))
				} else if nullable {
					fmt.Println(fmt.Sprintf("  null=yes"))
				} else {
					fmt.Println(fmt.Sprintf("  null=no "))
				}
				if _, ok := primaryKey[i]; ok {
					fmt.Println(fmt.Sprintf("  pri"))
				}
				fmt.Println(fmt.Sprintf("\n"))
			}
		}
	}
}

/*
	连接数据库解析增量数据的连接信息及增量位点
*/
func (my MySQLIncDataBinlogPrepareStruct) BinlogStreamer() interface{} {

	// Start sync with specified binlog file and position
	//streamer, _ := syncer.StartSync(mysql.Position{"mysql-bin.000016", 4})
	//var syncer1 replication.BinlogSyncer = *syncer
	return nil
}

func (my MySQLIncDataBinlogPrepareStruct) BinlogStreamerClose(cfg interface{}) {
	syncer := cfg.(*replication.BinlogSyncer)
	syncer.Close()
}

//func (my *MySQLIncDataBinlogPrepareStruct) getEvent() *replication.BinlogEvent {
//	streamer := my.binlogStreamer()
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	ev, err := streamer.GetEvent(ctx)
//	cancel()
//	if err == context.DeadlineExceeded {
//
//	}
//	return ev
//}

/*
	输出binlog的头部信息
*/
func (my MySQLIncDataBinlogPrepareStruct) incBinlogFormatInfo(ev *replication.BinlogEvent) {
	if ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
		fmt.Println("Version: ", ev.Event.(*replication.FormatDescriptionEvent).Version)
		fmt.Println("server version: ", string(ev.Event.(*replication.FormatDescriptionEvent).ServerVersion))
		fmt.Println("Checksum algorithm: ", ev.Event.(*replication.FormatDescriptionEvent).ChecksumAlgorithm)
		fmt.Println("Header Length: ", ev.Event.(*replication.FormatDescriptionEvent).EventHeaderLength)
		fmt.Println("Create Timestamp: ", ev.Event.(*replication.FormatDescriptionEvent).CreateTimestamp)
	}
}

/*
	输出binlog日志的轮转信息
*/
func (my MySQLIncDataBinlogPrepareStruct) incBinlogRotateInfo(ev *replication.BinlogEvent) {
	if ev.Header.EventType == replication.ROTATE_EVENT {
		rotateEvent := ev.Event.(*replication.RotateEvent)
		fmt.Println("----111----: ", string(rotateEvent.NextLogName), rotateEvent.Position)
	}
}

/*
	输出binlog的gtid信息
*/
func (my MySQLIncDataBinlogPrepareStruct) binlogGtid(ev *replication.BinlogEvent) gtidInfoStruct {
	var gtid = gtidInfoStruct{}
	if ev.Header.EventType == replication.GTID_EVENT {
		gtidEvent := ev.Event.(*replication.GTIDEvent)
		gtid.CommitFlag = gtidEvent.CommitFlag
		u, _ := uuid.FromBytes(gtidEvent.SID)
		gtid.lastCommitted = gtidEvent.LastCommitted
		gtid.sequenceNumber = gtidEvent.SequenceNumber
		gtid.immediateCommitTimestamp = gtidEvent.ImmediateCommitTimestamp
		gtid.originalCommitTimestamp = gtidEvent.OriginalCommitTimestamp
		gtid.transactionLength = gtidEvent.TransactionLength
		gtid.immediateServerVersion = gtidEvent.ImmediateServerVersion
		gtid.originalServerVersion = gtidEvent.OriginalServerVersion
		gtid.gtidVal = fmt.Sprintf("%v:%d", u.String(), gtidEvent.GNO)
	}
	return gtid
}
