package actions

import (
	"fmt"
	"gt-checksum/global"
	"gt-checksum/inputArg"
	"math/rand"
	"os"
	"strings"
	"time"
)

type SchedulePlan struct {
	chunkSize, mqQueueDepth int

	schema, table             string   //待校验库名、表名
	sourceSchema, destSchema  string   //源端和目标端库名
	columnName                []string //待校验表的列名，有可能是多个
	tmpTableDataFileDir       string   //临时表文件生成的相对路径
	tableIndexColumnMap       map[string][]string
	sdbPool, ddbPool          *global.Pool
	datafixType               string
	datafixSql                string
	sdrive, ddrive            string
	sfile                     *os.File
	checkMod, checkObject     string
	checkNoIndexTable         string //是否检查无索引表
	tableAllCol               map[string]global.TableAllColumnInfoS
	ratio                     int
	file                      *os.File
	TmpFileName               string
	bar                       *Bar
	fixTrxNum                 int
	chanrowCount, concurrency int //单次并发一次校验的行数
	TmpTablePath              string
	smtype                    string //是源端还是目标端
	indexColumnType           string
	pods                      *Pod
	tableMaxRows              uint64
	sampDataGroupNumber       int64
	djdbc                     string
	tableMappings             map[string]string // 表映射关系
}

// getDisplayTableName 返回表的显示名称，包含映射关系信息
// 如果存在映射关系，返回格式为 "sourceSchema.table:destSchema.table"
// 如果不存在映射关系，返回格式为 "schema.table"
func (sp *SchedulePlan) getDisplayTableName() string {
	// 检查是否存在映射关系
	if mappedSchema, exists := sp.tableMappings[sp.schema]; exists && mappedSchema != sp.schema {
		// 存在映射关系，返回包含映射信息的名称
		return fmt.Sprintf("%s.%s:%s.%s", sp.schema, sp.table, mappedSchema, sp.table)
	}

	// 不存在映射关系，返回普通名称
	return fmt.Sprintf("%s.%s", sp.schema, sp.table)
}

/*
差异数据信息结构体
*/
type DifferencesDataStruct struct {
	Schema string //存在差异数据的库
	Table  string //存在差异数据的表
	Spoint string //校验开始时的源端全局一致性点
	Dpoint string //校验开始时的目端全局一致性点
	//TableColumnInfo []map[string]string //该表的所有列信息，包括列类型
	TableColumnInfo global.TableAllColumnInfoS //该表的所有列信息，包括列类型
	SqlWhere        map[string]string          //差异数据查询的where 条件
	indexColumnType string                     //索引列类型
}

/*
查询索引列信息，并发执行调度生成
*/
func (sp *SchedulePlan) Schedulingtasks() {
	sp.bar = &Bar{}
	rand.Seed(time.Now().UnixNano())
	for k, v := range sp.tableIndexColumnMap {
		//是否校验无索引表
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		sp.file, _ = os.OpenFile(sp.TmpFileName, os.O_CREATE|os.O_RDWR, 0777)
		// 解析key中的源表和目标表信息
		// key格式: sourceSchema/*gtchecksumSchemaTable*/sourceTable/*indexColumnType*/indexType/*mapping*/destSchema/*mappingTable*/destTable
		vlog := fmt.Sprintf("Processing key: %s", k)
		global.Wlog.Debug(vlog)

		var sourceSchema, sourceTable, destSchema, destTable, indexType string

		// 解析源表schema和表名
		if strings.Contains(k, "/*gtchecksumSchemaTable*/") {
			parts := strings.Split(k, "/*gtchecksumSchemaTable*/")
			if len(parts) >= 2 {
				sourceSchema = parts[0]
				remainingPart := parts[1]

				// 解析源表表名
				if strings.Contains(remainingPart, "/*indexColumnType*/") {
					tableParts := strings.Split(remainingPart, "/*indexColumnType*/")
					if len(tableParts) >= 2 {
						sourceTable = tableParts[0]
						remainingPart = tableParts[1]

						// 解析索引类型
						if strings.Contains(remainingPart, "/*mapping*/") {
							indexParts := strings.Split(remainingPart, "/*mapping*/")
							if len(indexParts) >= 2 {
								indexType = indexParts[0]
								remainingPart = indexParts[1]

								// 解析目标表schema和表名
								if strings.Contains(remainingPart, "/*mappingTable*/") {
									destParts := strings.Split(remainingPart, "/*mappingTable*/")
									if len(destParts) >= 2 {
										destSchema = destParts[0]
										destTable = destParts[1]
									}
								}
							}
						}
					}
				} else if strings.Contains(remainingPart, "/*mapping*/") {
					// 处理无索引表的情况
					tableParts := strings.Split(remainingPart, "/*mapping*/")
					if len(tableParts) >= 2 {
						sourceTable = tableParts[0]
						remainingPart = tableParts[1]

						// 解析目标表schema和表名
						if strings.Contains(remainingPart, "/*mappingTable*/") {
							destParts := strings.Split(remainingPart, "/*mappingTable*/")
							if len(destParts) >= 2 {
								destSchema = destParts[0]
								destTable = destParts[1]
							}
						}
					}
				}
			}
		}

		// 设置SchedulePlan的属性
		sp.schema = sourceSchema
		sp.table = sourceTable
		sp.sourceSchema = sourceSchema
		sp.destSchema = destSchema
		sp.indexColumnType = indexType

		vlog = fmt.Sprintf("Parsed key: sourceSchema=%s, sourceTable=%s, destSchema=%s, destTable=%s, indexType=%s",
			sourceSchema, sourceTable, destSchema, destTable, indexType)
		global.Wlog.Debug(vlog)
		if len(v) == 0 { //校验无索引表
			sp.chanrowCount = sp.chunkSize
			logThreadSeq := rand.Int63()
			sp.SingleTableCheckProcessing(sp.chanrowCount, logThreadSeq)
		} else { //校验有索引的表
			sp.chanrowCount = sp.chunkSize
			sp.columnName = v
			// 开始新表的进度显示
			displayTableName := sp.getDisplayTableName()
			tableName := fmt.Sprintf("begin checkSum index table %s", displayTableName)
			sp.bar.NewTableProgress(tableName)
			sp.doIndexDataCheck() // 确保SchedulePlan结构体已定义此方法
			fmt.Println()

			// 显示映射关系信息
			if sp.sourceSchema != sp.destSchema || sp.table != sp.table {
				fmt.Println(fmt.Sprintf("table %s checksum complete (Schema: %s:%s, Table: %s:%s)",
					displayTableName, sp.sourceSchema, sp.destSchema, sp.table, sp.table))
			} else {
				fmt.Println(fmt.Sprintf("table %s checksum complete", displayTableName))
			}
		}
		sp.file.Close()
		os.Remove(sp.TmpFileName)
	}
}

func CheckTableQuerySchedule(sdb, ddb *global.Pool, tableIndexColumnMap map[string][]string, tableAllCol map[string]global.TableAllColumnInfoS, m inputArg.ConfigParameter) *SchedulePlan {
	// 解析表映射关系
	tableMappings := make(map[string]string)

	// 处理表映射关系
	if m.SecondaryL.SchemaV.Tables != "" {
		for _, pattern := range strings.Split(m.SecondaryL.SchemaV.Tables, ",") {
			if strings.Contains(pattern, ":") {
				mapping := strings.SplitN(pattern, ":", 2)
				if len(mapping) == 2 {
					srcPattern := mapping[0]
					dstPattern := mapping[1]

					// 处理 db1.*:db2.* 格式
					if strings.HasSuffix(srcPattern, ".*") && strings.HasSuffix(dstPattern, ".*") {
						srcDB := strings.TrimSuffix(srcPattern, ".*")
						dstDB := strings.TrimSuffix(dstPattern, ".*")
						tableMappings[srcDB] = dstDB
					} else if strings.Contains(srcPattern, ".") && strings.Contains(dstPattern, ".") {
						// 处理 db1.t1:db2.t2 格式
						srcParts := strings.Split(srcPattern, ".")
						dstParts := strings.Split(dstPattern, ".")

						if len(srcParts) > 0 && len(dstParts) > 0 {
							srcDB := srcParts[0]
							dstDB := dstParts[0]
							tableMappings[srcDB] = dstDB
						}
					}
				}
			}
		}
	}

	return &SchedulePlan{
		concurrency:         m.SecondaryL.RulesV.ParallelThds,
		sdbPool:             sdb,
		ddbPool:             ddb,
		chunkSize:           m.SecondaryL.RulesV.ChanRowCount,
		tableIndexColumnMap: tableIndexColumnMap,
		tableAllCol:         tableAllCol,
		datafixType:         m.SecondaryL.RepairV.Datafix,
		datafixSql:          m.SecondaryL.RepairV.FixFileName,
		sdrive:              m.SecondaryL.DsnsV.SrcDrive,
		ddrive:              m.SecondaryL.DsnsV.DestDrive,
		mqQueueDepth:        m.SecondaryL.RulesV.QueueSize,
		checkNoIndexTable:   m.SecondaryL.SchemaV.CheckNoIndexTable,
		checkMod:            m.SecondaryL.RulesV.CheckMode,
		ratio:               m.SecondaryL.RulesV.Ratio,
		sfile:               m.SecondaryL.RepairV.FixFileFINE,
		checkObject:         m.SecondaryL.RulesV.CheckObject,
		TmpFileName:         m.NoIndexTableTmpFile,
		fixTrxNum:           m.SecondaryL.RepairV.FixTrxNum,
		djdbc:               m.SecondaryL.DsnsV.DestJdbc,
		tableMappings:       tableMappings,
	}
}
