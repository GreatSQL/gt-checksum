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

	schema, table            string   //待校验库名、表名
	sourceSchema, destSchema string   //源端和目标端库名
	columnName               []string //待校验表的列名，有可能是多个
	tmpTableDataFileDir      string   //临时表文件生成的相对路径
	tableIndexColumnMap      map[string][]string
	sdbPool, ddbPool         *global.Pool
	datafixType              string
	datafixSql               string
	sdrive, ddrive           string
	sfile                    *os.File
	checkObject              string
	checkNoIndexTable        string //是否检查无索引表
	tableAllCol              map[string]global.TableAllColumnInfoS
	caseSensitiveObjectName  string //是否区分对象名大小写

	file                      *os.File
	TmpFileName               string
	fixTrxNum                 int
	chanrowCount, concurrency int //单次并发一次校验的行数
	TmpTablePath              string
	smtype                    string //是源端还是目标端
	indexColumnType           string
	pods                      *Pod
	tableMaxRows              uint64
	djdbc                     string
	tableMappings             map[string]string // 表映射关系
	bar                       *Bar              // 进度条
	forceFullTableCheck       bool              // 是否强制进行全表检查
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
	SourceData      string                     //源端查询到的数据，避免重复查询
	DestData        string                     //目标端查询到的数据，避免重复查询
}

/*
查询索引列信息，并发执行调度生成
*/
func (sp *SchedulePlan) Schedulingtasks() {
	rand.Seed(time.Now().UnixNano())

	for k, v := range sp.tableIndexColumnMap {
		//是否校验无索引表
		if sp.checkNoIndexTable == "no" && len(v) == 0 {
			continue
		}
		// 为每个表创建独立的SchedulePlan副本，避免并发冲突
		spCopy := *sp
		spCopy.file, _ = os.OpenFile(sp.TmpFileName, os.O_CREATE|os.O_RDWR, 0777)
		// 解析key中的源表和目标表信息
		// key格式: sourceSchema/*gtchecksumSchemaTable*/sourceTable/*indexColumnType*/indexType/*mapping*/destSchema/*mappingTable*/destTable
		vlog := fmt.Sprintf("Processing table key: %s", k)
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

		// 设置SchedulePlan副本的属性
		spCopy.schema = sourceSchema
		spCopy.table = sourceTable
		spCopy.sourceSchema = sourceSchema
		spCopy.destSchema = destSchema
		spCopy.indexColumnType = indexType

		vlog = fmt.Sprintf("Key parsed - Source: %s.%s, Target: %s.%s, Index: %s",
			sourceSchema, sourceTable, destSchema, destTable, indexType)
		global.Wlog.Debug(vlog)

		if len(v) == 0 { //校验无索引表
			spCopy.chanrowCount = spCopy.chunkSize
			logThreadSeq := rand.Int63()

			// 开始新表的进度显示
			tableName := fmt.Sprintf("begin checksum no-index table %s.%s", spCopy.schema, spCopy.table)
			fmt.Printf("\n%s\n", tableName)

			// 为每个表创建独立的进度条，但不在初始化时设置总数
			// 总数将在SingleTableCheckProcessing中根据实际行数设置
			spCopy.bar = &Bar{}

			spCopy.SingleTableCheckProcessing(spCopy.chanrowCount, logThreadSeq)

			// 显示完成消息
			fmt.Printf("table %s.%s checksum completed\n", spCopy.schema, spCopy.table)
		} else { //校验有索引的表
			spCopy.chanrowCount = spCopy.chunkSize
			spCopy.columnName = v
			// 显示开始信息
			displayTableName := spCopy.getDisplayTableName()
			fmt.Printf("\nbegin checksum index table %s\n", displayTableName)

			// 为每个表创建独立的进度条，以100为总数显示百分比进度
			spCopy.bar = &Bar{}
			spCopy.bar.NewOption(0, 100, "task") // 以100为总数，显示百分比进度

			spCopy.doIndexDataCheck()

			// 显示完成消息
			fmt.Printf("table %s checksum completed\n", displayTableName)
		}
		spCopy.file.Close()
		os.Remove(spCopy.TmpFileName)
	}

}

// NewTableProgress is deprecated and removed as each table now creates its own progress bar directly in Schedulingtasks

func CheckTableQuerySchedule(sdb, ddb *global.Pool, tableIndexColumnMap map[string][]string, tableAllCol map[string]global.TableAllColumnInfoS, m inputArg.ConfigParameter) *SchedulePlan {
	// 清空之前的结果数据
	measuredDataPods = []Pod{}

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
		concurrency:             m.SecondaryL.RulesV.ParallelThds,
		sdbPool:                 sdb,
		ddbPool:                 ddb,
		chunkSize:               m.SecondaryL.RulesV.ChanRowCount,
		tableIndexColumnMap:     tableIndexColumnMap,
		tableAllCol:             tableAllCol,
		datafixType:             m.SecondaryL.RepairV.Datafix,
		datafixSql:              m.SecondaryL.RepairV.FixFileName,
		sdrive:                  m.SecondaryL.DsnsV.SrcDrive,
		ddrive:                  m.SecondaryL.DsnsV.DestDrive,
		mqQueueDepth:            m.SecondaryL.RulesV.QueueSize,
		checkNoIndexTable:       m.SecondaryL.SchemaV.CheckNoIndexTable,
		sfile:                   m.SecondaryL.RepairV.FixFileFINE,
		checkObject:             m.SecondaryL.RulesV.CheckObject,
		TmpFileName:             m.NoIndexTableTmpFile,
		caseSensitiveObjectName: m.SecondaryL.SchemaV.CaseSensitiveObjectName,
		fixTrxNum:               m.SecondaryL.RepairV.FixTrxNum,
		djdbc:                   m.SecondaryL.DsnsV.DestJdbc,
		tableMappings:           tableMappings,
	}
}
