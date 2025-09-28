package inputArg

import (
	"fmt"
	"strings"

	"gopkg.in/ini.v1"
)

// 一级、二级参数标签合法性校验
func (rc *ConfigParameter) LevelParameterCheck() {
	var (
		err error
	)
	if rc.FirstL.DSNs, err = rc.ConfFine.GetSection("DSNs"); rc.FirstL.DSNs == nil && err != nil {
		rc.getErr("Failed to set [DSNs] options", err)
	}
	if rc.FirstL.Schema, err = rc.ConfFine.GetSection("Schema"); rc.FirstL.Schema == nil && err != nil {
		rc.getErr("Failed to set [Schema] options", err)
	}
	//Source Destination connection 获取jdbc连接信息
	for _, i := range []string{"srcDSN", "dstDSN"} {
		if _, err = rc.FirstL.DSNs.GetKey(i); err != nil {
			rc.getErr(fmt.Sprintf("Failed to set option %s", i), err)
		}
	}
	//Schema 获取校验库表信息
	for _, i := range []string{"tables"} {
		if _, err = rc.FirstL.Schema.GetKey(i); err != nil {
			rc.getErr(fmt.Sprintf("Failed to set option %s", i), err)
		}
	}

	if rc.FirstL.Logs, err = rc.ConfFine.GetSection("Logs"); rc.FirstL.Logs == nil && err != nil {
		fmt.Println("Using default values for [Logs] options")
	}
	if rc.FirstL.Rules, err = rc.ConfFine.GetSection("Rules"); rc.FirstL.Rules == nil && err != nil {
		fmt.Println("Using default values for [Rules] options")
	}
	if rc.FirstL.Repair, err = rc.ConfFine.GetSection("Repair"); rc.FirstL.Repair == nil && err != nil {
		fmt.Println("Using default values for [Repair] options")
	}
	// 已删除对[Struct]区间参数的读取
	//Schema 获取校验库表信息
	for _, i := range []string{"checkNoIndexTable", "caseSensitiveObjectName"} {
		if _, err = rc.FirstL.Schema.GetKey(i); err != nil {
			fmt.Println(fmt.Sprintf("Using default value for option %s", i))
		}
	}
	//Logs 二级参数信息
	if rc.FirstL.Logs != nil {
		for _, i := range []string{"logFile", "logLevel"} {
			if _, err = rc.FirstL.Logs.GetKey(i); err != nil {
				fmt.Printf("Failed to set option %s, using default value\n", i)
			}
		}
	}
	//Rules 二级参数检测
	if rc.FirstL.Rules != nil {
		for _, i := range []string{"parallelThds", "queueSize", "checkObject", "chunkSize", "memoryLimit"} {
			if _, err = rc.FirstL.Rules.GetKey(i); err != nil {
				fmt.Printf("Failed to set option %s, using default value\n", i)
			}
		}
	}
	// 已删除Struct二级参数检测
	//Repair 二级参数校验
	if rc.FirstL.Repair != nil {
		for _, i := range []string{"datafix", "fixTrxNum", "fixFileName"} {
			if _, err = rc.FirstL.Repair.GetKey(i); err != nil {
				fmt.Printf("Failed to set option %s, using default value\n", i)
			}
		}
	}
}

/*
二级参数值获取校验
*/
func (rc *ConfigParameter) secondaryLevelParameterCheck() {
	var (
		err error
	)
	//Source Destination connection 获取jdbc连接信息
	rc.SecondaryL.DsnsV.SrcDSN = rc.FirstL.DSNs.Key("srcDSN").String() // 将结果转为string
	if strings.Contains(rc.SecondaryL.DsnsV.SrcDSN, "|") {
		rc.SecondaryL.DsnsV.SrcDrive = strings.Split(rc.SecondaryL.DsnsV.SrcDSN, "|")[0]
		rc.SecondaryL.DsnsV.SrcJdbc = strings.Split(rc.SecondaryL.DsnsV.SrcDSN, "|")[1]
	} else {
		rc.SecondaryL.DsnsV.SrcJdbc = rc.SecondaryL.DsnsV.SrcDSN
	}
	rc.SecondaryL.DsnsV.DstDSN = rc.FirstL.DSNs.Key("dstDSN").String()
	if strings.Contains(rc.SecondaryL.DsnsV.DstDSN, "|") {
		rc.SecondaryL.DsnsV.DestDrive = strings.Split(rc.SecondaryL.DsnsV.DstDSN, "|")[0]
		rc.SecondaryL.DsnsV.DestJdbc = strings.Split(rc.SecondaryL.DsnsV.DstDSN, "|")[1]
	} else {
		rc.SecondaryL.DsnsV.DestJdbc = rc.SecondaryL.DsnsV.DstDSN
	}

	//校验库表设置
	rc.SecondaryL.SchemaV.Tables = strings.TrimSpace(rc.FirstL.Schema.Key("tables").String())
	rc.SecondaryL.SchemaV.IgnoreTables = strings.TrimSpace(rc.FirstL.Schema.Key("ignoreTables").String())
	if rc.SecondaryL.SchemaV.IgnoreTables == "" {
		rc.SecondaryL.SchemaV.IgnoreTables = "nil"
	}

	rc.SecondaryL.SchemaV.CaseSensitiveObjectName = rc.FirstL.Schema.Key("caseSensitiveObjectName").In("no", []string{"yes", "no"})
	rc.SecondaryL.SchemaV.CheckNoIndexTable = rc.FirstL.Schema.Key("checkNoIndexTable").In("no", []string{"yes", "no"})

	//Logs 获取相关参数
	if rc.FirstL.Logs != nil {
		rc.SecondaryL.LogV.LogFile = rc.FirstL.Logs.Key("logFile").String()
		if rc.SecondaryL.LogV.LogFile == "" {
			rc.SecondaryL.LogV.LogFile = "./gt-checksum.log"
			fmt.Println("Using default value './gt-checksum.log' for option LogFile")
		}
	} else {
		rc.SecondaryL.LogV.LogFile = "./gt-checksum.log"
		fmt.Println("Using default value './gt-checksum.log' for option LogFile")
	}
	if rc.FirstL.Logs != nil {
		rc.SecondaryL.LogV.LogLevel = rc.FirstL.Logs.Key("logLevel").In("info", []string{"debug", "info", "warn", "error"})
	} else {
		rc.SecondaryL.LogV.LogLevel = "info"
	}

	if rc.FirstL.Rules != nil {
		if rc.SecondaryL.RulesV.ParallelThds, err = rc.FirstL.Rules.Key("parallelThds").Int(); err != nil {
			fmt.Println("Using default value '10' for option parallelThds")
			rc.SecondaryL.RulesV.ParallelThds = 10
		}
	} else {
		fmt.Println("Using default value '10' for option parallelThds")
		rc.SecondaryL.RulesV.ParallelThds = 10
	}
	if rc.FirstL.Rules != nil {
		if rc.SecondaryL.RulesV.ChanRowCount, err = rc.FirstL.Rules.Key("chunkSize").Int(); err != nil {
			fmt.Println("Using default value '1000' for option chunkSize")
			rc.SecondaryL.RulesV.ChanRowCount = 10000
		}
	} else {
		fmt.Println("Using default value '1000' for option chunkSize")
		rc.SecondaryL.RulesV.ChanRowCount = 10000
	}
	if rc.FirstL.Rules != nil {
		if rc.SecondaryL.RulesV.QueueSize, err = rc.FirstL.Rules.Key("queueSize").Int(); err != nil {
			fmt.Println("Using default value '100' for option queueSize")
			rc.SecondaryL.RulesV.QueueSize = 1000
		}
	} else {
		fmt.Println("Using default value '100' for option queueSize")
		rc.SecondaryL.RulesV.QueueSize = 1000
	}

	if rc.FirstL.Rules != nil {
		// 获取用户设置的原始值
		userSetCheckObject := rc.FirstL.Rules.Key("checkObject").String()
		// 获取验证后的值（如果无效则使用默认值"data"）
		// 注意：index, partitions, foreign 已被合并到 struct 中
		rc.SecondaryL.RulesV.CheckObject = rc.FirstL.Rules.Key("checkObject").In("data", []string{"data", "struct", "trigger", "func", "proc"})

		// 如果用户设置了值但与验证后的值不同，说明使用了默认值
		if userSetCheckObject != "" && userSetCheckObject != rc.SecondaryL.RulesV.CheckObject {
			fmt.Printf("Warning: Invalid checkObject value '%s', using default value 'data' instead\n", userSetCheckObject)
		}

		// 删除对checkMode和ratio参数的解析，始终使用rows模式（全量校验）
	} else {
		fmt.Println("Using default value 'data' for option checkObject")
		rc.SecondaryL.RulesV.CheckObject = "data"
	}
	if rc.FirstL.Rules != nil {
		if rc.SecondaryL.RulesV.MemoryLimit, err = rc.FirstL.Rules.Key("memoryLimit").Int(); err != nil {
			fmt.Println("Using default value '1024' for option memoryLimit")
			rc.SecondaryL.RulesV.MemoryLimit = 1024
		}
	} else {
		fmt.Println("Using default value '1024' for option memoryLimit")
		rc.SecondaryL.RulesV.MemoryLimit = 1024
	}

	if rc.FirstL.Repair != nil {
		if rc.SecondaryL.RepairV.FixTrxNum, err = rc.FirstL.Repair.Key("fixTrxNum").Int(); err != nil {
			fmt.Println("Using default value '100' for option fixTrxNum")
			rc.SecondaryL.RepairV.FixTrxNum = 100
		}
	} else {
		fmt.Println("Using default value '100' for option fixTrxNum")
		rc.SecondaryL.RepairV.FixTrxNum = 100
	}
	if rc.FirstL.Repair != nil {
		rc.SecondaryL.RepairV.Datafix = rc.FirstL.Repair.Key("datafix").In("file", []string{"file", "table"})
	} else {
		rc.SecondaryL.RepairV.Datafix = "file"
	}
	if rc.SecondaryL.RepairV.Datafix == "file" {
		if rc.FirstL.Repair != nil {
			if _, err = rc.FirstL.Repair.GetKey("fixFileName"); err != nil {
				fmt.Println("Using default value './gt-checksum-datafix.sql' for option fixFileName")
				rc.SecondaryL.RepairV.FixFileName = "./gt-checksum-datafix.sql"
			} else {
				rc.SecondaryL.RepairV.FixFileName = rc.FirstL.Repair.Key("fixFileName").String()
			}
		} else {
			fmt.Println("Using default value './gt-checksum-datafix.sql' for option fixFileName")
			rc.SecondaryL.RepairV.FixFileName = "./gt-checksum-datafix.sql"
		}
	}
}

/*
该函数用于读取配置文件中的配置参数
*/
func (rc *ConfigParameter) GetConfig() {
	var (
		err error
	)
	//读取配置文件信息
	//处理配置文件中的特殊字符
	rc.ConfFine, err = ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, rc.Config)
	if err != nil {
		rc.getErr("configuration file error.", err)
	}
	rc.LevelParameterCheck()
	rc.secondaryLevelParameterCheck()
}
