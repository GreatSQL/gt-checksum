package inputArg

import (
	"fmt"
	"gopkg.in/ini.v1"
	"strings"
)

//一级、二级参数标签合法性校验
func (rc *ConfigParameter) LevelParameterCheck() {
	var (
		err error
	)
	if rc.FirstL.DSNs, err = rc.ConfFine.GetSection("DSNs"); rc.FirstL.DSNs == nil && err != nil {
		rc.getErr("Failed to get DSNs parameters", err)
	}
	if rc.FirstL.Schema, err = rc.ConfFine.GetSection("Schema"); rc.FirstL.Schema == nil && err != nil {
		rc.getErr("Failed to get Schema parameters", err)
	}
	//Source Destination connection 获取jdbc连接信息
	for _, i := range []string{"srcDSN", "dstDSN"} {
		if _, err = rc.FirstL.DSNs.GetKey(i); err != nil {
			rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
		}
	}
	//Schema 获取校验库表信息
	for _, i := range []string{"tables"} {
		if _, err = rc.FirstL.Schema.GetKey(i); err != nil {
			rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
		}
	}
	if rc.ParametersSwitch {
		if rc.FirstL.Logs, err = rc.ConfFine.GetSection("Logs"); rc.FirstL.Logs == nil && err != nil {
			rc.getErr("Failed to get Logs parameters", err)
		}
		if rc.FirstL.Rules, err = rc.ConfFine.GetSection("Rules"); rc.FirstL.Rules == nil && err != nil {
			rc.getErr("Failed to get Rules parameters", err)
		}
		if rc.FirstL.Repair, err = rc.ConfFine.GetSection("Repair"); rc.FirstL.Repair == nil && err != nil {
			rc.getErr("Failed to get Repair parameters", err)
		}
		if rc.FirstL.Struct, err = rc.ConfFine.GetSection("Struct"); rc.FirstL.Repair == nil && err != nil {
			rc.getErr("Failed to get Struct parameters", err)
		}
		//Schema 获取校验库表信息
		for _, i := range []string{"checkNoIndexTable", "lowerCaseTableNames"} {
			if _, err = rc.FirstL.Schema.GetKey(i); err != nil {
				rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
			}
		}
		//Logs 二级参数信息
		for _, i := range []string{"log", "logLevel"} {
			if _, err = rc.FirstL.Logs.GetKey(i); err != nil {
				rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
			}
		}
		//Rules 二级参数检测
		for _, i := range []string{"parallel-thds", "queue-size", "checkMode", "checkObject", "ratio", "chanRowCount"} {
			if _, err = rc.FirstL.Rules.GetKey(i); err != nil {
				rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
			}
		}
		//Struct 二级参数检测
		for _, i := range []string{"ScheckMod", "ScheckOrder", "ScheckFixRule"} {
			if _, err = rc.FirstL.Struct.GetKey(i); err != nil {
				rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
			}
		}
		//Repair 二级参数校验
		for _, i := range []string{"datafix", "fixTrxNum", "fixFileName"} {
			if _, err = rc.FirstL.Repair.GetKey(i); err != nil {
				rc.getErr(fmt.Sprintf("Failed to get %s parameters", i), err)
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
	rc.SecondaryL.SchemaV.IgnoreTables = strings.TrimSpace(rc.FirstL.Schema.Key("ignore-tables").String())
	if rc.SecondaryL.SchemaV.IgnoreTables == "" {
		rc.SecondaryL.SchemaV.IgnoreTables = "nil"
	}
	if rc.ParametersSwitch {
		rc.SecondaryL.SchemaV.LowerCaseTableNames = rc.FirstL.Schema.Key("lowerCaseTableNames").In("no", []string{"yes", "no"})
		rc.SecondaryL.SchemaV.CheckNoIndexTable = rc.FirstL.Schema.Key("checkNoIndexTable").In("no", []string{"yes", "no"})
		//Struct
		rc.SecondaryL.StructV.ScheckMod = rc.FirstL.Struct.Key("ScheckMod").In("strict", []string{"loose", "strict"})
		rc.SecondaryL.StructV.ScheckOrder = rc.FirstL.Struct.Key("ScheckOrder").In("no", []string{"yes", "no"})
		rc.SecondaryL.StructV.ScheckFixRule = rc.FirstL.Struct.Key("ScheckFixRule").In("src", []string{"src", "dst"})

		//Logs 获取相关参数
		rc.SecondaryL.LogV.LogFile = rc.FirstL.Logs.Key("log").String()
		if rc.SecondaryL.LogV.LogFile == "" {
			rc.getErr("Failed to convert log parameter to int", err)
		}
		rc.SecondaryL.LogV.LogLevel = rc.FirstL.Logs.Key("logLevel").In("info", []string{"debug", "info", "warn", "error"})

		if rc.SecondaryL.RulesV.ParallelThds, err = rc.FirstL.Rules.Key("parallel-thds").Int(); err != nil {
			rc.getErr("Failed to convert parallel-thds parameter to int", err)
		}
		if rc.SecondaryL.RulesV.ChanRowCount, err = rc.FirstL.Rules.Key("chanRowCount").Int(); err != nil {
			rc.getErr("Failed to convert chanRowCount parameter to int", err)
		}
		if rc.SecondaryL.RulesV.QueueSize, err = rc.FirstL.Rules.Key("queue-size").Int(); err != nil {
			rc.getErr("Failed to convert queue-size parameter to int", err)
		}
		if rc.SecondaryL.RulesV.Ratio, err = rc.FirstL.Rules.Key("ratio").Int(); err != nil {
			rc.getErr("Failed to convert Ratio parameter to int", err)
		}
		rc.SecondaryL.RulesV.CheckMode = rc.FirstL.Rules.Key("checkMode").In("rows", []string{"count", "rows", "sample"})
		rc.SecondaryL.RulesV.CheckObject = rc.FirstL.Rules.Key("checkObject").In("data", []string{"data", "struct", "index", "partitions", "foreign", "trigger", "func", "proc"})

		if rc.SecondaryL.RepairV.FixTrxNum, err = rc.FirstL.Repair.Key("fixTrxNum").Int(); err != nil {
			rc.getErr("Failed to convert fixTrxNum parameter to int", err)
		}
		rc.SecondaryL.RepairV.Datafix = rc.FirstL.Repair.Key("datafix").In("file", []string{"file", "table"})
		if rc.SecondaryL.RepairV.Datafix == "file" {
			if _, err = rc.FirstL.Repair.GetKey("fixFileName"); err != nil {
				rc.getErr("Failed to get fixFileName parameters", err)
			}
			rc.SecondaryL.RepairV.FixFileName = rc.FirstL.Repair.Key("fixFileName").String()
		}
	} else {
		rc.SecondaryL.RulesV.ChanRowCount = 10000
		rc.SecondaryL.RulesV.ParallelThds = 10
		rc.SecondaryL.RulesV.QueueSize = 100
		rc.SecondaryL.RulesV.Ratio = 10
		rc.SecondaryL.LogV.LogFile = "./gt-checksum.log"
		rc.SecondaryL.LogV.LogLevel = "info"
		rc.SecondaryL.SchemaV.LowerCaseTableNames = "no"
		rc.SecondaryL.SchemaV.CheckNoIndexTable = "no"
		rc.SecondaryL.RulesV.CheckMode = "rows"
		rc.SecondaryL.RulesV.CheckObject = "data"
		rc.SecondaryL.RepairV.Datafix = "file"
		rc.SecondaryL.RepairV.FixFileName = "./gt-checksum-DataFix.sql"
		rc.SecondaryL.RepairV.FixTrxNum = 100
	}
}

/*
该函数用于读取配置文件中的配置参数
*/
func (rc *ConfigParameter) getConfig() {
	var (
		err error
	)
	//读取配置文件信息
	if strings.HasSuffix(rc.Config, "gc.conf") {
		rc.ParametersSwitch = true
	}
	if strings.HasSuffix(rc.Config, "gc.conf-simple") {
		rc.ParametersSwitch = false
	}
	//处理配置文件中的特殊字符
	rc.ConfFine, err = ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, rc.Config)
	if err != nil {
		rc.getErr("configuration file error.", err)
	}
	rc.LevelParameterCheck()
	rc.secondaryLevelParameterCheck()
}
