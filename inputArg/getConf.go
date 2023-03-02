package inputArg

import (
	"gopkg.in/ini.v1"
	"strings"
)

type readConf struct{}

/*
该函数用于读取配置文件中的配置参数
*/
func (rc *readConf) getConfig(configName string, q *ConfigParameter) {
	var (
		sdc, do, ls, cr, sr *ini.Section
		err1                error
		parametersSwitch    bool
	)
	//读取配置文件信息
	//cfg, err := ini.Load(configName)
	if strings.HasSuffix(configName, "gc.conf") {
		parametersSwitch = true
	}
	if strings.HasSuffix(configName, "gc.conf-simple") {
		parametersSwitch = false
	}
	//处理配置文件中的特殊字符
	cfg, err := ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, configName)
	if err != nil {
		rc.getErr("configuration file error.", err)
	}
	//判断一级标题是否正确
	if sdc, err1 = cfg.GetSection("DSNs"); sdc == nil && err1 != nil {
		rc.getErr("Failed to get DSNs parameters", err1)
	}
	if do, err1 = cfg.GetSection("Schema"); do == nil && err1 != nil {
		rc.getErr("Failed to get Schema parameters", err1)
	}
	if parametersSwitch {
		if ls, err1 = cfg.GetSection("Logs"); ls == nil && err1 != nil {
			rc.getErr("Failed to get Logs parameters", err1)
		}
		if cr, err1 = cfg.GetSection("Rules"); cr == nil && err1 != nil {
			rc.getErr("Failed to get Rules parameters", err1)
		}
		//if idc, err1 = cfg.GetSection("increment Data Check"); idc == nil && err1 != nil {
		//	rc.getErr("Failed to get increment Data Check parameters", err1)
		//}
		if sr, err1 = cfg.GetSection("Repair"); sr == nil && err1 != nil {
			rc.getErr("Failed to get Repair parameters", err1)
		}
	}
	//二级参数正确性验证
	//Source Destination connection 获取jdbc连接信息
	if _, err2 := sdc.GetKey("srcDSN"); err2 != nil {
		rc.getErr("Failed to get srcDSN parameters", err2)
	}
	if _, err2 := sdc.GetKey("dstDSN"); err2 != nil {
		rc.getErr("Failed to get dstDSN parameters", err2)
	}

	//Database Conn Pool Setting 获取一致性快照连接池大小
	//Schema 获取校验库表信息
	if _, err2 := do.GetKey("tables"); err2 != nil {
		rc.getErr("Failed to get tables parameters", err2)
	}
	//if _, err2 := do.GetKey("ignore-tables"); err2 != nil {
	//	rc.getErr("Failed to get ignore-tables parameters", err2)
	//}
	if parametersSwitch {
		//Logs 二级参数信息
		if _, err2 := ls.GetKey("log"); err2 != nil {
			rc.getErr("Failed to get log parameters", err2)
		}
		if _, err2 := ls.GetKey("logLevel"); err2 != nil {
			rc.getErr("Failed to get logLevel parameters", err2)
		}
		//Rules 二级参数检测
		if _, err2 := cr.GetKey("parallel-thds"); err2 != nil {
			rc.getErr("Failed to get parallel-thds parameters", err2)
		}
		if _, err2 := cr.GetKey("singleIndexChanRowCount"); err2 != nil {
			rc.getErr("Failed to get singleIndexChanRowCount parameters", err2)
		}
		if _, err2 := cr.GetKey("jointIndexChanRowCount"); err2 != nil {
			rc.getErr("Failed to get jointIndexChanRowCount parameters", err2)
		}
		if _, err2 := cr.GetKey("queue-size"); err2 != nil {
			rc.getErr("Failed to get queue-size parameters", err2)
		}
		if _, err2 := cr.GetKey("checkMode"); err2 != nil {
			rc.getErr("Failed to get checkMode parameters", err2)
		}
		if _, err2 := cr.GetKey("checkObject"); err2 != nil {
			rc.getErr("Failed to get checkObject parameters", err2)
		}
		if _, err2 := cr.GetKey("ratio"); err2 != nil {
			rc.getErr("Failed to get Ratio parameters", err2)
		}
		// increment Data Check 二级参数校验
		//if _, err2 := idc.GetKey("incSwitch"); err2 != nil {
		//	rc.getErr("Failed to get incSwitch parameters", err2)
		//}
		//Repair 二级参数校验
		if _, err2 := sr.GetKey("datafix"); err2 != nil {
			rc.getErr("Failed to get datafix parameters", err2)
		}
	}
	//获取参数
	//Source Destination connection 获取jdbc连接信息
	sjdbc := sdc.Key("srcDSN").String() // 将结果转为string
	if strings.Contains(sjdbc, "|") {
		q.SourceDrive = strings.Split(sjdbc, "|")[0]
		q.SourceJdbc = strings.Split(sjdbc, "|")[1]
	} else {
		q.SourceJdbc = sjdbc
	}
	djdbc := sdc.Key("dstDSN").String()
	if strings.Contains(djdbc, "|") {
		q.DestDrive = strings.Split(djdbc, "|")[0]
		q.DestJdbc = strings.Split(djdbc, "|")[1]
	} else {
		q.DestJdbc = djdbc
	}

	//校验库表设置
	q.Table = strings.TrimSpace(do.Key("tables").String())
	q.Igtable = strings.TrimSpace(do.Key("ignore-tables").String())
	if q.Igtable == "" {
		q.Igtable = "nil"
	}
	//判断并发设置，并判断设置的是否正确
	if parametersSwitch {
		if _, err2 := do.GetKey("checkNoIndexTable"); err2 != nil {
			rc.getErr("Failed to get checkNoIndexTable parameters", err2)
		}
		if _, err2 := do.GetKey("lowerCaseTableNames"); err2 != nil {
			rc.getErr("Failed to get lowerCaseTableNames parameters", err2)
		}
		q.LowerCaseTableNames = do.Key("lowerCaseTableNames").In("no", []string{"yes", "no"})
		q.CheckNoIndexTable = do.Key("checkNoIndexTable").In("no", []string{"yes", "no"})
		//Logs 获取相关参数
		q.LogFile = ls.Key("log").String()
		if q.LogFile == "" {
			rc.getErr("Failed to convert log parameter to int", err1)
		}
		q.LogLevel = ls.Key("logLevel").In("info", []string{"debug", "info", "warn", "error"})
		if q.Concurrency, err1 = cr.Key("parallel-thds").Int(); err1 != nil {
			rc.getErr("Failed to convert parallel-thds parameter to int", err1)
		}
		if q.SingleIndexChanRowCount, err1 = cr.Key("singleIndexChanRowCount").Int(); err1 != nil {
			rc.getErr("Failed to convert singleIndexChanRowCount parameter to int", err1)
		}
		if q.JointIndexChanRowCount, err1 = cr.Key("jointIndexChanRowCount").Int(); err1 != nil {
			rc.getErr("Failed to convert jointIndexChanRowCount parameter to int", err1)
		}
		if q.QueueDepth, err1 = cr.Key("queue-size").Int(); err1 != nil {
			rc.getErr("Failed to convert queue-size parameter to int", err1)
		}
		if q.Ratio, err1 = cr.Key("ratio").Int(); err1 != nil {
			rc.getErr("Failed to convert Ratio parameter to int", err1)
		}
		if q.FixTrxNum, err1 = sr.Key("fixTrxNum").Int(); err1 != nil {
			rc.getErr("Failed to convert fixTrxNum parameter to int", err1)
		}
		q.CheckMode = cr.Key("checkMode").In("rows", []string{"count", "rows", "sample"})
		q.CheckObject = cr.Key("checkObject").In("data", []string{"data", "struct", "index", "partitions", "foreign", "trigger", "func", "proc"})
		q.Datafix = sr.Key("datafix").In("file", []string{"file", "table"})
	} else {
		q.JointIndexChanRowCount = 10000
		q.SingleIndexChanRowCount = 10000
		q.Concurrency = 10
		q.QueueDepth = 100
		q.Ratio = 10
		q.FixTrxNum = 100
		q.LogFile = "./gt-checksum.log"
		q.LogLevel = "info"
		q.LowerCaseTableNames = "no"
		q.CheckNoIndexTable = "no"
		q.CheckMode = "rows"
		q.CheckObject = "data"
		q.Datafix = "file"
	}
	//q.IncCheckSwitch = idc.Key("incSwitch").In("no", []string{"yes", "no"})

	if q.Datafix == "file" {
		if parametersSwitch {
			if _, err2 := sr.GetKey("fixFileName"); err2 != nil {
				rc.getErr("Failed to get fixFileName parameters", err2)
			}
			q.FixFileName = sr.Key("fixFileName").String()
		} else {
			q.FixFileName = "./gt-checksum-DataFix.sql"
		}
	}
}
