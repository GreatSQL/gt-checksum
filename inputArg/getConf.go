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
	//var cfs = &ConfigFileStruct{}
	//读取配置文件信息
	//cfg, err := ini.Load(configName)
	//处理配置文件中的特殊字符
	cfg, err := ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, configName)
	if err != nil {
		rc.getErr("configuration file error.", err)
	}
	var (
		sdc, dcps, do, ls, cr, idc, sr *ini.Section
		err1                           error
	)

	//判断一级标题是否正确
	if sdc, err1 = cfg.GetSection("Source Destination connection"); sdc == nil && err1 != nil {
		rc.getErr("", err1)
	}
	if dcps, err1 = cfg.GetSection("Database Conn Pool Setting"); dcps == nil && err1 != nil {
		rc.getErr("", err1)
	}
	if do, err1 = cfg.GetSection("Detection object"); do == nil && err1 != nil {
		rc.getErr("", err1)
	}
	if ls, err1 = cfg.GetSection("Log Setting"); ls == nil && err1 != nil {
		rc.getErr("Failed to get Log Setting parameters", err1)
	}
	if cr, err1 = cfg.GetSection("CheckSum Rules"); cr == nil && err1 != nil {
		rc.getErr("Failed to get CheckSum Rules parameters", err1)
	}
	if idc, err1 = cfg.GetSection("increment Data Check"); idc == nil && err1 != nil {
		rc.getErr("Failed to get increment Data Check parameters", err1)
	}
	if sr, err1 = cfg.GetSection("Sql Repair"); sr == nil && err1 != nil {
		rc.getErr("Failed to get Sql Repair parameters", err1)
	}

	//二级参数正确性验证
	//Source Destination connection 获取jdbc连接信息
	if _, err2 := sdc.GetKey("sourceJdbc"); err2 != nil {
		rc.getErr("", err2)
	}
	if _, err2 := sdc.GetKey("destJdbc"); err2 != nil {
		rc.getErr("", err2)
	}

	//Database Conn Pool Setting 获取一致性快照连接池大小
	if _, err2 := dcps.GetKey("poolMin"); err2 != nil {
		rc.getErr("Failed to convert poolMin parameter to int", err2)
	}
	if _, err2 := dcps.GetKey("poolMax"); err2 != nil {
		rc.getErr("Failed to convert poolMax parameter to int", err2)
	}
	//Detection object 获取校验库表信息
	if _, err2 := do.GetKey("schema"); err2 != nil {
		rc.getErr("Failed to get schema parameters", err2)
	}
	if _, err2 := do.GetKey("table"); err2 != nil {
		rc.getErr("Failed to get table parameters", err2)
	}
	if _, err2 := do.GetKey("ignoreSchema"); err2 != nil {
		rc.getErr("Failed to get ignoreSchema parameters", err2)
	}
	if _, err2 := do.GetKey("ignoreTable"); err2 != nil {
		rc.getErr("Failed to get ignoreTable parameters", err2)
	}
	if _, err2 := do.GetKey("checkNoIndexTable"); err2 != nil {
		rc.getErr("Failed to get checkNoIndexTable parameters", err2)
	}
	if _, err2 := do.GetKey("lowerCaseTableNames"); err2 != nil {
		rc.getErr("Failed to get lowerCaseTableNames parameters", err2)
	}
	//Log Setting 二级参数信息
	if _, err2 := ls.GetKey("logPath"); err2 != nil {
		rc.getErr("Failed to get logPath parameters", err2)
	}
	if _, err2 := ls.GetKey("logFile"); err2 != nil {
		rc.getErr("Failed to get logFile parameters", err2)
	}
	if _, err2 := ls.GetKey("logLevel"); err2 != nil {
		rc.getErr("Failed to get logLevel parameters", err2)
	}
	//CheckSum Rules 二级参数检测
	if _, err2 := cr.GetKey("concurrency"); err2 != nil {
		rc.getErr("Failed to get concurrency parameters", err2)
	}
	if _, err2 := cr.GetKey("singleIndexChanRowCount"); err2 != nil {
		rc.getErr("Failed to get singleIndexChanRowCount parameters", err2)
	}
	if _, err2 := cr.GetKey("jointIndexChanRowCount"); err2 != nil {
		rc.getErr("Failed to get jointIndexChanRowCount parameters", err2)
	}
	if _, err2 := cr.GetKey("queueDepth"); err2 != nil {
		rc.getErr("Failed to get queueDepth parameters", err2)
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
	if _, err2 := idc.GetKey("incSwitch"); err2 != nil {
		rc.getErr("Failed to get incSwitch parameters", err2)
	}
	//Sql Repair 二级参数校验
	if _, err2 := sr.GetKey("datafix"); err2 != nil {
		rc.getErr("Failed to get datafix parameters", err2)
	}
	if _, err2 := sr.GetKey("fixFileName"); err2 != nil {
		rc.getErr("Failed to get fixFileName parameters", err2)
	}
	if _, err2 := sr.GetKey("fixPath"); err2 != nil {
		rc.getErr("Failed to get fixPath parameters", err2)
	}

	//获取参数
	//Source Destination connection 获取jdbc连接信息
	sjdbc := sdc.Key("sourceJdbc").String() // 将结果转为string
	if strings.Contains(sjdbc, "|") {
		q.SourceDrive = strings.Split(sjdbc, "|")[0]
		q.SourceJdbc = strings.Split(sjdbc, "|")[1]
	} else {
		q.SourceJdbc = sjdbc
	}
	djdbc := sdc.Key("destJdbc").String()
	if strings.Contains(djdbc, "|") {
		q.DestDrive = strings.Split(djdbc, "|")[0]
		q.DestJdbc = strings.Split(djdbc, "|")[1]
	} else {
		q.DestJdbc = djdbc
	}

	//数据库连接池设置
	if q.PoolMin, err1 = dcps.Key("poolMin").Int(); err1 != nil {
		rc.getErr("Failed to convert poolMin parameter to time.Duration", err1)
	}
	if q.PoolMax, err1 = dcps.Key("poolMax").Int(); err1 != nil {
		rc.getErr("Failed to convert poolMax parameter to int", err1)
	}

	//校验库表设置
	q.LowerCaseTableNames = do.Key("lowerCaseTableNames").In("no", []string{"yes", "no"})
	q.CheckNoIndexTable = do.Key("checkNoIndexTable").In("no", []string{"yes", "no"})
	if q.LowerCaseTableNames == "yes" {
		q.Schema = strings.TrimSpace(do.Key("schema").String())
		q.Table = strings.TrimSpace(do.Key("table").String())
		q.Igschema = strings.TrimSpace(do.Key("ignoreSchema").String())
		q.Igtable = strings.TrimSpace(do.Key("ignoreTable").String())
	} else {
		q.Schema = strings.ToUpper(strings.TrimSpace(do.Key("schema").String()))
		q.Table = strings.ToUpper(strings.TrimSpace(do.Key("table").String()))
		q.Igschema = strings.ToUpper(strings.TrimSpace(do.Key("ignoreSchema").String()))
		q.Igtable = strings.ToUpper(strings.TrimSpace(do.Key("ignoreTable").String()))
	}

	//Log Setting 获取相关参数
	q.LogPath = ls.Key("logPath").String()
	q.LogFile = ls.Key("logFile").String()
	q.LogLevel = ls.Key("logLevel").In("info", []string{"debug", "info", "warning", "error"})

	//判断并发设置，并判断设置的是否正确
	if q.Concurrency, err1 = cr.Key("concurrency").Int(); err1 != nil {
		rc.getErr("Failed to convert Concurrency parameter to int", err1)
	}
	if q.SingleIndexChanRowCount, err1 = cr.Key("singleIndexChanRowCount").Int(); err1 != nil {
		rc.getErr("Failed to convert singleIndexChanRowCount parameter to int", err1)
	}
	if q.JointIndexChanRowCount, err1 = cr.Key("jointIndexChanRowCount").Int(); err1 != nil {
		rc.getErr("Failed to convert jointIndexChanRowCount parameter to int", err1)
	}
	if q.QueueDepth, err1 = cr.Key("queueDepth").Int(); err1 != nil {
		rc.getErr("Failed to convert queueDepth parameter to int", err1)
	}
	q.CheckMode = cr.Key("checkMode").In("rows", []string{"count", "rows", "sample"})
	q.CheckObject = cr.Key("checkObject").In("data", []string{"data", "struct", "trigger", "func", "proc"})

	//q.IncCheckSwitch = idc.Key("incSwitch").In("no", []string{"yes", "no"})
	if q.Ratio, err1 = cr.Key("ratio").Int(); err1 != nil {
		rc.getErr("Failed to convert Ratio parameter to int", err1)
	}
	q.FixPath = sr.Key("fixPath").String()
	q.FixFileName = sr.Key("fixFileName").String()
	q.Datafix = sr.Key("datafix").In("file", []string{"file", "table"})
}
