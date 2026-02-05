package inputArg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/ini.v1"
)

// 一级、二级参数标签合法性校验
func (rc *ConfigParameter) LevelParameterCheck() {
	var (
		err error
	)

	// 直接使用默认section，不再处理具体的section标签
	defaultSection := rc.ConfFine.Section("")

	//Source Destination connection 获取jdbc连接信息
	for _, i := range []string{"srcDSN", "dstDSN"} {
		if _, err = defaultSection.GetKey(i); err != nil {
			rc.getErr(fmt.Sprintf("Failed to set option %s", i), err)
		}
	}

	//Schema 获取校验库表信息
	for _, i := range []string{"tables"} {
		if _, err = defaultSection.GetKey(i); err != nil {
			rc.getErr(fmt.Sprintf("Failed to set option %s", i), err)
		}
	}

	// 其他参数为可选，使用默认值
}

/*
二级参数值获取校验
*/
func (rc *ConfigParameter) secondaryLevelParameterCheck() {
	// 直接使用默认section，不再处理具体的section标签
	defaultSection := rc.ConfFine.Section("")

	// 通用函数：从配置文件中读取指定参数的最后一个值
	getLastConfigValue := func(paramName string) string {
		if content, err := os.ReadFile(rc.Config); err == nil {
			lines := strings.Split(string(content), "\n")
			var value string
			for _, line := range lines {
				// 忽略注释和空行
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, ";") {
					continue
				}
				// 检查是否是目标参数
				prefix := paramName + "="
				if strings.HasPrefix(line, prefix) {
					// 提取值
					value = strings.TrimSpace(strings.TrimPrefix(line, prefix))
				}
			}
			return value
		}
		// 如果读取文件失败，回退到原来的方法
		key := defaultSection.Key(paramName)
		return strings.TrimSpace(key.String())
	}

	//Source Destination connection 获取jdbc连接信息
	srcDSNValue := getLastConfigValue("srcDSN")
	rc.SecondaryL.DsnsV.SrcDSN = srcDSNValue
	if strings.Contains(rc.SecondaryL.DsnsV.SrcDSN, "|") {
		rc.SecondaryL.DsnsV.SrcDrive = strings.Split(rc.SecondaryL.DsnsV.SrcDSN, "|")[0]
		rc.SecondaryL.DsnsV.SrcJdbc = strings.Split(rc.SecondaryL.DsnsV.SrcDSN, "|")[1]
	} else {
		rc.SecondaryL.DsnsV.SrcJdbc = rc.SecondaryL.DsnsV.SrcDSN
	}

	dstDSNValue := getLastConfigValue("dstDSN")
	rc.SecondaryL.DsnsV.DstDSN = dstDSNValue
	if strings.Contains(rc.SecondaryL.DsnsV.DstDSN, "|") {
		rc.SecondaryL.DsnsV.DestDrive = strings.Split(rc.SecondaryL.DsnsV.DstDSN, "|")[0]
		rc.SecondaryL.DsnsV.DestJdbc = strings.Split(rc.SecondaryL.DsnsV.DstDSN, "|")[1]
	} else {
		rc.SecondaryL.DsnsV.DestJdbc = rc.SecondaryL.DsnsV.DstDSN
	}

	//校验库表设置
	tablesValue := getLastConfigValue("tables")
	if tablesValue != "" {
		rc.SecondaryL.SchemaV.Tables = tablesValue
	}

	ignoreTablesValue := getLastConfigValue("ignoreTables")
	if ignoreTablesValue != "" {
		rc.SecondaryL.SchemaV.IgnoreTables = ignoreTablesValue
	} else {
		rc.SecondaryL.SchemaV.IgnoreTables = "nil"
	}

	caseSensitiveObjectNameValue := getLastConfigValue("caseSensitiveObjectName")
	if caseSensitiveObjectNameValue != "" {
		rc.SecondaryL.SchemaV.CaseSensitiveObjectName = caseSensitiveObjectNameValue
	} else {
		rc.SecondaryL.SchemaV.CaseSensitiveObjectName = "no"
	}

	checkNoIndexTableValue := getLastConfigValue("checkNoIndexTable")
	if checkNoIndexTableValue != "" {
		rc.SecondaryL.SchemaV.CheckNoIndexTable = checkNoIndexTableValue
	} else {
		rc.SecondaryL.SchemaV.CheckNoIndexTable = "no"
	}

	//Logs 获取相关参数
	logFileValue := getLastConfigValue("logFile")
	if logFileValue != "" {
		rc.SecondaryL.LogV.LogFile = logFileValue
	} else {
		rc.SecondaryL.LogV.LogFile = "./gt-checksum.log"
		fmt.Println("Using default value './gt-checksum.log' for option LogFile")
	}

	logLevelValue := getLastConfigValue("logLevel")
	if logLevelValue != "" {
		rc.SecondaryL.LogV.LogLevel = logLevelValue
	} else {
		rc.SecondaryL.LogV.LogLevel = "info"
	}

	//Rules 获取相关参数
	parallelThdsValue := getLastConfigValue("parallelThds")
	if parallelThdsValue != "" {
		if val, err := strconv.Atoi(parallelThdsValue); err == nil {
			rc.SecondaryL.RulesV.ParallelThds = val
		} else {
			fmt.Println("Using default value '10' for option parallelThds")
			rc.SecondaryL.RulesV.ParallelThds = 10
		}
	} else {
		fmt.Println("Using default value '10' for option parallelThds")
		rc.SecondaryL.RulesV.ParallelThds = 10
	}

	chunkSizeValue := getLastConfigValue("chunkSize")
	if chunkSizeValue != "" {
		if val, err := strconv.Atoi(chunkSizeValue); err == nil {
			rc.SecondaryL.RulesV.ChanRowCount = val
		} else {
			fmt.Println("Using default value '1000' for option chunkSize")
			rc.SecondaryL.RulesV.ChanRowCount = 1000
		}
	} else {
		fmt.Println("Using default value '1000' for option chunkSize")
		rc.SecondaryL.RulesV.ChanRowCount = 1000
	}

	queueSizeValue := getLastConfigValue("queueSize")
	if queueSizeValue != "" {
		if val, err := strconv.Atoi(queueSizeValue); err == nil {
			rc.SecondaryL.RulesV.QueueSize = val
		} else {
			fmt.Println("Using default value '100' for option queueSize")
			rc.SecondaryL.RulesV.QueueSize = 1000
		}
	} else {
		fmt.Println("Using default value '100' for option queueSize")
		rc.SecondaryL.RulesV.QueueSize = 1000
	}

	checkObjectValue := getLastConfigValue("checkObject")
	if checkObjectValue != "" {
		// 检查是否使用了已废弃的func或proc选项
		if checkObjectValue == "func" || checkObjectValue == "proc" {
			// 将其强制改为默认的data，并发出info级别的提示
			fmt.Printf("Warning: checkObject value '%s' is deprecated. Using default value 'data' instead. Consider using 'routine' for checking stored procedures and functions.\n", checkObjectValue)
			rc.SecondaryL.RulesV.CheckObject = "data"
		} else {
			// 验证值是否有效
			validValues := []string{"data", "struct", "trigger", "routine"}
			valid := false
			for _, v := range validValues {
				if checkObjectValue == v {
					valid = true
					break
				}
			}
			if valid {
				rc.SecondaryL.RulesV.CheckObject = checkObjectValue
			} else {
				// 检查是否使用了已合并到struct的选项
				if checkObjectValue == "index" || checkObjectValue == "partitions" || checkObjectValue == "foreign" {
					fmt.Printf("Note: checkObject value '%s' has been merged into 'struct'. Using 'struct' instead.\n", checkObjectValue)
					rc.SecondaryL.RulesV.CheckObject = "struct"
				} else {
					fmt.Printf("Warning: Invalid checkObject value '%s', using default value 'data' instead\n", checkObjectValue)
					rc.SecondaryL.RulesV.CheckObject = "data"
				}
			}
		}

		// 如果用户设置了routine，将其转换为内部处理逻辑
		if rc.SecondaryL.RulesV.CheckObject == "routine" {
			// 在内部记录这是一个组合检查（同时检查proc和func）
			rc.SecondaryL.RulesV.IsRoutineCheck = true
		}
	} else {
		fmt.Println("Using default value 'data' for option checkObject")
		rc.SecondaryL.RulesV.CheckObject = "data"
	}

	memoryLimitValue := getLastConfigValue("memoryLimit")
	if memoryLimitValue != "" {
		if val, err := strconv.Atoi(memoryLimitValue); err == nil {
			rc.SecondaryL.RulesV.MemoryLimit = val
		} else {
			fmt.Println("Using default value '1024' for option memoryLimit")
			rc.SecondaryL.RulesV.MemoryLimit = 1024
		}
	} else {
		fmt.Println("Using default value '1024' for option memoryLimit")
		rc.SecondaryL.RulesV.MemoryLimit = 1024
	}

	//Repair 获取相关参数
	fixTrxNumValue := getLastConfigValue("fixTrxNum")
	if fixTrxNumValue != "" {
		if val, err := strconv.Atoi(fixTrxNumValue); err == nil {
			rc.SecondaryL.RepairV.FixTrxNum = val
		} else {
			fmt.Println("Using default value '1000' for option fixTrxNum")
			rc.SecondaryL.RepairV.FixTrxNum = 1000
		}
	} else {
		fmt.Println("Using default value '1000' for option fixTrxNum")
		rc.SecondaryL.RepairV.FixTrxNum = 1000
	}

	datafixValue := getLastConfigValue("datafix")
	if datafixValue != "" {
		validValues := []string{"file", "table"}
		valid := false
		for _, v := range validValues {
			if datafixValue == v {
				valid = true
				break
			}
		}
		if valid {
			rc.SecondaryL.RepairV.Datafix = datafixValue
		} else {
			rc.SecondaryL.RepairV.Datafix = "file"
		}
	} else {
		rc.SecondaryL.RepairV.Datafix = "file"
	}

	// Get fixFilePerTable parameter
	fixFilePerTableValue := getLastConfigValue("fixFilePerTable")
	if fixFilePerTableValue != "" {
		validValues := []string{"ON", "OFF"}
		valid := false
		for _, v := range validValues {
			if fixFilePerTableValue == v {
				valid = true
				break
			}
		}
		if valid {
			rc.SecondaryL.RepairV.FixFilePerTable = fixFilePerTableValue
		} else {
			rc.SecondaryL.RepairV.FixFilePerTable = "OFF"
		}
	} else {
		rc.SecondaryL.RepairV.FixFilePerTable = "OFF"
	}

	if rc.SecondaryL.RepairV.Datafix == "file" {
		fixFileDirValue := getLastConfigValue("fixFileDir")
		if fixFileDirValue != "" {
			rc.SecondaryL.RepairV.FixFileDir = fixFileDirValue
		} else {
			// 使用默认值：fixsql-当前时间戳
			timestamp := time.Now().Format("20060102150405")
			rc.SecondaryL.RepairV.FixFileDir = fmt.Sprintf("fixsql-%s", timestamp)
			fmt.Printf("Using default value '%s' for option fixFileDir\n", rc.SecondaryL.RepairV.FixFileDir)
		}

		// 检查目录是否存在
		if _, err := os.Stat(rc.SecondaryL.RepairV.FixFileDir); err == nil {
			// 目录已存在，检查是否为空
			files, err := os.ReadDir(rc.SecondaryL.RepairV.FixFileDir)
			if err == nil && len(files) > 0 {
				fmt.Printf("Error: Directory '%s' already exists and is not empty\n", rc.SecondaryL.RepairV.FixFileDir)
				os.Exit(0)
			}
		} else if os.IsNotExist(err) {
			// 目录不存在，创建目录
			if err := os.MkdirAll(rc.SecondaryL.RepairV.FixFileDir, 0755); err != nil {
				fmt.Printf("Error: Failed to create directory '%s': %v\n", rc.SecondaryL.RepairV.FixFileDir, err)
				os.Exit(0)
			}
		} else {
			// 其他错误
			fmt.Printf("Error: Failed to check directory '%s': %v\n", rc.SecondaryL.RepairV.FixFileDir, err)
			os.Exit(0)
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

	// 检查配置文件是否包含section标签（如[DSNs], [Schema]等）
	if content, err := os.ReadFile(rc.Config); err == nil {
		lines := strings.Split(string(content), "\n")
		var sections []string
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
				sections = append(sections, line)
			}
		}
		if len(sections) > 0 {
			fmt.Println("Error: Found unrecognized configuration sections:", strings.Join(sections, ", "))
			fmt.Println("Please remove these sections and set parameters directly without section tags.")
			os.Exit(0)
		}
	}

	//读取配置文件信息
	//处理配置文件中的特殊字符
	rc.ConfFine, err = ini.LoadSources(ini.LoadOptions{
		IgnoreInlineComment:    true,
		AllowNonUniqueSections: true,
		AllowShadows:           true,
	}, rc.Config)
	if err != nil {
		rc.getErr("configuration file error.", err)
	}
	rc.LevelParameterCheck()
	rc.secondaryLevelParameterCheck()
}
