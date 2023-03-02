package inputArg

import (
	"fmt"
	"github.com/urfave/cli"
	"os"
	"strings"
)

var jdbcDispos = func(jdbc string) (string, string) {
	var drivS, jdbcS string
	if strings.Contains(jdbc, ",") {
		tmpa := strings.Split(jdbc, ",")
		var tmpc = make(map[string]string)
		for _, i := range tmpa {
			if strings.Contains(i, "=") {
				tmpb := strings.Split(i, "=")
				tmpc[strings.ToUpper(tmpb[0])] = tmpb[1]
			}
		}

		if _, ok := tmpc["CHARSET"]; !ok {
			tmpc["CHARSET"] = "utf8mb4"
		}
		if _, ok := tmpc["PORT"]; !ok {
			tmpc["PORT"] = "3306"
		}
		drivS = tmpc["TYPE"]
		switch drivS {
		case "mysql":
			jdbcS = fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema?charset=%s", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["CHARSET"])
		case "oracle":
			jdbcS = fmt.Sprintf("%s/%s@%s:%s/%s", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["SID"])
		default:
			jdbcS = fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema?charset=%s", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["CHARSET"])
		}
	}
	return drivS, jdbcS
}

func cliHelp(q *ConfigParameter) {
	app := cli.NewApp()
	app.Name = "gt-checksum"                                             //应用名称
	app.Usage = "An opensource table and data checksum tool by GreatSQL" //应用功能说明
	app.Author = "GreatSQL"                                              //作者
	app.Email = "GreatSQL <greatsql@greatdb.com>"                        //邮箱
	app.Version = "1.2.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config,f",                                                           //命令名称
			Usage:       "Specifies config file. For example: --config gc.conf or -f gc.conf", //命令说明
			Value:       "",                                                                   //默认值
			Destination: &q.config,                                                            //赋值
		},
		cli.StringFlag{
			Name:        "srcDSN,S",
			Usage:       "Set source DSN. For example: -S type=oracle,user=root,passwd=abc123,host=127.0.0.1,port=1521,sid=helowin",
			Value:       "",
			Destination: &q.SourceJdbc,
		},
		cli.StringFlag{
			Name:        "dstDSN,D",
			Usage:       "Set destination DSN. For example: -D type=mysql,user=root,passwd=abc123,host=127.0.0.1,port=3306,charset=utf8",
			Value:       "",
			Destination: &q.DestJdbc,
		},
		cli.StringFlag{
			Name:        "tables,t",
			Usage:       "Specify which tables to check. For example: --tables db1.*",
			Value:       "nil",
			EnvVar:      "nil,schema.table,...",
			Destination: &q.Table,
		},
		cli.StringFlag{
			Name:        "ignore-table,it",
			Usage:       "Specify which tables ignore to check. For example: -it nil",
			Value:       "nil",
			EnvVar:      "nil,database.table,...",
			Destination: &q.Igtable,
		},
		cli.StringFlag{
			Name:        "noIndexTable,nit",
			Usage:       "Specify whether to check non-indexed tables. For example: --nit no",
			Value:       "no",
			EnvVar:      "yes,no",
			Destination: &q.CheckNoIndexTable,
		},
		cli.StringFlag{
			Name:        "lowerCase,lc",
			Usage:       "Specify whether to use lowercase table names. For example: --lc no",
			Value:       "no",
			EnvVar:      "yes,no",
			Destination: &q.LowerCaseTableNames,
		},
		cli.StringFlag{
			Name:        "logFile,lf",
			Usage:       "Specify output log file name. For example: --lf ./gt-checksum.log",
			Value:       "./gt-checksum.log",
			Destination: &q.LogFile,
		},
		cli.StringFlag{
			Name:        "logLevel,ll",
			Usage:       "Specify output log level. For example: --ll info",
			Value:       "info",
			EnvVar:      "debug,info,warn,error",
			Destination: &q.LogLevel,
		},
		cli.IntFlag{
			Name:        "parallel-thds,thds",
			Usage:       "Specify the number of parallel threads for data checksum. For example: --thds 5",
			Value:       5,
			Destination: &q.Concurrency,
		},
		cli.IntFlag{
			Name:        "singleIndexChanRowCount,sicr",
			Usage:       "Specifies how many rows are retrieved to check each time for single column index. For example: --sicr 10000",
			Value:       10000,
			Destination: &q.SingleIndexChanRowCount,
		},
		cli.IntFlag{
			Name:        "jointIndexChanRowCount,jicr",
			Usage:       "Specifies how many rows are retrieved to check each time for multi-column index. For example: --jicr 1000",
			Value:       1000,
			Destination: &q.JointIndexChanRowCount,
		},
		cli.StringFlag{
			Name:        "checkMode,cm",
			Usage:       "Specify data check mode. For example: --cm count",
			EnvVar:      "count,rows,sample",
			Value:       "rows",
			Destination: &q.CheckMode,
		},
		cli.StringFlag{
			Name:        "checkObject,co",
			Usage:       "Specify data check object. For example: --co struct",
			EnvVar:      "data,struct,index,partitions,foreign,trigger,func,proc",
			Value:       "data",
			Destination: &q.CheckObject,
		},
		cli.IntFlag{
			Name:        "ratio,r",
			Usage:       "When checkMode is set to sample, specify the data sampling rate, set the range of 1-100, in percentage. For example: -r 10",
			Value:       10,
			Destination: &q.Ratio,
		},
		cli.IntFlag{
			Name:        "queue-size,qs",
			Usage:       "Specify data check queue depth. for example: --qs 100",
			Value:       100,
			Destination: &q.QueueDepth,
		},

		cli.StringFlag{
			Name:        "datafix,df",
			Usage:       "Specify data repair mode. For example: --df table",
			Value:       "file",
			EnvVar:      "file,table",
			Destination: &q.Datafix,
		},
		cli.StringFlag{
			Name:        "fixFileName,ffn",
			Usage:       "Set data repair SQL file name. For example: --ffn ./gt-checksum-DataFix.sql",
			Value:       "./gt-checksum-DataFix.sql",
			Destination: &q.FixFileName,
		},
		cli.IntFlag{
			Name:        "fixTrxNum,ftn",
			Usage:       "Maximum number of concurrent transactions when repairing data. For example: --ftn 20",
			Value:       20,
			Destination: &q.FixTrxNum,
		},
	}
	app.Action = func(c *cli.Context) { //应用执行函数
		if (q.SourceJdbc != "" || q.DestJdbc != "") && q.config != "" {
			fmt.Println("Specify the config, srcDSN and dstDSN options at the same time, causing conflicts, run gt-checksum with option --help or -h")
			os.Exit(0)
		}
		if (q.SourceJdbc == "" || q.DestJdbc == "") && q.config == "" {
			fmt.Println("If no options are specified, run gt-checksum with option --help or -h")
			os.Exit(0)
		}
		q.SourceDrive, q.SourceJdbc = jdbcDispos(q.SourceJdbc)
		q.DestDrive, q.DestJdbc = jdbcDispos(q.DestJdbc)
	}
	app.Run(os.Args)
	aa := os.Args
	for i := range aa {
		if aa[i] == "--help" || aa[i] == "-h" || aa[i] == "-v" || aa[i] == "--version" {
			os.Exit(0)
		}
	}
}
