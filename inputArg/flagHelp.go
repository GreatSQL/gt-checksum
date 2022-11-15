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
		jdbcS = fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema?charset=%s", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["CHARSET"])
	}
	return drivS, jdbcS
}

func cliHelp(q *ConfigParameter) {
	app := cli.NewApp()
	app.Name = "gt-checkOut"                           //应用名称
	app.Usage = "mysql Oracle table data verification" //应用功能说明
	app.Author = "lianghang"                           //作者
	app.Email = "xing.liang@greatdb.com"               //邮箱
	app.Version = "1.1.7"                              //版本
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config,f",                                                                      //命令名称
			Usage:       "Specifies the configuration file. for example: --config gc.conf or -f gc.conf", //命令说明
			Value:       "",                                                                              //默认值
			Destination: &q.config,                                                                       //赋值
		},
		cli.StringFlag{
			Name:        "sourceJdbc,S",
			Usage:       "Configures source connection information. for example: -S type=mysql,user=root,passwd=abc123,host=127.0.0.1",
			Value:       "",
			Destination: &q.SourceJdbc,
		},
		cli.StringFlag{
			Name:        "destJdbc,D",
			Usage:       "Configures dest connection information. for example: -D type=mysql,user=root,passwd=abc123,host=127.0.0.1,port=3306,charset=jbk",
			Value:       "",
			Destination: &q.DestJdbc,
		},
		cli.IntFlag{
			Name:        "poolMin,pi",
			Usage:       "configure the min connection pool. for example: --poolMin 50",
			Value:       50,
			Destination: &q.PoolMin,
		},
		cli.IntFlag{
			Name:        "poolMax,pa",
			Usage:       "configure the max connection pool. for example: --poolMin 100",
			Value:       100,
			Destination: &q.PoolMax,
		},
		cli.StringFlag{
			Name:        "schema,s",
			Usage:       "configure the check schema. for example: --schema all or --schema sysbench,benchmarksql",
			Value:       "nil",
			Destination: &q.Schema,
			EnvVar:      "nil,schema,...",
		},
		cli.StringFlag{
			Name:        "igschema,is",
			Usage:       "configure the ignore check schema. for example: --igschema cc,bb",
			Value:       "nil",
			EnvVar:      "nil,schema,...",
			Destination: &q.Igschema,
		},
		cli.StringFlag{
			Name:        "table,t",
			Usage:       "configure the check table. for example: --table nil",
			Value:       "nil",
			EnvVar:      "nil,schema.table,...",
			Destination: &q.Table,
		},
		cli.StringFlag{
			Name:        "igtable,it",
			Usage:       "configure the ignore check table. for example: --igtable nil",
			Value:       "nil",
			EnvVar:      "nil,schema.table,...",
			Destination: &q.Igtable,
		},
		cli.StringFlag{
			Name:        "noIndexTable,nit",
			Usage:       "Specifies whether to verify non-indexed tables. for example: --nit no",
			Value:       "no",
			EnvVar:      "yes,no",
			Destination: &q.CheckNoIndexTable,
		},
		cli.StringFlag{
			Name:        "lowerCase,lc",
			Usage:       "Configures whether the checklist ignores case. for example: --lc no",
			Value:       "no",
			EnvVar:      "yes,no",
			Destination: &q.LowerCaseTableNames,
		},
		cli.StringFlag{
			Name:        "logPath,lp",
			Usage:       "configures the log output path. for example: --lp /tmp",
			Value:       "./",
			Destination: &q.LogPath,
		},
		cli.StringFlag{
			Name:        "logFile,lf",
			Usage:       "configures the log output file. for example: --lf greatdb.log",
			Value:       "gt-checkOut.log",
			Destination: &q.LogFile,
		},
		cli.StringFlag{
			Name:        "logLevel,ll",
			Usage:       "configures the log output level. for example: --ll info",
			Value:       "info",
			EnvVar:      "debug,info,warning,error",
			Destination: &q.LogLevel,
		},
		cli.IntFlag{
			Name:        "concurrency,cc",
			Usage:       "configures the number of concurrent checks to check data blocks. for example: --cc 5",
			Value:       5,
			Destination: &q.Concurrency,
		},
		cli.IntFlag{
			Name:        "singleIndexChanRowCount,sicr",
			Usage:       "configure a single column index single check database. for example: --sicr 1000",
			Value:       10000,
			Destination: &q.SingleIndexChanRowCount,
		},
		cli.IntFlag{
			Name:        "jointIndexChanRowCount,jicr",
			Usage:       "configures single-check data blocks with multi-column indexes. for example: --jicr 100",
			Value:       1000,
			Destination: &q.JointIndexChanRowCount,
		},
		cli.StringFlag{
			Name:        "checkMode,cm",
			Usage:       "Select the method for verifying data. for example: --cm count",
			EnvVar:      "count,rows,sample",
			Value:       "rows",
			Destination: &q.CheckMode,
		},
		cli.StringFlag{
			Name:        "checkObject,co",
			Usage:       "xample Query the parity object of data. for example: --co struct",
			EnvVar:      "data,struct,index,partitions,foreign,trigger,func,proc",
			Value:       "data",
			Destination: &q.CheckObject,
		},
		cli.IntFlag{
			Name:        "ratio,r",
			Usage:       "When checkmod is set to sample, you can set the percentage of spot checks ranging from 1 to 100%. for example: -r 1",
			Value:       10,
			Destination: &q.Ratio,
		},
		cli.IntFlag{
			Name:        "queueDepth,qd",
			Usage:       "configure queue depth. for example: --qd 100",
			Value:       100,
			Destination: &q.QueueDepth,
		},

		cli.StringFlag{
			Name:        "datafix,df",
			Usage:       "configures repair statements. for example: --df table",
			Value:       "file",
			EnvVar:      "file,table",
			Destination: &q.Datafix,
		},

		cli.StringFlag{
			Name:        "fixPath,fp",
			Usage:       "configuration repair file path. for example: --fp /tmp",
			Value:       "./",
			Destination: &q.FixPath,
		},

		cli.StringFlag{
			Name:        "fixFileName,ffn",
			Usage:       "configuration repair file name. for example: --ffn greatdbCheckDataFix.sql",
			Value:       "gt-checkOutDataFix.sql",
			Destination: &q.FixFileName,
		},
	}
	app.Action = func(c *cli.Context) { //应用执行函数
		if (q.SourceJdbc != "" || q.DestJdbc != "") && q.config != "" {
			fmt.Println("The command line parameter transmission conflicts with the configuration file parameter transmission. Select either method, use --help or -h")
			os.Exit(0)
		}
		if (q.SourceJdbc == "" || q.DestJdbc == "") && q.config == "" {
			fmt.Println("If no parameters are loaded, view the command with --help or -h")
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
