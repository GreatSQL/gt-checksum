package inputArg

import (
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli"
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
			jdbcS = fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema?charset=%s&timeout=30s&readTimeout=300s&writeTimeout=300s&maxAllowedPacket=16777216&interpolateParams=true&parseTime=true&reconnect=true", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["CHARSET"])
		case "oracle":
			jdbcS = fmt.Sprintf("%s/%s@%s:%s/%s", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["SID"])
		default:
			jdbcS = fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema?charset=%s&timeout=30s&readTimeout=300s&writeTimeout=300s&maxAllowedPacket=16777216&interpolateParams=true&parseTime=true&reconnect=true", tmpc["USER"], tmpc["PASSWD"], tmpc["HOST"], tmpc["PORT"], tmpc["CHARSET"])
		}
	}
	return drivS, jdbcS
}

func (rc *ConfigParameter) cliHelp() {
	app := cli.NewApp()
	app.Name = "gt-checksum"                                                   //应用名称
	app.Usage = "opensource MySQL database checksum and sync tool by GreatSQL" //应用功能说明
	app.Author = "GreatSQL"                                                    //作者
	app.Email = "GreatSQL <greatsql@greatdb.com>"                              //邮箱
	app.Version = "1.2.4"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "c,f",                                                        //命令名称
			Usage:       "Specify config file. For example: -c gc.conf or -f gc.conf", //命令说明
			Value:       "",                                                           //默认值
			Destination: &rc.Config,                                                   //赋值
		},
		cli.IntFlag{
			Name:        "fixTrxSize",
			Usage:       "Override fixTrxSize (MB) from system parameter, e.g. --fixTrxSize 8",
			Value:       0,
			Destination: &rc.CliFixTrxSize,
		},
		cli.IntFlag{
			Name:        "insertSqlSize",
			Usage:       "Override insertSqlSize (KB) from system parameter, e.g. --insertSqlSize 1024",
			Value:       0,
			Destination: &rc.CliInsertSqlSize,
		},
		cli.IntFlag{
			Name:        "deleteSqlSize",
			Usage:       "Override deleteSqlSize (KB) from system parameter, e.g. --deleteSqlSize 16",
			Value:       0,
			Destination: &rc.CliDeleteSqlSize,
		},
	}
	app.Action = func(c *cli.Context) { //应用执行函数
		if rc.Config == "" {
			if _, err := os.Stat("gc.conf"); err != nil {
				fmt.Println("No config file specified and no gc.conf found in current directory. Use -h or --help for usage")
				os.Exit(0)
			} else {
				rc.Config = "gc.conf"
				fmt.Println("\ngt-checksum: reading 'gc.conf' from current directory.")
			}
		}
	}
	app.Run(os.Args)
	aa := os.Args
	for i := range aa {
		if aa[i] == "-h" || aa[i] == "-v" {
			os.Exit(0)
		}
	}
}
