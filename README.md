# MySQL体系数据库 静态数据一致性校验 #

----------
##  Introductory ##

          很高兴大家能使用gt-checkOut工具进行数据的校验比对，该工具设计的初衷是当前市面上开源的数据校验工具很少，作为DBA的我们在日常维护或者架构变更中经常
      会因为数据变动又无法进行数据校验而苦恼，那么我们只能求助于github大哥，而求助github大哥有时也不太好使，不是bug就是运行不起来（其实出现bug只是代码的场景与你的
      场景不符合而已），那么我们该怎么办呢？
     
       gt-checkOut适用哪些场景呢？什么样的情况下才会用到呢？我在这里列了几点（仅为抛砖引玉）：
        1）业务主从场景: 因某些原因导致主从中断后差异数据太多且数据量太大，除了重做从库还有什么办法
        2）业务mgr场景: 因某些原因导致mgr架构崩溃或者某个节点异常退出，如何快速的恢复集群，除了重做异常节点还有什么办法
        3）上云下云场景: 目前现在云业务盛行，我们需要做数据的迁移操作，如果字符集改变导致特殊数据出现乱码或其他的情况该如何处理呢？如果数据迁移工具在迁移过程中出现bug或者数据异常而又迁移成功，此时除了业务发现又该如何处理呢？等等（静态数据或动态数据）
        4）异构迁移场景: 有时我们会遇到异构数据迁移场景，例如从oracle迁移到MySQL，那么我们数据迁移完成后，该如何确定数据是否一致呢？（因字符集不同，数据类型不同等情况）
        5）定期校验场景: 作为DBA在维护高可用架构中为了保证主出现异常后能够且正常切换，并且保证主从的数据一致，不影响业务，那么我们就需要定期的做主从的数据校验工作，那么我们该又什么办法呢？
------

## Download  ##

&emsp;&emsp;&emsp;你可以从 [这里](https://gitee.com/gt-tools/gt-check-out/releases) 下载二进制可执行文件，我已经在ubuntu、centos、redhat下测试过

-----
## Usage  ##
&emsp;&emsp;假如需要校验oracle数据库，则需要下载oracle相应版本的驱动，例如：待校验的数据库为11-2则需要去下载11-2的驱动,并生效,否则连接Oracle会报错

###   安装Oracle Instant Client 
     从https://www.oracle.com/database/technologies/instant-client/downloads.html下载免费的Basic或Basic Light软件包。
     #oracle basic client
     instantclient-basic-linux.x64-11.2.0.4.0.zip
     # oracle sqlplus
     instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
     # oracle sdk
     instantclient-sdk-linux.x64-11.2.0.4.0.zip

###  配置oracle client并生效
     shell> unzip instantclient-basic-linux.x64-11.2.0.4.0.zip
     shell> unzip instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
     shell> unzip instantclient-sdk-linux.x64-11.2.0.4.0.zip
     shell> mv instantclient_11_2 /usr/local
     shell> echo "export LD_LIBRARY_PATH=/usr/local/instantclient_11_2:$LD_LIBRARY_PATH" >>/etc/profile
     shell> source /etc/profile

###   工具使用说明

    shell> ./gt-checkOut
    -- GreatdbCheck init os Args files --
    No output configuration parameters, use --help or -h

    shell> ./gt-checkOut -v
    -- GreatdbCheck init os Args files --
    gt-checkOut version 1.1.7

    shell> ./gt-checkOut -h
    -- GreatdbCheck init configuration files --
    NAME:
    gt-checkOut - mysql Oracle table data verification

    USAGE:
    greatdbCheck.exe [global options] command [command options] [arguments...]

    VERSION:
    1.1.7
    
    AUTHOR:
    lianghang <xing.liang@greatdb.com>
    
    COMMANDS:
    help, h  Shows a list of commands or help for one command
    
    GLOBAL OPTIONS:
    --config value, -f value                       Specifies the configuration file. for example: --config gc.conf or -f gc.conf
    --sourceJdbc value, -S value                   Configures source connection information. for example: -S type=mysql,user=root,passwd=abc123,host=127.0.0.1
    --destJdbc value, -D value                     Configures dest connection information. for example: -D type=mysql,user=root,passwd=abc123,host=127.0.0.1,port=3306,charset=jbk
    --poolMin value, --pi value                    configure the min connection pool. for example: --poolMin 50 (default: 50)
    --poolMax value, --pa value                    configure the max connection pool. for example: --poolMin 100 (default: 100)
    --schema value, -s value                       configure the check schema. for example: --schema all or --schema sysbench,benchmarksql (default: "nil") [%nil%, %schema%, %...%]
    --igschema value, --is value                   configure the ignore check schema. for example: --igschema cc,bb (default: "nil") [%nil%, %schema%, %...%]
    --table value, -t value                        configure the check table. for example: --table nil (default: "nil") [%nil%, %schema.table%, %...%]
    --igtable value, --it value                    configure the ignore check table. for example: --igtable nil (default: "nil") [%nil%, %schema.table%, %...%]
    --noIndexTable value, --nit value              Specifies whether to verify non-indexed tables. for example: --nit no (default: "no") [%yes%, %no%]
    --lowerCase value, --lc value                  Configures whether the checklist ignores case. for example: --lc no (default: "no") [%yes%, %no%]
    --logPath value, --lp value                    configures the log output path. for example: --lp /tmp (default: "./")
    --logFile value, --lf value                    configures the log output file. for example: --lf greatdb.log (default: "gt-checkOut.log")
    --logLevel value, --ll value                   configures the log output level. for example: --ll info (default: "info") [%debug%, %info%, %warning%, %error%]
    --concurrency value, --cc value                configures the number of concurrent checks to check data blocks. for example: --cc 5 (default: 5)
    --singleIndexChanRowCount value, --sicr value  configure a single column index single check database. for example: --sicr 1000 (default: 10000)
    --jointIndexChanRowCount value, --jicr value   configures single-check data blocks with multi-column indexes. for example: --jicr 100 (default: 1000)
    --checkMode value, --cm value                  Select the method for verifying data. for example: --cm count (default: "rows") [%count%, %rows%, %sample%]
    --checkObject value, --co value                xample Query the parity object of data. for example: --co struct (default: "data") [%data%, %struct%, %index%, %partitions%, %foreign%, %trigger%, %func%, %proc%]
    --ratio value, -r value                        When checkmod is set to sample, you can set the percentage of spot checks ranging from 1 to 100%. for example: -r 1 (default: 10)
    --queueDepth value, --qd value                 configure queue depth. for example: --qd 100 (default: 100)
    --datafix value, --df value                    configures repair statements. for example: --df table (default: "file") [%file%, %table%]
    --fixPath value, --fp value                    configuration repair file path. for example: --fp /tmp (default: "./")
    --fixFileName value, --ffn value               configuration repair file name. for example: --ffn greatdbCheckDataFix.sql (default: "gt-checkOutDataFix.sql")
    --help, -h                                     show help
    --version, -v                                  print the version

--------
## Examples ##

     1）加载配置文件执行数据校验的命令
     shell> ./gt-checkOut -f ./gc.conf
     2）使用命令行传参执行数据校验命令
     shell> ./gt-checkOut -S type=mysql,user=root,passwd=abc123,host=xxxx -D type=mysql,user=root,passwd=abc123,host=xxxx -s benchmarksql,sysbench,aaa -is benchmarksql -it sysbench.sbtest3 -nit yes
     3）示例：
     shell> ./gt-checkOut -f gc.conf
    -- GreatdbCheck init configuration files --
    -- GreatdbCheck init log files --
    -- GreatdbCheck init check table --
    -- GreatdbCheck init check table column --
    -- GreatdbCheck init check table index column --
    -- GreatdbCheck Obtain global consensus sites --
    -- GreatdbCheck init source and dest transaction snapshoot conn pool --
    -- GreatdbCheck init cehck table query plan and check data --
    begin checkSum no index table sysbench.sbtest4
    begin checkSum index table sysbench.sbtest1
    table Index Column Data done! 2022-10-23 13:04:1010
    table QuerySql Where Data Generate done! 2022-10-23 13:04:1010
    table All Measured Data CheckSum done! 2022-10-23 13:04:1010
    table Differences in Data CheckSum done! 2022-10-23 13:04:1010
    sysbench.sbtest1 校验完成
    begin checkSum index table sysbench.sbtest2
    table Index Column Data done! 2022-10-23 13:06:1010
    table QuerySql Where Data Generate done! 2022-10-23 13:06:1010
    table All Measured Data CheckSum done! 2022-10-23 13:06:1010
    table Differences in Data CheckSum done! 2022-10-23 13:06:1010
    sysbench.sbtest2 校验完成
    sysbench.sbtest4 校验完成
    
    ** GreatdbCheck Overview of verification results **
    Check time:  360.15s (Seconds)
    Schema          Table   IndexCol        Rows            Differences     Datafix
    sysbench        sbtest1 id              100000,100000     yes             file
    sysbench        sbtest4 noIndex         99994,99994       yes             file
    sysbench        sbtest2 id              100000,100000     yes             file
-------
## Building ##

    mycheck needs go version > 1.17 for go mod

    shell> git clone https://gitee.com/gt-tools/gt-check-out.git
    shell> go build -o gt-checkOut greatdbCheck.go
    shell> chmod +x gt-checkOut
    shell> mv gt-checkOut /usr/bin

-----
## Requirements ##

    数据校验目前支持MySQL and oracle 体系的静态数据校验
_____
## doc ##
    操作使用手册：
    在线手册：
            https://bbkv6krkep.feishu.cn/wiki/wikcn92c6R9Eh7hJ0mvw1NEwfld
    离线手册：
            /doc/GreatdbToolKit1.1.7.pdf
-----
## Author ##

    - name: lianghang
    - mail: xing.liang@greatdb.com