## 4.0.0
- [功能新增]: 新增 SSL 加密连接支持，支持源端和目标端独立配置 SSL 参数（srcSslCa/srcSslCert/srcSslKey/srcSslMode、dstSslCa/dstSslCert/dstSslKey/dstSslMode），支持 DISABLED/PREFERRED/REQUIRED/VERIFY_CA/VERIFY_IDENTITY 五种模式。
- [功能新增]: repairDB 工具新增目标端 SSL 连接配置支持（dstSslCa/dstSslCert/dstSslKey/dstSslMode）。
- [功能优化]: refactor(repairDB): 拆分 main.go 为多文件模块化结构（config/executor/lock/plan/sql_parser/stage/stats/types）。
- [功能优化]: refactor(oracle_random_data_load): 拆分 main.go 为多文件模块化结构（config/generator/schema/types/util/worker）。
