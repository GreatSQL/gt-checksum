## 3.0.0
- [功能新增]: 新增 `requirePK` 参数（ON|OFF，默认 OFF），支持在 `checkObject=struct` 模式下为无主键表自动添加 `my_row_id` 隐藏列，用于 MySQL 单机实例迁移到 MGR 环境的场景。仅在目标端未启用 `sql_generate_invisible_primary_key` 且表无主键、无 NOT NULL 唯一索引时生效。
- [功能新增]: repairDB 工具新增预执行报告功能，执行修复前自动收集并展示修复 SQL 文件统计信息（文件数量、大小、语句类型分布、影响行数、预估 binlog 大小等），帮助用户在执行前评估修复影响范围。新增交互式确认机制，默认在执行修复前提示用户确认，可通过 `-f` 或 `--force` 参数跳过确认直接执行；新增 `--dry-run` 参数，仅展示统计报告而不执行实际修复操作。
- [功能新增]: repairDB 工具新增 CSV 执行报告导出功能，执行完成后自动生成包含执行汇总和明细的 CSV 报告文件（UTF-8 BOM 编码，可直接用 Excel 打开），记录每个对象的 `INSERT/DELETE/ALTER/CREATE/DROP` 各操作成功与失败数、耗时及失败原因。新增 `resultFile` 配置参数和 `--result-file` CLI 参数，支持自定义 CSV 报告输出路径。
- [测试完善]: 新增 `actions/schema_tab_struct_myrowid_test.go` 测试文件，覆盖 `my_row_id` 相关功能的单元测试场景
- [测试完善]: 新增 `cmd/repairDB/utils_test.go` 测试文件，覆盖 `formatSize/formatNumber/identifyStatementType/collectFixSQLStatistics` 等核心函数的单元测试。
- [测试完善]: 新增 `cmd/repairDB/csv_export_test.go` 测试文件，覆盖 CSV BOM、汇总置顶、统一表头、ObjectType 列、行数统计、汇总值验证、DROP 列、目录创建、文件权限、逗号转义、空结果、路径解析等场景。
- [测试完善]: 新增 `TestExtractSchemaAndObject` 和 `TestResultCollector_Concurrent` 单元测试。
