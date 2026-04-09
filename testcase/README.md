# testcase 使用说明

本目录包含 gt-checksum 的综合测例（SQL 初始化脚本），覆盖 MySQL 双端数据校验、部分列校验、Oracle→MySQL 跨库校验等场景。

## 文件一览

| 文件 | 用途 |
|------|------|
| `MySQL-source.sql` | MySQL 源端综合测例 |
| `MySQL-target.sql` | MySQL 目标端综合测例 |
| `MySQL-columns-source.sql` | `columns` 部分列校验功能源端测例 |
| `MySQL-columns-target.sql` | `columns` 部分列校验功能目标端测例 |
| `Oracle.sql` | Oracle 源端综合测例（配合 MySQL 目标端使用） |

---

## MySQL-source.sql / MySQL-target.sql

**介绍**

MySQL 双端的综合测例，数据库名 `gt_checksum`，字符集 `utf8mb4`。覆盖触发器、存储函数/过程、分区表、外键、视图、多种基础数据类型，以及 Oracle→MySQL 类型映射（`t1` 表）。两端之间内置了结构和数据上的差异，可直接用于 struct/data compare 及 repair 场景回归。

**覆盖对象**

| 类型 | 对象 |
|------|------|
| 基础数据类型表 | `testint`、`testfloat`、`testbit`、`testtime`、`teststring`、`testbin` |
| 视图 | `v_teststring` |
| 触发器 | `accountInsert`、`accountDelete`、`accountUpdate`、`tri_test` |
| 分区表 | `range_partition_table`、`customer`、`customer1`、`list_partition_table`、`hash_partition_table`、`range_hash_partition_table` |
| 外键约束 | `tb_dept1`、`tb_emp6` |
| 存储函数/过程 | `getAgeStr`、`myAdd` |
| 索引综合 | `indext`（含函数索引、前缀索引、中文索引名、中文列名） |
| 跨库类型映射 | `t1`（Oracle 类型在 MySQL 中的等效映射，含 Emoji、边界值、全 NULL） |

**源/目标端主要差异**

- `testint`：列默认值、ZEROFILL 精度、NOT NULL 约束存在差异；
- `testtime`：源端含 `COMMENT` 和 `ON UPDATE CURRENT_TIMESTAMP`，目标端去掉部分约束；
- `teststring`：`f2`/`f3` 的类型互换（`CHAR` ↔ `VARCHAR`）；
- `accountInsert` 触发器：目标端 INSERT 日志内容改为 `"INSERT-x"`；
- `customer`：源端 2 个分区，目标端 4 个分区；
- `customer1`：源端 2 个分区，目标端 1 个分区；
- `myAdd`：目标端计算逻辑多 `+ 1`；
- `indext`：源端含函数索引 `ABS(price)` 和前缀索引 `goods_name(20)`，目标端均改为普通索引；索引名 `idx 2`/`idx_3` 与目标端 `idx_2`/`idx 3` 互换。

**使用方法**

```bash
# 源端装载
mysql -h <src_host> -P <src_port> -u <user> -p < MySQL-source.sql

# 目标端装载
mysql -h <dst_host> -P <dst_port> -u <user> -p < MySQL-target.sql
```

---

## MySQL-columns-source.sql / MySQL-columns-target.sql

**介绍**

`columns` 参数（部分列校验）功能的专项回归测例，数据库名 `gt_checksum_cols`，字符集 `utf8mb4_general_ci`。每张表对应一个具体测试场景（TC），覆盖忽略列、选中列修复、source-only 行、跨表列名映射、无主键表、目标端多余行等边界情况。

**测试场景**

| TC | 表名 | 场景说明 |
|----|------|---------|
| TC-01 | `col_data` | 非选中列两端不同，选中列一致 → 预期 `Diffs=no` |
| TC-02 | `order_data` | 选中列 `amount` 两端不同，修复后收敛 |
| TC-03 | `events` | 源端独有行（source-only）→ 生成 advisory 注释文件 |
| TC-04 | `product` | 简单语法 `columns=score`，非选中列 `note` 不计入差异 |
| TC-05 | `old_orders` / `new_orders` | 跨表列名映射（`src_total` → `dst_total`），修复后收敛 |
| TC-06 | `heap_data` | 无主键/唯一键表，columns 模式下标记 `DDL-yes` 跳过 |
| TC-07 | `inventory` | 目标端多出行，`extraRowsSyncToSource=ON` 生成 DELETE 后收敛 |

**使用方法**

```bash
# 源端装载
mysql -h <src_host> -P <src_port> -u <user> -p < MySQL-columns-source.sql

# 目标端装载
mysql -h <dst_host> -P <dst_port> -u <user> -p < MySQL-columns-target.sql
```

装载后，在配置文件中设置 `tables=gt_checksum_cols.*` 并指定 `columns=<列名列表>` 参数执行 gt-checksum。完整自动化回归流程可参考 `scripts/regression-test-columns.sh`。

---

## Oracle.sql

**介绍**

Oracle 源端综合测例，用于初始化 Oracle 源实例，配合 `MySQL-target.sql` 做 **Oracle → MySQL** 跨库数据校验。脚本首先以幂等方式创建 `gt_checksum` 用户并授权，删除旧对象后重建所有测试表和存储对象，最后提交数据。

脚本中的对象与 `MySQL-source.sql` 一一对应，类型上做了 Oracle→MySQL 的等效映射（如 `NUMBER`→`INT/DECIMAL`、`VARCHAR2`→`VARCHAR`、`CLOB`→`LONGTEXT`、`RAW`→`BINARY/VARBINARY`、`BLOB`→`BLOB` 等）。

**覆盖对象**

| 类型 | Oracle 对象 |
|------|-------------|
| 基础数据类型表 | `testint`、`testfloat`、`testbit`、`testtime`、`teststring`、`testbin` |
| 触发器 | `accountInsert`、`accountDelete`、`accountUpdate`、`tri_test` |
| 分区表 | `range_partition_table`、`customer`、`customer1`、`list_partition_table`、`hash_partition_table`、`range_hash_partition_table` |
| 外键约束 | `tb_dept1`、`tb_emp6` |
| 存储函数 | `getAgeStr` |
| 索引 | `indext`（无中文列名和函数索引，保持 Oracle 兼容） |
| 跨库类型映射测试 | `t1`（覆盖 `VARCHAR2`、`NCHAR`、`NVARCHAR2`、`NUMBER`、`FLOAT`、`DATE`、`TIMESTAMP`、`CLOB` 等类型及边界值） |

**使用方法**

```bash
# 以 sqlplus 执行（替换连接串）
sqlplus sys/<password>@//<host>:1521/<service> AS SYSDBA @Oracle.sql
```

执行完成后，配置 gt-checksum 进行跨库校验：

```ini
srcDSN=oracle|gt_checksum/gt_checksum@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<ora_host>)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=<service>)))
dstDSN=mysql|<user>:<pass>@tcp(<mysql_host>:<port>)/information_schema?charset=utf8mb4
tables=gt_checksum.*
checkObject=data
```

> **注意**：执行前需要 DBA 权限（`CREATE USER`、`GRANT` 等操作）。`t1` 表中包含 Emoji、极大极小值、全 NULL 等边界数据，用于验证跨库类型映射的正确性。
