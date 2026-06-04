# gt-checksum 回归测试脚本使用说明

本目录下提供多个回归测试脚本，覆盖 MySQL/MariaDB、Oracle→MySQL、columns、struct-column 以及 v4.0.0 关键能力的 smoke/专项回归。除 baseline 脚本外，脚本默认会自动完成编译、初始化、执行用例、生成报告等步骤，运行产物统一写入 `test-artifacts/<run-id>/`；baseline 仅输出终端日志并使用退出时清理的临时目录。

| 脚本 | 场景 | 覆盖源 → 目标 | 覆盖模式/重点 |
| --- | --- | --- | --- |
| `regression-test.sh` | MySQL/MariaDB 多版本互测 | MySQL 5.6/5.7/8.0/8.4、MariaDB 10.0/10.5/10.6/10.11/12.3 交叉组合 | `data` / `struct` / `trigger` / `routine`，可选 `--with-v4-cases` |
| `regression-test-columns.sh` | `columns` 选项（列级校验）功能回归 | 任意一对 MySQL 实例 | `data`，包含 columns 核心用例与 v4.0.0 `datafix=table` / `Fixed` smoke |
| `regression-test-struct-column.sh` | struct-column / `TableColumnNameCheck` 专项回归 | 任意一对 MySQL 实例 | `struct` / `data`，包含类型映射 preview 与非 data 在线修复安全策略 |
| `regression-test-oracle.sh` | Oracle → MySQL 异构校验 | Oracle 11g/19c → MySQL 8.0/8.4 | `struct` / `data`，`--with-scenarios` 执行异构专项场景 |
| `struct-migration-test-baseline.sh` | 快速基线与非 DB smoke | 无需数据库 | v4.0.0 关键包测试、编译、dtype mapping preview、repairDB dry-run 配置解析 |

---

## 公共约定

- **运行入口**：在项目根目录执行 `bash scripts/<script>.sh [选项]`
- **依赖工具**：`mysql` 客户端、`go`、可选的 `timeout`/`gtimeout`；Oracle 脚本额外需要 `sqlplus` 和 Oracle Instant Client
- **数据库账号默认**：`checksum` / `checksum`，主机 `127.0.0.1`
- **产物目录**：除 baseline 外为 `test-artifacts/<run-id>/`，包含 `report.txt`、`report.json`（部分脚本）、`results.csv`、`cases/<case-id>/` 下每一轮的 `gt-checksum.conf`、输出和日志；file 修复用例还包含 `repairDB.conf`，`datafix=table` 在线修复用例通常不包含 `repairDB.conf`
- **退出码**：存在 FAIL/ERROR/TIMEOUT 时返回 1，否则返回 0
- **中断处理**：收到 `Ctrl+C` 会生成部分报告后退出

判定说明：常规 file 修复用例最多跑 `MAX_REPAIR_ROUNDS+1` 轮，每轮先跑 `gt-checksum` 产生 `fixsql`，再用 `repairDB` 应用修复，直至 `Diffs=no` 判 PASS，或达到上限判 FAIL。data 模式遇到仅剩 `DDL-yes` 时会显式报告 `PASS-DDL`，表示存在结构预检/无索引等不可由 DML 修复的状态但没有真实 data `yes`。VIEW / columns source-only 等仅含注释的 advisory 文件会判为 `PASS-ADVISORY`。`datafix=table` 在线修复用例使用专用 helper 判定 `Fixed`、CSV、progress 与二次校验结果。

---

## v4.0.0 回归覆盖分层

v4.0.0 新增能力按稳定性和环境依赖分层纳入脚本：

| 功能 | 默认覆盖位置 | 说明 |
| --- | --- | --- |
| `resume=OFF/ON` | `struct-migration-test-baseline.sh`、`regression-test.sh --with-v4-cases`、`regression-test-columns.sh` | 覆盖配置解析、progress 文件与 fixed 状态弱断言；`ASK` 为交互模式，不纳入默认自动化 |
| `dTypeMappingFile` / `--preview-dtype-mapping` | `struct-migration-test-baseline.sh`、`regression-test-struct-column.sh` | 使用脚本内临时 mapping 文件做 preview smoke；CLI preview 当前会校验 DB 连接，baseline 默认仅在提供 DSN 环境变量时执行 |
| `genRollSQL` / `maxRollRowNum` / `rollFileDir` | `regression-test.sh --with-v4-cases` | 覆盖 `datafix=file` 与 `datafix=table` 生成 rollback SQL |
| `datafix=table` `Fixed` 列 | `regression-test-columns.sh`、`regression-test.sh --with-v4-cases` | 断言终端/CSV `Fixed` 列、合法取值与在线修复后二次收敛 |
| repairDB `splitInsertOnDupKey` | `struct-migration-test-baseline.sh`、`regression-test.sh --with-v4-cases` | 默认做 dry-run/config smoke；真实 duplicate key 拆分需数据库专项环境 |
| 权限预检增强 | README 可选说明 | 需要低权限账号，不默认创建用户或修改权限 |
| SSL 参数 | README 可选说明 | 需要证书和开启 SSL 的 MySQL-family 实例，不默认连接验证 |
| Oracle 类型映射实测 | `regression-test-oracle.sh --with-scenarios` 后续扩展 | 本轮不新增 Oracle fixture，默认只保留现有 scenarios |

建议执行顺序：

```bash
# 1. 无数据库快速基线
bash scripts/struct-migration-test-baseline.sh

# 2. 查看测例列表 / 参数过滤是否正确
bash scripts/regression-test-struct-column.sh --dry-run
bash scripts/regression-test-columns.sh --dry-run
bash scripts/regression-test.sh --dry-run --with-v4-cases

# 3. MySQL 专项回归（按本地端口调整）
bash scripts/regression-test-columns.sh --src-port=3406 --dst-port=3408 --skip-build
bash scripts/regression-test-struct-column.sh --src-port=3406 --dst-port=3408 --skip-build

# 4. 主矩阵 + v4.0.0 小型专项
bash scripts/regression-test.sh --src=mysql80 --dst=mysql84 --mode=data --with-v4-cases --skip-build

# 5. Oracle 可选场景
bash scripts/regression-test-oracle.sh --with-scenarios
```

---

## 1. `regression-test.sh` —— 多源数据库矩阵

**适用场景**：验证 MySQL、MariaDB 各主版本两两之间的校验与修复闭环。

### 常用选项

| 选项 | 含义 |
| --- | --- |
| `--src=label1,label2` | 仅测试指定源（如 `mysql57,mariadb106`） |
| `--dst=label1,label2` | 仅测试指定目标 |
| `--mode=m1,m2` | 仅测试指定模式（`data,struct,trigger,routine`） |
| `--host` / `--user` / `--pass` | 数据库连接参数 |
| `--skip-init` | 跳过 fixture 初始化（假定已导入） |
| `--skip-build` | 跳过二进制编译 |
| `--timeout=SEC` | 单用例超时，默认 600s |
| `--max-rounds=N` | 最大修复轮次，默认 3 |
| `--dry-run` | 仅打印测试矩阵，不执行 |
| `--final-repair` | 回归完成后对每个 src→dst 按 struct→routine→trigger→data 顺序做一次完整修复闭环 |
| `--with-v4-cases` | 在常规矩阵后追加 v4.0.0 data 专项 smoke（resume、rollsql、Fixed、repairDB split dry-run） |
| `--artifacts-dir=PATH` | 自定义输出目录 |

已内置的实例端口（`mysql56=3404`、`mysql57=3405`、`mysql80=3406`、`mysql84=3408`、`mariadb100=3411`、`mariadb105=3407`、`mariadb106=3410`、`mariadb1011=3409`、`mariadb123=3412`）若与本地环境不一致，请直接修改脚本顶部的 `SOURCES`/`TARGETS` 数组。

矩阵规则：MySQL→MySQL 仅允许低版本→高版本；MariaDB→MySQL 仅允许目标为 8.0/8.4；MariaDB→MariaDB 仅允许低版本→高版本；同端口（同实例）自动跳过。

### 示例

```bash
# 全矩阵跑一遍
bash scripts/regression-test.sh

# 只校验 mysql57 → mysql80 的 data 模式
bash scripts/regression-test.sh --src=mysql57 --dst=mysql80 --mode=data

# 跳过编译 + 跳过 fixture 导入，只打印矩阵和 v4 专项列表
bash scripts/regression-test.sh --skip-build --skip-init --dry-run --with-v4-cases

# 跑完回归再做一次完整的修复闭环验证
bash scripts/regression-test.sh --final-repair
```

---

## 2. `regression-test-columns.sh` —— `columns` 选项功能回归

**适用场景**：验证 `columns` 配置（列级校验 / 跨表列名映射 / 简单语法 / advisory 文件等）在 MySQL 端的行为，并覆盖 v4.0.0 `datafix=table` 修复状态输出。

### 必填参数

- `--src-port=PORT`：源端 MySQL 实例端口
- `--dst-port=PORT`：目标端 MySQL 实例端口（必须与源端不同）

### 可选参数

| 选项 | 含义 |
| --- | --- |
| `--host` / `--user` / `--pass` | 数据库连接参数 |
| `--skip-init` / `--skip-build` | 跳过 fixture 导入 / 二进制编译 |
| `--timeout=SEC` | 单用例超时，默认 120s |
| `--artifacts-dir=PATH` | 自定义输出目录 |
| `--dry-run` | 仅打印测例列表 |
| `--enable-oracle` | 附加 `TC-ORA-01` 负向用例，验证 Oracle srcDSN 在 columns 模式下被正确拒绝 |

fixture 使用 `testcase/MySQL-columns-source.sql` 与 `testcase/MySQL-columns-target.sql`，预期覆盖的测例：

| 用例 | 预期 | 场景 |
| --- | --- | --- |
| TC-01-cols-basic-ignore | PASS | 非选中列差异被忽略 |
| TC-02-cols-selected-diff-fix | PASS | 选中列差异修复后收敛 |
| TC-03-cols-source-only-advisory | PASS-ADVISORY | source-only 行生成 advisory 文件 |
| TC-04-cols-simple-syntax | PASS | 简单语法 `columns=score` |
| TC-05-cols-cross-table-mapping | PASS | 跨表列名映射修复后收敛 |
| TC-06-cols-no-pk-ddl-yes | ERROR-EXPECTED | 无主键表→非零退出（预期行为） |
| TC-07-cols-target-only-extra | PASS | target-only 行 + `extraRowsSyncToSource=ON` |
| TC-08-cols-simple-multi-col | PASS | 简单语法多字段 `columns=score,note` |
| TC-09-cols-datafix-table-fixed | PASS | v4.0.0 `datafix=table` 在线修复输出 `Fixed` 列 |
| TC-10-cols-datafix-table-resume-progress | PASS | v4.0.0 `resume=ON` 下 progress 记录 fixed 状态 |
| TC-ORA-01-cols-oracle-stub | ERROR-EXPECTED | 需 `--enable-oracle`，验证 Oracle srcDSN 被拒 |

### 示例

```bash
# 使用 mysql80 做源、mysql84 做目标
bash scripts/regression-test-columns.sh --src-port=3406 --dst-port=3408

# 跳过编译 + 附加 Oracle 负向用例
bash scripts/regression-test-columns.sh --src-port=3406 --dst-port=3408 \
    --skip-build --enable-oracle

# 仅查看测例列表
bash scripts/regression-test-columns.sh --dry-run
```

---

## 3. `regression-test-struct-column.sh` —— struct-column 专项回归

**适用场景**：验证 `TableColumnNameCheck` 相关结构检查、列名大小写、advisory、columnPlan data 预检、源端对象不可见诊断，以及 v4.0.0 `dTypeMappingFile` preview 与非 data 对象在线修复安全策略。

### 必填参数

- `--src-port=PORT`：源端 MySQL 实例端口
- `--dst-port=PORT`：目标端 MySQL 实例端口（必须与源端不同）

### 常用选项

| 选项 | 含义 |
| --- | --- |
| `--host` / `--user` / `--pass` | 数据库连接参数 |
| `--skip-init` / `--skip-build` | 跳过 fixture 导入 / 二进制编译 |
| `--timeout=SEC` | 单用例超时，默认 180s |
| `--artifacts-dir=PATH` | 自定义输出目录 |
| `--dry-run` | 仅打印测例列表 |

新增 v4.0.0 用例：

| 用例 | 预期 | 场景 |
| --- | --- | --- |
| TC-ST-09 | PASS-DDL | columnPlan 列映射豁免，data 预检 `DDL-yes` 显式暴露 |
| TC-ST-10-dtype-preview | PASS | 生成临时 `dTypeMappingFile` 并执行 `--preview-dtype-mapping` |
| TC-ST-11-struct-datafix-table-safe | PASS | `checkObject=struct,datafix=table` 强制导出 fix SQL，再由 repairDB 修复 |

---

## 4. `regression-test-oracle.sh` —— Oracle → MySQL 异构回归

**适用场景**：验证 Oracle 源到 MySQL 8.0/8.4 的 `struct` / `data` 校验与修复链路。`data` 模式会先自动跑一次 `struct` 预修复，使目标端结构收敛后再做数据校验。

### 常用选项

| 选项 | 含义 |
| --- | --- |
| `--src=label` | Oracle 源别名，默认 `oracle11g`（在脚本顶部 `ORACLE_SOURCES` 数组维护 `label\|schema\|dsn`） |
| `--dst=label1,label2` | 目标过滤，仅限 `mysql80` / `mysql84`，默认两者 |
| `--mode=m1,m2` | 仅测试指定模式，默认 `struct,data` |
| `--host` / `--user` / `--pass` | MySQL 目标连接参数 |
| `--init-oracle` | 通过 `sqlplus` 执行 `testcase/Oracle.sql` 初始化 Oracle 源 |
| `--with-scenarios` | 执行 Oracle→MySQL 异构专项 scenarios |
| `--skip-init` / `--skip-build` | 跳过 MySQL fixture 导入 / 二进制编译 |
| `--timeout=SEC` | 单用例超时，默认 600s |
| `--max-rounds=N` | 最大修复轮次，默认 3 |
| `--dry-run` | 仅打印测试矩阵 |
| `--final-repair` | 回归完成后按 struct→data 顺序做一次完整修复闭环 |
| `--artifacts-dir=PATH` | 自定义输出目录 |

**注意**：默认会以 `CGO_ENABLED=1` 重新编译 `gt-checksum` / `repairDB` 以启用 `godror` 驱动，需要本机具备可用的 Oracle Instant Client。若 Instant Client 未安装到系统默认库路径，需设置 `LD_LIBRARY_PATH`，必要时设置 `ORACLE_HOME` / `TNS_ADMIN`。`--skip-build` 时请确保现有二进制已启用 Oracle 驱动。本轮 v4.0.0 默认不新增 Oracle fixture，dtype mapping 真实异构场景建议后续在 `--with-scenarios` 中扩展。

### 示例

```bash
# 默认：oracle11g → mysql80 / mysql84，struct + data
bash scripts/regression-test-oracle.sh

# 仅校验 struct，目标只跑 mysql84
bash scripts/regression-test-oracle.sh --dst=mysql84 --mode=struct

# 使用 sqlplus 初始化 Oracle 源，然后跑完再做完整修复
bash scripts/regression-test-oracle.sh --init-oracle --final-repair

# 新增 Oracle 源：在脚本顶部 ORACLE_SOURCES 追加一行，然后：
bash scripts/regression-test-oracle.sh --src=oracle19c
```

---

## 5. `struct-migration-test-baseline.sh` —— 快速基线与非 DB smoke

**适用场景**：不依赖数据库，快速验证 v4.0.0 关键包、主二进制、dtype mapping preview 与 repairDB 新配置项解析。

默认执行内容：

- `CGO_ENABLED=0 go test -vet=off` 跑关键包：`schemacompat`、`actions`、`dbExec`、`inputArg`、`global`、`cmd/repairDB`、`progress`
- 编译 `gt-checksum` 与 `repairDB`
- 生成临时 `dTypeMappingFile`；若设置 `BASELINE_PREVIEW_SRC_DSN` 与 `BASELINE_PREVIEW_DST_DSN`，执行 `./gt-checksum -c <conf> --preview-dtype-mapping`
- 生成临时 fix SQL 与 `repairDB.conf`，验证 `splitInsertOnDupKey=ON/OFF`、`resume=ON` 可被 `repairDB -dry-run` 解析

示例：

```bash
bash scripts/struct-migration-test-baseline.sh
```

---

## 排查建议

- 首先看 `test-artifacts/<run-id>/report.txt`，FAIL 的用例可进入 `cases/<case-id>/` 查看每轮 `roundN-output.txt`、`roundN-gt-checksum.log` 以及 `fixsql/*.sql`
- 数据库连通性失败通常是端口或账号不符，直接在脚本顶部调整实例表
- 想复用现有二进制或现有 fixture，组合使用 `--skip-build --skip-init` 可以最快迭代
- `--dry-run` 是调试过滤参数（`--src` / `--dst` / `--mode`）是否生效的首选手段
- v4.0.0 `resume=ON` 场景会在 case 目录生成 `gt-checksum-progress-*.json` 或 `.repairDB-progress.json`，重复运行同一 artifacts 目录可能受旧 progress 影响；建议每轮使用独立目录
- `rollFileDir` 非空时会生成 rollback SQL，`datafix=table` 只生成 rollback 文件，不自动执行 rollback
- `Fixed` 合法取值包括 `yes`、`no`、`skipped` 或非适用场景空值；脚本只做弱 schema 断言，不绑定完整终端表格格式
- SSL、低权限账号、Oracle scenarios 等环境敏感用例默认不作为普通开发环境必跑项；缺少环境时应 skip 并说明原因，而不是判产品失败
