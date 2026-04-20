# gt-checksum 回归测试脚本使用说明

本目录下提供三个回归测试脚本，覆盖不同的源/目标数据库组合场景。所有脚本都会自动完成编译、初始化、执行用例、生成报告等步骤，运行产物统一写入 `test-artifacts/<run-id>/`。

| 脚本 | 场景 | 覆盖源 → 目标 | 覆盖模式 |
| --- | --- | --- | --- |
| `regression-test.sh` | MySQL/MariaDB 多版本互测 | MySQL 5.6/5.7/8.0/8.4、MariaDB 10.0/10.5/10.6/10.11/12.3 交叉组合 | `data` / `struct` / `trigger` / `routine` |
| `regression-test-columns.sh` | `columns` 选项（列级校验）功能回归 | 任意一对 MySQL 实例 | `data`（8 个核心用例 + 1 个可选 Oracle 负向） |
| `regression-test-oracle.sh` | Oracle → MySQL 异构校验 | Oracle 11g/19c → MySQL 8.0/8.4 | `struct` / `data` |

---

## 公共约定

- **运行入口**：在项目根目录执行 `bash scripts/<script>.sh [选项]`
- **依赖工具**：`mysql` 客户端、`go`、可选的 `timeout`/`gtimeout`；Oracle 脚本额外需要 `sqlplus`
- **数据库账号默认**：`checksum` / `checksum`，主机 `127.0.0.1`
- **产物目录**：`test-artifacts/<run-id>/`，包含 `report.txt`、`report.json`（部分脚本）、`results.csv`、`cases/<case-id>/` 下每一轮的 `gt-checksum.conf`、`repairDB.conf`、输出和日志
- **退出码**：存在 FAIL/ERROR/TIMEOUT 时返回 1，否则返回 0
- **中断处理**：收到 `Ctrl+C` 会生成部分报告后退出

判定说明：每个用例最多跑 `MAX_REPAIR_ROUNDS+1` 轮，每轮先跑 `gt-checksum` 产生 `fixsql`，再用 `repairDB` 应用修复，直至 `Diffs=no` 判 PASS，或达到上限判 FAIL。VIEW / columns source-only 等仅含注释的 advisory 文件会判为 `PASS-ADVISORY`。

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
| `--artifacts-dir=PATH` | 自定义输出目录 |

已内置的实例端口（`mysql56=3404`、`mysql57=3405`、`mysql80=3406`、`mysql84=3408`、`mariadb100=3411`、`mariadb105=3407`、`mariadb106=3410`、`mariadb1011=3409`、`mariadb123=3412`）若与本地环境不一致，请直接修改脚本顶部的 `SOURCES`/`TARGETS` 数组。

矩阵规则：MySQL→MySQL 仅允许低版本→高版本；MariaDB→MySQL 仅允许目标为 8.0/8.4；MariaDB→MariaDB 仅允许低版本→高版本；同端口（同实例）自动跳过。

### 示例

```bash
# 全矩阵跑一遍
bash scripts/regression-test.sh

# 只校验 mysql57 → mysql80 的 data 模式
bash scripts/regression-test.sh --src=mysql57 --dst=mysql80 --mode=data

# 跳过编译 + 跳过 fixture 导入，只打印矩阵
bash scripts/regression-test.sh --skip-build --skip-init --dry-run

# 跑完回归再做一次完整的修复闭环验证
bash scripts/regression-test.sh --final-repair
```

---

## 2. `regression-test-columns.sh` —— `columns` 选项功能回归

**适用场景**：验证 `columns` 配置（列级校验 / 跨表列名映射 / 简单语法 / advisory 文件等）在 MySQL 端的行为。

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

## 3. `regression-test-oracle.sh` —— Oracle → MySQL 异构回归

**适用场景**：验证 Oracle 源到 MySQL 8.0/8.4 的 `struct` / `data` 校验与修复链路。`data` 模式会先自动跑一次 `struct` 预修复，使目标端结构收敛后再做数据校验。

### 常用选项

| 选项 | 含义 |
| --- | --- |
| `--src=label` | Oracle 源别名，默认 `oracle11g`（在脚本顶部 `ORACLE_SOURCES` 数组维护 `label\|schema\|dsn`） |
| `--dst=label1,label2` | 目标过滤，仅限 `mysql80` / `mysql84`，默认两者 |
| `--mode=m1,m2` | 仅测试指定模式，默认 `struct,data` |
| `--host` / `--user` / `--pass` | MySQL 目标连接参数 |
| `--init-oracle` | 通过 `sqlplus` 执行 `testcase/Oracle.sql` 初始化 Oracle 源 |
| `--skip-init` / `--skip-build` | 跳过 MySQL fixture 导入 / 二进制编译 |
| `--timeout=SEC` | 单用例超时，默认 600s |
| `--max-rounds=N` | 最大修复轮次，默认 3 |
| `--dry-run` | 仅打印测试矩阵 |
| `--final-repair` | 回归完成后按 struct→data 顺序做一次完整修复闭环 |
| `--artifacts-dir=PATH` | 自定义输出目录 |

**注意**：默认会以 `CGO_ENABLED=1` 重新编译 `gt-checksum` / `repairDB` 以启用 `godror` 驱动，需要本机具备可用的 Oracle Instant Client。`--skip-build` 时请确保现有二进制已启用 Oracle 驱动。

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

## 排查建议

- 首先看 `test-artifacts/<run-id>/report.txt`，FAIL 的用例可进入 `cases/<case-id>/` 查看每轮 `roundN-output.txt`、`roundN-gt-checksum.log` 以及 `fixsql/*.sql`
- 数据库连通性失败通常是端口或账号不符，直接在脚本顶部调整实例表
- 想复用现有二进制或现有 fixture，组合使用 `--skip-build --skip-init` 可以最快迭代
- `--dry-run` 是调试过滤参数（`--src` / `--dst` / `--mode`）是否生效的首选手段
