# gt-checksum v1.3.0 变更代码审计报告

> 文档版本：2026-03-24
> 分支：`v1.2.5`（基于 commit `d532cfd`）
> 变更范围：本次所有未提交（uncommitted）修改
> 关联需求：[I6KMQF 增加检查结果自动导出为 csv](https://gitee.com/GreatSQL/gt-checksum/issues/I6KMQF)
> 审计目的：提交 Codex 进行专业代码审计，重点关注设计合理性、潜在缺陷与安全风险

---

## 执行摘要

本次变更围绕一个独立特性实现：**校验结果自动导出为 CSV 文件**。同时引入统一运行标识 `RunID` 和终端异常过滤模式 `terminalResultMode`。

变更共涉及 **16 个文件**，新增约 **800 行代码**，删除约 **14 行**，净增 **约 800 行**。

| 模块 | 变更类型 | 文件数 | 净增行数 | 风险等级 |
|------|----------|--------|----------|----------|
| [M1] 配置层与运行标识 | 功能新增 | 4 | +107 | 🟡 低-中 |
| [M2] 结果标准化模型 | 功能新增（新文件） | 2 | +403 | 🟡 低-中 |
| [M3] CSV 导出器 | 功能新增（新文件） | 2 | +397 | 🟡 低-中 |
| [M4] 终端输出过滤 | 功能修改 | 1 | +32 / -14 | 🟢 低 |
| [M5] 主程序接入 | 功能修改 | 1 | +7 | 🟢 低 |
| [M6] 测试数据 | 测试补充 | 2 | +6 | 🟢 低 |
| [M7] 文档同步 | 文档 | 4 | — | 🟢 低 |

---

## M1. 配置层与运行标识

### 1.1 变更文件

| 文件 | 变更类型 |
|------|----------|
| `inputArg/inputInit.go` | 修改 |
| `inputArg/getConf.go` | 修改 |
| `inputArg/checkParameter.go` | 修改 |
| `inputArg/flagHelp.go` | 修改 |

### 1.2 变更内容

#### `inputArg/inputInit.go`

**新增字段（`RulesS` 结构体）：**

```go
// Result export options
ResultExport       string // OFF | csv
ResultFile         string // explicit output path; empty = auto-generate from RunID
TerminalResultMode string // all | abnormal
```

**新增字段（`ConfigParameter` 结构体）：**

```go
// RunID is a stable identifier for this run, generated once at startup (format: YYYYMMDDHHmmss).
RunID                string
CliResultExport      string
CliResultFile        string
CliTerminalResultMode string
```

**`init()` 函数首行生成 RunID：**

```go
rc.RunID = time.Now().Format("20060102150405")
```

#### `inputArg/getConf.go`

在 `secondaryLevelParameterCheck()` 中新增三个参数的读取逻辑，模式与现有 `showActualRows` 完全一致：

- `resultExport`：`strings.ToUpper()` 后匹配 `CSV` / `OFF`，其余回退默认值 `csv`
- `resultFile`：直接 `TrimSpace`，不强制合法性（空值合法）
- `terminalResultMode`：`strings.ToLower()` 后匹配 `all` / `abnormal`

随后执行 CLI override（`CliResultExport`、`CliResultFile`、`CliTerminalResultMode`），覆盖配置文件值。

#### `inputArg/checkParameter.go`

在 `checkPar()` 末尾（连接池设置之后、`NoIndexTableTmpFile` 赋值之前）追加合法性校验：

```go
// resultExport 校验：空值补默认，非法值 os.Exit(1)
if rc.SecondaryL.RulesV.ResultExport != "OFF" && rc.SecondaryL.RulesV.ResultExport != "csv" {
    os.Exit(1)
}
// terminalResultMode 校验：空值补默认，非法值 os.Exit(1)
if rc.SecondaryL.RulesV.TerminalResultMode != "all" && rc.SecondaryL.RulesV.TerminalResultMode != "abnormal" {
    os.Exit(1)
}
```

同时在 debug 日志中输出三个新参数的最终值。

#### `inputArg/flagHelp.go`

版本号从 `1.2.5` 升级为 `1.3.0`，新增三个 CLI flag：

```go
cli.StringFlag{Name: "resultExport",       Destination: &rc.CliResultExport}
cli.StringFlag{Name: "resultFile",         Destination: &rc.CliResultFile}
cli.StringFlag{Name: "terminalResultMode", Destination: &rc.CliTerminalResultMode}
```

### 1.3 设计说明

`RunID` 在 `init()` 的第一行生成（甚至先于 CLI 解析），确保整个进程生命周期内唯一稳定，可关联 CSV 文件名、fixsql 目录名和日志。`getConf.go` 中故意先读配置文件值、再用 CLI 值覆盖，与现有 `showActualRows` 的三层优先级（默认→配置文件→CLI）完全一致。

### 1.4 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R1-01 | `checkPar()` 在连接数据库之后才校验 `resultExport` / `terminalResultMode`，非法值会在已建立连接后才 exit，造成无谓的连接消耗 | 🟡 低 | 可将参数格式校验前移到 `secondaryLevelParameterCheck()` 末尾，在连接数据库前完成；本次设计在连接后校验，与 `showActualRows` 的校验位置保持一致，尚可接受 |
| R1-02 | `RunID` 使用本地时钟 `time.Now()` 生成，不具备全局唯一性；同一秒内多次启动会产生相同 RunID | 🟡 低 | RunID 的用途是文件命名，秒级精度通常足够；若需强唯一性可改用 `time.UnixNano()` 后缀 |
| R1-03 | `CliResultExport = "csv"` 时，`strings.ToLower(override)` 返回 `"csv"`，赋值路径正确；但 `override == "CSV"` 分支下 `rc.SecondaryL.RulesV.ResultExport = strings.ToLower(override)` 会将其变为 `"csv"`，再进入 `if override == "OFF"` 判断为 false，最终结果正确——但逻辑分支冗余，可读性较差 | 🟢 极低 | 建议简化为 `rc.SecondaryL.RulesV.ResultExport = strings.ToLower(strings.TrimSpace(rc.CliResultExport))` |

---

## M2. 结果标准化模型

### 2.1 变更文件

| 文件 | 变更类型 |
|------|----------|
| `actions/result_record.go` | 新增（185 行） |
| `actions/result_record_test.go` | 新增（218 行） |

### 2.2 变更内容

#### `actions/result_record.go`

新增**结果标准化层**，将内部 `Pod` 转换为对外导出格式 `ResultRecord`，是 CSV 导出的规范化数据源。**注意**：终端渲染当前仍直接使用 `Pod`，`ResultRecord` 主要用于 CSV 导出；`ShouldDisplayInTerminal()` 已在 v1.3.0 修订中被终端预过滤器引用，但完整的终端/CSV 双路径统一尚未完成（见 D1/D2）。

**`ResultRecord` 结构体（13 字段）：**

```go
type ResultRecord struct {
    RunID       string
    CheckTime   string
    CheckObject string
    Schema      string
    Table       string   // 非表对象留空
    ObjectName  string   // 统一对象名
    ObjectType  string   // table/procedure/function/trigger/sequence
    IndexColumn string
    Rows        string   // DDL-yes 时置空
    Diffs       string
    Datafix     string
    Mapping     string
    Definer     string
}
```

**核心函数：**

| 函数 | 职责 |
|------|------|
| `BuildResultRecords(m)` | 从全局 `measuredDataPods` 批量转换，统一 `checkTime` 快照 |
| `normalizePodToRecord(m, pod, checkTime)` | 单条转换：处理 `differencesSchemaTable` override、DDL-yes rows 置空 |
| `resolveObjectIdentity(pod)` | 按 `CheckObject` 类型解析 `objectName`/`objectType` |
| `normalizeSchemaObjectName(schema, name)` | 处理三种编码格式：`db1.*:db2.*`、`schema:name`、`schema.name` |
| `resolveMappingForRecord(schema, name, pod)` | 优先 `pod.MappingInfo`，fallback `getSchemaMappings()` |
| `ShouldDisplayInTerminal(record, mode)` | 终端过滤判断：`abnormal` 模式只透出 `yes/DDL-yes/warn-only` |

**关键设计点——`differencesSchemaTable` override 处理：**

```go
// data 模式下，differencesSchemaTable 可能将 DIFFS 提升为 "yes"
if strings.ToLower(pod.CheckObject) == "data" {
    for k := range differencesSchemaTable {
        parts := strings.SplitN(k, "gtchecksum_gtchecksum", 2)
        if len(parts) == 2 && pod.Schema == parts[0] && pod.Table == parts[1] {
            diffs = "yes"
            break
        }
    }
}
```

**DDL-yes Rows 处理：**

```go
// DDL-yes rows 始终置空（与 dataResultRows helper 语义一致）
rows := pod.Rows
if pod.DIFFS == "DDL-yes" {
    rows = ""
}
```

#### `actions/result_record_test.go`

覆盖以下场景（v1.3.0 修订后共 22 个测试用例，含新增的 normalizeCheckObject / resolveEffectiveDiffs 测试）：

| 测试组 | 用例数 | 覆盖点 |
|--------|--------|--------|
| `normalizeSchemaObjectName` | 5 | 点分隔、冒号分隔（有/无 schema）、通配符映射、纯名称 |
| `resolveObjectIdentity` | 6 | data/struct/procedure/function/trigger/sequence |
| `normalizePodToRecord` | 6 | 基础字段映射、DDL-yes rows 置空、routine/function CheckObject 规范化为 routine、routine/trigger 无 Table 字段 |
| `normalizeCheckObject` | 4 | Procedure→routine、Function→routine、小写透传、大写降格 |
| `resolveEffectiveDiffs` | 3 | 非 data 模式透传、data 模式无 override、data 模式 differencesSchemaTable 提升 |
| `ShouldDisplayInTerminal` | 3 | all 模式全显示、abnormal 过滤、未知模式 fallback |

### 2.3 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R2-01 | `BuildResultRecords` 直接读取包级变量 `measuredDataPods`，与 `terminal_result_output.go` 的收集逻辑存在隐式耦合：若 `CheckResultOut()` 未被调用，`measuredDataPods` 将为空，CSV 也为空 | 🟡 低 | 当前调用顺序在 `gt-checksum.go` 中正确（`CheckResultOut` → `BuildResultRecords`），但缺少文档说明。建议在 `BuildResultRecords` 注释中标注"必须在 `CheckResultOut` 之后调用" |
| R2-02 | `normalizeSchemaObjectName` 对 `db1.*:db2.*` 的处理：匹配后 `name` 未被修正（仍包含原始 `db1.*:db2.*` 字符串）直接写入 `ObjectName` 字段，在 CSV 中将显示为映射规则字符串而非实际表名 | 🟡 低 | 此行为与现有终端渲染逻辑保持一致（termianl output 也未修正 name），属设计约束，但 CSV 消费者需了解此语义 |
| R2-03 | `resolveObjectIdentity` 中 `"routine"` case 未作处理（实际 pods 的 `CheckObject` 在 routine 模式下会被设为 `"procedure"` 或 `"function"`，而非 `"routine"`），因此 `routine` 分支理论上不可达，但未添加注释说明 | 🟢 极低 | 建议补充注释说明 "routine" CheckObject 在运行时永远不会出现，已在 getConf.go 中被拆分为 procedure/function |
| R2-04 | `checkTime` 在 `BuildResultRecords` 调用时生成一次（`time.Now()`），所有行共享同一时间戳。若 CSV 导出本身耗时较长（极大结果集），时间戳可能与实际差异较大 | 🟢 极低 | 当前设计为"导出时间"快照，是合理的语义设计；不视为缺陷 |

---

## M3. CSV 导出器

### 3.1 变更文件

| 文件 | 变更类型 |
|------|----------|
| `actions/result_export_csv.go` | 新增（107 行） |
| `actions/result_export_csv_test.go` | 新增（290 行） |

### 3.2 变更内容

#### `actions/result_export_csv.go`

```go
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

func csvHeader() []string                                       // 固定 13 列
func recordToCSVRow(r ResultRecord) []string                    // 字段顺序映射
func ResolveResultFilePath(m *ConfigParameter) string           // 路径解析
func WriteCSVResults(path string, records []ResultRecord) error // 写文件
func ExportResultsIfNeeded(m *ConfigParameter, records []ResultRecord) error // 顶层入口
```

**`WriteCSVResults` 实现要点：**

```go
func WriteCSVResults(path string, records []ResultRecord) error {
    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
    // ...
    f.Write(utf8BOM)              // UTF-8 BOM
    w := csv.NewWriter(f)
    w.Write(csvHeader())          // 固定列头
    for _, r := range records {
        w.Write(recordToCSVRow(r)) // 标准 encoding/csv 转义
    }
    w.Flush()
    return w.Error()
}
```

**`ExportResultsIfNeeded` 实现要点：**

- `resultExport == "OFF"` 时立即返回 `nil`，不创建文件
- `ResolveResultFilePath()` 决定路径：`resultFile` 非空直接用，否则生成 `gt-checksum-result-<RunID>.csv`
- 成功后打印 `"Result exported to: <path>"`

#### `actions/result_export_csv_test.go`

覆盖场景（18 个测试用例）：

| 测试组 | 用例数 | 覆盖点 |
|--------|--------|--------|
| `csvHeader` | 2 | 列数=13、列顺序固定 |
| `recordToCSVRow` | 1 | 字段顺序与 header 一一对应 |
| `ResolveResultFilePath` | 3 | 默认命名（RunID）、显式路径、路径 TrimSpace |
| `WriteCSVResults` | 5 | UTF-8 BOM、header 存在、逗号转义、双引号转义、行数正确 |
| `ExportResultsIfNeeded` | 2 | OFF 不创建文件、csv 创建文件 |

### 3.3 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R3-01 | `WriteCSVResults` 使用 `O_TRUNC` 模式创建文件，若同一秒内重复执行且 `resultFile` 显式指定固定路径，会静默覆盖上次结果 | 🟡 低 | 当前行为与日志文件覆盖行为一致；若需保护历史结果，可添加存在性检测或追加时间戳后缀 |
| R3-02 | 当 `resultFile` 指定的目录不存在时，`os.OpenFile` 返回错误，`ExportResultsIfNeeded` 将错误向上传递，主程序输出 Warning 但不退出。用户若未注意 Warning，可能不知道 CSV 未生成 | 🟡 低 | 建议在 `WriteCSVResults` 中增加 `os.MkdirAll` 自动创建父目录，或在参数校验阶段（`checkParameter.go`）对 `resultFile` 的父目录做预检 |
| R3-03 | CSV 文件权限固定为 `0644`，在多租户或共享目录场景下可能存在信息泄露风险（其他用户可读） | 🟡 低 | 可考虑使用 `0600` 或参考用户 umask，当前 `0644` 与日志文件权限一致，视场景决定 |
| R3-04 | `encoding/csv` 使用 `\n` 换行，在 Windows 环境下 Excel 兼容性通常无问题，但若与某些 Windows 原生工具链配合使用可能需要 `\r\n` | 🟢 极低 | 可在必要时设置 `w.UseCRLF = true`；设计草案明确选择 `\n`，暂不处理 |
| R3-05 | 若 `records` 为空（所有表均被 skip 且未收集），`WriteCSVResults` 仍会生成仅含 header 的 CSV；这是合理行为，但用户可能误以为运行失败 | 🟢 极低 | 可在 `ExportResultsIfNeeded` 中判断 `len(records) == 0` 时打印提示信息 |

---

## M4. 终端输出过滤

### 4.1 变更文件

| 文件 | 变更类型 |
|------|----------|
| `actions/terminal_result_output.go` | 修改（+32 行，-14 行） |

### 4.2 变更内容

在 `CheckResultOut()` 函数内，`measuredDataPods` 收集完成后、`switch m.SecondaryL.RulesV.CheckObject` 之前，新增预过滤块：

```go
terminalPods := measuredDataPods
if m.SecondaryL.RulesV.TerminalResultMode == "abnormal" {
    terminalPods = make([]Pod, 0, len(measuredDataPods))
    for _, p := range measuredDataPods {
        diffs := p.DIFFS
        // data 模式下，differencesSchemaTable 可能将 DIFFS 提升为 "yes"
        if strings.ToLower(p.CheckObject) == "data" {
            for k := range differencesSchemaTable {
                parts := strings.SplitN(k, "gtchecksum_gtchecksum", 2)
                if len(parts) == 2 && p.Schema == parts[0] && p.Table == parts[1] {
                    diffs = "yes"
                    break
                }
            }
        }
        if diffs == "yes" || diffs == "DDL-yes" || diffs == "warn-only" {
            terminalPods = append(terminalPods, p)
        }
    }
}
```

随后将 `switch` 内全部 9 个渲染循环从 `for _, pod := range measuredDataPods` 改为 `for _, pod := range terminalPods`：

- `case "routine"`（1 处）
- `case "struct"`（1 处）
- `case "index"`（1 处，legacy）
- `case "partitions"`（1 处，legacy）
- `case "foreign"`（1 处，legacy）
- `case "func"`（1 处，legacy）
- `case "proc"`（1 处，legacy）
- `case "trigger"`（1 处）
- `case "data"`（2 处，hasMappings 分支各一）

`measuredDataPods` 在函数开头的跳过表收集段（pod 收集逻辑，非渲染逻辑）保持不变，确保不影响 CSV 导出的完整性。

### 4.3 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R4-01 | 预过滤块中对 `data` 模式的 `differencesSchemaTable` override 逻辑与 `normalizePodToRecord` 中的逻辑是**代码重复**（两处独立实现了相同的 key 匹配逻辑），未来维护时若修改一处容易遗漏另一处 | 🟡 低 | 建议将此逻辑提取为包私有 helper `resolveEffectiveDiffs(pod Pod) string`，供两处共同调用 |
| R4-02 | `terminalPods` 为 `abnormal` 模式时新建切片，在结果集极大时（数万行）会有额外内存分配；但与 CSV 全量写出相比，此开销可忽略 | 🟢 极低 | 不需要优化 |
| R4-03 | legacy case（`index`、`partitions`、`foreign`、`func`、`proc`）已在 `getConf.go` 中被重定向到有效值，实际不可达，但仍做了改动；改动本身是正确的，只是有死代码维护成本 | 🟢 极低 | 不属于本次变更引入的问题，长期可考虑清理 |

---

## M5. 主程序接入

### 5.1 变更文件

| 文件 | 变更类型 |
|------|----------|
| `gt-checksum.go` | 修改（+7 行） |

### 5.2 变更内容

在 `CheckResultOut(m)` 之后、`LogMemoryPeakSummary()` 之前插入：

```go
// Export result CSV (honours resultExport=OFF to skip).
resultRecords := actions.BuildResultRecords(m)
if err := actions.ExportResultsIfNeeded(m, resultRecords); err != nil {
    fmt.Printf("Warning: failed to export result file: %v\n", err)
}
```

**调用顺序依赖**：`BuildResultRecords` 依赖 `measuredDataPods`（在 `CheckResultOut` 内填充），因此必须在 `CheckResultOut` 之后调用。当前代码顺序正确。

### 5.3 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R5-01 | CSV 导出失败时只打印 Warning，不影响程序退出码（退出码 0）。若 CI/CD 流水线依赖 CSV 文件存在性做后续处理，会因此静默失败 | 🟡 低 | 设计草案明确"首期不影响主流程退出码"，属已知取舍；建议在文档中明确说明 |
| R5-02 | `BuildResultRecords` 读取的是 `measuredDataPods` 在 `CheckResultOut` 执行完毕时的**最终状态**，而 `terminalPods` 是过滤后的子集；二者之间的不变量（CSV 始终完整）依赖正确的调用顺序，而非类型系统保证 | 🟡 低 | 可通过函数注释或包级文档明确调用约定 |

---

## M6. 测试数据补充

### 6.1 变更文件

| 文件 | 变更类型 |
|------|----------|
| `testcase/MySQL-source.sql` | 修改（+3 行） |
| `testcase/MySQL-target.sql` | 修改（+3 行） |

### 6.2 变更内容

两个 testcase SQL 文件各新增一个 view 定义，用于测试视图对象在校验场景下的行为：

```sql
-- MySQL-source.sql
create view v_teststring as select * from teststring where f1>'3';

-- MySQL-target.sql
create view v_teststring as select * from teststring where f1<='3';
```

两端视图的 WHERE 条件**故意不同**（`>'3'` vs `<='3'`），用于覆盖视图定义不一致的 struct 检测场景。

### 6.3 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R6-01 | view 定义不使用 `IF NOT EXISTS`，且无 `DROP VIEW IF EXISTS` 前置语句，若重复执行 testcase SQL 会报错 | 🟡 低 | 建议添加 `DROP VIEW IF EXISTS v_teststring;` 前置语句，与同文件中 `DROP TABLE IF EXISTS` 保持一致 |
| R6-02 | 两个 testcase 文件的 view 变更与本次 CSV 导出特性**无直接关联**，混入同批变更可能造成审计混淆 | 🟢 极低 | 建议将 testcase 变更单独提交，与功能代码分离 |

---

## M7. 文档同步

### 7.1 变更文件

| 文件 | 变更内容概述 |
|------|-------------|
| `CHANGELOG.md` | v1.3.0 条目：新增 4 条变更说明，含 `#I6KMQF` issue 引用 |
| `README.md` | v1.3.0 关键变化：新增 4 条；快速运行章节：补充 CSV 示例；配置参数节：补充新参数表 |
| `gc-sample.conf` | 新增 `resultExport`、`resultFile`、`terminalResultMode` 三个参数的完整注释说明块 |
| `gt-checksum-manual.md` | 新增独立章节"结果文件导出"（含 CSV 列头说明、参数表、CLI 示例、注意事项）；扩充 CLI 参数说明列表 |

### 7.2 潜在风险

| 风险编号 | 描述 | 等级 | 建议 |
|----------|------|------|------|
| R7-01 | `gc-sample.conf` 中 `resultExport=csv` 默认值为注释形式，用户初次使用时若不清楚默认行为（即不配置 `resultExport` 也会自动生成 CSV），可能感到困惑 | 🟢 极低 | 已在注释中说明默认值，可接受 |
| R7-02 | 手册中 CSV 示例展示了带 BOM 的输出（`head -3` 命令在 Linux 终端会显示 BOM 乱码），可能误导用户 | 🟢 极低 | 建议手册示例改用 `python3 -c "import csv; ..."` 或 Excel 打开截图说明 |

---

## 整体架构评估

### 数据流向图

```
gt-checksum.go
    │
    ├─ CheckResultOut(m)          ← 1. 收集 Pod 并填充 measuredDataPods
    │       │
    │       └─ [terminalResultMode=abnormal] → 过滤 terminalPods → 终端渲染
    │
    ├─ BuildResultRecords(m)      ← 2. 从 measuredDataPods 全量转换为 []ResultRecord
    │       │
    │       └─ normalizePodToRecord() × N
    │               ├─ resolveObjectIdentity()
    │               ├─ normalizeSchemaObjectName()
    │               └─ resolveMappingForRecord()
    │
    └─ ExportResultsIfNeeded(m, records)  ← 3. 按 resultExport 决定是否写 CSV
            │
            └─ WriteCSVResults(path, records)
                    ├─ UTF-8 BOM
                    ├─ csvHeader()
                    └─ recordToCSVRow() × N
```

### 设计优点

1. **低侵入**：核心 checksum 逻辑（compare 引擎、fixsql 生成、repairDB）完全未修改。
2. **向后兼容**：`resultExport` 默认 `csv`（新增行为），但 `terminalResultMode` 默认 `all`（完全保持旧行为）；`resultExport=OFF` 可回退到旧版行为。
3. **稳定契约**：`CheckObject` 字段统一规范化为用户配置值（`routine` 模式下 Procedure/Function 均输出 `routine`），`ObjectType` 字段区分具体类型，CSV 消费方可安全枚举。
4. **测试覆盖**：`result_record.go` 共 22 个单元测试（含 CheckObject 规范化和 resolveEffectiveDiffs）；CSV 导出共 12 个单元测试，覆盖列头、转义、BOM、路径、父目录创建逻辑。

### 主要设计缺陷（需 Codex 重点关注）

| 编号 | 缺陷 | 受影响范围 | 状态 |
|------|------|------------|------|
| D1 | `differencesSchemaTable` override 逻辑重复实现（M4 预过滤 + M2 normalizePodToRecord 各一份） | `terminal_result_output.go` + `result_record.go` | **已修复**：提取为 `resolveEffectiveDiffs(pod Pod) string`，两处均调用此 helper |
| D2 | `BuildResultRecords` 与 `CheckResultOut` 存在隐式调用顺序依赖，类型系统无法保证 | `gt-checksum.go` | **待跟进**：已在 `BuildResultRecords` 函数注释中明确前置条件 |
| D3 | `resultFile` 指向的父目录不存在时 CSV 静默失败 | `result_export_csv.go` | **已修复**：加入 `os.MkdirAll(filepath.Dir(path), 0755)` |
| D4 | testcase 中 view 变更混入功能变更 | `testcase/MySQL-source.sql` / `MySQL-target.sql` | **待跟进**：下次提交时独立拆分 |

---

## 变更文件索引

| 文件路径 | 变更类型 | 所属模块 |
|----------|----------|----------|
| `inputArg/inputInit.go` | 修改 | M1 |
| `inputArg/getConf.go` | 修改 | M1 |
| `inputArg/checkParameter.go` | 修改 | M1 |
| `inputArg/flagHelp.go` | 修改 | M1 |
| `actions/result_record.go` | **新增** | M2 |
| `actions/result_record_test.go` | **新增** | M2 |
| `actions/result_export_csv.go` | **新增** | M3 |
| `actions/result_export_csv_test.go` | **新增** | M3 |
| `actions/terminal_result_output.go` | 修改 | M4 |
| `gt-checksum.go` | 修改 | M5 |
| `testcase/MySQL-source.sql` | 修改 | M6 |
| `testcase/MySQL-target.sql` | 修改 | M6 |
| `CHANGELOG.md` | 修改 | M7 |
| `README.md` | 修改 | M7 |
| `gc-sample.conf` | 修改 | M7 |
| `gt-checksum-manual.md` | 修改 | M7 |

---

## 风险汇总表

| 编号 | 描述 | 等级 | 建议处理 |
|------|------|------|----------|
| R1-01 | `resultExport` 校验位置在连接 DB 之后 | 🟡 低 | 可前移，非紧急 |
| R1-02 | `RunID` 同秒内不唯一 | 🟡 低 | 可改用 `UnixNano` 后缀 |
| R1-03 | CLI override 中 `resultExport` 赋值逻辑冗余 | 🟢 极低 | 代码简化 |
| R2-01 | `BuildResultRecords` 隐式依赖 `CheckResultOut` 调用顺序 | 🟡 低 | 加注释或重构 |
| R2-02 | 映射规则字符串出现在 CSV ObjectName 列 | 🟡 低 | 属设计约束，需文档说明 |
| R2-03 | `"routine"` case 不可达但无注释 | 🟢 极低 | 加注释 |
| R2-04 | CheckTime 为导出时刻快照 | 🟢 极低 | 合理设计，无需处理 |
| R3-01 | `O_TRUNC` 模式覆盖历史结果 | 🟡 低 | 按需决定是否保护 |
| R3-02 | `resultFile` 父目录不存在时 CSV 静默失败 | 🟡 低 | 建议加 MkdirAll |
| R3-03 | CSV 文件权限 `0644` 可被其他用户读取 | 🟡 低 | 视安全要求决定 |
| R3-04 | `\n` 换行 Windows 兼容性 | 🟢 极低 | 当前可接受 |
| R3-05 | 空结果集仍生成 header-only CSV | 🟢 极低 | 可加提示信息 |
| R4-01 | `differencesSchemaTable` override 逻辑重复实现 | 🟡 低 | 提取 helper 函数 |
| R4-02 | `abnormal` 模式额外内存分配 | 🟢 极低 | 无需处理 |
| R4-03 | legacy case 死代码 | 🟢 极低 | 长期可清理 |
| R5-01 | CSV 失败不影响退出码（CI 静默失败） | 🟡 低 | 文档说明或增加 `--fail-on-export-error` 选项 |
| R5-02 | CSV 完整性依赖调用顺序 | 🟡 低 | 同 R2-01 |
| R6-01 | testcase view 无 DROP IF EXISTS 前置 | 🟡 低 | 补充前置语句 |
| R6-02 | testcase 变更混入功能变更 | 🟢 极低 | 独立提交 |
| R7-01 | gc-sample.conf 默认行为可能令用户困惑 | 🟢 极低 | 当前注释已说明 |
| R7-02 | 手册 CSV 示例含 BOM 可能误导用户 | 🟢 极低 | 改用非终端展示方式 |
