# CSV Result Export 评审报告（第五版）

> 日期：2026-03-24
> 评审对象：v1.3.0 分支，最新提交 `62228da`
> 本版为第四轮修复后完整现状回写，供 Codex 下一轮审计使用

---

## 总体结论

历经三轮审计、四轮代码修复，CSV 结果导出功能已达到生产可用状态。当前实现：

- **功能正确性**：`高`
- **架构可维护性**：`中高`
- **文档与实现一致性**：`高`
- **测试覆盖**：`高`（`actions` 包 58 个测试全部通过，CSV 专项 36 个）

---

## 已验证文件清单

| 文件 | 版本（commit） |
|------|---------------|
| `actions/result_record.go` | `e8ddfe1` |
| `actions/result_export_csv.go` | `3ad5d05` |
| `actions/terminal_result_output.go` | `e8ddfe1` |
| `actions/result_record_test.go` | `c8271f1` |
| `actions/result_export_csv_test.go` | `62228da` |
| `inputArg/getConf.go` | `23cfc70` |
| `inputArg/checkParameter.go` | `e8ddfe1` |
| `inputArg/inputInit.go` | `aeba3b8` |
| `inputArg/flagHelp.go` | `aeba3b8` |
| `gt-checksum.go` | `ae75af9` |
| `gc-sample.conf` | `6b2cd2a` |
| `README.md` | `6b2cd2a` |
| `gt-checksum-manual.md` | `c8271f1` |

已执行校验：

```bash
go test ./actions/... -count=1   # 强制跳过缓存
go build ./actions/... ./inputArg/...
```

结果：

```
ok  gt-checksum/actions  0.682s   (58 个测试，0 失败)
编译成功，无错误
```

---

## 完整实现描述

### 1. 配置层（`inputArg/`）

#### 1.1 新增参数

**`inputArg/inputInit.go`**

```go
// RulesS 新增
ResultExport       string  // "csv" | "OFF"
ResultFile         string  // 自定义输出路径
TerminalResultMode string  // "all" | "abnormal"

// ConfigParameter 新增
RunID                 string  // YYYYMMDDHHmmss，init() 首行生成
CliResultExport       string
CliResultFile         string
CliTerminalResultMode string
```

RunID 生成：`rc.RunID = time.Now().Format("20060102150405")`（精度秒级，同一秒内多次启动会产生相同值）。

**`inputArg/flagHelp.go`**

- 版本号更新为 `1.3.0`
- 新增三个 `cli.StringFlag`：`resultExport`、`resultFile`、`terminalResultMode`

#### 1.2 参数解析（`inputArg/getConf.go`）

配置文件值解析（三层优先级：默认值 → 配置文件 → CLI）：

```go
// 配置文件非法值：透传原始值，由 checkParameter.go 捕获并退出
switch resultExportValue {
case "", "CSV": rc.SecondaryL.RulesV.ResultExport = "csv"
case "OFF":     rc.SecondaryL.RulesV.ResultExport = "OFF"
default:        rc.SecondaryL.RulesV.ResultExport = resultExportValue // checkPar 拦截
}

// CLI 非法值：即时 fail-fast
} else {
    fmt.Fprintf(os.Stderr, "gt-checksum: invalid value for --resultExport: %q (must be OFF or csv)\n", rc.CliResultExport)
    os.Exit(1)
}
```

`terminalResultMode` 同理。

#### 1.3 参数校验（`inputArg/checkParameter.go`）

校验块**位于 DB 建连之前**（`checkParameter.go:114`，DB 建连在 `checkParameter.go:139`）：

```go
// checkPar() 开头，DB 建连之前
rc.SecondaryL.RulesV.ResultExport = strings.TrimSpace(rc.SecondaryL.RulesV.ResultExport)
if rc.SecondaryL.RulesV.ResultExport == "" {
    rc.SecondaryL.RulesV.ResultExport = "csv"
}
if rc.SecondaryL.RulesV.ResultExport != "OFF" && rc.SecondaryL.RulesV.ResultExport != "csv" {
    // os.Exit(1) ← 不触发 DB 连接
}
// terminalResultMode 同理
// ...
tmpDbc := dbExec.DBConnStruct{...}   // ← DB 建连在此之后
srcDB, err := tmpDbc.OpenDB()
```

**完整 fail-fast 行为对照：**

| 场景 | 行为 |
|------|------|
| 配置文件 `resultExport=BAD` | DB 建连前报错退出 |
| 配置文件 `terminalResultMode=BAD` | DB 建连前报错退出 |
| CLI `--resultExport BAD` | `getConf.go` 立即 `stderr + os.Exit(1)` |
| CLI `--terminalResultMode BAD` | `getConf.go` 立即 `stderr + os.Exit(1)` |

---

### 2. 结果标准化层（`actions/result_record.go`）

#### 2.1 ResultRecord 结构体（13 字段）

```go
// ResultRecord is the normalized, export-stable representation of a single check result.
// It is derived from Pod and serves as the canonical model for CSV export. Terminal output
// currently still renders directly from Pod; ResultRecord is partially reused there via
// ShouldDisplayInTerminal() and resolveEffectiveDiffs().
type ResultRecord struct {
    RunID, CheckTime, CheckObject string
    Schema, Table, ObjectName, ObjectType string
    IndexColumn, Rows, Diffs, Datafix string
    Mapping, Definer string
}
```

#### 2.2 核心函数

**`normalizeCheckObject(raw string) string`**

```go
// Pod.CheckObject 内部值 "Procedure"/"Function" → 用户配置值 "routine"
switch strings.ToLower(strings.TrimSpace(raw)) {
case "procedure", "function": return "routine"
default: return strings.ToLower(strings.TrimSpace(raw))
}
```

**`resolveEffectiveDiffs(pod Pod) string`（单一权威实现）**

```go
// differencesSchemaTable override 的唯一实现，三处调用方均使用此函数：
// 1. terminal_result_output.go 预过滤（terminalPods 构建）
// 2. terminal_result_output.go data 渲染（两个循环）
// 3. normalizePodToRecord（CSV 规范化）
func resolveEffectiveDiffs(pod Pod) string {
    if strings.ToLower(pod.CheckObject) != "data" {
        return pod.DIFFS
    }
    for k := range differencesSchemaTable {
        if k == "" { continue }
        parts := strings.SplitN(k, "gtchecksum_gtchecksum", 2)
        if len(parts) == 2 && pod.Schema == parts[0] && pod.Table == parts[1] {
            return "yes"
        }
    }
    return pod.DIFFS
}
```

**字段规则：**

- `Table`：仅 `table`/`sequence` 类型填充，routine/trigger 为空
- `Rows`：`DDL-yes` 时强制置空（与终端 `dataResultRows` 语义一致）
- `CheckObject`：通过 `normalizeCheckObject` 规范化，`routine` 模式下 Procedure/Function 均输出 `routine`

**`ShouldDisplayInTerminal(record ResultRecord, mode string) bool`**

用于 `terminalResultMode=abnormal` 时过滤终端显示行；在终端预过滤和 CSV 路径均已生效。

---

### 3. CSV 导出器（`actions/result_export_csv.go`）

```go
func WriteCSVResults(path string, records []ResultRecord) error {
    // 自动创建父目录
    if dir := filepath.Dir(path); dir != "." && dir != "" {
        if err := os.MkdirAll(dir, 0755); err != nil {
            return fmt.Errorf("result csv: mkdir %q: %w", dir, err)
        }
    }
    // 权限 0600（仅属主可读写）
    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
    // UTF-8 BOM + encoding/csv（13 列固定列头）
}
```

- `resultExport=OFF` 时 `ExportResultsIfNeeded` 直接返回，不写文件
- 导出失败返回 error，主程序打印 Warning，不影响退出码
- 列头固定，列顺序在 v1.3.x 内稳定

---

### 4. 终端输出（`actions/terminal_result_output.go`）

#### 4.1 terminalPods 预过滤

```go
terminalPods := measuredDataPods
if m.SecondaryL.RulesV.TerminalResultMode == "abnormal" {
    terminalPods = make([]Pod, 0, len(measuredDataPods))
    for _, p := range measuredDataPods {
        if ShouldDisplayInTerminal(ResultRecord{Diffs: resolveEffectiveDiffs(p)}, "abnormal") {
            terminalPods = append(terminalPods, p)
        }
    }
}
```

#### 4.2 data 模式渲染

```go
case "data":
    for _, pod := range terminalPods {
        // resolveEffectiveDiffs is the single authoritative implementation of the
        // differencesSchemaTable override logic; do not inline the map iteration here.
        differences := resolveEffectiveDiffs(pod)
        // ...
    }
```

**`differencesSchemaTable` override 调用点统计：**

| 位置 | 用途 | 实现方式 |
|------|------|---------|
| `terminal_result_output.go:243` | terminalPods 预过滤 | `resolveEffectiveDiffs(p)` |
| `terminal_result_output.go:774` | data 渲染（hasMappings=true） | `resolveEffectiveDiffs(pod)` |
| `terminal_result_output.go:789` | data 渲染（hasMappings=false） | `resolveEffectiveDiffs(pod)` |
| `result_record.go:83` | CSV 规范化 | `resolveEffectiveDiffs(pod)` |

三处调用方均通过 `resolveEffectiveDiffs()`，原始 map 遍历逻辑仅在该函数内存在一份。

---

### 5. 主程序接入（`gt-checksum.go`）

```go
actions.CheckResultOut(m)                    // 终端渲染（依赖 measuredDataPods）
resultRecords := actions.BuildResultRecords(m) // 必须在 CheckResultOut 之后调用
if err := actions.ExportResultsIfNeeded(m, resultRecords); err != nil {
    fmt.Printf("Warning: failed to export result file: %v\n", err)
}
actions.LogMemoryPeakSummary()
```

> **已知隐式顺序依赖**：`BuildResultRecords` 读取 `measuredDataPods`，该全局变量由 `CheckResultOut` 内部填充。类型系统无法约束调用顺序，由注释约定。主程序当前调用顺序正确。

---

### 6. 文档同步状态

| 文档 | 更新内容 |
|------|---------|
| `README.md` | v1.3.0 关键变化 4 条、CSV 快速运行示例、参数表 |
| `gc-sample.conf` | 新增 `resultExport`/`resultFile`/`terminalResultMode` 配置块及注释 |
| `gt-checksum-manual.md` | 新增"结果文件导出"章节（含 CSV 列头说明、参数表、使用示例、注意事项）；修正 CLI 参数列表说明；补充 RunID 精度说明、`resultFile` 父目录行为和 0600 权限说明 |
| `CHANGELOG.md` | v1.3.0 条目含 `#I6KMQF` issue 引用 |

---

## 测试覆盖

### `actions/result_record_test.go`（22 个测试函数）

| 测试组 | 用例数 | 覆盖点 |
|--------|--------|--------|
| `normalizeSchemaObjectName` | 5 | 点分隔、冒号分隔（有/无 schema）、通配符映射、纯名称 |
| `resolveObjectIdentity` | 6 | data / struct / procedure / function / trigger / sequence |
| `normalizePodToRecord` | 6 | 基础字段映射、DDL-yes rows 置空、Procedure→routine、Function→routine、routine/trigger 无 Table |
| `normalizeCheckObject` | 4 | Procedure→routine、Function→routine、小写透传、大写降格 |
| `resolveEffectiveDiffs` | 3 | 非 data 透传、data 无 override、data differencesSchemaTable 提升 |
| `ShouldDisplayInTerminal` | 3 | all 全显示、abnormal 过滤、未知模式 fallback |

### `actions/result_export_csv_test.go`（14 个测试函数）

| 测试组 | 用例数 | 覆盖点 |
|--------|--------|--------|
| `csvHeader` | 2 | 列头数量、列名稳定性 |
| `recordToCSVRow` | 1 | 字段顺序与列头对齐 |
| `ResolveResultFilePath` | 3 | 默认命名、自定义路径、TrimSpace |
| `WriteCSVResults` | 7 | UTF-8 BOM、列头存在、逗号转义、引号转义、行数正确、**父目录自动创建**、**0600 文件权限** |
| `ExportResultsIfNeeded` | 2 | OFF 不生成文件、csv 正确创建 |

**说明：**

- CSV 结果导出专项测试：`22 + 14 = 36` 个
- `actions` 包总量：58 个（含既有能力测试，不等于 CSV 专项覆盖数）
- 全量运行：`ok gt-checksum/actions 0.682s`，0 失败

---

## 已知遗留问题

以下问题经评估均不影响 v1.3.0 的功能正确性和外部接口稳定性，列为后续迭代事项。

### L-a：终端与 CSV 仍为两套渲染路径

- 终端渲染直接操作 `Pod`，CSV 导出基于 `ResultRecord`
- `resolveEffectiveDiffs()` 和 `ShouldDisplayInTerminal()` 已将两条路径在 override 和过滤逻辑上收敛
- 后续可将终端渲染切换到 `ResultRecord`，彻底统一数据源

### L-b：`BuildResultRecords` 与 `CheckResultOut` 存在隐式调用顺序依赖

- `BuildResultRecords(m)` 读取包级变量 `measuredDataPods`，必须在 `CheckResultOut` 之后调用
- 函数注释已明确此前置条件，主程序调用顺序当前正确
- 后续可将 `measuredDataPods` 改为显式参数传递

### L-c：RunID 精度为秒级，不具备严格唯一性

- 同一秒内多次启动产生相同 RunID，导致 CSV 文件名冲突（新覆盖旧）
- 文档已说明此限制
- 后续可附加 PID 或随机后缀

### L-d：`actions` 包级全局状态（`measuredDataPods`、`differencesSchemaTable`）无 reset 函数

- 单次 CLI 进程中不影响正确性
- 同进程多轮执行或集成测试复用 `main` 逻辑时存在状态污染风险
- 后续可新增 `actions.ResetResultState()`

---

## CSV 字段稳定性承诺

| 承诺级别 | 内容 |
|---------|------|
| **稳定** | 全部 13 列列名和列顺序在 v1.3.x 内不变 |
| **值域稳定** | `CheckObject`：`data / struct / routine / trigger` |
| **值域稳定** | `Diffs`：`yes / no / DDL-yes / warn-only / collation-mapped` |
| **值域稳定** | `ObjectType`：`table / procedure / function / trigger / sequence` |
| **可为空** | `Table`、`IndexColumn`、`Rows`、`Mapping`、`Definer`：未使用时为空字符串，列始终存在 |

---

## 安全性说明

| 项目 | 当前值 |
|------|-------|
| CSV 文件权限 | `0600`（仅属主可读写） |
| 父目录创建权限 | `0755` |
| 字段转义 | 标准库 `encoding/csv`，自动处理逗号、引号、换行 |
| BOM | `0xEF 0xBB 0xBF`，确保 Excel 正确识别 UTF-8 |
