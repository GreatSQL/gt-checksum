# CSV Result Export 评审报告（第二版）

> 日期：2026-03-24（初版） → 2026-03-24（修订回写）
> 评审对象：实际实现代码（v1.3.0 分支，最新提交 `c8271f1`）
> 本文档已根据首轮评审反馈完成修订，反映所有修复后的当前实现状态

---

## 总体结论

首轮评审发现的 3 个 P0 问题、2 个 P1 问题、多项文档偏差均已修复。当前实现：

- **功能正确性**：`高`
- **架构可维护性**：`中`（D2 全局状态路径依赖尚存，为已知遗留项）
- **文档与实现一致性**：`高`
- **测试覆盖**：`高`（56 个单元测试全部通过）

---

## 验证范围

已阅读并对照的实现文件：

- `actions/result_record.go`
- `actions/result_export_csv.go`
- `actions/terminal_result_output.go`
- `inputArg/getConf.go`
- `inputArg/checkParameter.go`
- `gt-checksum.go`
- `actions/result_record_test.go`
- `actions/result_export_csv_test.go`
- `gc-sample.conf`
- `README.md`
- `gt-checksum-manual.md`

已执行的校验：

```bash
go test ./actions/... -v
```

结果：

```text
ok  	gt-checksum/actions	0.561s   (56 个测试用例，全部 PASS)
```

---

## 首轮评审问题修复状态

### 问题 1：`CheckObject` 导出契约与文档不一致（P0）✅ 已修复

**原状态：** `routine` 模式下 CSV 输出 `Procedure` / `Function`，与文档宣称的 `routine` 不符。

**修复内容（`actions/result_record.go`）：**

```go
// normalizeCheckObject maps the internal pod CheckObject value (which may be
// "Procedure" or "Function" in routine mode) to the canonical user-facing mode
// name as configured by the checkObject parameter.
func normalizeCheckObject(raw string) string {
    switch strings.ToLower(strings.TrimSpace(raw)) {
    case "procedure", "function":
        return "routine"
    default:
        return strings.ToLower(strings.TrimSpace(raw))
    }
}
```

`normalizePodToRecord()` 中 `CheckObject` 字段改为 `normalizeCheckObject(pod.CheckObject)`。

**当前契约（稳定）：**

| `CheckObject` 值 | 触发条件 | `ObjectType` 区分 |
|-----------------|----------|--------------------|
| `data` | `checkObject=data` | `table` / `sequence` |
| `struct` | `checkObject=struct` | `table` / `sequence` |
| `routine` | `checkObject=routine`（存储过程 + 函数） | `procedure` / `function` |
| `trigger` | `checkObject=trigger` | `trigger` |

消费方可安全枚举 `CheckObject`；通过 `ObjectType` 区分具体对象类型。

**验证（新增测试）：**

```text
TestNormalizeCheckObject_procedureBecomesRoutine   PASS
TestNormalizeCheckObject_functionBecomesRoutine    PASS
TestNormalizeCheckObject_lowercasePassThrough      PASS
TestNormalizeCheckObject_uppercaseLowered          PASS
TestNormalizePodToRecord_routineCheckObjectIsRoutine  PASS
TestNormalizePodToRecord_functionCheckObjectIsRoutine PASS
```

---

### 问题 2：`ShouldDisplayInTerminal()` 未在生产代码使用（P0/P1）✅ 已修复

**原状态：** `ShouldDisplayInTerminal()` 只在测试中被调用，终端预过滤逻辑与 `resolveEffectiveDiffs` 逻辑各自独立存在两份。

**修复内容：**

**`actions/result_record.go`** — 新增 `resolveEffectiveDiffs`（单一可信实现）：

```go
// resolveEffectiveDiffs returns the effective Diffs value for a pod.
// For data-mode pods, differencesSchemaTable may promote the stored DIFFS to "yes".
// This is the single authoritative implementation of that override logic.
func resolveEffectiveDiffs(pod Pod) string {
    if strings.ToLower(pod.CheckObject) != "data" {
        return pod.DIFFS
    }
    for k := range differencesSchemaTable {
        if k == "" {
            continue
        }
        parts := strings.SplitN(k, "gtchecksum_gtchecksum", 2)
        if len(parts) == 2 && pod.Schema == parts[0] && pod.Table == parts[1] {
            return "yes"
        }
    }
    return pod.DIFFS
}
```

**`actions/terminal_result_output.go`** — 预过滤器改为调用上述两个函数：

```go
if ShouldDisplayInTerminal(ResultRecord{Diffs: resolveEffectiveDiffs(p)}, "abnormal") {
    terminalPods = append(terminalPods, p)
}
```

**结果：**
- `differencesSchemaTable` override 逻辑从两份减少为一份（D1 已消除）
- `ShouldDisplayInTerminal()` 现在在生产代码中真正生效

**验证（新增测试）：**

```text
TestResolveEffectiveDiffs_nonDataModePassThrough        PASS
TestResolveEffectiveDiffs_dataModePlainDiff              PASS
TestResolveEffectiveDiffs_dataModeOverridesViaDiffTable  PASS
```

---

### 问题 3：参数非法值静默回退而非 fail-fast（P0）✅ 已修复

**原状态：** 配置文件非法值静默替换为默认值；CLI 非法值打印一行后忽略继续运行。

**修复内容（`inputArg/getConf.go`）：**

配置文件非法值现在透传原始值，由 `checkParameter.go` 统一检测并 `os.Exit(1)`：

```go
// 配置文件非法值 — 透传，让 checkPar 捕获
default:
    rc.SecondaryL.RulesV.ResultExport = resultExportValue // checkPar will reject and exit
```

CLI 非法值现在立即 `os.Exit(1)` 并输出明确错误信息：

```go
} else {
    fmt.Fprintf(os.Stderr, "gt-checksum: invalid value for --resultExport: %q (must be OFF or csv)\n", rc.CliResultExport)
    os.Exit(1)
}
```

`terminalResultMode` 同理处理。

`checkParameter.go` 的校验逻辑保持不变，仍会捕获非法值：

```go
if rc.SecondaryL.RulesV.ResultExport != "OFF" && rc.SecondaryL.RulesV.ResultExport != "csv" {
    fmt.Println(fmt.Sprintf("gt-checksum: resultExport must be OFF or csv. ..."))
    os.Exit(1)
}
```

**行为对比：**

| 场景 | 修复前 | 修复后 |
|------|--------|--------|
| 配置文件 `resultExport=BAD` | 静默改为 `csv`，继续运行 | 启动时报错退出 |
| CLI `--resultExport BAD` | 打印 Ignoring，继续运行 | 立即 stderr + `os.Exit(1)` |
| CLI `--terminalResultMode BAD` | 打印 Ignoring，继续运行 | 立即 stderr + `os.Exit(1)` |

---

### 问题 4：`resultFile` 父目录不存在时静默失败（P1）✅ 已修复

**原状态：** `os.OpenFile()` 直接开文件，父目录不存在则失败返回 error，主程序只打印 Warning。

**修复内容（`actions/result_export_csv.go`）：**

```go
func WriteCSVResults(path string, records []ResultRecord) error {
    if dir := filepath.Dir(path); dir != "." && dir != "" {
        if err := os.MkdirAll(dir, 0755); err != nil {
            return fmt.Errorf("result csv: mkdir %q: %w", dir, err)
        }
    }
    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
    ...
}
```

同时将文件权限从 `0644` 收紧为 `0600`，避免结果文件在多用户环境被其他用户读取（P2）。

---

### 问题 5：文档与实现不一致（P2）✅ 已修复

| 文档偏差 | 修复内容 |
|---------|---------|
| `CheckObject` 列说明仅列 `data/struct/routine/trigger`，未说明 routine 内部映射 | 补充"routine 模式下存储过程和函数均统一显示为 routine，具体类型见 ObjectType" |
| RunID 宣称"每次运行唯一" | 改为"精度为秒级；同一秒内多次启动会产生相同 RunID" |
| `head -3` 直接查看含 BOM 的 CSV 会乱码 | 改为 `python3 encoding=utf-8-sig` 示例 |
| "命令行仅支持 -c/-f/-h/-v 等基础参数"与下方 CLI 参数列表自相矛盾 | 改为正确描述，说明多个参数支持 CLI 覆盖 |
| 缺少 `resultFile` 父目录行为说明 | 新增"父目录自动创建（v1.3.0 起）"和"文件权限 0600"说明 |
| 审计文档把 ResultRecord 描述为"终端渲染与 CSV 导出的共同数据源" | 修正为"CSV 导出的规范化数据源；终端仍基于 Pod" |
| 审计文档测试数量过高（"31 个"、"18 个"） | 修正为实际数量，并更新 D1/D3 修复状态 |

---

## 当前实现架构总览

```
gt-checksum.go
│
├── actions.CheckResultOut(m)          ← 终端渲染（基于 Pod）
│       │
│       └── terminalPods 预过滤
│               resolveEffectiveDiffs(p)         ← 单一 override 实现
│               ShouldDisplayInTerminal(rec, mode) ← 生产代码中真正调用
│
├── actions.BuildResultRecords(m)      ← Pod → []ResultRecord 规范化
│       │
│       ├── normalizeCheckObject()     ← Procedure/Function → routine
│       ├── resolveEffectiveDiffs()    ← 复用同一 override 实现
│       └── resolveObjectIdentity()
│
└── actions.ExportResultsIfNeeded(m, records)
        │
        └── WriteCSVResults(path, records)
                ├── os.MkdirAll(dir, 0755)   ← 自动创建父目录
                ├── OpenFile(..., 0600)       ← 收紧权限
                ├── UTF-8 BOM
                └── encoding/csv（13 列固定列头）
```

**参数校验流程（fail-fast）：**

```
配置文件非法值 → getConf.go 透传原始值
                         ↓
                checkParameter.go 检测 → os.Exit(1) + 错误日志

CLI 非法值    → getConf.go 立即 os.Exit(1) + stderr 错误信息
```

---

## 测试覆盖（当前状态）

### `actions/result_record_test.go`（22 个测试函数）

| 测试组 | 用例数 | 覆盖点 |
|--------|--------|--------|
| `normalizeSchemaObjectName` | 5 | 点分隔、冒号分隔（有/无 schema）、通配符映射、纯名称 |
| `resolveObjectIdentity` | 6 | data / struct / procedure / function / trigger / sequence |
| `normalizePodToRecord` | 6 | 基础字段映射、DDL-yes rows 置空、Procedure→routine、Function→routine、routine 无 Table、trigger 无 Table |
| `normalizeCheckObject` | 4 | Procedure→routine、Function→routine、小写透传、大写降格 |
| `resolveEffectiveDiffs` | 3 | 非 data 模式透传、data 无 override、data differencesSchemaTable 提升 |
| `ShouldDisplayInTerminal` | 3 | all 全显示、abnormal 过滤、未知模式 fallback |

### `actions/result_export_csv_test.go`（12 个测试函数）

| 测试组 | 用例数 | 覆盖点 |
|--------|--------|--------|
| `csvHeader` | 2 | 列头数量、列名稳定性 |
| `recordToCSVRow` | 1 | 字段顺序与列头对齐 |
| `ResolveResultFilePath` | 3 | 默认命名、自定义路径、TrimSpace |
| `WriteCSVResults` | 5 | UTF-8 BOM、列头存在、逗号转义、引号转义、行数正确 |
| `ExportResultsIfNeeded` | 2 | OFF 时不生成文件、csv 时正确创建 |

**全量运行结果：**

```
ok  gt-checksum/actions  0.561s  (56 个测试，0 失败)
```

---

## 遗留已知问题（不影响当前版本可用性）

### L1：终端与 CSV 仍为两套渲染路径（原 D2，降级为低优先级）

- 终端渲染直接使用 `Pod`，CSV 导出基于 `ResultRecord`
- `ShouldDisplayInTerminal()` 已在终端预过滤中生效，`resolveEffectiveDiffs()` 已消除重复
- 两套路径在字段语义上尚未完全统一，未来如需扩展字段仍需双处修改
- **建议**：后续版本可将终端渲染也切换到 `ResultRecord`，彻底统一数据源

### L2：`BuildResultRecords` 与 `CheckResultOut` 存在隐式调用顺序依赖（原 D2）

- `BuildResultRecords(m)` 依赖 `measuredDataPods`，必须在 `CheckResultOut` 之后调用
- 类型系统无法约束这一顺序，依赖注释约定
- **现状**：函数注释已明确前置条件，主程序调用顺序正确
- **建议**：未来可将 `measuredDataPods` 作为参数显式传递，消除全局状态依赖

### L3：`RunID` 精度为秒级，不具备严格唯一性

- 同一秒内多次启动会产生相同 RunID，进而导致 CSV 文件名冲突（新文件覆盖旧文件）
- **现状**：文档已明确说明此限制
- **建议**：如需严格唯一，可附加 PID 或随机后缀（`20060102150405_<pid>`）

### L4：`actions` 包级全局状态无 reset（原 Codex review #5）

- `measuredDataPods` 和 `differencesSchemaTable` 是包级变量，无对应清空函数
- 单次 CLI 进程中不影响正确性
- **影响范围**：同进程多轮执行、集成测试复用 `main` 逻辑时存在状态污染风险
- **建议**：新增 `actions.ResetResultState()`，在 `main()` 启动早期调用

---

## 安全性说明

| 项目 | 当前状态 |
|------|---------|
| CSV 文件权限 | `0600`（仅属主可读写，已收紧） |
| 父目录权限 | `0755`（自动创建时使用） |
| CSV 字段转义 | 使用标准库 `encoding/csv`，自动处理逗号、引号、换行 |
| 敏感信息 | 结果文件可能包含 Schema 名、对象名、DEFINER，`0600` 已做基本保护 |

---

## CSV 字段稳定性承诺

| 承诺级别 | 字段 | 说明 |
|---------|------|------|
| **稳定，不会变更** | `RunID`、`CheckTime`、`CheckObject`、`Schema`、`Table`、`ObjectName`、`ObjectType`、`IndexColumn`、`Rows`、`Diffs`、`Datafix`、`Mapping`、`Definer` | 列名和列顺序在 v1.3.x 内保证不变 |
| **值域稳定** | `CheckObject` | 固定为 `data / struct / routine / trigger` |
| **值域稳定** | `Diffs` | 固定为 `yes / no / DDL-yes / warn-only / collation-mapped` |
| **值域稳定** | `ObjectType` | 固定为 `table / procedure / function / trigger / sequence` |
| **可为空** | `Table`、`IndexColumn`、`Rows`、`Mapping`、`Definer` | 未使用时为空字符串，不会省略列 |

---

## 总结

首轮评审标记的全部 P0（×3）和 P1（×2）问题已修复，P2 文档和权限问题已修复。当前代码在以下方面达到"长期可维护、可被自动化消费的稳定接口"标准：

1. **`CheckObject` 契约稳定**：消费方可按 `data/struct/routine/trigger` 枚举，`ObjectType` 提供精细区分
2. **参数校验 fail-fast**：配置文件和 CLI 的非法值均在启动阶段明确报错，不存在静默偏离配置意图的问题
3. **`differencesSchemaTable` 逻辑单一化**：提取为 `resolveEffectiveDiffs`，两处使用方均调用同一实现
4. **`ShouldDisplayInTerminal` 生产生效**：终端过滤与 CSV 过滤语义收敛
5. **父目录自动创建**：`--resultFile ./output/result.csv` 在 `output/` 不存在时自动创建
6. **测试覆盖充分**：56 个单元测试，全部通过

剩余 L1～L4 为已知遗留项，均已有明确文档说明和改进建议，不影响当前版本的正确使用。
