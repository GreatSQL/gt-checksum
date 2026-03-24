# CSV Result Export 评审报告（第四版）

> 日期：2026-03-24（初版） → 2026-03-24（修订回写） → 2026-03-24（复审回写） → 2026-03-24（第三轮修复回写）
> 评审对象：实际实现代码（v1.3.0 分支，最新提交 `62228da`）
> 本版根据第三轮复审（第三版报告 M1/M2/L1/L2 四项发现）完成对应代码修复后回写

---

## 总体结论

第三版报告发现的 4 项问题（2 中优先级 + 2 低优先级）均已修复。当前实现：

- **功能正确性**：`高`
- **架构可维护性**：`中高`（`differencesSchemaTable` override 逻辑已完全统一；终端/CSV 双路径为已知遗留，不影响当前正确性）
- **文档与实现一致性**：`高`
- **测试覆盖**：`高`（`actions` 包 58 个测试全部通过，CSV 专项 36 个，含目录创建和权限断言）

---

## 第三版报告问题修复状态

### M1：`differencesSchemaTable` override 逻辑在终端 data 渲染中仍有重复（中优先级）✅ 已修复

**原状态：** `terminal_result_output.go:767-804` 中有两个手工遍历 `differencesSchemaTable` 的循环，与 `resolveEffectiveDiffs()` 形成重复实现。

**修复内容（`actions/terminal_result_output.go`）：**

```go
// 修复前
for k, _ := range differencesSchemaTable {
    if k != "" {
        KI := strings.Split(k, "gtchecksum_gtchecksum")
        if pod.Schema == KI[0] && pod.Table == KI[1] {
            differences = "yes"
        }
    }
}

// 修复后
// resolveEffectiveDiffs is the single authoritative implementation of the
// differencesSchemaTable override logic; do not inline the map iteration here.
differences := resolveEffectiveDiffs(pod)
```

两处循环均已替换，data 模式下 `differencesSchemaTable` override 现在**全程仅有一处实现**（`result_record.go:62-76`）。D1 设计缺陷完全消除。

---

### M2：配置文件非法值校验发生在 DB 建连之后，不是真正的 fail-fast（中优先级）✅ 已修复

**原状态：** `resultExport`/`terminalResultMode` 合法性检查在 `checkParameter.go:364`，而 DB 连接在 `checkParameter.go:114` 就已建立，导致非法配置文件值仍会触发数据库连接尝试。

**修复内容（`inputArg/checkParameter.go`）：**

```go
func (rc *ConfigParameter) checkPar() {
    // ...DSN 驱动名规范化...

    // Validate result export parameters early — before any DB connections — so that
    // misconfigured values fail fast without triggering expensive side effects.
    rc.SecondaryL.RulesV.ResultExport = strings.TrimSpace(rc.SecondaryL.RulesV.ResultExport)
    if rc.SecondaryL.RulesV.ResultExport == "" {
        rc.SecondaryL.RulesV.ResultExport = "csv"
    }
    if rc.SecondaryL.RulesV.ResultExport != "OFF" && rc.SecondaryL.RulesV.ResultExport != "csv" {
        // os.Exit(1) ← 在 DB 建连之前
    }
    // terminalResultMode 同理...

    tmpDbc := dbExec.DBConnStruct{...}  // ← DB 连接在此之后
    srcDB, err := tmpDbc.OpenDB()
    ...
```

原位置（`checkPar()` 末尾）的重复校验块已移除。

**行为对比（最终状态）：**

| 场景 | 当前行为 |
|------|---------|
| 配置文件 `resultExport=BAD` | DB 建连之前即报错退出（真正的 fail-fast） |
| CLI `--resultExport BAD` | `getConf.go` 立即 `stderr + os.Exit(1)`，早于任何参数解析后步骤 |
| 配置文件 `terminalResultMode=BAD` | DB 建连之前即报错退出 |
| CLI `--terminalResultMode BAD` | `getConf.go` 立即 `stderr + os.Exit(1)` |

---

### L1：`result_record.go` 注释仍声称 ResultRecord 是"终端+CSV 单一数据源"（低优先级）✅ 已修复

**原状态：** `actions/result_record.go:10-13` 注释：
```go
// It is derived from Pod and serves as the single source of truth for both terminal output
// and CSV export.
```

**修复后：**
```go
// ResultRecord is the normalized, export-stable representation of a single check result.
// It is derived from Pod and serves as the canonical model for CSV export. Terminal output
// currently still renders directly from Pod; ResultRecord is partially reused there via
// ShouldDisplayInTerminal() and resolveEffectiveDiffs(). Fields are intentionally stable
// across all checkObject modes; unused fields are left empty rather than omitted so that
// CSV column order never changes.
```

注释现在准确描述当前事实：ResultRecord 是 CSV 的规范模型，终端通过两个 helper 函数部分复用其语义。

---

### L2：测试覆盖结论偏宽，缺少目录创建和权限专项测试（低优先级）✅ 已修复

**原状态：** `WriteCSVResults` 的 `os.MkdirAll` 和 `0600` 权限行为无专项测试。

**新增测试（`actions/result_export_csv_test.go`）：**

```go
func TestWriteCSVResults_autoCreatesParentDir(t *testing.T) {
    // resultFile 指向不存在的子目录，WriteCSVResults 应自动创建
    dir := filepath.Join(t.TempDir(), "subdir", "nested")
    path := filepath.Join(dir, "result.csv")
    if err := WriteCSVResults(path, nil); err != nil {
        t.Fatalf("WriteCSVResults failed with non-existent parent dir: %v", err)
    }
    // 验证文件存在
}

func TestWriteCSVResults_filePermission0600(t *testing.T) {
    // 验证输出文件权限为 0600
    const want = os.FileMode(0600)
    if got := info.Mode().Perm(); got != want {
        t.Errorf("file permission = %04o, want %04o", got, want)
    }
}
```

两个测试均通过。

---

## 验证范围

最新提交后的测试结果：

```bash
go test ./actions/...
```

```text
ok  gt-checksum/actions  0.496s   (58 个测试用例，全部 PASS)
```

---

## 当前实现架构总览

```
gt-checksum.go
│
├── actions.CheckResultOut(m)          ← 终端渲染（基于 Pod）
│       │
│       └── terminalPods 预过滤
│               resolveEffectiveDiffs(p)           ← 复用单一 override 实现
│               ShouldDisplayInTerminal(rec, mode) ← 生产代码中真正调用
│       │
│       └── data 模式表格渲染
│               resolveEffectiveDiffs(pod)          ← 复用同一实现（D1 完全消除）
│
├── actions.BuildResultRecords(m)      ← Pod → []ResultRecord 规范化
│       │
│       ├── normalizeCheckObject()     ← Procedure/Function → routine
│       ├── resolveEffectiveDiffs()    ← 单一权威实现（三处调用方均使用此函数）
│       └── resolveObjectIdentity()
│
└── actions.ExportResultsIfNeeded(m, records)
        │
        └── WriteCSVResults(path, records)
                ├── os.MkdirAll(dir, 0755)   ← 自动创建父目录
                ├── OpenFile(..., 0600)       ← 权限 0600
                ├── UTF-8 BOM
                └── encoding/csv（13 列固定列头）
```

**参数校验流程（当前实际行为）：**

```
配置文件非法值 → getConf.go 透传原始值
                         ↓
                checkParameter.go 前段（DB 建连之前）检测 → os.Exit(1)
                                                           ↑
                                                    真正的 fail-fast

CLI 非法值    → getConf.go 立即 os.Exit(1) + stderr（早于 checkPar）
```

---

## 测试覆盖（当前状态）

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
| `WriteCSVResults` | 7 | UTF-8 BOM、列头存在、逗号转义、引号转义、行数正确、**父目录自动创建**、**0600 权限** |
| `ExportResultsIfNeeded` | 2 | OFF 不生成文件、csv 正确创建 |

**说明：**

- `22 + 14 = 36` 个是 CSV 结果导出相关的专项测试
- `actions` 包总量 58 个（含既有能力测试），不等于 CSV 专项覆盖数
- 仍未覆盖的场景：`CheckResultOut()` → `BuildResultRecords()` 顺序依赖的集成行为（属遗留 L2 范畴）

**全量运行结果：**

```
ok  gt-checksum/actions  0.496s  (58 个测试，0 失败)
```

---

## 遗留已知问题（不影响当前版本可用性）

### L-a：终端与 CSV 仍为两套渲染路径

- 终端渲染直接使用 `Pod`，CSV 导出基于 `ResultRecord`
- 预过滤器和 data 渲染均已调用 `resolveEffectiveDiffs()`，但终端完整渲染仍未切换到 `ResultRecord`
- **影响**：两套路径在字段语义上尚未完全统一，未来如需扩展字段仍需双处修改
- **建议**：后续版本将终端渲染也切换到 `ResultRecord`，彻底统一数据源

### L-b：`BuildResultRecords` 与 `CheckResultOut` 存在隐式调用顺序依赖

- `BuildResultRecords(m)` 依赖 `measuredDataPods`，必须在 `CheckResultOut` 之后调用
- 类型系统无法约束顺序，依赖注释约定；主程序调用顺序当前正确
- **建议**：将 `measuredDataPods` 改为显式参数传递，消除全局状态隐式依赖

### L-c：`RunID` 精度为秒级，不具备严格唯一性

- 同一秒内多次启动会产生相同 RunID 和 CSV 文件名（新文件覆盖旧文件）
- 文档已说明此限制
- **建议**：如需严格唯一，可附加 PID 或随机后缀

### L-d：`actions` 包级全局状态无 reset

- `measuredDataPods`、`differencesSchemaTable` 是包级变量，无对应清空函数
- 单次 CLI 进程中不影响正确性；同进程多轮执行或集成测试复用时存在状态污染风险
- **建议**：新增 `actions.ResetResultState()`，在 `main()` 启动早期调用

---

## CSV 字段稳定性承诺

| 承诺级别 | 字段 | 说明 |
|---------|------|------|
| **稳定，不会变更** | 全部 13 列列名和列顺序 | 在 v1.3.x 内保证不变 |
| **值域稳定** | `CheckObject` | 固定为 `data / struct / routine / trigger` |
| **值域稳定** | `Diffs` | 固定为 `yes / no / DDL-yes / warn-only / collation-mapped` |
| **值域稳定** | `ObjectType` | 固定为 `table / procedure / function / trigger / sequence` |
| **可为空** | `Table`、`IndexColumn`、`Rows`、`Mapping`、`Definer` | 未使用时为空字符串，不会省略列 |

---

## 安全性说明

| 项目 | 当前状态 |
|------|---------|
| CSV 文件权限 | `0600`（仅属主可读写） |
| 父目录权限 | `0755`（自动创建时使用） |
| CSV 字段转义 | 使用标准库 `encoding/csv` 自动处理逗号、引号、换行 |

---

## 总结

第三版报告发现的全部 4 项问题已修复：

1. **M1（D1 完全消除）**：data 模式终端渲染的两处 `differencesSchemaTable` 手工遍历已替换为 `resolveEffectiveDiffs(pod)`，全代码库中该 override 逻辑现仅有一处实现
2. **M2（真正 fail-fast）**：`resultExport`/`terminalResultMode` 合法性校验前移至 DB 建连之前，配置文件非法值现在不会触发数据库连接
3. **L1（注释准确）**：`ResultRecord` 类型注释修正，不再声称"终端+CSV 单一数据源"
4. **L2（测试补全）**：新增目录自动创建和 0600 权限两组专项断言测试，CSV 专项测试从 34 增至 36

当前实现已满足"长期可维护、可被自动化消费的稳定接口"的基本要求。遗留 L-a～L-d 四项均不影响当前版本的正确使用，已有明确改进路径。
