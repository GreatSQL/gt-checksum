# repairDB SQL Execution Order Optimization

## 背景

在大规模修复场景下，如果按固定顺序连续执行大表相关 SQL 文件，容易导致锁资源长期被占用，进而让小表修复出现等待。  
本次改造将 `repairDB` 的文件执行流程调整为“两阶段 + 随机化”，用于降低锁等待热点。

## 目标

1. 严格保证 `x-DELETE-x.sql` 文件先执行。
2. 非 DELETE 文件采用随机顺序执行，避免固定顺序下的锁争用放大。
3. 保证每个非 DELETE 文件仅被调度一次（失败重试仅针对死锁文件）。
4. 增加执行计划与实际执行序号日志，便于审计与排查。

## 设计与实现

### 1. 两阶段执行

- Phase-1: 执行所有匹配 `x-DELETE-x.sql` 的文件。
- Phase-2: 执行其余 `.sql` 文件。
- 两个阶段串行执行，禁止混跑。

### 2. DELETE 文件识别规则

新增 `isDeleteStageFile(path string)`，仅当文件名（basename）匹配以下模式时归入第一阶段：

```text
^.+-DELETE-.+\.sql$
```

### 3. 第二阶段随机化

新增 `shuffleSQLFiles(files []string)`：

- 使用 `math/rand` 和时间种子打乱顺序；
- 随机顺序仅用于第二阶段；
- 死锁重试时，第二阶段待重试文件再次随机化，避免重复锁冲突模式。

### 4. 去重保障

新增 `uniqueFiles(files []string)`，在调度前对文件路径去重，保证同一文件不会被重复调度。

### 5. 执行顺序日志

新增两类日志：

1. **计划执行顺序**：`logExecutionPlan(stageName, files)`，输出每个阶段的完整文件列表顺序。
2. **实际执行顺序**：并发执行时通过原子计数器记录 `execution sequence #N`，输出实际启动顺序（含阶段与重试轮次）。

## 关键改动文件

- `/Users/yejinrong/gitee/gt-checksum/repairDB.go`
  - 新增文件分类、去重、随机化与执行序号日志逻辑；
  - 移除按数字后缀排序作为主执行策略；
  - 维持原有并行执行与死锁重试框架。

## 验证建议

1. 构造包含以下类型文件的目录：
   - `lineitem-DELETE-1.sql`, `orders-DELETE-2.sql`
   - `lineitem-1.sql`, `orders-1.sql`, `supplier-1.sql`
2. 执行 `repairDB`，确认日志中：
   - 先打印并执行 `PHASE-1-DELETE`；
   - 后打印并执行 `PHASE-2-OTHER`；
   - `PHASE-2-OTHER` 顺序为随机顺序；
   - `execution sequence #N` 连续递增，文件无重复调度。
3. 在制造死锁场景下，确认第二阶段重试仍仅针对死锁文件，且重试顺序重新随机。

## 风险与说明

1. 第二阶段随机顺序会导致不同批次执行路径不一致，这是预期行为。
2. 对存在强依赖顺序的 SQL 文件集合，随机执行可能暴露原先隐藏依赖；本次按需求不做依赖排序处理。
3. 单文件 `datafix.sql` 直执路径保持不变（与两阶段无冲突）。
