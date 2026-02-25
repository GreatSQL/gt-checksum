# repairDB 计划日志范围压缩改造说明

## 1. 改造目标

将 `repairDB` 中计划执行列表的逐文件输出，压缩为按“同前缀 + 连续编号”合并后的范围格式，降低日志噪音并保留可审计信息。

目标输出风格：

- `[PHASE-1-DELETE] #X fixsql/<table>-DELETE-(start-end).sql`
- `[PHASE-2-OTHER] #X fixsql/<table>-(start-end).sql`

并保持以下信息不变：

- `Found X DELETE files, Y other SQL files`
- `[PHASE-1-DELETE] planned execution order (X files):`
- `[PHASE-2-OTHER] planned execution order (Y files):`

## 2. 代码变更位置

### 2.1 主要改动文件

- `/Users/yejinrong/gitee/gt-checksum/repairDB.go`
- `/Users/yejinrong/gitee/gt-checksum/repairDB_test.go`

### 2.2 关键函数（repairDB.go）

1. `splitConsecutiveRanges(numbers []int) []rangeSegment`
   - 作用：将编号切分为连续区间。
2. `normalizePlanPath(file, fixFileDir string) string`
   - 作用：将绝对路径标准化为日志路径（优先显示为 `fixFileDir` 基名开头，如 `fixsql/...`）。
3. `buildCompressedPlanEntries(files []string, fixFileDir string) []string`
   - 作用：按前缀分组并合并连续编号，输出范围行。
4. `logExecutionPlan(stageName string, files []string, fixFileDir string)`
   - 作用：保持阶段标题不变，改为打印压缩后的 `#X` 计划列表。
5. `main()`
   - 调用 `logExecutionPlan` 时传入 `config.FixFileDir`，启用新格式输出。

## 3. 合并算法说明

`buildCompressedPlanEntries` 核心步骤：

1. 对每个文件做路径标准化，得到类似 `fixsql/lineitem-DELETE-1.sql` 的日志路径。
2. 使用正则 `^(.+?-)(\d+)(\.sql)$` 解析文件名：
   - 前缀：`lineitem-DELETE-` 或 `lineitem-`
   - 编号：`1`
   - 后缀：`.sql`
3. 按 `(dir, prefix, suffix)` 分组收集编号。
4. 对每组编号排序去重后，调用 `splitConsecutiveRanges` 拆分连续段：
   - `[1,2,3,7,8] -> (1-3), (7-8)`
5. 生成范围路径：
   - `lineitem-DELETE-(1-3).sql`
   - `lineitem-(7-8).sql`
6. 按首次出现顺序与区间起点稳定排序，输出带 `#X` 的计划列表。

## 4. 测试用例与验证结果

测试文件：`/Users/yejinrong/gitee/gt-checksum/repairDB_test.go`

覆盖点：

1. `TestIsDeleteStageFile`
   - 验证 DELETE 文件识别规则正确。
2. `TestUniqueFiles`
   - 验证去重逻辑不丢失顺序。
3. `TestSplitConsecutiveRanges`
   - 验证连续区间拆分正确。
4. `TestBuildCompressedPlanEntriesForDeleteAndOther`
   - 验证 DELETE/OTHER 两类范围合并输出格式。
5. `TestNormalizePlanPath`
   - 验证日志路径标准化格式（`fixsql/...`）。

执行结果：

- `CGO_ENABLED=0 go test repairDB.go repairDB_test.go` 通过。
- `go build -o gt-checksum gt-checksum.go` 通过。
- `CGO_ENABLED=0 go build -o repairDB repairDB.go` 通过。

## 5. 边界情况处理说明

1. **单文件编号**
   - 输出为 `(n-n)`，例如 `customer-(9-9).sql`，保持统一范围风格。
2. **编号不连续**
   - 拆分为多段范围，例如 `(1-3)` 与 `(7-8)`。
3. **重复文件路径**
   - 由既有 `uniqueFiles` 去重后再合并，避免重复统计/重复展示。
4. **非数字结尾文件**
   - 无法解析编号时，按原路径单独保留，不参与范围合并。
5. **非 fixFileDir 下路径**
   - 回退到原始路径（去掉 `./` 前缀）进行日志展示。

## 6. 输出示例

```text
Found 706 DELETE files, 1284 other SQL files
[PHASE-1-DELETE] planned execution order (706 files):
[PHASE-1-DELETE] #1 fixsql/lineitem-DELETE-(1-706).sql
[PHASE-1-DELETE] #2 fixsql/orders-DELETE-(1-120).sql
[PHASE-2-OTHER] planned execution order (1284 files):
[PHASE-2-OTHER] #1 fixsql/lineitem-(1-900).sql
[PHASE-2-OTHER] #2 fixsql/partsupp-(1-120).sql
```
