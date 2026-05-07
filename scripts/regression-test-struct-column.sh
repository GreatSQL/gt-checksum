#!/usr/bin/env bash
# =============================================================================
# gt-checksum TableColumnNameCheck 结构检查回归测试脚本
#
# 对应 docs/large-file-v3-dev-plan-cc-20260422.md Step 0-B：为主函数
# TableColumnNameCheck 按业务阶段抽取重构（P0–P7）提供端到端护栏。
#
# 覆盖清单（Step 0 ①-⑤、⑧、⑨；⑥⑦ Oracle 在 regression-test-oracle.sh --with-scenarios）：
#   TC-ST-01  双端存在、无差异
#   TC-ST-02  目标缺表（期望生成 CREATE TABLE 并修复收敛）
#   TC-ST-03  源缺表  （期望生成 DROP   TABLE 并修复收敛）
#   TC-ST-04  列增/删/类型变更
#   TC-ST-05a 列名大小写差异（caseSensitiveObjectName=yes）
#   TC-ST-05b 列名大小写差异（caseSensitiveObjectName=no，视为一致）
#   TC-ST-08  不支持特性 advisory（generated column / CHECK）
#   TC-ST-09  columnPlan 列映射豁免（data 预检）
#
# 用法:
#   bash scripts/regression-test-struct-column.sh --src-port=3406 --dst-port=3408
# =============================================================================
set -euo pipefail

# ============================================================
# SECTION 1: 常量与默认值
# ============================================================
DB_HOST="127.0.0.1"
DB_USER="checksum"
DB_PASS="checksum"
DB_SCHEMA="gt_checksum_sc"

SRC_PORT=""
DST_PORT=""

CASE_TIMEOUT=180
MAX_REPAIR_ROUNDS=3
SKIP_INIT=false
SKIP_BUILD=false
DRY_RUN=false

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="struct-column-$(date +%Y%m%d-%H%M%S)"
ARTIFACTS_DIR="${ROOT_DIR}/test-artifacts/${RUN_ID}"
GT_CHECKSUM="${ROOT_DIR}/gt-checksum"
REPAIR_DB="${ROOT_DIR}/repairDB"

SRC_FIXTURE="${ROOT_DIR}/testcase/MySQL-struct-column-source.sql"
DST_FIXTURE="${ROOT_DIR}/testcase/MySQL-struct-column-target.sql"

TIMEOUT_CMD=""

TOTAL=0
PASSED=0
FAILED=0
ERRORS=0
TIMEOUTS=0

# ============================================================
# SECTION 2: 日志与参数
# ============================================================
log_info()  { echo "[INFO]  $*"; }
log_warn()  { echo "[WARN]  $*" >&2; }
log_error() { echo "[ERROR] $*" >&2; }

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --host=*)          DB_HOST="${1#--host=}" ;;
            --user=*)          DB_USER="${1#--user=}" ;;
            --pass=*)          DB_PASS="${1#--pass=}" ;;
            --src-port=*)      SRC_PORT="${1#--src-port=}" ;;
            --dst-port=*)      DST_PORT="${1#--dst-port=}" ;;
            --timeout=*)       CASE_TIMEOUT="${1#--timeout=}" ;;
            --artifacts-dir=*) ARTIFACTS_DIR="${1#--artifacts-dir=}" ;;
            --skip-init)       SKIP_INIT=true ;;
            --skip-build)      SKIP_BUILD=true ;;
            --dry-run)         DRY_RUN=true ;;
            --help|-h)
                sed -n '3,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
                exit 0
                ;;
            *) log_error "未知参数: $1"; exit 1 ;;
        esac
        shift
    done
    if [[ "$DRY_RUN" == "false" && ( -z "$SRC_PORT" || -z "$DST_PORT" ) ]]; then
        log_error "必须指定 --src-port 和 --dst-port"
        exit 1
    fi
}

# ============================================================
# SECTION 3: 工具函数（与 regression-test-columns.sh 同步）
# ============================================================
mysql_exec() {
    local port="$1"; shift
    mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASS}" -P"${port}" \
        --default-character-set=utf8mb4 -f "$@" 2>&1
}

run_with_timeout() {
    local seconds="$1"; shift
    if [[ -n "$TIMEOUT_CMD" ]]; then
        "$TIMEOUT_CMD" "$seconds" "$@"
    else
        "$@"
    fi
}

strip_ansi() { sed $'s/\x1B\\[[0-9;]*m//g'; }

setup_timeout_cmd() {
    if command -v timeout &>/dev/null; then
        TIMEOUT_CMD="timeout"
    elif command -v gtimeout &>/dev/null; then
        TIMEOUT_CMD="gtimeout"
    else
        log_warn "未找到 timeout 命令，测试将不受超时保护"
    fi
}

# ============================================================
# SECTION 4: 前置检查 / 编译 / 初始化
# ============================================================
check_prerequisites() {
    [[ "$DRY_RUN" == "true" ]] && return
    local ok=true
    command -v mysql >/dev/null 2>&1 || { log_error "未找到 mysql 客户端"; ok=false; }
    if [[ "$SKIP_BUILD" == "true" ]]; then
        [[ -x "$GT_CHECKSUM" ]] || { log_error "gt-checksum 缺失: $GT_CHECKSUM"; ok=false; }
        [[ -x "$REPAIR_DB" ]]   || { log_error "repairDB 缺失: $REPAIR_DB"; ok=false; }
    fi
    [[ -f "$SRC_FIXTURE" ]] || { log_error "源端 fixture 不存在: $SRC_FIXTURE"; ok=false; }
    [[ -f "$DST_FIXTURE" ]] || { log_error "目标端 fixture 不存在: $DST_FIXTURE"; ok=false; }
    [[ "$ok" == "true" ]] || { log_error "前置检查失败"; exit 1; }
}

build_binaries() {
    [[ "$SKIP_BUILD" == "true" ]] && { log_info "跳过编译"; return; }
    log_info "=== 编译阶段 ==="
    cd "$ROOT_DIR"
    CGO_ENABLED=0 go build -tags nooracle -o gt-checksum ./cmd/gt-checksum
    CGO_ENABLED=0 go build -o repairDB ./cmd/repairDB
    chmod +x gt-checksum repairDB
    log_info "  编译完成"
}

check_connectivity() {
    log_info "检查数据库连通性..."
    local ok=true
    for port in "$SRC_PORT" "$DST_PORT"; do
        if mysql_exec "$port" -e "SELECT 1" >/dev/null 2>&1; then
            log_info "  [OK] port $port"
        else
            log_error "  [FAIL] port $port 无法连接"; ok=false
        fi
    done
    [[ "$ok" == "true" ]] || exit 1
}

init_databases() {
    [[ "$SKIP_INIT" == "true" ]] && { log_info "跳过数据库初始化"; return; }
    log_info "=== 初始化数据库 ==="
    mysql_exec "$SRC_PORT" < "$SRC_FIXTURE" > "${ARTIFACTS_DIR}/init-source.log" 2>&1 \
        || log_warn "源端初始化有告警（已忽略）"
    mysql_exec "$DST_PORT" < "$DST_FIXTURE" > "${ARTIFACTS_DIR}/init-target.log" 2>&1 \
        || log_warn "目标端初始化有告警（已忽略）"
}

# 用例间重置：源/目标端都重跑 fixture，避免上一轮 repairDB 改坏目标端结构
reinit_all() {
    mysql_exec "$SRC_PORT" < "$SRC_FIXTURE" \
        > "${ARTIFACTS_DIR}/reinit-src-$(date +%s%N).log" 2>&1 || true
    mysql_exec "$DST_PORT" < "$DST_FIXTURE" \
        > "${ARTIFACTS_DIR}/reinit-dst-$(date +%s%N).log" 2>&1 || true
}

# ============================================================
# SECTION 5: 配置生成
# ============================================================
generate_config() {
    local case_dir="$1" mode="$2" tables="$3" case_sensitive="$4" columns="${5:-}"
    cat > "${case_dir}/gt-checksum.conf" <<EOF
srcDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${SRC_PORT})/information_schema?charset=utf8mb4
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${DST_PORT})/information_schema?charset=utf8mb4
tables=${tables}
checkNoIndexTable=yes
caseSensitiveObjectName=${case_sensitive}
parallelThds=2
chunkSize=1000
queueSize=20
checkObject=${mode}
memoryLimit=3000
datafix=file
fixFileDir=${case_dir}/fixsql
logFile=${case_dir}/gt-checksum.log
logLevel=debug
logbin=ON
requirePK=ON
EOF
    [[ -n "$columns" ]] && echo "columns=${columns}" >> "${case_dir}/gt-checksum.conf"
    cat > "${case_dir}/repairDB.conf" <<EOF
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${DST_PORT})/information_schema?charset=utf8mb4
parallelThds=4
fixFileDir=${case_dir}/fixsql
EOF
}

# ============================================================
# SECTION 6: 输出解析（struct 与 data 双模式兼容）
# ============================================================
parse_diffs_from_output() {
    local output_file="$1" mode="$2"
    local clean
    clean="$(strip_ansi < "$output_file")"
    local filter='\bstruct\b|\bsequence\b'
    [[ "$mode" == "data" ]] && filter='\bdata\b'
    echo "$clean" \
        | grep -iE "$filter" \
        | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
        | awk '{for(i=1;i<=NF;i++){v=tolower($i);if(v=="yes"||v=="no"||v=="warn-only"||v=="collation-mapped"||v=="ddl-yes"){print $i;break}}}' \
        | sort -u | paste -sd',' -
}

fixsql_is_advisory_only() {
    local fixsql_dir="$1"
    local sql_files
    sql_files=$(find "$fixsql_dir" -name "*.sql" -type f 2>/dev/null)
    [[ -z "$sql_files" ]] && return 1
    local n
    n=$(echo "$sql_files" | xargs grep -hv '^\s*--\|^\s*$' 2>/dev/null | wc -l | tr -d ' ')
    [[ "$n" -eq 0 ]]
}

evaluate_diffs() {
    local diffs_csv="$1"
    [[ -z "$diffs_csv" ]] && { echo "NO_OUTPUT"; return; }
    local has_yes=false
    IFS=',' read -ra arr <<< "$diffs_csv"
    for v in "${arr[@]}"; do
        v="$(echo "$v" | tr -d '[:space:]')"
        case "$v" in
            no|warn-only|collation-mapped) ;;
            yes|DDL-yes|*) has_yes=true ;;
        esac
    done
    $has_yes && echo "NEEDS_REPAIR" || echo "PASS"
}

# ============================================================
# SECTION 7: 单用例执行
# 参数：case_id mode tables case_sensitive columns expected_verdict
#   expected_verdict: PASS / PASS-ADVISORY / ERROR-EXPECTED
# ============================================================
run_struct_case() {
    local case_id="$1" mode="$2" tables="$3" cs="$4" columns="$5" expected="$6"
    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"
    mkdir -p "${case_dir}/fixsql"

    reinit_all
    generate_config "$case_dir" "$mode" "$tables" "$cs" "$columns"

    local round=0 final="UNKNOWN" diffs=""
    while [[ $round -lt $((MAX_REPAIR_ROUNDS + 1)) ]]; do
        round=$((round + 1))
        rm -rf "${case_dir}/fixsql"; mkdir -p "${case_dir}/fixsql"
        local out="${case_dir}/round${round}-output.txt" ec=0
        run_with_timeout "$CASE_TIMEOUT" \
            "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" > "$out" 2>&1 || ec=$?
        [[ -f "${case_dir}/gt-checksum.log" ]] && \
            cp "${case_dir}/gt-checksum.log" "${case_dir}/round${round}-gt-checksum.log" 2>/dev/null || true

        if [[ $ec -eq 124 ]]; then final="TIMEOUT"; break; fi
        diffs="$(parse_diffs_from_output "$out" "$mode")"
        local eval; eval="$(evaluate_diffs "$diffs")"

        case "$eval" in
            PASS) final="PASS"; break ;;
            NO_OUTPUT)
                if [[ $ec -ne 0 ]]; then
                    final="ERROR"; log_error "  [${case_id}] exit=${ec} 无输出"
                else
                    final="PASS"; log_warn "  [${case_id}] exit=0 无 Diffs 行"
                fi
                break ;;
            NEEDS_REPAIR)
                if [[ $round -gt $MAX_REPAIR_ROUNDS ]]; then
                    final="FAIL"
                    log_error "  [${case_id}] ${MAX_REPAIR_ROUNDS} 轮后仍有差异: ${diffs}"
                    break
                fi
                local nfx
                nfx=$(find "${case_dir}/fixsql" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
                if [[ "$nfx" -eq 0 ]]; then
                    final="FAIL"; log_error "  [${case_id}] Diffs=yes 但未生成 fixsql"; break
                fi
                if fixsql_is_advisory_only "${case_dir}/fixsql"; then
                    final="PASS-ADVISORY"
                    log_info "  [${case_id}] advisory-only fixsql → PASS-ADVISORY"; break
                fi
                run_with_timeout "$CASE_TIMEOUT" \
                    "$REPAIR_DB" -f -conf "${case_dir}/repairDB.conf" \
                    > "${case_dir}/round${round}-repair.txt" 2>&1 \
                    || log_warn "  [${case_id}] Round ${round}: repairDB 非零退出"
                log_info "  [${case_id}] Round ${round}: 修复完成 (fixsql=${nfx})"
                ;;
        esac
    done

    # 预期比对
    if [[ "$final" != "TIMEOUT" && "$final" != "ERROR" && "$final" != "$expected" ]]; then
        log_error "  [${case_id}] 预期=${expected} 实际=${final}"
        final="UNEXPECTED"
    fi

    echo "${case_id}|${final}|${round}|${diffs}" >> "${ARTIFACTS_DIR}/results.csv"
    echo "$final" > "${case_dir}/verdict"

    TOTAL=$((TOTAL + 1))
    case "$final" in
        PASS|PASS-ADVISORY) PASSED=$((PASSED + 1));   log_info  "  [${case_id}] ${final} (rounds=${round})" ;;
        FAIL|UNEXPECTED)    FAILED=$((FAILED + 1));   log_error "  [${case_id}] ${final} (rounds=${round}, diffs=${diffs:-—})" ;;
        ERROR)              ERRORS=$((ERRORS + 1));   log_error "  [${case_id}] ERROR" ;;
        TIMEOUT)            TIMEOUTS=$((TIMEOUTS + 1)); log_error "  [${case_id}] TIMEOUT" ;;
        ERROR-EXPECTED)     log_info "  [${case_id}] ERROR-EXPECTED（不计失败）" ;;
    esac
}

# ============================================================
# SECTION 8: 报告
# ============================================================
generate_report() {
    local f="${ARTIFACTS_DIR}/results.csv" r="${ARTIFACTS_DIR}/report.txt"
    {
        echo "=================================================================="
        echo " gt-checksum struct-column Regression Report"
        echo " Run ID:   ${RUN_ID}"
        echo " Src/Dst:  ${SRC_PORT} -> ${DST_PORT}"
        echo " Date:     $(date '+%Y-%m-%d %H:%M:%S')"
        echo "=================================================================="
        printf "%-30s %-16s %-8s %s\n" "CASE" "VERDICT" "ROUNDS" "DIFFS"
        printf "%-30s %-16s %-8s %s\n" "----" "-------" "------" "-----"
        while IFS='|' read -r id v rd d; do
            printf "%-30s %-16s %-8s %s\n" "$id" "$v" "$rd" "${d:-—}"
        done < "$f"
        echo ""
        echo " Total: ${TOTAL}  Passed: ${PASSED}  Failed: ${FAILED}  Errors: ${ERRORS}  Timeouts: ${TIMEOUTS}"
    } | tee "$r"
}

# ============================================================
# SECTION 9: 测例清单与主流程
# ============================================================
print_test_cases() {
    echo "=================================================================="
    echo " struct-column 回归测例列表"
    echo "=================================================================="
    printf "%-28s %-16s %s\n" "用例 ID" "预期" "场景"
    printf "%-28s %-16s %s\n" "-------" "----" "----"
    printf "%-28s %-16s %s\n" "TC-ST-01" "PASS"          "双端存在、无差异"
    printf "%-28s %-16s %s\n" "TC-ST-02" "PASS"          "目标缺表→生成 CREATE TABLE 并收敛"
    printf "%-28s %-16s %s\n" "TC-ST-03" "PASS"          "源缺表→生成 DROP TABLE 并收敛"
    printf "%-28s %-16s %s\n" "TC-ST-04" "PASS"          "列增/删/类型变更→修复收敛"
    printf "%-28s %-16s %s\n" "TC-ST-05a" "PASS"         "列名大小写（敏感=yes）→ RENAME 收敛"
    printf "%-28s %-16s %s\n" "TC-ST-05b" "PASS"         "列名大小写（敏感=no）→ 视为一致"
    printf "%-28s %-16s %s\n" "TC-ST-08"  "PASS-ADVISORY" "generated/CHECK advisory-only"
    printf "%-28s %-16s %s\n" "TC-ST-09"  "PASS"         "columnPlan 列映射豁免（data 预检）"
}

main() {
    parse_arguments "$@"
    [[ "$DRY_RUN" == "true" ]] && { print_test_cases; exit 0; }

    setup_timeout_cmd
    mkdir -p "${ARTIFACTS_DIR}/cases"
    : > "${ARTIFACTS_DIR}/results.csv"

    log_info "=================================================================="
    log_info " gt-checksum struct-column Regression Test"
    log_info " Run ID:    ${RUN_ID}"
    log_info " Src/Dst:   ${SRC_PORT} -> ${DST_PORT}"
    log_info " Artifacts: ${ARTIFACTS_DIR}"
    log_info "=================================================================="

    check_prerequisites
    build_binaries
    check_connectivity
    init_databases

    log_info ""
    log_info "=== 执行测例 ==="

    # TC-ST-01
    log_info ""; log_info "--- TC-ST-01: 双端存在、无差异 ---"
    run_struct_case "TC-ST-01" "struct" "${DB_SCHEMA}.t_identical" "yes" "" "PASS"

    # TC-ST-02 目标缺表
    log_info ""; log_info "--- TC-ST-02: 目标缺表→CREATE TABLE 修复 ---"
    run_struct_case "TC-ST-02" "struct" "${DB_SCHEMA}.t_missing_dst" "yes" "" "PASS"

    # TC-ST-03 源缺表
    log_info ""; log_info "--- TC-ST-03: 源缺表→DROP TABLE 修复 ---"
    run_struct_case "TC-ST-03" "struct" "${DB_SCHEMA}.t_missing_src" "yes" "" "PASS"

    # TC-ST-04 列增/删/类型
    log_info ""; log_info "--- TC-ST-04: 列增/删/类型变更 ---"
    run_struct_case "TC-ST-04" "struct" "${DB_SCHEMA}.t_col_diff" "yes" "" "PASS"

    # TC-ST-05a / 05b 列名大小写
    log_info ""; log_info "--- TC-ST-05a: 大小写敏感=yes → RENAME 收敛 ---"
    run_struct_case "TC-ST-05a" "struct" "${DB_SCHEMA}.t_case" "yes" "" "PASS"

    log_info ""; log_info "--- TC-ST-05b: 大小写敏感=no → 视为一致 ---"
    run_struct_case "TC-ST-05b" "struct" "${DB_SCHEMA}.t_case" "no" "" "PASS"

    # TC-ST-08 advisory
    log_info ""; log_info "--- TC-ST-08: generated/CHECK advisory ---"
    run_struct_case "TC-ST-08" "struct" "${DB_SCHEMA}.t_advisory" "yes" "" "PASS-ADVISORY"

    # TC-ST-09 data columnPlan 豁免
    log_info ""; log_info "--- TC-ST-09: columnPlan 列映射豁免 (data) ---"
    run_struct_case "TC-ST-09" "data" "${DB_SCHEMA}.t_plan" "yes" \
        "${DB_SCHEMA}.t_plan.id:${DB_SCHEMA}.t_plan.id,${DB_SCHEMA}.t_plan.val:${DB_SCHEMA}.t_plan.val" \
        "PASS"

    log_info ""; log_info "=== 全部测例执行完毕 ==="
    generate_report

    [[ $((FAILED + ERRORS + TIMEOUTS)) -gt 0 ]] && exit 1 || exit 0
}

trap 'log_warn "中断信号，生成部分报告..."; generate_report; exit 130' INT TERM

main "$@"
