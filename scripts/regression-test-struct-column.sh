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
#   TC-ST-03  源缺表  （v4.0.0 源端对象不可见诊断，预期非零退出）
#   TC-ST-04  列增/删/类型变更
#   TC-ST-05a 列名大小写差异（caseSensitiveObjectName=yes）
#   TC-ST-05b 列名大小写差异（caseSensitiveObjectName=no，视为一致）
#   TC-ST-08  不支持特性 advisory（generated column / CHECK）
#   TC-ST-09  columnPlan 列映射豁免（data 预检）
#   TC-ST-10  dTypeMappingFile preview smoke
#   TC-ST-11  非 data 对象 datafix=table 强制导出 fix SQL
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
    local datafix="${6:-file}"
    local resume="${7:-OFF}"
    local dtype_mapping_file="${8:-}"
    local extra_config="${9:-}"

    # requirePK 逻辑：只校验部分列的场景（有 columns 参数）不启用 requirePK
    local require_pk="ON"
    if [[ -n "$columns" ]]; then
        require_pk="OFF"
    fi

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
datafix=${datafix}
fixFileDir=${case_dir}/fixsql
logFile=${case_dir}/gt-checksum.log
logLevel=debug
logbin=ON
requirePK=${require_pk}
EOF
    [[ -n "$columns" ]] && echo "columns=${columns}" >> "${case_dir}/gt-checksum.conf"
    [[ "$resume" != "OFF" ]] && echo "resume=${resume}" >> "${case_dir}/gt-checksum.conf"
    [[ -n "$dtype_mapping_file" ]] && echo "dTypeMappingFile=${dtype_mapping_file}" >> "${case_dir}/gt-checksum.conf"
    [[ -n "$extra_config" ]] && printf '%s\n' "$extra_config" >> "${case_dir}/gt-checksum.conf"

    cat > "${case_dir}/repairDB.conf" <<EOF
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${DST_PORT})/information_schema?charset=utf8mb4
parallelThds=4
fixFileDir=${case_dir}/fixsql
resume=${resume}
EOF
}

write_dtype_mapping_file() {
    local mapping_file="$1"
    cat > "${mapping_file}" <<'EOF'
dTypeMapping:
  oracle_to_mysql:
    - source_type: NUMBER
      target_type: BIGINT
      condition: "p <= 19 and s = 0"
      description: "struct-column preview integer mapping"
  mysql_upgrade:
    - source_type: CHAR
      target_type: VARCHAR
      object: gt_checksum_sc.t_col_diff.c_varchar
      description: "struct-column preview scoped char mapping"
  mariadb_to_mysql:
    - source_type: JSON
      target_type: JSON
      description: "struct-column preview json mapping"
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
        | sort -u | paste -sd',' - || true
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
    local diffs_csv="$1" mode="${2:-}"
    [[ -z "$diffs_csv" ]] && { echo "NO_OUTPUT"; return; }
    local has_yes=false
    local has_ddl_yes=false
    IFS=',' read -ra arr <<< "$diffs_csv"
    for v in "${arr[@]}"; do
        v="$(echo "$v" | tr -d '[:space:]')"
        case "$v" in
            no|warn-only|collation-mapped) ;;
            DDL-yes)
                if [[ "$mode" == "data" ]]; then
                    has_ddl_yes=true
                else
                    has_yes=true
                fi
                ;;
            yes|*) has_yes=true ;;
        esac
    done
    if $has_yes; then
        echo "NEEDS_REPAIR"
    elif $has_ddl_yes; then
        echo "PASS-DDL"
    else
        echo "PASS"
    fi
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
        local eval; eval="$(evaluate_diffs "$diffs" "$mode")"

        case "$eval" in
            PASS|PASS-DDL) final="$eval"; break ;;
            NO_OUTPUT)
                if [[ $ec -ne 0 ]]; then
                    if [[ "$expected" == "ERROR-EXPECTED" ]]; then
                        final="ERROR-EXPECTED"
                        log_info "  [${case_id}] exit=${ec} 无输出（预期行为）"
                    else
                        final="ERROR"; log_error "  [${case_id}] exit=${ec} 无输出"
                    fi
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
        PASS|PASS-DDL|PASS-ADVISORY) PASSED=$((PASSED + 1));   log_info  "  [${case_id}] ${final} (rounds=${round})" ;;
        FAIL|UNEXPECTED)    FAILED=$((FAILED + 1));   log_error "  [${case_id}] ${final} (rounds=${round}, diffs=${diffs:-—})" ;;
        ERROR)              ERRORS=$((ERRORS + 1));   log_error "  [${case_id}] ERROR" ;;
        TIMEOUT)            TIMEOUTS=$((TIMEOUTS + 1)); log_error "  [${case_id}] TIMEOUT" ;;
        ERROR-EXPECTED)     log_info "  [${case_id}] ERROR-EXPECTED（不计失败）" ;;
    esac
}

run_dtype_preview_case() {
    local case_id="TC-ST-10-dtype-preview"
    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"
    mkdir -p "${case_dir}/fixsql"

    local mapping_file="${case_dir}/dtype-mapping.yaml"
    write_dtype_mapping_file "${mapping_file}"
    generate_config "$case_dir" "struct" "${DB_SCHEMA}.t_col_diff" "yes" "" "file" "OFF" "$mapping_file"

    local out="${case_dir}/preview-output.txt" ec=0 final="PASS"
    run_with_timeout "$CASE_TIMEOUT" \
        "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" --preview-dtype-mapping \
        > "$out" 2>&1 || ec=$?

    if [[ $ec -eq 124 ]]; then
        final="TIMEOUT"
    elif [[ $ec -ne 0 ]]; then
        final="ERROR"
    elif ! grep -qiE '\[dTypeMapping\]|source_type|target_type' "$out"; then
        final="FAIL"
        log_error "  [${case_id}] preview 输出未包含 dTypeMapping 标记"
    fi

    echo "${case_id}|${final}|1|preview" >> "${ARTIFACTS_DIR}/results.csv"
    echo "$final" > "${case_dir}/verdict"

    TOTAL=$((TOTAL + 1))
    case "$final" in
        PASS)    PASSED=$((PASSED + 1)); log_info  "  [${case_id}] PASS" ;;
        FAIL)    FAILED=$((FAILED + 1)); log_error "  [${case_id}] FAIL" ;;
        ERROR)   ERRORS=$((ERRORS + 1)); log_error "  [${case_id}] ERROR (exit=${ec})" ;;
        TIMEOUT) TIMEOUTS=$((TIMEOUTS + 1)); log_error "  [${case_id}] TIMEOUT" ;;
    esac
}

run_struct_datafix_table_safe_case() {
    local case_id="TC-ST-11-struct-datafix-table-safe"
    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"
    mkdir -p "${case_dir}/fixsql"

    reinit_all
    generate_config "$case_dir" "struct" "${DB_SCHEMA}.t_col_diff" "yes" "" "table" "OFF" ""

    local out="${case_dir}/round1-output.txt" ec=0 final="PASS" diffs=""
    run_with_timeout "$CASE_TIMEOUT" \
        "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" > "$out" 2>&1 || ec=$?
    [[ -f "${case_dir}/gt-checksum.log" ]] && \
        cp "${case_dir}/gt-checksum.log" "${case_dir}/round1-gt-checksum.log" 2>/dev/null || true

    if [[ $ec -eq 124 ]]; then
        final="TIMEOUT"
    elif [[ $ec -ne 0 ]]; then
        final="ERROR"
    else
        diffs="$(parse_diffs_from_output "$out" "struct")"
        local nfx
        nfx=$(find "${case_dir}/fixsql" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
        if [[ "$nfx" -eq 0 ]]; then
            final="FAIL"
            log_error "  [${case_id}] datafix=table(struct) 未导出 fixsql"
        elif ! grep -qiE 'force exporting fix SQL|does not directly repair target objects' "$out" "${case_dir}/gt-checksum.log" 2>/dev/null; then
            final="FAIL"
            log_error "  [${case_id}] 未检测到非 data 对象 datafix=table 安全提示"
        else
            run_with_timeout "$CASE_TIMEOUT" \
                "$REPAIR_DB" -f -conf "${case_dir}/repairDB.conf" \
                > "${case_dir}/round1-repair.txt" 2>&1 || final="ERROR"

            if [[ "$final" == "PASS" ]]; then
                rm -rf "${case_dir}/fixsql"; mkdir -p "${case_dir}/fixsql"
                local verify_out="${case_dir}/round2-output.txt" verify_ec=0
                run_with_timeout "$CASE_TIMEOUT" \
                    "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" \
                    > "$verify_out" 2>&1 || verify_ec=$?
                diffs="$(parse_diffs_from_output "$verify_out" "struct")"
                local eval; eval="$(evaluate_diffs "$diffs" "struct")"
                if [[ $verify_ec -eq 124 ]]; then
                    final="TIMEOUT"
                elif [[ $verify_ec -ne 0 || "$eval" != "PASS" ]]; then
                    final="FAIL"
                    log_error "  [${case_id}] repairDB 后复核未收敛: diffs=${diffs:-—} exit=${verify_ec}"
                fi
            fi
        fi
    fi

    echo "${case_id}|${final}|2|${diffs}" >> "${ARTIFACTS_DIR}/results.csv"
    echo "$final" > "${case_dir}/verdict"

    TOTAL=$((TOTAL + 1))
    case "$final" in
        PASS)    PASSED=$((PASSED + 1)); log_info  "  [${case_id}] PASS" ;;
        FAIL)    FAILED=$((FAILED + 1)); log_error "  [${case_id}] FAIL" ;;
        ERROR)   ERRORS=$((ERRORS + 1)); log_error "  [${case_id}] ERROR" ;;
        TIMEOUT) TIMEOUTS=$((TIMEOUTS + 1)); log_error "  [${case_id}] TIMEOUT" ;;
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
    printf "%-28s %-16s %s\n" "TC-ST-03" "ERROR-EXPECTED" "源缺表→源端对象不可见诊断（预期非零退出）"
    printf "%-28s %-16s %s\n" "TC-ST-04" "PASS"          "列增/删/类型变更→修复收敛"
    printf "%-28s %-16s %s\n" "TC-ST-05a" "PASS"         "列名大小写（敏感=yes）→ RENAME 收敛"
    printf "%-28s %-16s %s\n" "TC-ST-05b" "PASS"         "列名大小写（敏感=no）→ 视为一致"
    printf "%-28s %-16s %s\n" "TC-ST-08"  "PASS"         "generated/CHECK warn-only 或修复后收敛"
    printf "%-28s %-16s %s\n" "TC-ST-09"  "PASS-DDL"     "columnPlan 列映射豁免（data 预检 DDL-yes 显式暴露）"
    printf "%-28s %-16s %s\n" "TC-ST-10"  "PASS"         "dTypeMappingFile preview smoke"
    printf "%-28s %-16s %s\n" "TC-ST-11"  "PASS"         "非 data 对象 datafix=table 强制导出 fix SQL"
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

    # TC-ST-03 源缺表：v4.0.0 会输出源端对象不可见/权限诊断并非零退出
    log_info ""; log_info "--- TC-ST-03: 源缺表→源端对象不可见诊断（预期非零退出） ---"
    run_struct_case "TC-ST-03" "struct" "${DB_SCHEMA}.t_missing_src" "yes" "" "ERROR-EXPECTED"

    # TC-ST-04 列增/删/类型
    log_info ""; log_info "--- TC-ST-04: 列增/删/类型变更 ---"
    run_struct_case "TC-ST-04" "struct" "${DB_SCHEMA}.t_col_diff" "yes" "" "PASS"

    # TC-ST-05a / 05b 列名大小写
    log_info ""; log_info "--- TC-ST-05a: 大小写敏感=yes → RENAME 收敛 ---"
    run_struct_case "TC-ST-05a" "struct" "${DB_SCHEMA}.t_case" "yes" "" "PASS"

    log_info ""; log_info "--- TC-ST-05b: 大小写敏感=no → 视为一致 ---"
    run_struct_case "TC-ST-05b" "struct" "${DB_SCHEMA}.t_case" "no" "" "PASS"

    # TC-ST-08 generated/CHECK warn-only 或修复后收敛
    log_info ""; log_info "--- TC-ST-08: generated/CHECK warn-only 或修复后收敛 ---"
    run_struct_case "TC-ST-08" "struct" "${DB_SCHEMA}.t_advisory" "yes" "" "PASS"

    # TC-ST-09 data columnPlan 豁免
    log_info ""; log_info "--- TC-ST-09: columnPlan 列映射豁免 (data) ---"
    run_struct_case "TC-ST-09" "data" "${DB_SCHEMA}.t_plan" "yes" \
        "${DB_SCHEMA}.t_plan.id:${DB_SCHEMA}.t_plan.id,${DB_SCHEMA}.t_plan.val:${DB_SCHEMA}.t_plan.val" \
        "PASS-DDL"

    # TC-ST-10 dTypeMappingFile preview
    log_info ""; log_info "--- TC-ST-10: dTypeMappingFile preview smoke ---"
    run_dtype_preview_case

    # TC-ST-11 非 data 对象 datafix=table 安全策略
    log_info ""; log_info "--- TC-ST-11: struct + datafix=table 强制导出 fix SQL ---"
    run_struct_datafix_table_safe_case

    log_info ""; log_info "=== 全部测例执行完毕 ==="
    generate_report

    [[ $((FAILED + ERRORS + TIMEOUTS)) -gt 0 ]] && exit 1 || exit 0
}

trap 'log_warn "中断信号，生成部分报告..."; generate_report; exit 130' INT TERM

main "$@"
