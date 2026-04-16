#!/usr/bin/env bash
# =============================================================================
# gt-checksum Oracle → MySQL 回归测试脚本
#
# 用法:
#   bash scripts/regression-test-oracle.sh [选项]
#
# 选项:
#   --src=label               Oracle 源别名（默认 oracle11g；可选值见脚本 ORACLE_SOURCES 表）
#   --dst=label1,label2       仅测试指定目标（mysql80 / mysql84），默认两者
#   --mode=m1,m2              仅测试指定模式（struct / data），默认 struct
#   --host=IP                 MySQL 目标主机（默认 127.0.0.1）
#   --user=USER               MySQL 目标用户名（默认 checksum）
#   --pass=PASS               MySQL 目标密码（默认 checksum）
#   --init-oracle             使用 sqlplus 执行 testcase/Oracle.sql 初始化 Oracle 源
#   --skip-init               跳过 MySQL 目标端初始化
#   --skip-build              跳过二进制编译（复用现有 gt-checksum / repairDB）
#   --timeout=SEC             单用例超时秒数（默认 600）
#   --max-rounds=N            最大修复轮次（默认 3）
#   --dry-run                 仅打印测试矩阵，不执行
#   --final-repair            回归完成后对目标库做一次完整修复闭环
#   --artifacts-dir=PATH      自定义输出目录
#   --help                    显示帮助
#
# 说明:
#   * 本脚本只支持 Oracle → MySQL 方向；目标端仅限 mysql80 / mysql84。
#   * gt-checksum 需要以 CGO_ENABLED=1 编译以启用 godror Oracle 驱动。
#   * Oracle 源数据需预先准备（或通过 --init-oracle + sqlplus 自动初始化）。
# =============================================================================
set -euo pipefail

# ============================================================
# SECTION 1: 配置常量
# ============================================================
# Oracle 源实例别名表：每行 "label|schema|dsn"（仿照 mysql80/mysql84 的短标签命名）
# 新增实例只需追加一行即可，命令行用 --src=<label> 选择。
# 注意：字段分隔符是 '|'，DSN 内部的 '|' 会参与第三段（rest），
# 解析时保留原样。schema 为空时默认 GT_CHECKSUM。
ORACLE_SOURCES=(
    "oracle11g|GT_CHECKSUM|oracle|checksum/checksum@192.168.5.160:1521/lhr11g"
)

SRC_LABEL="oracle11g"   # 默认源别名，命令行 --src=<label> 覆盖
SRC_DSN=""              # 延后在 resolve_src_alias 中解析
SRC_SCHEMA=""

DB_HOST="127.0.0.1"
DB_USER="checksum"
DB_PASS="checksum"
DB_SCHEMA="gt_checksum"

# 目标 MySQL 实例：label:port:family，仅限 80/84
TARGETS=(
    "mysql80:3406:mysql"
    "mysql84:3408:mysql"
)

# 支持的校验模式（Oracle→MySQL 当前主要覆盖 struct 与 data）
MODES=(struct data)

MAX_REPAIR_ROUNDS=3
CASE_TIMEOUT=600

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="regression-oracle-$(date +%Y%m%d-%H%M%S)"
ARTIFACTS_DIR="${ROOT_DIR}/test-artifacts/${RUN_ID}"
GT_CHECKSUM="${ROOT_DIR}/gt-checksum"
REPAIR_DB="${ROOT_DIR}/repairDB"

ORACLE_FIXTURE="${ROOT_DIR}/testcase/Oracle.sql"
DST_FIXTURE="${ROOT_DIR}/testcase/MySQL-target.sql"

SKIP_INIT=false
SKIP_BUILD=false
INIT_ORACLE=false
DRY_RUN=false
FINAL_REPAIR=false
FILTER_DST=""
FILTER_MODE=""

TIMEOUT_CMD=""

TOTAL=0
PASSED=0
FAILED=0
ERRORS=0
TIMEOUTS=0

# ============================================================
# SECTION 2: 命令行参数解析
# ============================================================
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --src=*)           SRC_LABEL="${1#--src=}" ;;
            --dst=*)           FILTER_DST="${1#--dst=}" ;;
            --mode=*)          FILTER_MODE="${1#--mode=}" ;;
            --host=*)          DB_HOST="${1#--host=}" ;;
            --user=*)          DB_USER="${1#--user=}" ;;
            --pass=*)          DB_PASS="${1#--pass=}" ;;
            --init-oracle)     INIT_ORACLE=true ;;
            --skip-init)       SKIP_INIT=true ;;
            --skip-build)      SKIP_BUILD=true ;;
            --dry-run)         DRY_RUN=true ;;
            --final-repair)    FINAL_REPAIR=true ;;
            --timeout=*)       CASE_TIMEOUT="${1#--timeout=}" ;;
            --max-rounds=*)    MAX_REPAIR_ROUNDS="${1#--max-rounds=}" ;;
            --artifacts-dir=*) ARTIFACTS_DIR="${1#--artifacts-dir=}" ;;
            --help|-h)         show_help; exit 0 ;;
            *)                 log_error "Unknown option: $1"; show_help; exit 1 ;;
        esac
        shift
    done
}

show_help() {
    sed -n '2,/^# ===/p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
}

# ============================================================
# SECTION 3: 工具函数
# ============================================================
log_info()  { echo "[$(date '+%H:%M:%S')] [INFO]  $*"; }
log_warn()  { echo "[$(date '+%H:%M:%S')] [WARN]  $*" >&2; }
log_error() { echo "[$(date '+%H:%M:%S')] [ERROR] $*" >&2; }

get_label()  { echo "$1" | cut -d: -f1; }
get_port()   { echo "$1" | cut -d: -f2; }
get_family() { echo "$1" | cut -d: -f3; }

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

label_in_filter() {
    local label="$1" filter="$2"
    [[ -z "$filter" ]] && return 0
    IFS=',' read -ra items <<< "$filter"
    for item in "${items[@]}"; do
        [[ "$label" == "$item" ]] && return 0
    done
    return 1
}

# 从 Oracle DSN 抽取 sqlplus 连接串：oracle|user/pass@host:port/sid → user/pass@host:port/sid
oracle_sqlplus_conn() {
    echo "${SRC_DSN#oracle|}"
}

# 解析 --src=<label> 别名到实际 DSN / schema；若已通过 --src-schema 显式覆盖则保留
resolve_src_alias() {
    local found=false
    local entry label schema dsn
    for entry in "${ORACLE_SOURCES[@]}"; do
        label="${entry%%|*}"
        if [[ "$label" != "$SRC_LABEL" ]]; then
            continue
        fi
        # 去掉 label|，剩下 "schema|dsn..."
        local rest="${entry#${label}|}"
        schema="${rest%%|*}"
        dsn="${rest#${schema}|}"
        [[ -z "$SRC_DSN"    ]] && SRC_DSN="$dsn"
        [[ -z "$SRC_SCHEMA" ]] && SRC_SCHEMA="${schema:-GT_CHECKSUM}"
        found=true
        break
    done
    if ! $found && [[ -z "$SRC_DSN" ]]; then
        local labels=""
        for entry in "${ORACLE_SOURCES[@]}"; do
            labels+=" ${entry%%|*}"
        done
        log_error "未知 Oracle 源别名: ${SRC_LABEL}（可选:${labels}）"
        exit 1
    fi
    [[ -z "$SRC_SCHEMA" ]] && SRC_SCHEMA="GT_CHECKSUM"
    return 0
}

# ============================================================
# SECTION 4: 环境检查
# ============================================================
setup_timeout_cmd() {
    if command -v timeout &>/dev/null; then
        TIMEOUT_CMD="timeout"
    elif command -v gtimeout &>/dev/null; then
        TIMEOUT_CMD="gtimeout"
    else
        log_warn "未找到 timeout 命令，测试将不受超时保护"
        TIMEOUT_CMD=""
    fi
}

check_prerequisites() {
    local ok=true

    [[ "$DRY_RUN" == "true" ]] && return

    if ! command -v mysql &>/dev/null; then
        log_error "未找到 mysql 客户端"; ok=false
    fi

    if [[ "$INIT_ORACLE" == "true" ]] && ! command -v sqlplus &>/dev/null; then
        log_error "--init-oracle 需要 sqlplus 命令"; ok=false
    fi

    if [[ "$SKIP_BUILD" == "true" ]]; then
        [[ -x "$GT_CHECKSUM" ]] || { log_error "gt-checksum 二进制不存在: $GT_CHECKSUM"; ok=false; }
        [[ -x "$REPAIR_DB"   ]] || { log_error "repairDB 二进制不存在: $REPAIR_DB"; ok=false; }
    else
        if ! command -v go &>/dev/null; then
            log_error "未找到 go 命令，无法编译（可使用 --skip-build 跳过编译）"; ok=false
        fi
    fi

    [[ -f "$DST_FIXTURE" ]] || { log_error "MySQL 目标端初始化脚本不存在: $DST_FIXTURE"; ok=false; }
    if [[ "$INIT_ORACLE" == "true" ]]; then
        [[ -f "$ORACLE_FIXTURE" ]] || { log_error "Oracle 初始化脚本不存在: $ORACLE_FIXTURE"; ok=false; }
    fi

    if [[ "$ok" == "false" ]]; then
        log_error "前置检查失败，退出（以上错误信息如未显示，请检查 stderr）"
        exit 1
    fi
}

check_connectivity() {
    log_info "检查数据库连通性..."
    local all_ok=true

    for entry in "${TARGETS[@]}"; do
        local label port
        label="$(get_label "$entry")"
        port="$(get_port  "$entry")"
        label_in_filter "$label" "$FILTER_DST" || continue
        if mysql_exec "$port" -e "SELECT 1" >/dev/null 2>&1; then
            log_info "  [OK] $label (port $port)"
        else
            log_error "  [FAIL] $label (port $port) 无法连接"
            all_ok=false
        fi
    done

    # Oracle 连通性可通过 gt-checksum 自身在首轮用例中验证，此处仅在 --init-oracle 时用 sqlplus 检测
    if [[ "$INIT_ORACLE" == "true" ]]; then
        if echo "SELECT 1 FROM dual;" | sqlplus -L -S "$(oracle_sqlplus_conn)" >/dev/null 2>&1; then
            log_info "  [OK] oracle src"
        else
            log_error "  [FAIL] Oracle 源无法连接 (sqlplus)"
            all_ok=false
        fi
    fi

    if [[ "$all_ok" == "false" ]]; then
        log_error "部分数据库实例无法连接，退出"
        exit 1
    fi
}

# ============================================================
# SECTION 5: 编译
# ============================================================
build_binaries() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "跳过编译 (--skip-build)"
        return
    fi

    log_info "=== 编译阶段 (CGO_ENABLED=1，启用 godror Oracle 驱动) ==="
    cd "$ROOT_DIR"

    log_info "  编译 gt-checksum ..."
    if ! CGO_ENABLED=1 go build -o gt-checksum gt-checksum.go 2>&1 | tee -a "${ARTIFACTS_DIR}/build.log"; then
        log_error "gt-checksum 编译失败，详见 ${ARTIFACTS_DIR}/build.log"; exit 1
    fi

    log_info "  编译 repairDB ..."
    if ! CGO_ENABLED=1 go build -o repairDB repairDB.go 2>&1 | tee -a "${ARTIFACTS_DIR}/build.log"; then
        log_error "repairDB 编译失败，详见 ${ARTIFACTS_DIR}/build.log"; exit 1
    fi

    chmod +x gt-checksum repairDB
    log_info "  编译完成"
}

# ============================================================
# SECTION 6: 数据库初始化
# ============================================================
init_oracle_source() {
    [[ "$INIT_ORACLE" == "true" ]] || { log_info "跳过 Oracle 源初始化（未指定 --init-oracle）"; return; }
    log_info "初始化 Oracle 源 (sqlplus)"
    local logfile="${ARTIFACTS_DIR}/init-oracle.log"
    if sqlplus -L -S "$(oracle_sqlplus_conn)" @"$ORACLE_FIXTURE" >> "$logfile" 2>&1; then
        log_info "  [OK] Oracle 源初始化完成"
    else
        log_warn "  [WARN] Oracle 源初始化有警告，详见 $logfile"
    fi
}

init_mysql_target() {
    local port="$1" label="$2"
    local logfile="${ARTIFACTS_DIR}/init-${label}.log"
    log_info "  初始化 ${label} (port ${port})"
    if mysql_exec "$port" < "$DST_FIXTURE" >> "$logfile" 2>&1; then
        log_info "  [OK] ${label}"
    else
        log_warn "  [WARN] ${label} 初始化有兼容性警告（已忽略）"
    fi
}

init_databases() {
    if [[ "$SKIP_INIT" == "true" ]]; then
        log_info "跳过 MySQL 目标端初始化 (--skip-init)"
    else
        log_info "=== MySQL 目标端初始化阶段 ==="
        for entry in "${TARGETS[@]}"; do
            local label port
            label="$(get_label "$entry")"
            port="$(get_port  "$entry")"
            label_in_filter "$label" "$FILTER_DST" || continue
            init_mysql_target "$port" "dst-${label}"
        done
    fi

    init_oracle_source
    log_info "=== 初始化完成 ==="
}

reinit_target() {
    local dst_label="$1" dst_port="$2"
    local logfile="${ARTIFACTS_DIR}/reinit-${dst_label}.log"
    mysql_exec "$dst_port" < "$DST_FIXTURE" >> "$logfile" 2>&1 || true
}

# ============================================================
# SECTION 7: 测试矩阵
# ============================================================
generate_test_matrix() {
    for dst_entry in "${TARGETS[@]}"; do
        local dst_label dst_port
        dst_label="$(get_label "$dst_entry")"
        dst_port="$(get_port  "$dst_entry")"
        label_in_filter "$dst_label" "$FILTER_DST" || continue

        for mode in "${MODES[@]}"; do
            label_in_filter "$mode" "$FILTER_MODE" || continue
            echo "${SRC_LABEL}:-:${dst_label}:${dst_port}:${mode}"
        done
    done
}

# ============================================================
# SECTION 8: 配置文件生成
# ============================================================
generate_gt_checksum_config() {
    local dst_port="$1" mode="$2" case_dir="$3"

    cat > "${case_dir}/gt-checksum.conf" <<EOF
srcDSN=${SRC_DSN}
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${dst_port})/information_schema?charset=utf8mb4
tables=${SRC_SCHEMA}.*
checkNoIndexTable=yes
caseSensitiveObjectName=yes
parallelThds=2
chunkSize=1000
queueSize=20
checkObject=${mode}
memoryLimit=3000
datafix=file
fixFileDir=${case_dir}/fixsql
logFile=${case_dir}/gt-checksum.log
logLevel=debug
EOF
}

generate_repairdb_config() {
    local dst_port="$1" case_dir="$2"

    cat > "${case_dir}/repairDB.conf" <<EOF
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${dst_port})/information_schema?charset=utf8mb4
parallelThds=4
fixFileDir=${case_dir}/fixsql
EOF
}

# ============================================================
# SECTION 9: 输出解析与结果判定
# ============================================================
parse_diffs_from_output() {
    local output_file="$1" mode="$2"
    local clean diffs_values=""
    clean="$(strip_ansi < "$output_file")"

    case "$mode" in
        data)
            diffs_values=$(echo "$clean" \
                | grep -iE '\bdata\b' \
                | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
                | awk 'NF>=7 {print $(NF-1)}' \
                | grep -iE '^(yes|no|warn-only|collation-mapped|DDL-yes)$' || true)
            ;;
        struct)
            diffs_values=$(echo "$clean" \
                | grep -iE '\bstruct\b|\bsequence\b' \
                | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
                | awk 'NF>=5 {print $(NF-1)}' \
                | grep -iE '^(yes|no|warn-only|collation-mapped|DDL-yes)$' || true)
            ;;
    esac

    if [[ -n "$diffs_values" ]]; then
        echo "$diffs_values" | sort -u | paste -sd',' -
    else
        echo ""
    fi
}

evaluate_diffs() {
    local diffs_csv="$1"
    [[ -z "$diffs_csv" ]] && { echo "NO_OUTPUT"; return; }

    IFS=',' read -ra diffs_array <<< "$diffs_csv"
    local has_yes=false
    for diff_val in "${diffs_array[@]}"; do
        diff_val="$(echo "$diff_val" | tr -d '[:space:]')"
        case "$diff_val" in
            no|warn-only|collation-mapped) ;;
            yes|DDL-yes)                   has_yes=true ;;
            *)                             has_yes=true ;;
        esac
    done
    $has_yes && echo "NEEDS_REPAIR" || echo "PASS"
}

# ============================================================
# SECTION 10: 单用例执行
# ============================================================
run_single_test_case() {
    local dst_label="$1" dst_port="$2" mode="$3"
    local case_id="${SRC_LABEL}-to-${dst_label}-${mode}"
    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"

    mkdir -p "${case_dir}"
    reinit_target "$dst_label" "$dst_port"

    generate_gt_checksum_config "$dst_port" "$mode" "$case_dir"
    generate_repairdb_config "$dst_port" "$case_dir"

    local round=0 final_verdict="UNKNOWN" diffs_summary=""

    while [[ $round -lt $((MAX_REPAIR_ROUNDS + 1)) ]]; do
        round=$((round + 1))

        rm -rf "${case_dir}/fixsql"
        mkdir -p "${case_dir}/fixsql"

        local gt_output="${case_dir}/round${round}-output.txt"
        local gt_exit=0
        run_with_timeout "$CASE_TIMEOUT" \
            "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" \
            > "$gt_output" 2>&1 || gt_exit=$?

        [[ -f "${case_dir}/gt-checksum.log" ]] && \
            cp "${case_dir}/gt-checksum.log" "${case_dir}/round${round}-gt-checksum.log" 2>/dev/null || true

        if [[ $gt_exit -eq 124 ]]; then
            final_verdict="TIMEOUT"
            log_error "  [${case_id}] Round ${round}: 超时 (${CASE_TIMEOUT}s)"
            break
        fi

        diffs_summary="$(parse_diffs_from_output "$gt_output" "$mode")"
        local verdict
        verdict="$(evaluate_diffs "$diffs_summary")"

        case "$verdict" in
            PASS)
                final_verdict="PASS"; break ;;
            NO_OUTPUT)
                if [[ $gt_exit -ne 0 ]]; then
                    final_verdict="ERROR"
                    log_error "  [${case_id}] Round ${round}: gt-checksum 异常退出 (exit=${gt_exit}) 且无可解析输出"
                    break
                fi
                final_verdict="PASS"
                log_warn "  [${case_id}] Round ${round}: 正常退出但未解析到 Diffs 行，视为 PASS"
                break ;;
            NEEDS_REPAIR)
                if [[ $round -gt $MAX_REPAIR_ROUNDS ]]; then
                    final_verdict="FAIL"
                    log_error "  [${case_id}] ${MAX_REPAIR_ROUNDS} 轮修复后仍有差异: ${diffs_summary}"
                    break
                fi
                local fixsql_count
                fixsql_count=$(find "${case_dir}/fixsql" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
                if [[ "$fixsql_count" -eq 0 ]]; then
                    final_verdict="FAIL"
                    log_error "  [${case_id}] Round ${round}: Diffs=yes 但未生成 fixsql"
                    break
                fi

                local repair_output="${case_dir}/round${round}-repair-output.txt"
                run_with_timeout "$CASE_TIMEOUT" \
                    "$REPAIR_DB" -conf "${case_dir}/repairDB.conf" \
                    > "$repair_output" 2>&1 || {
                    log_warn "  [${case_id}] Round ${round}: repairDB 非零退出"
                }
                log_info "  [${case_id}] Round ${round}: 修复完成 (fixsql=${fixsql_count} files)，准备重新校验"
                ;;
        esac
    done

    echo "${case_id}|${final_verdict}|${round}|${diffs_summary}" >> "${ARTIFACTS_DIR}/results.csv"
    echo "$final_verdict" > "${case_dir}/verdict"
}

# ============================================================
# SECTION 11: 最终修复（--final-repair）
# ============================================================

# 所有回归用例完成后，对目标库做一次完整修复使其与 Oracle 源一致。
# 按 struct → data 顺序对每个目标库做修复闭环。
run_final_repair() {
    log_info "=================================================================="
    log_info " 最终修复阶段 (--final-repair)"
    log_info "=================================================================="

    # 收集去重的目标实例
    local -a dst_pairs=()
    local -A seen_pairs=()
    for entry in "${test_matrix[@]}"; do
        IFS=':' read -r _src_label _src_port dst_label dst_port _mode <<< "$entry"
        local pair_key="${dst_label}:${dst_port}"
        if [[ -z "${seen_pairs[$pair_key]:-}" ]]; then
            seen_pairs[$pair_key]=1
            dst_pairs+=("$pair_key")
        fi
    done

    # 修复顺序：struct 先于 data
    local -a repair_order=(struct data)

    for pair_key in "${dst_pairs[@]}"; do
        IFS=':' read -r dst_label dst_port <<< "$pair_key"
        log_info "修复 ${SRC_LABEL} -> ${dst_label}:"

        # 重新初始化目标端到基线状态
        reinit_target "$dst_label" "$dst_port"

        for mode in "${repair_order[@]}"; do
            if ! label_in_filter "$mode" "$FILTER_MODE"; then
                continue
            fi

            local repair_dir="${ARTIFACTS_DIR}/final-repair/${SRC_LABEL}-to-${dst_label}-${mode}"
            mkdir -p "${repair_dir}/fixsql"

            generate_gt_checksum_config "$dst_port" "$mode" "$repair_dir"
            generate_repairdb_config "$dst_port" "$repair_dir"

            local round=0
            local converged=false
            while [[ $round -lt $((MAX_REPAIR_ROUNDS + 1)) ]]; do
                round=$((round + 1))
                rm -rf "${repair_dir}/fixsql"
                mkdir -p "${repair_dir}/fixsql"

                local gt_exit=0
                run_with_timeout "$CASE_TIMEOUT" \
                    "$GT_CHECKSUM" -c "${repair_dir}/gt-checksum.conf" \
                    > "${repair_dir}/round${round}-output.txt" 2>&1 || gt_exit=$?

                [[ -f "${repair_dir}/gt-checksum.log" ]] && \
                    cp "${repair_dir}/gt-checksum.log" "${repair_dir}/round${round}-gt-checksum.log" 2>/dev/null || true

                local diffs
                diffs="$(parse_diffs_from_output "${repair_dir}/round${round}-output.txt" "$mode")"
                local verdict
                verdict="$(evaluate_diffs "$diffs")"

                if [[ "$verdict" == "PASS" ]]; then
                    log_info "  ${mode}: Diffs=no (round ${round})"
                    converged=true
                    break
                fi

                if [[ "$verdict" == "NO_OUTPUT" ]]; then
                    if [[ $gt_exit -ne 0 ]]; then
                        log_error "  ${mode}: gt-checksum 异常退出 (exit=${gt_exit}) 且无可解析输出 (round ${round})"
                    else
                        log_warn "  ${mode}: gt-checksum 正常退出但未解析到 Diffs 行 (round ${round})，视为 PASS"
                        converged=true
                    fi
                    break
                fi

                if [[ $round -gt $MAX_REPAIR_ROUNDS ]]; then
                    log_warn "  ${mode}: 修复未收敛 (${MAX_REPAIR_ROUNDS} 轮后仍有差异: ${diffs})"
                    break
                fi

                local fixsql_count
                fixsql_count=$(find "${repair_dir}/fixsql" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
                if [[ "$fixsql_count" -eq 0 ]]; then
                    log_warn "  ${mode}: Diffs=${diffs} 但未生成 fixsql"
                    break
                fi

                run_with_timeout "$CASE_TIMEOUT" \
                    "$REPAIR_DB" -conf "${repair_dir}/repairDB.conf" \
                    > "${repair_dir}/round${round}-repair-output.txt" 2>&1 || {
                    log_warn "  ${mode}: repairDB 非零退出 (round ${round})"
                }
                log_info "  ${mode}: 修复完成 (fixsql=${fixsql_count} files)，准备重新校验 (round ${round})"
            done

            if ! $converged; then
                log_warn "  ${mode}: 最终修复未完全收敛"
            fi
        done
    done

    log_info "最终修复阶段完成"
    log_info ""
}

# ============================================================
# SECTION 12: 报告生成
# ============================================================
generate_report() {
    local results_file="${ARTIFACTS_DIR}/results.csv"
    local report_file="${ARTIFACTS_DIR}/report.txt"

    {
        echo "=================================================================="
        echo " gt-checksum Oracle→MySQL Regression Test Report"
        echo " Run ID:    ${RUN_ID}"
        echo " Date:      $(date '+%Y-%m-%d %H:%M:%S')"
        echo " Src DSN:   ${SRC_DSN}"
        echo " Src Schema:${SRC_SCHEMA}"
        echo " MySQL Host:${DB_HOST}"
        echo " Timeout:   ${CASE_TIMEOUT}s per case"
        echo " Max Rounds:${MAX_REPAIR_ROUNDS}"
        echo "=================================================================="
        echo ""
        printf "%-45s %-10s %-8s %s\n" "CASE" "VERDICT" "ROUNDS" "DIFFS"
        printf "%-45s %-10s %-8s %s\n" "----" "-------" "------" "-----"
        while IFS='|' read -r case_id verdict rounds diffs; do
            printf "%-45s %-10s %-8s %s\n" "$case_id" "$verdict" "$rounds" "$diffs"
        done < "$results_file"

        echo ""
        echo "=================================================================="
        echo " Summary"
        echo "=================================================================="
        echo " Total:    ${TOTAL}"
        echo " Passed:   ${PASSED}"
        echo " Failed:   ${FAILED}"
        echo " Errors:   ${ERRORS}"
        echo " Timeouts: ${TIMEOUTS}"
        echo "=================================================================="
    } > "$report_file"

    cat "$report_file"
}

# ============================================================
# SECTION 12: 主流程
# ============================================================
main() {
    parse_arguments "$@"
    resolve_src_alias
    setup_timeout_cmd
    mkdir -p "${ARTIFACTS_DIR}/cases"

    log_info "=================================================================="
    log_info " gt-checksum Oracle→MySQL Regression Test"
    log_info " Run ID: ${RUN_ID}"
    log_info " Artifacts: ${ARTIFACTS_DIR}"
    log_info "=================================================================="

    check_prerequisites
    build_binaries

    [[ "$DRY_RUN" != "true" ]] && check_connectivity

    local -a test_matrix=()
    while IFS= read -r line; do
        test_matrix+=("$line")
    done < <(generate_test_matrix)

    local total_cases=${#test_matrix[@]}
    if [[ $total_cases -eq 0 ]]; then
        log_error "测试矩阵为空，请检查 --dst/--mode 过滤参数"
        exit 1
    fi
    log_info "测试矩阵: ${total_cases} 个用例"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo ""
        printf "%-10s %-20s %-10s\n" "SOURCE" "TARGET" "MODE"
        printf "%-10s %-20s %-10s\n" "------" "------" "----"
        for entry in "${test_matrix[@]}"; do
            IFS=':' read -r src_label _ dst_label dst_port mode <<< "$entry"
            printf "%-10s %-20s %-10s\n" "$src_label" "${dst_label}:${dst_port}" "$mode"
        done
        echo ""
        echo "Total: ${total_cases} cases"
        exit 0
    fi

    init_databases

    : > "${ARTIFACTS_DIR}/results.csv"
    trap 'log_warn "中断信号，生成部分报告..."; generate_report; exit 130' INT TERM

    local case_num=0
    for entry in "${test_matrix[@]}"; do
        case_num=$((case_num + 1))
        IFS=':' read -r _src_label _src_port dst_label dst_port mode <<< "$entry"
        TOTAL=$((TOTAL + 1))
        log_info "[${case_num}/${total_cases}] ${SRC_LABEL} -> ${dst_label}:${dst_port} (${mode})"

        run_single_test_case "$dst_label" "$dst_port" "$mode"
        local case_id="${SRC_LABEL}-to-${dst_label}-${mode}"
        local verdict
        verdict="$(cat "${ARTIFACTS_DIR}/cases/${case_id}/verdict" 2>/dev/null || echo "ERROR")"

        case "$verdict" in
            PASS)    PASSED=$((PASSED + 1));     log_info  "[${case_num}/${total_cases}] PASS" ;;
            FAIL)    FAILED=$((FAILED + 1));     log_error "[${case_num}/${total_cases}] FAIL" ;;
            TIMEOUT) TIMEOUTS=$((TIMEOUTS + 1)); log_error "[${case_num}/${total_cases}] TIMEOUT" ;;
            *)       ERRORS=$((ERRORS + 1));     log_error "[${case_num}/${total_cases}] ERROR: ${verdict}" ;;
        esac
    done

    if [[ "$FINAL_REPAIR" == "true" ]]; then
        run_final_repair
    fi

    echo ""
    generate_report
    log_info "详细日志和配置: ${ARTIFACTS_DIR}"

    [[ $((FAILED + ERRORS + TIMEOUTS)) -gt 0 ]] && exit 1
    exit 0
}

main "$@"
