#!/usr/bin/env bash
# =============================================================================
# gt-checksum columns 功能回归测试脚本
#
# 用法:
#   bash scripts/regression-test-columns.sh [选项]
#
# 选项:
#   --host=IP             数据库主机地址（默认 127.0.0.1）
#   --user=USER           数据库用户名（默认 checksum）
#   --pass=PASS           数据库密码（默认 checksum）
#   --src-port=PORT       源端实例端口（必须指定）
#   --dst-port=PORT       目标端实例端口（必须指定）
#   --skip-init           跳过数据库初始化
#   --skip-build          跳过二进制编译
#   --timeout=SEC         单用例超时秒数（默认 120）
#   --artifacts-dir=PATH  自定义输出目录
#   --dry-run             仅打印测例列表，不执行
#   --enable-oracle       运行 columns 模式下 Oracle 端被拒绝的负向用例（TC-ORA-01）
#                         注意：columns 模式不支持任何一端为 Oracle，该开关仅用于
#                         验证"遇到 Oracle srcDSN 时程序会非零退出并给出明确错误"，
#                         并非启用 Oracle 数据源参与校验
#   --help                显示帮助
#
# 示例:
#   bash scripts/regression-test-columns.sh --src-port=3406 --dst-port=3408
#   bash scripts/regression-test-columns.sh --src-port=3406 --dst-port=3408 \
#       --skip-build --enable-oracle
# =============================================================================
set -euo pipefail

# ============================================================
# SECTION 1: 常量与默认值
# ============================================================
DB_HOST="127.0.0.1"
DB_USER="checksum"
DB_PASS="checksum"
DB_SCHEMA="gt_checksum_cols"

SRC_PORT=""
DST_PORT=""

CASE_TIMEOUT=120
MAX_REPAIR_ROUNDS=3
SKIP_INIT=false
SKIP_BUILD=false
DRY_RUN=false
ENABLE_ORACLE=false

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="columns-$(date +%Y%m%d-%H%M%S)"
ARTIFACTS_DIR="${ROOT_DIR}/test-artifacts/${RUN_ID}"
GT_CHECKSUM="${ROOT_DIR}/gt-checksum"
REPAIR_DB="${ROOT_DIR}/repairDB"

SRC_FIXTURE="${ROOT_DIR}/testcase/MySQL-columns-source.sql"
DST_FIXTURE="${ROOT_DIR}/testcase/MySQL-columns-target.sql"

TIMEOUT_CMD=""

# 统计（不含 FAIL-EXPECTED / ERROR-EXPECTED）
TOTAL=0
PASSED=0
FAILED=0
ERRORS=0
TIMEOUTS=0

# ============================================================
# SECTION 2: 日志工具
# ============================================================
log_info()  { echo "[INFO]  $*"; }
log_warn()  { echo "[WARN]  $*" >&2; }
log_error() { echo "[ERROR] $*" >&2; }

# ============================================================
# SECTION 3: 命令行参数解析
# ============================================================
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --host=*)         DB_HOST="${1#--host=}" ;;
            --user=*)         DB_USER="${1#--user=}" ;;
            --pass=*)         DB_PASS="${1#--pass=}" ;;
            --src-port=*)     SRC_PORT="${1#--src-port=}" ;;
            --dst-port=*)     DST_PORT="${1#--dst-port=}" ;;
            --timeout=*)      CASE_TIMEOUT="${1#--timeout=}" ;;
            --artifacts-dir=*) ARTIFACTS_DIR="${1#--artifacts-dir=}" ;;
            --skip-init)      SKIP_INIT=true ;;
            --skip-build)     SKIP_BUILD=true ;;
            --dry-run)        DRY_RUN=true ;;
            --enable-oracle)  ENABLE_ORACLE=true ;;
            --help|-h)
                sed -n '3,28p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
                exit 0
                ;;
            *)
                log_error "未知参数: $1"
                exit 1
                ;;
        esac
        shift
    done

    if [[ "$DRY_RUN" == "false" && ( -z "$SRC_PORT" || -z "$DST_PORT" ) ]]; then
        log_error "必须指定 --src-port 和 --dst-port"
        exit 1
    fi
}

# ============================================================
# SECTION 4: 工具函数
# ============================================================
mysql_exec() {
    local port="$1"
    shift
    mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASS}" -P"${port}" \
        --default-character-set=utf8mb4 -f "$@" 2>&1
}

run_with_timeout() {
    local seconds="$1"
    shift
    if [[ -n "$TIMEOUT_CMD" ]]; then
        "$TIMEOUT_CMD" "$seconds" "$@"
    else
        "$@"
    fi
}

strip_ansi() {
    sed $'s/\x1B\\[[0-9;]*m//g'
}

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

# ============================================================
# SECTION 5: 前置检查与编译
# ============================================================
check_prerequisites() {
    if [[ "$DRY_RUN" == "true" ]]; then return; fi

    local ok=true

    if ! command -v mysql &>/dev/null; then
        log_error "未找到 mysql 客户端"
        ok=false
    fi

    if [[ "$SKIP_BUILD" == "true" ]]; then
        if [[ ! -x "$GT_CHECKSUM" ]]; then
            log_error "gt-checksum 二进制不存在: $GT_CHECKSUM"
            ok=false
        fi
        if [[ ! -x "$REPAIR_DB" ]]; then
            log_error "repairDB 二进制不存在: $REPAIR_DB"
            ok=false
        fi
    fi

    if [[ ! -f "$SRC_FIXTURE" ]]; then
        log_error "源端 fixture 不存在: $SRC_FIXTURE"
        ok=false
    fi
    if [[ ! -f "$DST_FIXTURE" ]]; then
        log_error "目标端 fixture 不存在: $DST_FIXTURE"
        ok=false
    fi

    if [[ "$ok" == "false" ]]; then
        log_error "前置检查失败，退出"
        exit 1
    fi
}

build_binaries() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "跳过编译 (--skip-build)"
        return
    fi

    log_info "=== 编译阶段 ==="
    cd "$ROOT_DIR"

    log_info "  编译 gt-checksum (nooracle)..."
    CGO_ENABLED=0 go build -tags nooracle -o gt-checksum gt-checksum.go

    log_info "  编译 repairDB..."
    CGO_ENABLED=0 go build -o repairDB repairDB.go

    chmod +x gt-checksum repairDB
    log_info "  编译完成"
}

# ============================================================
# SECTION 6: 数据库连通性与初始化
# ============================================================
check_connectivity() {
    log_info "检查数据库连通性..."
    local all_ok=true

    for port in "$SRC_PORT" "$DST_PORT"; do
        if mysql_exec "$port" -e "SELECT 1" >/dev/null 2>&1; then
            log_info "  [OK] port $port"
        else
            log_error "  [FAIL] port $port 无法连接"
            all_ok=false
        fi
    done

    if [[ "$all_ok" == "false" ]]; then
        log_error "部分数据库实例无法连接，退出"
        exit 1
    fi
}

init_databases() {
    if [[ "$SKIP_INIT" == "true" ]]; then
        log_info "跳过数据库初始化 (--skip-init)"
        return
    fi

    log_info "=== 初始化数据库 ==="
    local log_src="${ARTIFACTS_DIR}/init-source.log"
    local log_dst="${ARTIFACTS_DIR}/init-target.log"

    log_info "  初始化源端 (port ${SRC_PORT})..."
    if mysql_exec "$SRC_PORT" < "$SRC_FIXTURE" > "$log_src" 2>&1; then
        log_info "  [OK] 源端"
    else
        log_warn "  [WARN] 源端初始化有兼容性警告（已忽略）"
    fi

    log_info "  初始化目标端 (port ${DST_PORT})..."
    if mysql_exec "$DST_PORT" < "$DST_FIXTURE" > "$log_dst" 2>&1; then
        log_info "  [OK] 目标端"
    else
        log_warn "  [WARN] 目标端初始化有兼容性警告（已忽略）"
    fi
}

# 用例间重置目标端（重新执行 fixture）
reinit_target() {
    local reinit_log="${ARTIFACTS_DIR}/reinit-target-$(date +%s).log"
    if ! mysql_exec "$DST_PORT" < "$DST_FIXTURE" > "$reinit_log" 2>&1; then
        log_warn "  reinit_target: 目标端重置返回非零，可能存在脏环境，详见 ${reinit_log}"
    fi
}

# ============================================================
# SECTION 7: 配置文件生成
# ============================================================

# 生成 gt-checksum.conf（含可选 columns 参数）
# 参数：src_port dst_port case_dir tables columns extra_rows_sync check_no_index
generate_checksum_config() {
    local src_port="$1"
    local dst_port="$2"
    local case_dir="$3"
    local tables="$4"
    local columns="${5:-}"
    local extra_rows_sync="${6:-OFF}"
    local check_no_index="${7:-no}"

    cat > "${case_dir}/gt-checksum.conf" <<EOF
srcDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${src_port})/information_schema?charset=utf8mb4
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${dst_port})/information_schema?charset=utf8mb4
tables=${tables}
checkNoIndexTable=${check_no_index}
caseSensitiveObjectName=yes
parallelThds=2
chunkSize=1000
queueSize=20
checkObject=data
memoryLimit=3000
datafix=file
fixFileDir=${case_dir}/fixsql
logFile=${case_dir}/gt-checksum.log
logLevel=debug
extraRowsSyncToSource=${extra_rows_sync}
EOF
    # columns 参数仅非空时追加，避免 columns="" 导致解析报错
    if [[ -n "$columns" ]]; then
        echo "columns=${columns}" >> "${case_dir}/gt-checksum.conf"
    fi
}

# 生成 Oracle srcDSN 配置（TC-ORA-01）
generate_oracle_error_config() {
    local dst_port="$1"
    local case_dir="$2"
    local columns="$3"

    cat > "${case_dir}/gt-checksum.conf" <<EOF
srcDSN=oracle|gt_checksum/gt_checksum@127.0.0.1:1521/orcl
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${dst_port})/information_schema?charset=utf8mb4
tables=${DB_SCHEMA}.col_data
checkNoIndexTable=no
caseSensitiveObjectName=yes
parallelThds=2
chunkSize=1000
queueSize=20
checkObject=data
memoryLimit=3000
datafix=file
fixFileDir=${case_dir}/fixsql
logFile=${case_dir}/gt-checksum.log
logLevel=debug
EOF
    if [[ -n "$columns" ]]; then
        echo "columns=${columns}" >> "${case_dir}/gt-checksum.conf"
    fi
}

generate_repairdb_config() {
    local dst_port="$1"
    local case_dir="$2"

    cat > "${case_dir}/repairDB.conf" <<EOF
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${dst_port})/information_schema?charset=utf8mb4
parallelThds=4
fixFileDir=${case_dir}/fixsql
EOF
}

# ============================================================
# SECTION 8: 输出解析与结果判定
# ============================================================

# 从 gt-checksum 输出中提取 data 模式的 Diffs 值（逗号分隔，去重）
parse_diffs_from_output() {
    local output_file="$1"
    local clean
    clean="$(strip_ansi < "$output_file")"

    # 扫描整行匹配 Diffs 取值集合，避免受末尾 Mapping/Columns 列影响（columns 模式下列数可变）
    local diffs_values
    diffs_values=$(echo "$clean" \
        | grep -iE '\bdata\b' \
        | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
        | awk '{
            for (i=1;i<=NF;i++) {
                v=tolower($i)
                if (v=="yes"||v=="no"||v=="warn-only"||v=="collation-mapped"||v=="ddl-yes") {
                    print $i; break
                }
            }
          }' || true)

    if [[ -n "$diffs_values" ]]; then
        echo "$diffs_values" | sort -u | paste -sd',' -
    else
        echo ""
    fi
}

# 检查 fixsql 目录是否全为注释/空行（advisory-only）
fixsql_is_advisory_only() {
    local fixsql_dir="$1"
    local sql_files
    sql_files=$(find "$fixsql_dir" -name "*.sql" -type f 2>/dev/null)
    if [[ -z "$sql_files" ]]; then
        return 1
    fi
    local exec_lines
    exec_lines=$(echo "$sql_files" | xargs grep -hv '^\s*--\|^\s*$' 2>/dev/null | wc -l | tr -d ' ')
    [[ "$exec_lines" -eq 0 ]]
}

# 检查 fixsql 是否全为 columns-advisory.*.sql 且无可执行 SQL
fixsql_is_columns_advisory_only() {
    local fixsql_dir="$1"
    # 必须有 columns-advisory.*.sql 文件
    local advisory_files
    advisory_files=$(find "$fixsql_dir" -name "columns-advisory.*.sql" -type f 2>/dev/null)
    if [[ -z "$advisory_files" ]]; then
        return 1
    fi
    # 所有 .sql 文件均为注释/空行
    fixsql_is_advisory_only "$fixsql_dir"
}

# 判定 Diffs 结果：PASS / NEEDS_REPAIR / DDL_YES / NO_OUTPUT
evaluate_diffs() {
    local diffs_csv="$1"

    if [[ -z "$diffs_csv" ]]; then
        echo "NO_OUTPUT"
        return
    fi

    local has_yes=false
    local has_ddl_yes=false

    IFS=',' read -ra diffs_array <<< "$diffs_csv"
    for diff_val in "${diffs_array[@]}"; do
        diff_val="$(echo "$diff_val" | tr -d '[:space:]')"
        case "$diff_val" in
            no|warn-only|collation-mapped) ;;
            DDL-yes) has_ddl_yes=true ;;
            yes|*) has_yes=true ;;
        esac
    done

    if $has_ddl_yes && ! $has_yes; then
        echo "DDL_YES"
    elif $has_yes || $has_ddl_yes; then
        echo "NEEDS_REPAIR"
    else
        echo "PASS"
    fi
}

# ============================================================
# SECTION 9: 单用例执行
# ============================================================

# 运行单个 columns 测例（含修复循环）
# 参数：case_id tables columns extra_rows_sync check_no_index expected_verdict
run_single_case() {
    local case_id="$1"
    local tables="$2"
    local columns="$3"
    local extra_rows_sync="${4:-OFF}"
    local check_no_index="${5:-no}"
    local expected_verdict="${6:-PASS}"

    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"
    mkdir -p "${case_dir}/fixsql"

    # 用例间重置目标端
    reinit_target

    # 生成配置
    generate_checksum_config \
        "$SRC_PORT" "$DST_PORT" "$case_dir" \
        "$tables" "$columns" "$extra_rows_sync" "$check_no_index"
    generate_repairdb_config "$DST_PORT" "$case_dir"

    local round=0
    local final_verdict="UNKNOWN"
    local diffs_summary=""

    while [[ $round -lt $((MAX_REPAIR_ROUNDS + 1)) ]]; do
        round=$((round + 1))

        # 清空 fixsql
        rm -rf "${case_dir}/fixsql"
        mkdir -p "${case_dir}/fixsql"

        local gt_output="${case_dir}/round${round}-output.txt"
        local gt_exit=0

        run_with_timeout "$CASE_TIMEOUT" \
            "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" \
            > "$gt_output" 2>&1 || gt_exit=$?

        # 保存日志快照
        if [[ -f "${case_dir}/gt-checksum.log" ]]; then
            cp "${case_dir}/gt-checksum.log" \
               "${case_dir}/round${round}-gt-checksum.log" 2>/dev/null || true
        fi

        # 超时判断
        if [[ $gt_exit -eq 124 ]]; then
            final_verdict="TIMEOUT"
            log_error "  [${case_id}] Round ${round}: 超时 (${CASE_TIMEOUT}s)"
            break
        fi

        # 解析 Diffs
        diffs_summary="$(parse_diffs_from_output "$gt_output")"

        # 判定
        local eval_result
        eval_result="$(evaluate_diffs "$diffs_summary")"

        case "$eval_result" in
            PASS)
                final_verdict="PASS"
                break
                ;;

            DDL_YES)
                # TC-06：无主键表的预期行为
                if [[ "$expected_verdict" == "FAIL-EXPECTED" ]]; then
                    final_verdict="FAIL-EXPECTED"
                    log_info "  [${case_id}] Round ${round}: DDL-yes（预期行为），verdict=FAIL-EXPECTED"
                else
                    final_verdict="FAIL"
                    log_error "  [${case_id}] Round ${round}: 意外的 DDL-yes: ${diffs_summary}"
                fi
                break
                ;;

            NO_OUTPUT)
                if [[ $gt_exit -ne 0 ]]; then
                    if [[ "$expected_verdict" == "ERROR-EXPECTED" ]]; then
                        final_verdict="ERROR-EXPECTED"
                        log_info "  [${case_id}] Round ${round}: 非零退出（预期行为），verdict=ERROR-EXPECTED"
                    else
                        final_verdict="ERROR"
                        log_error "  [${case_id}] Round ${round}: gt-checksum 异常退出 (exit=${gt_exit}) 且无可解析输出"
                    fi
                else
                    final_verdict="PASS"
                    log_warn "  [${case_id}] Round ${round}: gt-checksum 正常退出但无 Diffs 行，视为 PASS"
                fi
                break
                ;;

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

                # TC-03：columns-advisory 文件（source-only 行），无可执行 SQL
                if fixsql_is_columns_advisory_only "${case_dir}/fixsql"; then
                    final_verdict="PASS-ADVISORY"
                    log_info "  [${case_id}] Round ${round}: fixsql 均为 columns-advisory（source-only 行），verdict=PASS-ADVISORY"
                    break
                fi

                # 运行 repairDB
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

    # 校验实际结果是否与预期一致（TIMEOUT/ERROR 独立统计，不参与期望比对）
    if [[ "$final_verdict" != "TIMEOUT" && "$final_verdict" != "ERROR" ]]; then
        if [[ "$final_verdict" != "$expected_verdict" ]]; then
            log_error "  [${case_id}] 结果不符预期：期望=${expected_verdict}，实际=${final_verdict}"
            final_verdict="UNEXPECTED"
        fi
    fi

    # 写入结果
    echo "${case_id}|${final_verdict}|${round}|${diffs_summary}" \
        >> "${ARTIFACTS_DIR}/results.csv"
    echo "$final_verdict" > "${case_dir}/verdict"

    # 更新计数（FAIL-EXPECTED / ERROR-EXPECTED 不计入普通统计）
    TOTAL=$((TOTAL + 1))
    case "$final_verdict" in
        PASS|PASS-ADVISORY)    PASSED=$((PASSED + 1)) ;;
        FAIL|UNEXPECTED)       FAILED=$((FAILED + 1)) ;;
        ERROR)                 ERRORS=$((ERRORS + 1)) ;;
        TIMEOUT)               TIMEOUTS=$((TIMEOUTS + 1)) ;;
        FAIL-EXPECTED|ERROR-EXPECTED)
            log_info "  [${case_id}] 预期结果 ${final_verdict}（不计入失败统计）"
            ;;
    esac

    # 打印结果行
    local status_str="$final_verdict"
    if [[ "$final_verdict" == "PASS" || "$final_verdict" == "PASS-ADVISORY" \
        || "$final_verdict" == "FAIL-EXPECTED" || "$final_verdict" == "ERROR-EXPECTED" ]]; then
        log_info "  [${case_id}] ${status_str} (rounds=${round}, diffs=${diffs_summary:-—})"
    else
        log_error "  [${case_id}] ${status_str} (rounds=${round}, diffs=${diffs_summary:-—})"
    fi
}

# ============================================================
# SECTION 10: Oracle stub 错误处理测例（负向用例）
# columns 模式产品约束：不支持任何一端为 Oracle。
# 本用例使用 oracle srcDSN 触发 gt-checksum，验证程序会非零退出且错误消息可识别。
# ============================================================
run_oracle_stub_case() {
    local case_id="TC-ORA-01-cols-oracle-stub-error"
    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"
    mkdir -p "${case_dir}/fixsql"

    local columns="${DB_SCHEMA}.col_data.id:${DB_SCHEMA}.col_data.id,${DB_SCHEMA}.col_data.name:${DB_SCHEMA}.col_data.name"
    generate_oracle_error_config "$DST_PORT" "$case_dir" "$columns"

    log_info "  [${case_id}] 运行 Oracle stub 错误处理测例..."

    local gt_output="${case_dir}/round1-output.txt"
    local gt_exit=0

    run_with_timeout "$CASE_TIMEOUT" \
        "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" \
        > "$gt_output" 2>&1 || gt_exit=$?

    if [[ -f "${case_dir}/gt-checksum.log" ]]; then
        cp "${case_dir}/gt-checksum.log" "${case_dir}/round1-gt-checksum.log" 2>/dev/null || true
    fi

    local final_verdict="FAIL"
    local output_content
    output_content=$(cat "$gt_output" 2>/dev/null || true)

    # 预期：非零退出且含特定错误消息
    if [[ $gt_exit -ne 0 ]]; then
        # 可接受的错误消息（两种情况均为正确行为）：
        # 1. nooracle stub 在 OpenDB 阶段报错（当前行为）
        # 2. columns 校验层面明确拒绝 Oracle 端（未来理想行为）
        if echo "$output_content" | grep -qiE \
            'columns mode.*oracle|oracle.*columns|Failed to (connect|open)|connect.*fail|srcDSN.*oracle|oracle.*not supported'; then
            final_verdict="ERROR-EXPECTED"
            log_info "  [${case_id}] 非零退出且含预期错误消息，verdict=ERROR-EXPECTED"
        else
            # 非零退出但消息不符合预期，视为非预期失败（避免吞掉 panic 等非相关错误）
            final_verdict="FAIL"
            log_error "  [${case_id}] 非零退出但错误消息未匹配预期模式，verdict=FAIL (exit=${gt_exit})"
            log_error "  输出前3行: $(echo "$output_content" | head -3 | tr '\n' '|')"
        fi
    else
        # Oracle stub 下 exit=0 说明代码路径有问题
        final_verdict="FAIL"
        log_error "  [${case_id}] 预期非零退出，实际 exit=0（Oracle stub 未阻断执行）"
    fi

    echo "${case_id}|${final_verdict}|1|" >> "${ARTIFACTS_DIR}/results.csv"
    echo "$final_verdict" > "${case_dir}/verdict"

    TOTAL=$((TOTAL + 1))
    if [[ "$final_verdict" == "ERROR-EXPECTED" ]]; then
        log_info "  [${case_id}] ERROR-EXPECTED（不计入失败统计）"
    else
        FAILED=$((FAILED + 1))
        log_error "  [${case_id}] FAIL"
    fi
}

# ============================================================
# SECTION 11: 报告生成
# ============================================================
generate_report() {
    local results_file="${ARTIFACTS_DIR}/results.csv"
    local report_file="${ARTIFACTS_DIR}/report.txt"

    {
        echo "=================================================================="
        echo " gt-checksum Columns Regression Test Report"
        echo " Run ID:    ${RUN_ID}"
        echo " Date:      $(date '+%Y-%m-%d %H:%M:%S')"
        echo " Host:      ${DB_HOST}"
        echo " Src Port:  ${SRC_PORT}"
        echo " Dst Port:  ${DST_PORT}"
        echo " Timeout:   ${CASE_TIMEOUT}s per case"
        echo "=================================================================="
        echo ""
        printf "%-45s %-16s %-8s %s\n" "CASE" "VERDICT" "ROUNDS" "DIFFS"
        printf "%-45s %-16s %-8s %s\n" "----" "-------" "------" "-----"

        while IFS='|' read -r case_id verdict rounds diffs; do
            printf "%-45s %-16s %-8s %s\n" "$case_id" "$verdict" "$rounds" "${diffs:-—}"
        done < "$results_file"

        echo ""
        echo "=================================================================="
        echo " Summary"
        echo "=================================================================="
        echo " Total:    ${TOTAL}  (含 FAIL-EXPECTED / ERROR-EXPECTED)"
        echo " Passed:   ${PASSED}  (PASS + PASS-ADVISORY)"
        echo " Failed:   ${FAILED}"
        echo " Errors:   ${ERRORS}"
        echo " Timeouts: ${TIMEOUTS}"
        echo "=================================================================="

    } | tee "$report_file"
}

# ============================================================
# SECTION 12: 测例定义与主流程
# ============================================================

# 打印测例列表（--dry-run 时使用）
print_test_cases() {
    echo "=================================================================="
    echo " columns 功能回归测例列表"
    echo "=================================================================="
    echo ""
    printf "%-40s %-18s %s\n" "用例 ID" "预期 Verdict" "场景描述"
    printf "%-40s %-18s %s\n" "-------" "------------" "--------"
    printf "%-40s %-18s %s\n" "TC-01-cols-basic-ignore"         "PASS"           "非选中列差异被忽略"
    printf "%-40s %-18s %s\n" "TC-02-cols-selected-diff-fix"    "PASS"           "选中列差异修复后收敛"
    printf "%-40s %-18s %s\n" "TC-03-cols-source-only-advisory" "PASS-ADVISORY"  "source-only 行生成 advisory 文件"
    printf "%-40s %-18s %s\n" "TC-04-cols-simple-syntax"        "PASS"           "简单语法 columns=score"
    printf "%-40s %-18s %s\n" "TC-05-cols-cross-table-mapping"  "PASS"           "跨表列名映射修复后收敛"
    printf "%-40s %-18s %s\n" "TC-06-cols-no-pk-ddl-yes"        "ERROR-EXPECTED" "无主键表→非零退出（预期行为）"
    printf "%-40s %-18s %s\n" "TC-07-cols-target-only-extra"    "PASS"           "target-only 行+extraRowsSyncToSource"
    printf "%-40s %-18s %s\n" "TC-08-cols-simple-multi-col"     "PASS"           "简单语法多字段 columns=score,note"
    if [[ "$ENABLE_ORACLE" == "true" ]]; then
    printf "%-40s %-18s %s\n" "TC-ORA-01-cols-oracle-stub"      "ERROR-EXPECTED" "Oracle srcDSN stub 错误处理"
    fi
    echo ""
}

main() {
    parse_arguments "$@"

    if [[ "$DRY_RUN" == "true" ]]; then
        print_test_cases
        exit 0
    fi

    setup_timeout_cmd

    mkdir -p "${ARTIFACTS_DIR}/cases"
    # 初始化结果文件
    : > "${ARTIFACTS_DIR}/results.csv"

    log_info "=================================================================="
    log_info " gt-checksum Columns Regression Test"
    log_info " Run ID:   ${RUN_ID}"
    log_info " Src Port: ${SRC_PORT}  Dst Port: ${DST_PORT}"
    log_info " Artifacts: ${ARTIFACTS_DIR}"
    log_info "=================================================================="

    check_prerequisites
    build_binaries
    check_connectivity
    init_databases

    log_info ""
    log_info "=== 执行测例 ==="

    # TC-01: 非选中列差异被忽略
    # ignored_col 两端不同，但选中列 id/name 完全一致 → Diffs=no
    log_info ""
    log_info "--- TC-01: 非选中列差异被忽略 ---"
    run_single_case \
        "TC-01-cols-basic-ignore" \
        "${DB_SCHEMA}.col_data" \
        "${DB_SCHEMA}.col_data.id:${DB_SCHEMA}.col_data.id,${DB_SCHEMA}.col_data.name:${DB_SCHEMA}.col_data.name" \
        "OFF" "no" "PASS"

    # TC-02: 选中列差异，修复后收敛
    # amount 两端不同，修复后 Diffs=no
    log_info ""
    log_info "--- TC-02: 选中列差异修复后收敛 ---"
    run_single_case \
        "TC-02-cols-selected-diff-fix" \
        "${DB_SCHEMA}.order_data" \
        "${DB_SCHEMA}.order_data.order_id:${DB_SCHEMA}.order_data.order_id,${DB_SCHEMA}.order_data.amount:${DB_SCHEMA}.order_data.amount" \
        "OFF" "no" "PASS"

    # TC-03: source-only 行 → advisory 文件
    # event_id=5 只在源端，目标端无此行
    log_info ""
    log_info "--- TC-03: source-only 行生成 advisory 文件 ---"
    run_single_case \
        "TC-03-cols-source-only-advisory" \
        "${DB_SCHEMA}.events" \
        "${DB_SCHEMA}.events.event_id:${DB_SCHEMA}.events.event_id,${DB_SCHEMA}.events.status:${DB_SCHEMA}.events.status" \
        "OFF" "no" "PASS-ADVISORY"

    # TC-04: 简单语法（格式一）
    # tables=单表，columns=score（不含 schema.table 前缀）
    log_info ""
    log_info "--- TC-04: 简单语法 columns=score ---"
    run_single_case \
        "TC-04-cols-simple-syntax" \
        "${DB_SCHEMA}.product" \
        "score" \
        "OFF" "no" "PASS"

    # TC-05: 跨表列名映射
    # old_orders.src_total 对应 new_orders.dst_total
    log_info ""
    log_info "--- TC-05: 跨表列名映射修复后收敛 ---"
    run_single_case \
        "TC-05-cols-cross-table-mapping" \
        "${DB_SCHEMA}.old_orders:${DB_SCHEMA}.new_orders" \
        "${DB_SCHEMA}.old_orders.src_total:${DB_SCHEMA}.new_orders.dst_total" \
        "OFF" "no" "PASS"

    # TC-06: 无主键表 → 预期非零退出（ERROR-EXPECTED）
    # heap_data 无主键，columns 模式下校验链路会在预检阶段拒绝无主键表，程序以非零退出
    # 注：columns 选项强制所有列对同属一张表对，因此不能再混入 col_data 规避
    log_info ""
    log_info "--- TC-06: 无主键表→非零退出（预期行为） ---"
    run_single_case \
        "TC-06-cols-no-pk-ddl-yes" \
        "${DB_SCHEMA}.heap_data" \
        "${DB_SCHEMA}.heap_data.val:${DB_SCHEMA}.heap_data.val" \
        "OFF" "yes" "ERROR-EXPECTED"

    # TC-07: target-only 行 + extraRowsSyncToSource=ON
    # 目标端多出 item_id=99，开启 extraRowsSyncToSource 生成 DELETE
    log_info ""
    log_info "--- TC-07: target-only 行+extraRowsSyncToSource=ON ---"
    run_single_case \
        "TC-07-cols-target-only-extra" \
        "${DB_SCHEMA}.inventory" \
        "${DB_SCHEMA}.inventory.item_id:${DB_SCHEMA}.inventory.item_id,${DB_SCHEMA}.inventory.qty:${DB_SCHEMA}.inventory.qty" \
        "ON" "no" "PASS"

    # TC-08: 简单语法多字段 columns=score,note
    # product 表两列均有差异（score/note），简单语法不带 schema.table 前缀
    # 预期：Round1 Diffs=yes → 修复 score + note → Round2 Diffs=no → PASS
    log_info ""
    log_info "--- TC-08: 简单语法多字段 columns=score,note ---"
    run_single_case \
        "TC-08-cols-simple-multi-col" \
        "${DB_SCHEMA}.product" \
        "score,note" \
        "OFF" "no" "PASS"

    # TC-ORA-01: Oracle stub 错误处理（需 --enable-oracle）
    if [[ "$ENABLE_ORACLE" == "true" ]]; then
        log_info ""
        log_info "--- TC-ORA-01: Oracle srcDSN stub 错误处理 ---"
        run_oracle_stub_case
    fi

    log_info ""
    log_info "=== 全部测例执行完毕 ==="
    log_info ""

    generate_report

    # 决定退出码（FAIL-EXPECTED / ERROR-EXPECTED 不触发非零退出）
    if [[ $FAILED -gt 0 || $ERRORS -gt 0 || $TIMEOUTS -gt 0 ]]; then
        exit 1
    fi
}

# 信号处理：收到中断时仍生成部分报告
trap 'log_warn "收到中断信号，生成部分报告..."; generate_report; exit 130' INT TERM

main "$@"
