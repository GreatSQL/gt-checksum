#!/usr/bin/env bash
# =============================================================================
# gt-checksum 多源数据库回归测试脚本
#
# 用法:
#   bash scripts/regression-test.sh [选项]
#
# 选项:
#   --src=label1,label2       仅测试指定源（如 --src=mysql56,mariadb105）
#   --dst=label1,label2       仅测试指定目标（如 --dst=mysql80）
#   --mode=m1,m2              仅测试指定模式（如 --mode=struct,data）
#   --host=IP                 数据库主机地址（默认 127.0.0.1）
#   --user=USER               数据库用户名（默认 checksum）
#   --pass=PASS               数据库密码（默认 checksum）
#   --skip-init               跳过数据库初始化
#   --skip-build              跳过二进制编译
#   --timeout=SEC             单用例超时秒数（默认 600）
#   --max-rounds=N            最大修复轮次（默认 3）
#   --dry-run                 仅打印测试矩阵，不执行
#   --final-repair            回归完成后对目标库做一次完整修复闭环
#   --artifacts-dir=PATH      自定义输出目录
#   --help                    显示帮助
# =============================================================================
set -euo pipefail

# ============================================================
# SECTION 1: 配置常量
# ============================================================
DB_HOST="127.0.0.1"
DB_USER="checksum"
DB_PASS="checksum"
DB_SCHEMA="gt_checksum"

# 源数据库实例：label:port:family
SOURCES=(
    "mysql56:3404:mysql"
    "mysql57:3405:mysql"
    "mysql80:3406:mysql"
    "mysql84:3408:mysql"
    "mariadb100:3411:mariadb"
    "mariadb105:3407:mariadb"
    "mariadb106:3410:mariadb"
    "mariadb1011:3409:mariadb"
    "mariadb123:3412:mariadb"
)

# 目标数据库实例：label:port:family
# Note: the manual also lists MariaDB 11.4 / 11.5 for MariaDB->MariaDB,
# but the current regression environment only has confirmed ports for the
# versions below, so the matrix is constrained to locally wired instances.
TARGETS=(
    "mysql56:3404:mysql"
    "mysql57:3405:mysql"
    "mysql80:3406:mysql"
    "mysql84:3408:mysql"
    "mariadb105:3407:mariadb"
    "mariadb105:3407:mariadb"
    "mariadb106:3410:mariadb"
    "mariadb1011:3409:mariadb"
    "mariadb123:3412:mariadb"
)

# 校验模式
MODES=(data struct trigger routine)

# 执行参数
MAX_REPAIR_ROUNDS=3
CASE_TIMEOUT=600

# 路径
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="regression-$(date +%Y%m%d-%H%M%S)"
ARTIFACTS_DIR="${ROOT_DIR}/test-artifacts/${RUN_ID}"
GT_CHECKSUM="${ROOT_DIR}/gt-checksum"
REPAIR_DB="${ROOT_DIR}/repairDB"

# 测试数据
SRC_FIXTURE="${ROOT_DIR}/testcase/MySQL-source.sql"
DST_FIXTURE="${ROOT_DIR}/testcase/MySQL-target.sql"

# 控制标志
SKIP_INIT=false
SKIP_BUILD=false
DRY_RUN=false
FINAL_REPAIR=false
FILTER_SRC=""
FILTER_DST=""
FILTER_MODE=""

# 超时命令
TIMEOUT_CMD=""

# 统计
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
            --src=*)        FILTER_SRC="${1#--src=}" ;;
            --dst=*)        FILTER_DST="${1#--dst=}" ;;
            --mode=*)       FILTER_MODE="${1#--mode=}" ;;
            --host=*)       DB_HOST="${1#--host=}" ;;
            --user=*)       DB_USER="${1#--user=}" ;;
            --pass=*)       DB_PASS="${1#--pass=}" ;;
            --skip-init)    SKIP_INIT=true ;;
            --skip-build)   SKIP_BUILD=true ;;
            --dry-run)      DRY_RUN=true ;;
            --final-repair) FINAL_REPAIR=true ;;
            --timeout=*)    CASE_TIMEOUT="${1#--timeout=}" ;;
            --max-rounds=*) MAX_REPAIR_ROUNDS="${1#--max-rounds=}" ;;
            --artifacts-dir=*) ARTIFACTS_DIR="${1#--artifacts-dir=}" ;;
            --help|-h)      show_help; exit 0 ;;
            *)              log_error "Unknown option: $1"; show_help; exit 1 ;;
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

# 从 entry 字符串提取端口
get_port() {
    echo "$1" | cut -d: -f2
}

# 从 entry 字符串提取标签
get_label() {
    echo "$1" | cut -d: -f1
}

# 从 entry 字符串提取数据库家族
get_family() {
    echo "$1" | cut -d: -f3
}

mysql_version_rank() {
    case "$1" in
        mysql56) echo 56 ;;
        mysql57) echo 57 ;;
        mysql80) echo 80 ;;
        mysql84) echo 84 ;;
        *) echo 0 ;;
    esac
}

mariadb_version_rank() {
    case "$1" in
        mariadb100) echo 100 ;;
        mariadb105) echo 105 ;;
        mariadb106) echo 106 ;;
        mariadb1011) echo 1011 ;;
        mariadb123) echo 1203 ;;
        *) echo 0 ;;
    esac
}

is_supported_pair() {
    local src_label="$1"
    local src_family="$2"
    local dst_label="$3"
    local dst_family="$4"
    local src_rank dst_rank

    if [[ "$src_family" == "mysql" && "$dst_family" == "mysql" ]]; then
        src_rank="$(mysql_version_rank "$src_label")"
        dst_rank="$(mysql_version_rank "$dst_label")"
        [[ "$src_rank" -gt 0 && "$dst_rank" -gt 0 && "$src_rank" -le "$dst_rank" ]]
        return
    fi

    if [[ "$src_family" == "mariadb" && "$dst_family" == "mysql" ]]; then
        [[ "$dst_label" == "mysql80" || "$dst_label" == "mysql84" ]]
        return
    fi

    if [[ "$src_family" == "mariadb" && "$dst_family" == "mariadb" ]]; then
        src_rank="$(mariadb_version_rank "$src_label")"
        dst_rank="$(mariadb_version_rank "$dst_label")"
        [[ "$src_rank" -gt 0 && "$dst_rank" -gt 0 && "$src_rank" -le "$dst_rank" ]]
        return
    fi

    return 1
}

# 执行 MySQL 命令（force 模式，忽略版本兼容错误）
mysql_exec() {
    local port="$1"
    shift
    mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASS}" -P"${port}" \
        --default-character-set=utf8mb4 -f "$@" 2>&1
}

# 带超时执行命令
run_with_timeout() {
    local seconds="$1"
    shift
    if [[ -n "$TIMEOUT_CMD" ]]; then
        "$TIMEOUT_CMD" "$seconds" "$@"
    else
        "$@"
    fi
}

# 剥离 ANSI 颜色码
strip_ansi() {
    sed $'s/\x1B\\[[0-9;]*m//g'
}

# 检查标签是否在过滤列表中
label_in_filter() {
    local label="$1"
    local filter="$2"
    if [[ -z "$filter" ]]; then
        return 0  # 无过滤，全部通过
    fi
    IFS=',' read -ra items <<< "$filter"
    for item in "${items[@]}"; do
        if [[ "$label" == "$item" ]]; then
            return 0
        fi
    done
    return 1
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

    if [[ "$DRY_RUN" == "true" ]]; then
        return
    fi

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
        log_error "源端初始化脚本不存在: $SRC_FIXTURE"
        ok=false
    fi
    if [[ ! -f "$DST_FIXTURE" ]]; then
        log_error "目标端初始化脚本不存在: $DST_FIXTURE"
        ok=false
    fi

    if [[ "$ok" == "false" ]]; then
        log_error "前置检查失败，退出"
        exit 1
    fi
}

check_connectivity() {
    log_info "检查数据库连通性..."
    local all_ok=true

    for entry in "${SOURCES[@]}"; do
        local label port
        label="$(get_label "$entry")"
        port="$(get_port "$entry")"
        if ! label_in_filter "$label" "$FILTER_SRC"; then
            continue
        fi
        if mysql_exec "$port" -e "SELECT 1" >/dev/null 2>&1; then
            log_info "  [OK] $label (port $port)"
        else
            log_error "  [FAIL] $label (port $port) 无法连接"
            all_ok=false
        fi
    done

    for entry in "${TARGETS[@]}"; do
        local label port
        label="$(get_label "$entry")"
        port="$(get_port "$entry")"
        if ! label_in_filter "$label" "$FILTER_DST"; then
            continue
        fi
        if mysql_exec "$port" -e "SELECT 1" >/dev/null 2>&1; then
            log_info "  [OK] $label (port $port)"
        else
            log_error "  [FAIL] $label (port $port) 无法连接"
            all_ok=false
        fi
    done

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

    log_info "=== 编译阶段 ==="
    cd "$ROOT_DIR"

    log_info "  编译 gt-checksum (nooracle)..."
    CGO_ENABLED=0 go build -tags nooracle -o gt-checksum gt-checksum.go

    log_info "  编译 repairDB..."
    CGO_ENABLED=0 go build -o repairDB repairDB.go

    log_info "  运行 repairDB 单元测试..."
    CGO_ENABLED=0 go test -count=1 repairDB.go repairDB_test.go

    chmod +x gt-checksum repairDB
    log_info "  编译完成"
}

# ============================================================
# SECTION 6: 数据库初始化
# ============================================================
init_database_instance() {
    local port="$1"
    local label="$2"
    local fixture="$3"
    local logfile="${ARTIFACTS_DIR}/init-${label}.log"

    log_info "  初始化 ${label} (port ${port})"
    if mysql_exec "$port" < "$fixture" >> "$logfile" 2>&1; then
        log_info "  [OK] ${label}"
    else
        log_warn "  [WARN] ${label} 初始化有兼容性警告（已忽略）"
    fi
}

init_databases() {
    if [[ "$SKIP_INIT" == "true" ]]; then
        log_info "跳过数据库初始化 (--skip-init)"
        return
    fi

    log_info "=== 数据库初始化阶段 ==="

    # 初始化所有源端
    for entry in "${SOURCES[@]}"; do
        local label port
        label="$(get_label "$entry")"
        port="$(get_port "$entry")"
        if ! label_in_filter "$label" "$FILTER_SRC"; then
            continue
        fi
        init_database_instance "$port" "src-${label}" "$SRC_FIXTURE"
    done

    # 初始化所有目标端
    for entry in "${TARGETS[@]}"; do
        local label port
        label="$(get_label "$entry")"
        port="$(get_port "$entry")"
        if ! label_in_filter "$label" "$FILTER_DST"; then
            continue
        fi
        init_database_instance "$port" "dst-${label}" "$DST_FIXTURE"
    done

    log_info "=== 数据库初始化完成 ==="
}

# 重新初始化目标端（用例间隔离）
reinit_target() {
    local dst_label="$1"
    local dst_port="$2"
    local logfile="${ARTIFACTS_DIR}/reinit-${dst_label}.log"

    mysql_exec "$dst_port" < "$DST_FIXTURE" >> "$logfile" 2>&1 || true
}

# 重新初始化源端（trigger/routine 模式前重置）
reinit_source() {
    local src_label="$1"
    local src_port="$2"
    local logfile="${ARTIFACTS_DIR}/reinit-${src_label}.log"

    mysql_exec "$src_port" < "$SRC_FIXTURE" >> "$logfile" 2>&1 || true
}

# ============================================================
# SECTION 7: 测试矩阵生成
# ============================================================
generate_test_matrix() {
    for src_entry in "${SOURCES[@]}"; do
        local src_label src_port src_family
        src_label="$(get_label "$src_entry")"
        src_port="$(get_port "$src_entry")"
        src_family="$(get_family "$src_entry")"

        if ! label_in_filter "$src_label" "$FILTER_SRC"; then
            continue
        fi

        for dst_entry in "${TARGETS[@]}"; do
            local dst_label dst_port dst_family
            dst_label="$(get_label "$dst_entry")"
            dst_port="$(get_port "$dst_entry")"
            dst_family="$(get_family "$dst_entry")"

            if ! label_in_filter "$dst_label" "$FILTER_DST"; then
                continue
            fi

            if ! is_supported_pair "$src_label" "$src_family" "$dst_label" "$dst_family"; then
                continue
            fi

            # Regression env exposes one instance per version, so same-port
            # pairs would compare the same database against itself.
            if [[ "$src_port" == "$dst_port" ]]; then
                continue
            fi

            for mode in "${MODES[@]}"; do
                if ! label_in_filter "$mode" "$FILTER_MODE"; then
                    continue
                fi
                echo "${src_label}:${src_port}:${dst_label}:${dst_port}:${mode}"
            done
        done
    done
}

# ============================================================
# SECTION 8: 配置文件生成
# ============================================================
generate_gt_checksum_config() {
    local src_port="$1" dst_port="$2" mode="$3" case_dir="$4"

    cat > "${case_dir}/gt-checksum.conf" <<EOF
srcDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${src_port})/information_schema?charset=utf8mb4
dstDSN=mysql|${DB_USER}:${DB_PASS}@tcp(${DB_HOST}:${dst_port})/information_schema?charset=utf8mb4
tables=${DB_SCHEMA}.*
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

# 从 gt-checksum 输出中提取所有 Diffs 值
parse_diffs_from_output() {
    local output_file="$1"
    local mode="$2"

    local clean
    clean="$(strip_ansi < "$output_file")"

    local diffs_values=""

    case "$mode" in
        data)
            # 列：Schema Table IndexColumn CheckObject Rows Diffs Datafix
            # 匹配包含 "data" 的行，取倒数第2列
            diffs_values=$(echo "$clean" \
                | grep -iE '\bdata\b' \
                | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
                | awk 'NF>=7 {print $(NF-1)}' \
                | grep -iE '^(yes|no|warn-only|collation-mapped|DDL-yes)$' || true)
            ;;
        struct)
            # 列：Schema Table CheckObject Diffs Datafix
            diffs_values=$(echo "$clean" \
                | grep -iE '\bstruct\b|\bsequence\b' \
                | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
                | awk 'NF>=5 {print $(NF-1)}' \
                | grep -iE '^(yes|no|warn-only|collation-mapped|DDL-yes)$' || true)
            ;;
        trigger)
            # 列：Schema TriggerName CheckObject Diffs Datafix
            diffs_values=$(echo "$clean" \
                | grep -iE '\bTrigger\b' \
                | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
                | awk 'NF>=5 {print $(NF-1)}' \
                | grep -iE '^(yes|no|warn-only|collation-mapped|DDL-yes)$' || true)
            ;;
        routine)
            # 列：Schema RoutineName CheckObject DIFFS Datafix
            diffs_values=$(echo "$clean" \
                | grep -iE '\bProcedure\b|\bFunction\b' \
                | grep -vE '^\[|^Initializing|^Opening|^Checking|^gt-checksum|^$|^Schema' \
                | awk 'NF>=5 {print $(NF-1)}' \
                | grep -iE '^(yes|no|warn-only|collation-mapped|DDL-yes)$' || true)
            ;;
    esac

    # 去重，逗号分隔
    if [[ -n "$diffs_values" ]]; then
        echo "$diffs_values" | sort -u | paste -sd',' -
    else
        echo ""
    fi
}

# 检查 fixsql 目录中的所有 .sql 文件是否均为 advisory-only（无可执行 SQL）。
# VIEW 差异的修复建议以注释形式写入（-- advisory begin ... -- advisory end），
# repairDB 无法执行这类文件；检测到 advisory-only 时回归测试应视为 PASS-ADVISORY
# 而非触发修复循环。
# 返回值：0 = advisory-only，1 = 含可执行 SQL
fixsql_is_advisory_only() {
    local fixsql_dir="$1"
    local sql_files
    sql_files=$(find "$fixsql_dir" -name "*.sql" -type f 2>/dev/null)
    if [[ -z "$sql_files" ]]; then
        return 1  # 无文件，不视为 advisory-only
    fi
    # 检查是否存在既非空行也非注释行的内容
    local exec_lines
    exec_lines=$(echo "$sql_files" | xargs grep -hv '^\s*--\|^\s*$' 2>/dev/null | wc -l | tr -d ' ')
    [[ "$exec_lines" -eq 0 ]]
}

# 仅当 fixsql 全为 advisory-only，且 advisory kind 全部属于 VIEW 相关项时返回成功。
# 这样可以避免把 TABLE/CHECK/partition 等其他 advisory-only 结果误判成 VIEW 已收敛。
fixsql_is_view_advisory_only() {
    local fixsql_dir="$1"
    local sql_files
    sql_files=$(find "$fixsql_dir" -name "*.sql" -type f 2>/dev/null)
    if [[ -z "$sql_files" ]]; then
        return 1
    fi
    fixsql_is_advisory_only "$fixsql_dir" || return 1

    local kind_lines
    kind_lines=$(echo "$sql_files" | xargs grep -hE '^\s*--\s*kind:' 2>/dev/null || true)
    if [[ -z "$kind_lines" ]]; then
        return 1
    fi

    local non_view_kind
    non_view_kind=$(echo "$kind_lines" | grep -viE 'VIEW DEFINITION|VIEW COLUMN METADATA' || true)
    [[ -z "$non_view_kind" ]]
}

# 判定 Diffs 结果：返回 PASS / NEEDS_REPAIR / NO_OUTPUT
evaluate_diffs() {
    local diffs_csv="$1"
    local src_label="$2"

    if [[ -z "$diffs_csv" ]]; then
        echo "NO_OUTPUT"
        return
    fi

    IFS=',' read -ra diffs_array <<< "$diffs_csv"

    local has_yes=false

    for diff_val in "${diffs_array[@]}"; do
        diff_val="$(echo "$diff_val" | tr -d '[:space:]')"
        case "$diff_val" in
            no)
                ;;
            warn-only)
                # 符合预期
                ;;
            collation-mapped)
                # MariaDB 12.3 的 collation-mapped 符合预期；
                # 其他 MariaDB 版本如果出现也视为可接受（uca1400 映射）
                ;;
            yes|DDL-yes)
                has_yes=true
                ;;
            *)
                # 未知值视为需要修复
                has_yes=true
                ;;
        esac
    done

    if $has_yes; then
        echo "NEEDS_REPAIR"
    else
        echo "PASS"
    fi
}

# ============================================================
# SECTION 10: 单用例执行
# ============================================================
run_single_test_case() {
    local src_label="$1" src_port="$2" dst_label="$3" dst_port="$4" mode="$5"
    local case_id="${src_label}-to-${dst_label}-${mode}"
    local case_dir="${ARTIFACTS_DIR}/cases/${case_id}"

    mkdir -p "${case_dir}"

    # 重新初始化目标端（用例隔离）
    reinit_target "$dst_label" "$dst_port"

    # trigger/routine 模式下重新初始化源端
    if [[ "$mode" == "trigger" || "$mode" == "routine" ]]; then
        reinit_source "$src_label" "$src_port"
    fi

    # 生成配置
    generate_gt_checksum_config "$src_port" "$dst_port" "$mode" "$case_dir"
    generate_repairdb_config "$dst_port" "$case_dir"

    local round=0
    local final_verdict="UNKNOWN"
    local diffs_summary=""

    while [[ $round -lt $((MAX_REPAIR_ROUNDS + 1)) ]]; do
        round=$((round + 1))

        # 清空 fixsql
        rm -rf "${case_dir}/fixsql"
        mkdir -p "${case_dir}/fixsql"

        # 运行 gt-checksum
        local gt_output="${case_dir}/round${round}-output.txt"
        local gt_exit=0

        run_with_timeout "$CASE_TIMEOUT" \
            "$GT_CHECKSUM" -c "${case_dir}/gt-checksum.conf" \
            > "$gt_output" 2>&1 || gt_exit=$?

        # 保存日志
        if [[ -f "${case_dir}/gt-checksum.log" ]]; then
            cp "${case_dir}/gt-checksum.log" "${case_dir}/round${round}-gt-checksum.log" 2>/dev/null || true
        fi

        # 超时判断
        if [[ $gt_exit -eq 124 ]]; then
            final_verdict="TIMEOUT"
            log_error "  [${case_id}] Round ${round}: 超时 (${CASE_TIMEOUT}s)"
            break
        fi

        # 解析 Diffs
        diffs_summary="$(parse_diffs_from_output "$gt_output" "$mode")"

        # 判定
        local verdict
        verdict="$(evaluate_diffs "$diffs_summary" "$src_label")"

        case "$verdict" in
            PASS)
                final_verdict="PASS"
                break
                ;;
            NO_OUTPUT)
                # gt-checksum 未产生可解析的结果行（可能崩溃或输出格式异常）
                if [[ $gt_exit -ne 0 ]]; then
                    final_verdict="ERROR"
                    log_error "  [${case_id}] Round ${round}: gt-checksum 异常退出 (exit=${gt_exit}) 且无可解析输出"
                    break
                fi
                # exit=0 但无输出，视为 PASS（如目标对象为空）
                final_verdict="PASS"
                log_warn "  [${case_id}] Round ${round}: gt-checksum 正常退出但未解析到 Diffs 行，视为 PASS"
                break
                ;;
            NEEDS_REPAIR)
                if [[ $round -gt $MAX_REPAIR_ROUNDS ]]; then
                    final_verdict="FAIL"
                    log_error "  [${case_id}] ${MAX_REPAIR_ROUNDS} 轮修复后仍有差异: ${diffs_summary}"
                    break
                fi

                # 检查 fixsql 是否生成
                local fixsql_count
                fixsql_count=$(find "${case_dir}/fixsql" -name "*.sql" -type f 2>/dev/null | wc -l | tr -d ' ')
                if [[ "$fixsql_count" -eq 0 ]]; then
                    final_verdict="FAIL"
                    log_error "  [${case_id}] Round ${round}: Diffs=yes 但未生成 fixsql"
                    break
                fi

                # VIEW 差异的修复建议均以 advisory 注释形式写入，无可执行 SQL。
                # repairDB 对这类文件无法做任何修复；直接判定为 PASS-ADVISORY，
                # 不进入无意义的修复循环。
                if fixsql_is_view_advisory_only "${case_dir}/fixsql"; then
                    final_verdict="PASS-ADVISORY"
                    log_info "  [${case_id}] Round ${round}: fixsql 均为 advisory-only (VIEW 差异)，视为 PASS-ADVISORY"
                    break
                fi

                # 运行 repairDB
                local repair_output="${case_dir}/round${round}-repair-output.txt"
                run_with_timeout "$CASE_TIMEOUT" \
                    "$REPAIR_DB" -conf "${case_dir}/repairDB.conf" \
                    > "$repair_output" 2>&1 || {
                    log_warn "  [${case_id}] Round ${round}: repairDB 非零退出"
                }

                log_info "  [${case_id}] Round ${round}: 修复完成 (fixsql=${fixsql_count} files), 准备重新校验"
                ;;
        esac
    done

    # 写入结果
    echo "${case_id}|${final_verdict}|${round}|${diffs_summary}" >> "${ARTIFACTS_DIR}/results.csv"

    # 写入 verdict 文件供调用方读取（避免 $() 捕获吞掉日志）
    echo "$final_verdict" > "${case_dir}/verdict"
}

# ============================================================
# SECTION 11: 最终修复（--final-repair）
# ============================================================

# 在所有回归用例完成后，对目标库做一次完整修复，使其与源库一致。
# 回归测试的每个用例会重新初始化目标库（用例隔离），因此多模式测试后
# 只有最后一个模式的修复保留。--final-repair 解决这个问题：按
# struct → routine → trigger → data 顺序对每个 src→dst 对做修复闭环。
run_final_repair() {
    log_info "=================================================================="
    log_info " 最终修复阶段 (--final-repair)"
    log_info "=================================================================="

    # 收集去重的 src→dst 对
    local -a pairs=()
    local -A seen_pairs=()
    for entry in "${test_matrix[@]}"; do
        IFS=':' read -r src_label src_port dst_label dst_port _mode <<< "$entry"
        local pair_key="${src_label}:${src_port}:${dst_label}:${dst_port}"
        if [[ -z "${seen_pairs[$pair_key]:-}" ]]; then
            seen_pairs[$pair_key]=1
            pairs+=("$pair_key")
        fi
    done

    # 修复顺序：struct 先于 data（DDL 修复优先于 DML 修复）
    local -a repair_order=(struct routine trigger data)

    for pair_key in "${pairs[@]}"; do
        IFS=':' read -r src_label src_port dst_label dst_port <<< "$pair_key"
        log_info "修复 ${src_label} -> ${dst_label}:"

        # 重新初始化目标端到基线状态
        reinit_target "$dst_label" "$dst_port"
        # trigger/routine 模式需要源端干净
        reinit_source "$src_label" "$src_port"

        for mode in "${repair_order[@]}"; do
            if ! label_in_filter "$mode" "$FILTER_MODE"; then
                continue
            fi

            local repair_dir="${ARTIFACTS_DIR}/final-repair/${src_label}-to-${dst_label}-${mode}"
            mkdir -p "${repair_dir}/fixsql"

            # 生成配置
            generate_gt_checksum_config "$src_port" "$dst_port" "$mode" "$repair_dir"
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

                if [[ -f "${repair_dir}/gt-checksum.log" ]]; then
                    cp "${repair_dir}/gt-checksum.log" "${repair_dir}/round${round}-gt-checksum.log" 2>/dev/null || true
                fi

                local diffs
                diffs="$(parse_diffs_from_output "${repair_dir}/round${round}-output.txt" "$mode")"
                local verdict
                verdict="$(evaluate_diffs "$diffs" "$src_label")"

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

                # VIEW advisory-only fixsql 无可执行 SQL，跳过 repairDB
                if fixsql_is_view_advisory_only "${repair_dir}/fixsql"; then
                    log_info "  ${mode}: fixsql 均为 advisory-only (VIEW 差异)，跳过 repairDB，视为收敛"
                    converged=true
                    break
                fi

                run_with_timeout "$CASE_TIMEOUT" \
                    "$REPAIR_DB" -conf "${repair_dir}/repairDB.conf" \
                    > "${repair_dir}/round${round}-repair-output.txt" 2>&1 || {
                    log_warn "  ${mode}: repairDB 非零退出 (round ${round})"
                }
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
        echo " gt-checksum Regression Test Report"
        echo " Run ID:    ${RUN_ID}"
        echo " Date:      $(date '+%Y-%m-%d %H:%M:%S')"
        echo " Host:      ${DB_HOST}"
        echo " Timeout:   ${CASE_TIMEOUT}s per case"
        echo " Max Rounds: ${MAX_REPAIR_ROUNDS}"
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

        # 交叉矩阵（使用 awk 生成，兼容 bash 3.2）
        echo ""
        echo "=== Source x Target-Mode Matrix ==="
        echo ""

        awk -F'|' '
        {
            case_id = $1; verdict = $2
            # 解析 src 和 dst-mode
            n = index(case_id, "-to-")
            src = substr(case_id, 1, n-1)
            dst_mode = substr(case_id, n+4)

            if (!(src in src_seen)) { src_order[++src_cnt] = src; src_seen[src] = 1 }
            if (!(dst_mode in col_seen)) { col_order[++col_cnt] = dst_mode; col_seen[dst_mode] = 1 }
            data[src, dst_mode] = verdict
        }
        END {
            # 表头
            printf "%-15s", ""
            for (c = 1; c <= col_cnt; c++) printf "%-22s", col_order[c]
            print ""
            printf "%-15s", ""
            for (c = 1; c <= col_cnt; c++) printf "%-22s", "--------------------"
            print ""
            # 数据行
            for (r = 1; r <= src_cnt; r++) {
                printf "%-15s", src_order[r]
                for (c = 1; c <= col_cnt; c++) {
                    key = src_order[r] SUBSEP col_order[c]
                    val = (key in data) ? data[key] : "N/A"
                    printf "%-22s", val
                }
                print ""
            }
        }' "$results_file"

    } > "$report_file"

    # 输出到终端
    cat "$report_file"

    # 生成 JSON 报告
    generate_json_report
}

generate_json_report() {
    local results_file="${ARTIFACTS_DIR}/results.csv"
    local json_file="${ARTIFACTS_DIR}/report.json"

    {
        echo "{"
        echo "  \"run_id\": \"${RUN_ID}\","
        echo "  \"timestamp\": \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\","
        echo "  \"host\": \"${DB_HOST}\","
        echo "  \"summary\": {"
        echo "    \"total\": ${TOTAL},"
        echo "    \"passed\": ${PASSED},"
        echo "    \"failed\": ${FAILED},"
        echo "    \"errors\": ${ERRORS},"
        echo "    \"timeouts\": ${TIMEOUTS}"
        echo "  },"
        echo "  \"cases\": ["

        local first=true
        while IFS='|' read -r case_id verdict rounds diffs; do
            if $first; then
                first=false
            else
                echo ","
            fi
            # 转义 JSON 字符串
            printf '    {"id": "%s", "verdict": "%s", "rounds": %s, "diffs": "%s"}' \
                "$case_id" "$verdict" "$rounds" "$diffs"
        done < "$results_file"

        echo ""
        echo "  ]"
        echo "}"
    } > "$json_file"
}

# ============================================================
# SECTION 12: 主流程
# ============================================================
main() {
    parse_arguments "$@"

    setup_timeout_cmd

    mkdir -p "${ARTIFACTS_DIR}/cases"

    log_info "=================================================================="
    log_info " gt-checksum Regression Test"
    log_info " Run ID: ${RUN_ID}"
    log_info " Artifacts: ${ARTIFACTS_DIR}"
    log_info "=================================================================="

    # 前置检查
    check_prerequisites

    # 编译
    build_binaries

    # 连通性检查（dry-run 跳过）
    if [[ "$DRY_RUN" != "true" ]]; then
        check_connectivity
    fi

    # 生成测试矩阵
    local -a test_matrix=()
    while IFS= read -r line; do
        test_matrix+=("$line")
    done < <(generate_test_matrix)

    local total_cases=${#test_matrix[@]}

    if [[ $total_cases -eq 0 ]]; then
        log_error "测试矩阵为空，请检查 --src/--dst/--mode 过滤参数"
        exit 1
    fi

    log_info "测试矩阵: ${total_cases} 个用例"

    # dry-run 模式：仅打印矩阵
    if [[ "$DRY_RUN" == "true" ]]; then
        echo ""
        printf "%-15s %-15s %-15s %-10s\n" "SOURCE" "SRC_PORT" "TARGET" "MODE"
        printf "%-15s %-15s %-15s %-10s\n" "------" "--------" "------" "----"
        for entry in "${test_matrix[@]}"; do
            IFS=':' read -r src_label src_port dst_label dst_port mode <<< "$entry"
            printf "%-15s %-15s %-15s %-10s\n" "$src_label" "$src_port" "${dst_label}:${dst_port}" "$mode"
        done
        echo ""
        echo "Total: ${total_cases} cases"
        exit 0
    fi

    # 初始化数据库
    init_databases

    # CSV 头
    : > "${ARTIFACTS_DIR}/results.csv"

    # 信号处理：中断时生成部分报告
    trap 'log_warn "中断信号，生成部分报告..."; generate_report; exit 130' INT TERM

    # 执行测试
    local case_num=0
    for entry in "${test_matrix[@]}"; do
        case_num=$((case_num + 1))
        IFS=':' read -r src_label src_port dst_label dst_port mode <<< "$entry"

        TOTAL=$((TOTAL + 1))
        log_info "[${case_num}/${total_cases}] ${src_label}:${src_port} -> ${dst_label}:${dst_port} (${mode})"

        run_single_test_case "$src_label" "$src_port" "$dst_label" "$dst_port" "$mode"
        local case_id="${src_label}-to-${dst_label}-${mode}"
        local verdict
        verdict="$(cat "${ARTIFACTS_DIR}/cases/${case_id}/verdict" 2>/dev/null || echo "ERROR")"

        case "$verdict" in
            PASS)              PASSED=$((PASSED + 1));   log_info  "[${case_num}/${total_cases}] PASS" ;;
            PASS-ADVISORY)     PASSED=$((PASSED + 1));   log_info  "[${case_num}/${total_cases}] PASS-ADVISORY (VIEW advisory diffs)" ;;
            FAIL)              FAILED=$((FAILED + 1));   log_error "[${case_num}/${total_cases}] FAIL" ;;
            TIMEOUT)           TIMEOUTS=$((TIMEOUTS + 1)); log_error "[${case_num}/${total_cases}] TIMEOUT" ;;
            *)                 ERRORS=$((ERRORS + 1));   log_error "[${case_num}/${total_cases}] ERROR: ${verdict}" ;;
        esac
    done

    # 最终修复（可选）
    if [[ "$FINAL_REPAIR" == "true" ]]; then
        run_final_repair
    fi

    # 生成报告
    echo ""
    generate_report

    log_info "详细日志和配置: ${ARTIFACTS_DIR}"

    # 返回码：有失败返回 1
    if [[ $((FAILED + ERRORS + TIMEOUTS)) -gt 0 ]]; then
        exit 1
    fi
    exit 0
}

main "$@"
