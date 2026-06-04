#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GT_CHECKSUM="${ROOT_DIR}/gt-checksum"
REPAIR_DB="${ROOT_DIR}/repairDB"
TMP_DIR=""

cleanup() {
    if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
        rm -rf "${TMP_DIR}"
    fi
}
trap cleanup EXIT

cd "${ROOT_DIR}"

TMP_DIR="$(mktemp -d)"

echo "[baseline] running CGO-disabled package tests for v4.0.0 regression scope"
CGO_ENABLED=0 go test -vet=off \
    ./schemacompat \
    ./actions \
    ./dbExec \
    ./inputArg \
    ./global \
    ./cmd/repairDB \
    ./progress \
    -count=1

echo "[baseline] building main binaries used by the regression workflow"
CGO_ENABLED=0 go build -tags nooracle -o gt-checksum ./cmd/gt-checksum
CGO_ENABLED=0 go build -o repairDB ./cmd/repairDB

write_dtype_mapping_file() {
    local mapping_file="$1"
    cat > "${mapping_file}" <<'EOF'
dTypeMapping:
  oracle_to_mysql:
    - source_type: NUMBER
      target_type: BIGINT
      condition: "p <= 19 and s = 0"
      description: "baseline preview integer mapping"
  mysql_upgrade:
    - source_type: CHAR
      target_type: VARCHAR
      description: "baseline preview char mapping"
  mariadb_to_mysql:
    - source_type: JSON
      target_type: JSON
      description: "baseline preview json mapping"
EOF
}

run_dtype_mapping_preview_smoke() {
    local src_dsn="${BASELINE_PREVIEW_SRC_DSN:-}"
    local dst_dsn="${BASELINE_PREVIEW_DST_DSN:-}"

    # 当前 --preview-dtype-mapping 入口会先校验源/目标连接；无 DB 环境时保持 baseline 可离线通过。
    if [[ -z "${src_dsn}" || -z "${dst_dsn}" ]]; then
        echo "[baseline] skipping dtype mapping CLI preview smoke (set BASELINE_PREVIEW_SRC_DSN and BASELINE_PREVIEW_DST_DSN to enable)"
        return
    fi

    local case_dir="${TMP_DIR}/dtype-preview"
    local mapping_file="${case_dir}/dtype-mapping.yaml"
    mkdir -p "${case_dir}/fixsql"
    write_dtype_mapping_file "${mapping_file}"

    cat > "${case_dir}/gt-checksum.conf" <<EOF
srcDSN=${src_dsn}
dstDSN=${dst_dsn}
tables=information_schema.tables
checkNoIndexTable=yes
caseSensitiveObjectName=yes
parallelThds=1
chunkSize=1000
queueSize=10
checkObject=struct
memoryLimit=1000
datafix=file
fixFileDir=${case_dir}/fixsql
logFile=${case_dir}/gt-checksum.log
logLevel=debug
logbin=ON
requirePK=OFF
dTypeMappingFile=${mapping_file}
EOF

    echo "[baseline] running dtype mapping CLI preview smoke"
    local out="${case_dir}/preview.out"
    "${GT_CHECKSUM}" -c "${case_dir}/gt-checksum.conf" --preview-dtype-mapping > "${out}" 2>&1
    if ! grep -qiE '\[dTypeMapping\]|source_type|target_type' "${out}"; then
        echo "[baseline] dtype mapping preview output did not contain expected markers" >&2
        return 1
    fi
}

run_repairdb_dry_run_smoke() {
    echo "[baseline] running repairDB dry-run config smoke for splitInsertOnDupKey/resume"

    local split_mode
    for split_mode in ON OFF; do
        local case_dir="${TMP_DIR}/repairdb-${split_mode}"
        local fix_dir="${case_dir}/fixsql"
        mkdir -p "${fix_dir}"

        cat > "${fix_dir}/table.gt_checksum_baseline.t_baseline.INSERT-1.sql" <<'EOF'
SET NAMES utf8mb4;
BEGIN;
INSERT INTO `gt_checksum_baseline`.`t_baseline` (`id`, `name`) VALUES (1, 'dup'), (1, 'dup-again'), (2, 'ok');
COMMIT;
EOF

        cat > "${case_dir}/repairDB.conf" <<EOF
dstDSN=mysql|checksum:checksum@tcp(127.0.0.1:3306)/information_schema?charset=utf8mb4
parallelThds=1
fixFileDir=${fix_dir}
logbin=OFF
splitInsertOnDupKey=${split_mode}
resume=ON
EOF

        local out="${case_dir}/repairdb-dry-run.out"
        (cd "${case_dir}" && "${REPAIR_DB}" -dry-run -conf "${case_dir}/repairDB.conf" > "${out}" 2>&1)
        if ! grep -qiE 'Dry-run|dry-run|Stage classification|统计' "${out}"; then
            echo "[baseline] repairDB dry-run output did not contain expected markers for splitInsertOnDupKey=${split_mode}" >&2
            return 1
        fi
    done
}

run_dtype_mapping_preview_smoke
run_repairdb_dry_run_smoke

echo "[baseline] struct migration and v4.0.0 smoke baseline passed"
