#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

echo "[baseline] running CGO-disabled package tests for struct migration scope"
CGO_ENABLED=0 go test -vet=off ./schemacompat ./actions ./dbExec ./inputArg ./global -count=1

echo "[baseline] building main binaries used by the regression workflow"
go build -o gt-checksum gt-checksum.go
CGO_ENABLED=0 go build -o repairDB repairDB.go

echo "[baseline] struct migration test baseline passed"
