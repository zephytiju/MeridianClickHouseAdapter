#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_dir}/tests/integration/docker-compose.yml"

cleanup() {
  docker compose -f "${compose_file}" down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose -f "${compose_file}" up --detach --wait
CLICKHOUSE_INTEGRATION=1 \
MERIDIAN_EVIDENCE_PATH="${repo_dir}/build/evidence" \
PYTHONPATH="${repo_dir}/src" \
  "${repo_dir}/.venv/bin/pytest" -q "${repo_dir}/tests/integration"
