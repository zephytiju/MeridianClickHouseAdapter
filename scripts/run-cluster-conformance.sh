#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-meridian-clickhouse-cluster}"
compose_file="${repo_dir}/tests/cluster/docker-compose.yml"

cleanup() {
  docker compose -f "${compose_file}" down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose -f "${compose_file}" up --detach --wait
MERIDIAN_COMPOSE_FILE="${compose_file}" \
CLICKHOUSE_CLUSTER=1 \
MERIDIAN_EVIDENCE_PATH="${repo_dir}/build/evidence" \
  "${repo_dir}/.venv/bin/pytest" --junitxml="${repo_dir}/build/evidence/replicated-junit.xml" -q "${repo_dir}/tests/cluster"

MERIDIAN_COMPOSE_FILE="${compose_file}" "${repo_dir}/.venv/bin/python" "${repo_dir}/scripts/write-engine-evidence.py" replicated
