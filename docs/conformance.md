<!-- SPDX-License-Identifier: Apache-2.0 -->

# Conformance evidence

Version 1.0.1 targets released Meridian Core 1.0.1, Semantics 2.0.0, and Query 1.0.2
artifacts and ClickHouse 25.3.14.14. Mocks are used only for fast unit tests; engine acceptance is
performed against disposable genuine ClickHouse containers.

## Reproduce locally

```console
python -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/pytest -m 'not integration and not cluster'
./scripts/run-standalone-conformance.sh
./scripts/run-cluster-conformance.sh
```

The standalone profile verifies the released Core conformance runner, authenticated startup,
deterministic capability and physical fingerprints, retry deduplication, scope isolation,
mandatory bounded scans, exact p99 pushdown, and accepted retention DDL. It also round-trips
Usage event and aggregate Decimal(76,18) values through append-version writes and normalized
queries. Contract tests compile both Usage layouts on both topologies, preserve scope columns,
and reject authoritative structured.put v2 modes before driver writes.

The cluster profile starts one Keeper and two ClickHouse replicas. It verifies the released Core
runner, replicated DDL/topology probes, active replica metadata, and write-on-replica-1/read-on-
replica-2 visibility.

## Immutable environment selection

- ClickHouse server:
  `clickhouse/clickhouse-server@sha256:b627d7a9bc0e0c1bac26cdbe9d2fc6316faa29c5d8a174f28f5abd57d0fa6ba2`
- ClickHouse Keeper:
  `clickhouse/clickhouse-keeper@sha256:2c8b97bb628999adfa597172091ce495304e0a260d791fe6e475d64a314193b4`
- Python driver: `clickhouse-connect==0.15.1`
- Meridian predecessors: `meridian-storage-core==1.0.1`,
  `meridian-storage-semantics==2.0.0`, and `meridian-storage-query==1.0.2`

The hash-pinned runtime resolution is committed as `requirements.lock`.

## Checked-in reports

- [Standalone ClickHouse 25.3](../evidence/clickhouse-25.3-standalone.json)
- [Replicated ClickHouse 25.3](../evidence/clickhouse-25.3-replicated.json)

The checked-in reports describe the original 1.0.0 release. CI reruns both profiles and uploads
newly generated reports as workflow artifacts. Checked-in
reports are reviewed release evidence and are changed only when the selected engine/profile or
adapter behavior changes.
