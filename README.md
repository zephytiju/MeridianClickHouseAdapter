<!-- SPDX-License-Identifier: Apache-2.0 -->

# Meridian Storage ClickHouse

[![CI](https://github.com/zephytiju/MeridianClickHouseAdapter/actions/workflows/ci.yml/badge.svg)](https://github.com/zephytiju/MeridianClickHouseAdapter/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12--3.14-blue.svg)](pyproject.toml)

`meridian-storage-clickhouse` is the independently released ClickHouse adapter for Meridian
V1. It executes mapping-first, engine-neutral Operations for stored logs, spans, metric points,
usage, cost, and generic analytical time-series Records. It provides deterministic schema and
migration plans, bounded idempotent batch ingestion, mandatory time-bounded query pushdown,
live keyset pagination, result normalization, authenticated probes, and physical fingerprints.

The distribution contributes exactly one adapter package,
`meridian_storage.adapters.clickhouse`, and discovers through the released Meridian Core 1.1
Adapter SPI. It consumes only the released `meridian-storage-core>=1.1,<2`,
`meridian-storage-semantics>=2.0.1,<3`, and `meridian-storage-query>=1.0.3,<2` packages.
Adapter, layout, append, and Query plan contract versions remain unchanged.

## Boundary

Application code does not import this package, create a ClickHouse client, issue SQL, select a
table engine, or provide an endpoint. It uses the registered `evidence` or `structured` Catalog
Expression surface. The composition root starts Meridian after deployment IaC has injected an
opaque Binding:

```python
from meridian_storage import Meridian, OperationContext

meridian = Meridian.from_environment()
meridian.start()
evidence = meridian.catalog("evidence")

with meridian.context(
    OperationContext(
        principal_ref="identity:service/worker",
        tenant="tenant-a",
        scope={"workspace": "operations"},
    )
):
    result = meridian.execute(
        evidence.query(
            resource="runtime.logs",
            where={
                "event_time": {
                    "gte": "2026-08-25T00:00:00Z",
                    "lt": "2026-08-26T00:00:00Z",
                }
            },
            limit=100,
        )
    )
```

Collector deployment, topology, credentials, ACLs, retention, backups, restores, and migration
job lifecycle remain owned by Platform or Vangu IaC through MeridianConstructs. Runtime startup
only authenticates, probes compatibility, and verifies pre-provisioned physical fingerprints.

## Guarantees and limits

- Writes are append-oriented and idempotent only within the advertised retry/deduplication
  window. The adapter does not claim general exactly-once execution.
- Every scan and aggregate requires a bounded UTC time range within the Binding limit.
- Scope isolation is injected ahead of user predicates and leads every table sort key.
- Visibility is eventual. General transactions, compare-and-set, authoritative patch, foreign
  keys, cross-Binding joins, and native SQL Expressions are denied.
- Decimal, timestamp, UUID, bytes, JSON, nullable, and quantile values are normalized back to
  Meridian wire representations. Percentiles use the exact ClickHouse quantile function in V1.

## Adapter-author and IaC integration

The public adapter-author surface includes `ClickHouseAdapterFactory`,
`ClickHouseSchemaCompiler`, `ClickHouseMigrator`, `ClickHouseQueryTranslator`,
`adapter_descriptor`, `capability_manifest`, and `plan_initial_migration`. These are deployment
and adapter integration contracts, not business APIs. See
[architecture.md](docs/architecture.md), [configuration.md](docs/configuration.md),
[operations.md](docs/operations.md), [migrations.md](docs/migrations.md), and
[conformance.md](docs/conformance.md).

## Development

```console
python -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/ruff check .
.venv/bin/mypy src
.venv/bin/pytest -m 'not integration and not cluster'
./scripts/run-standalone-conformance.sh
./scripts/run-cluster-conformance.sh
```

Real-engine tests use disposable ClickHouse containers and never target a deployment-owned
database. See `tests/integration` and `tests/cluster` for the exact profiles.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Deployment-owned releases and retained gates are documented in [release selection](docs/release-selection.md).
