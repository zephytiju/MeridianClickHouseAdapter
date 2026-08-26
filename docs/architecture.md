<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture and ownership

`meridian-storage-clickhouse` is one independently versioned Meridian Adapter distribution. It
implements released Core, Semantics, and Query contracts; it does not add a Catalog or expose a
ClickHouse-shaped consumer API.

## Contract boundary

Consumers submit mapping-first Expressions and serialized Operations through the `structured`
or `evidence` Catalog. Core resolves a Binding and invokes the Adapter SPI. Inside this package,
the adapter validates the closed Binding, translates the operation to parameterized ClickHouse
commands, executes it, and normalizes the result to Meridian wire values.

The adapter advertises record profiles for logs, spans, metrics, usage, cost, time-series, and
analytical records. These are capabilities of `structured` and `evidence` Resources, never
Telemetry, Audit, Lineage, Usage, or Cost Catalogs. The V1 Catalog registry remains exactly
`structured`, `object`, `cache`, `evidence`, and `streaming`.

Native SQL is deliberately absent. A consumer cannot choose ClickHouse types, engines,
endpoints, credentials, database names, tables, partitions, or indexes. The Adapter SPI and the
deployment-time schema/migration helpers are adapter-author contracts rather than application
contracts.

## Authority split

Platform or Vangu IaC, using MeridianConstructs, owns engine selection, provisioning, service
references, state, credentials, identity, ACLs, topology, retention selection, migration jobs,
backups, recovery, and lifecycle. A deployment job may use `ClickHouseSchemaCompiler` and
`ClickHouseMigrator` to render and apply reviewed bundles. Runtime startup never creates or
migrates state; it authenticates, probes the server, verifies the capability pin, and checks the
pre-provisioned physical fingerprint.

The adapter owns only capability declaration, closed Binding parsing, logical-to-physical
translation, batch execution, result normalization, health/compatibility probes, and deterministic
conformance vectors.

## Runtime data path

1. Core validates a serialized Operation and provides an `ExecutionRequest` plus immutable
   operation context.
2. The adapter selects the Binding-pinned `ResourceLayout` and verifies schema fingerprints.
3. Writes receive stable scope, tenant, batch, row, Resource, schema, and ingestion metadata.
4. Queries inject scope isolation before consumer predicates, require a bounded timestamp range,
   and use parameterized commands only.
5. Pagination uses signed, expiring live-keyset cursors pinned to the plan, schema, registry,
   scope, and page size.
6. ClickHouse values are normalized to Meridian JSON/wire values and constrained by the Core
   result budget.

Visibility is eventual. Retry tokens provide bounded deduplication rather than a general
exactly-once guarantee. Transactions, compare-and-set, authoritative patch, joins across
Bindings, traversal, foreign keys, and NativeQuery are unsupported.

## Locked design baseline

Version 1.0.0 was implemented against these revisions:

- [Meridian HLD rev 56](https://qcnwge0wy4s0.feishu.cn/wiki/N8ATwRBzuiwl39k2SMVcBvYRnuf)
- [Catalogs and Public Interfaces rev 70](https://qcnwge0wy4s0.feishu.cn/docx/YrcBdDMJ5omSnExNmLxcak5bnPf)
- [Engine Adapters rev 24](https://qcnwge0wy4s0.feishu.cn/wiki/A9wVwOYfciyopfkhziicE3gBnQc)
- [Kafka Streaming Adapter LLD rev 6](https://qcnwge0wy4s0.feishu.cn/wiki/PJFXwVWy7iRihOk0JewcnR9Fnjc)
- [MeridianConstructs LLD rev 45](https://qcnwge0wy4s0.feishu.cn/wiki/D5MBwLF2di54gJkv24fcg26Ungh)
- [ClickHouse Adapter LLD rev 12](https://qcnwge0wy4s0.feishu.cn/wiki/AUqIwHXo3ipsENkFwAUcUvxHn6e)

No design change was required by this implementation.
