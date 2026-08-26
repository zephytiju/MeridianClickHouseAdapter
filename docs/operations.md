<!-- SPDX-License-Identifier: Apache-2.0 -->

# Operations and guarantees

The authenticated descriptor declares these Meridian V1 operations:

| Capability | Behavior | Primary guarantees |
| --- | --- | --- |
| `meridian.evidence.append` | Bounded append batch | eventual visibility, retry-window deduplication, scope isolation |
| `meridian.evidence.query` | Bounded record scan | bounded time range, single Binding, scope isolation |
| `meridian.structured.put` | Append-version write | eventual visibility, retry-window deduplication, scope isolation |
| `meridian.structured.get` | Identity lookup | eventual visibility, single Binding, scope isolation |
| `meridian.structured.query` | Bounded record scan | bounded time range, single Binding, scope isolation |
| `meridian.structured.aggregate` | Grouped/ungrouped aggregate | bounded time range, single Binding, scope isolation |

Writes reject empty batches, batch limit violations, missing/unknown fields, invalid logical
values, mismatched schema pins, and unstable retry identities. They populate hidden isolation and
provenance columns and use ClickHouse deduplication tokens derived from the request, scope,
Resource, schema, and records.

Queries support logical projection, boolean predicates, comparisons, membership, null checks,
timestamp ranges, deterministic order, exact percentile, `count`, `sum`, `avg`, `min`, `max`,
grouping, and signed live-keyset pagination. Every generated command is parameterized, and the
scope predicate is emitted before consumer predicates.

The only consistency class is `eventual`. Results state complete pushdown provenance. The
adapter rejects strong consistency, multi-Resource plans, joins, traversal, offset pagination,
native SQL, general transactions, compare-and-set, authoritative patch, and cross-Binding work.

## Logical types

The schema compiler supports boolean, signed integers, float64, string, bytes, decimal up to
precision 76, UUID, UTC timestamp, date, duration, enum, JSON, record/object references, and
WGS84 points, plus nullable and repeated fields. Driver values are converted back to canonical
JSON-compatible Meridian representations.

## Health and compatibility

An authenticated startup probe checks engine identity and version, UTC timezone, required
functions, and topology. Physical verification reads ClickHouse metadata and compares the
database, Resource mapping, schema and layout fingerprints, table engine, columns, and active
replica state. Core Binding pins fail closed on a manifest or physical fingerprint mismatch.
