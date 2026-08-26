<!-- SPDX-License-Identifier: Apache-2.0 -->

# Binding configuration

Only a deployment composition root supplies ClickHouse configuration. Application code receives
Catalogs and never sees this Binding.

## Required identity

- `adapterId`: exactly `meridian.storage.clickhouse`
- `adapterContract`: exactly `1.0.0`
- `engineProfile`: `clickhouse-standalone` or `clickhouse-replicated`
- `engineVersion`: exactly `25.3` for adapter release 1.0.0
- `physicalNamespace`: an existing ClickHouse database identifier
- `endpoint`: an HTTP(S) origin resolved from the deployment-owned `serviceRef`
- `requiredCapabilityFingerprint`: the authenticated manifest pin
- `requiredPhysicalFingerprint`: the expected deployed layout fingerprint when required

Credentials are resolved by Core from `identityRef` and `secretRef`. They must not appear in the
endpoint or `settings`. HTTPS is mandatory when TLS mode is not `disabled`; optional CA and
client-certificate material is handled through Core secret references and short-lived mode-0600
files.

## Closed adapter settings

The `settings` object rejects unknown fields:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `layouts` | array | required | One immutable, fingerprinted layout for each Resource |
| `maxBatchRows` | integer | 10,000 | Maximum rows in one write batch |
| `maxBatchBytes` | integer | 16 MiB | Maximum canonical encoded batch size |
| `maxTimeRangeSeconds` | integer | 31 days | Maximum query/aggregate timestamp span |
| `retryWindowSeconds` | integer | 24 hours | Advertised bounded deduplication window |
| `cursorTtlSeconds` | integer | 15 minutes | Signed live-keyset cursor lifetime |
| `insertQuorum` | integer | 1 | ClickHouse insert quorum |
| `requiredFunctions` | string array | `count`, `quantile`, `sum` | Functions required by the startup probe |

Core client settings provide operation deadlines and maximum result bytes. Values are bounded by
the parser even when the enclosing Core Binding has already been validated.

## Resource layouts

Layouts are generated from published Meridian Schemas with `ClickHouseSchemaCompiler`; operators
do not hand-author ClickHouse DDL. Each layout contains:

- the logical Resource, Resource fingerprint, schema version, and schema fingerprint;
- a profile (`log`, `span`, `metric`, `usage`, `cost`, `time-series`, or `analytical`);
- a stable generated table and logical-to-physical column map;
- timestamp, identity, dimension, and measurement roles;
- retention and partition inputs;
- standalone or replicated topology;
- optional reviewed administrative profile names and schema-derived indexes; and
- a canonical `layoutFingerprint` that is checked while parsing.

`retentionSeconds` is rendered into a table TTL, but changing retention remains an explicit
migration owned by IaC. `administrativeProfiles` are descriptive deployment inputs; they do not
disable runtime query bounds or grant application privileges.

## Query mapping grammar

The mapping-first `where` object maps a logical field to either an equality value or an operator
object. Supported operator names are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `notIn`, and
`isNull`. For query and aggregate operations, the layout timestamp field must have both a lower
bound (`gt` or `gte`) and upper bound (`lt` or `lte`), and the span must not exceed
`maxTimeRangeSeconds`.

All logical field names are resolved through the pinned layout. Table and column identifiers are
never taken from operation input.
