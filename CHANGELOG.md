<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes to this project are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## 1.1.3

- Make public Evidence append executable through Core by advertising `append-only`
  only for explicitly compiled immutable-content layouts.
- Include the canonical row fingerprint in new Evidence replacement keys, preserving
  different contents with the same timestamp and identity through merges and recovery.
- Verify real sorting keys before advertising or recording migration metadata;
  retain legacy layout, descriptor and physical locks without silently upgrading tables.
- Preserve structured append-version behavior and deny atomic Evidence at the SPI boundary.
- Add public Core/Evidence pagination, retry, merge/restart, replica and backup regressions.

## 1.1.2

- Preserve DateTime64(9) primary timestamps in batch preparation, stored reads,
  projections, min/max results and signed live pagination using integer nanoseconds.
- Bind exact UTC timestamp parameters and validate ranges at nanosecond precision.
- Retain valid legacy cursor and microsecond result formats, canonical row/batch
  identities, binary cursor correction, released dependency bounds and physical layouts.
- Add real public Observability log/span/metric, replicated and signed-context regressions.

## 1.1.1

- Repair physical row-fingerprint keyset comparison for native FixedString bytes,
  preventing repeated or omitted telemetry page boundaries. Preserve both
  existing signed cursor representations and all validation gates.
- Add real-engine log/span/metric pagination with timestamp and identity ties,
  fingerprint prefixes 0/f, binary payloads and historical cursor continuation.

## 1.1.0

- Separate deployment-selected releases and authenticated observations from historical descriptor metadata.
- Consume Core 1.1 SPI and compatible public Query/Semantics dependency bounds.
- Preserve required feature, UTC, topology, physical Schema, auth/TLS and deployment-drift checks.
- Normalize telemetry binary identifiers and JSON fields from declared Schema types.
- Exercise standalone/replicated ClickHouse 25.3 and 25.8 with actual restart, member loss/catchup and backup/restore.

## 1.0.1 — 2026-09-06

- Consume released Core 1.0.1, Semantics 2.0.0, and Query 1.0.2 with a refreshed runtime lock.
- Retain append/query, layout, normalization, precision, scope, and capability contracts.
- Verify the candidate shared package set and installed wheel test extra in CI.


## 1.0.0 - 2026-08-25

- Add the Meridian V1 ClickHouse Adapter SPI implementation.
- Add deterministic ClickHouse schema, migration, ingestion, query, normalization,
  health, and physical-verification contracts.
- Add real-engine standalone and replicated conformance profiles.
