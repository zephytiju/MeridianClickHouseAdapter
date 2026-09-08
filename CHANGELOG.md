<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes to this project are documented here. The project follows
[Semantic Versioning](https://semver.org/).

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
