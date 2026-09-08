<!-- SPDX-License-Identifier: Apache-2.0 -->

# Physical cursor compatibility

Release 1.1.1 repairs record pagination for both mapping-first queries and
released Query wire plans. The final sort key is the hidden
`_meridian_row_fingerprint`: ClickHouse stores a lowercase 64-character SHA-256
hex digest in `FixedString(64)`, and clickhouse-connect returns it as bytes.
The existing cursor JSON serialization represents those bytes as Base64.
Release 1.1.0 compared that Base64 text directly with the stored digest. A digest
beginning with `f` can repeat the boundary; `0` can omit later tied rows.

The signed cursor wire format, issuance, stable plan fingerprint, sort order and
public Query contracts are unchanged. Only after signature, expiry, scope,
Schema, registry, plan and page-size verification, the adapter converts the
hidden fingerprint component back to its physical comparison value. The
Base64 encoding must be canonical and decode to exactly 64 lowercase hex
characters. The previous raw-hex representation from string-returning clients
is also accepted. These encodings have disjoint lengths (88 and 64), so there
is no ambiguous reinterpretation or cursor version transition. Invalid physical
components fail deterministically. All other cursor components and logical
binary result serialization retain their existing behavior.

Valid, unexpired 1.1.0 cursors continue at their originally signed boundary with
the same signing keys and query context. The repair cannot recover rows already
skipped by a previous 1.1.0 request; a fresh scan is needed for that history.
Pagination remains a live keyset traversal, with existing concurrent ingestion
and ReplacingMergeTree merge semantics rather than a snapshot guarantee.

Unit regressions cover both encodings, malformed components, both query paths,
and retained signature/scope/Schema/registry/plan/page-size checks. Real engine
regressions cover logs, spans and metrics with binary and typed JSON payloads,
tied timestamps, distinct identities and tied identities, with prefixes `0` and
`f`. For tied identities, the existing `queryFinal=false` live layout is used;
separate inserts and temporarily stopped merges keep the fixture's rows visible
throughout traversal. Default `queryFinal=true` is tested with distinct identities.
No production layout, deduplication setting, or recovery behavior is changed.

## Nanosecond timestamps (1.1.2)

The timestamp and every timestamp sort component use the existing `DateTime64(9, 'UTC')`
columns. Batch preparation converts offset-qualified RFC 3339 text to signed integer
nanoseconds, which clickhouse-connect writes as exact ticks. Canonical row and batch
hashes continue to cover the original logical data, including every fractional digit.
No layout, Schema, physical fingerprint, deduplication identity or migration changes.

Both query entry points request integer decoding for timestamp results and hidden
sort aliases through the compiled command's `columnFormats`. Result normalization
renders UTC text without floats or a microsecond-only intermediate. This includes
aliased projections, nullable/array values and timestamp min/max results. Submicrosecond
values retain nine fractional digits; exact microsecond and whole-second values keep
the earlier public spelling. Direct translator consumers must forward `columnFormats`
to the driver, just as for Schema-directed binary decoding.

Signed sort tuples still contain RFC 3339 text and the original signature/context
fields. Binding produces all nine fractional digits, and range validation uses integer
nanoseconds. One-nanosecond windows remain nonempty, and the configured maximum cannot
be exceeded by rounding. Inputs with more than nine digits or outside signed 64-bit
nanoseconds fail before transmission. Aware offsets are normalized to UTC exactly.

Valid legacy cursors retain their signed boundary and checks; no format migration or
reinterpretation is needed. A timestamp already truncated by an older issuer cannot
recover its original final three digits. Restart that scan with the corrected package
to recover complete nanosecond history. The physical fingerprint repair above remains
in force, independently of timestamp and logical binary normalization.

The regression suite covers the three adjacent instants `.123456789`, `.123456790`,
`.123456791`, tied timestamps, ascending/descending ordering, one-row pages, both query
compilers, and public Observability logs/spans/metric series with released plugin 1.0.3.
Replicated reads verify both servers. Existing standalone restart, replica failover,
backup/restore, binary cursors, deduplication and selected-release gates remain required.
