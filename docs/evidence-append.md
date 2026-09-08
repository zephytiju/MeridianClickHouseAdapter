<!-- SPDX-License-Identifier: Apache-2.0 -->

# Public Evidence append (1.1.3)

Evidence 1.0.2 requires `append-only` when Core resolves an append Expression.
ClickHouse 1.1.2 omitted that guarantee, so authenticated Core startup could succeed
while `runtime.execute(runtime.catalog("evidence").append(...))` failed with
`MERIDIAN_CAPABILITY_UNSUPPORTED`. Its replacement key also collapsed different
contents sharing scope, timestamp and Schema identity.

New Evidence compilations serialize `appendOnly: true` and add the existing
canonical logical row SHA-256 fingerprint to that replacement key. Different
contents are retained across insert blocks, `FINAL` reads, background merges,
restarts and replicated backup/restore. Identical canonical records in the same
scope/time/identity can coalesce. Appending a second occurrence that must remain
distinct requires a distinct event identity or timestamp in the Schema data.
Changing content appends another record; it does not amend the earlier record.

The existing stable batch token and bounded retry contract remain in place.
Visibility and duplicate suppression are eventual; `queryFinal: true` resolves
identical canonical rows at read time. Raw storage and `queryFinal: false` reads
can contain retry copies before merging. This is not an exactly-once receipt or
a promise of one physical insert beyond the declared retry window. Retention
and administrative deletion remain deployment policy; this contract is not WORM.

The adapter advertises `append-only` only when every Evidence layout in the
Binding carries the explicit marker. A mixture containing a legacy Evidence
layout cannot advertise the Binding-wide guarantee. Structured layouts retain
their append-version semantics. Atomic Evidence, transactions, CAS and patch
remain unsupported; direct SPI atomic requests are also rejected before insert.

## Existing deployments and fingerprints

Legacy serialized layouts omit `appendOnly`. They retain their original canonical
layout, descriptor and manifest fingerprints, DDL and query behavior. The immutable
release-selection vectors continue to validate that historical representation.
The new marker changes the layout fingerprint and capability manifest; actual
sorting-key verification contributes to the new physical fingerprint. Existing
signed cursor formats, logical row hashes and nanosecond transport are unchanged.

Installing 1.1.3 does not migrate tables or make a legacy layout satisfy Evidence
append. Compile a new Evidence layout and let the deployment owner provision its
physical table and handle any explicitly authorized data transfer. Use a new
owned physical table when retaining the old table, then generate and verify the
new layout, physical and capability locks before switching the Binding. Do not
copy new fingerprints onto old tables or reuse old conformance as new evidence.
Prior records already discarded by replacement cannot be reconstructed here.

Both startup and physical verification compare `system.tables.sorting_key` with
the compiled key. The initial migrator checks that key before writing metadata,
so `CREATE TABLE IF NOT EXISTS` against an old table fails without relabeling it.
Startup remains read-only. Production migration is outside this release.

## Acceptance

Real standalone and replicated suites use installed public Evidence/Core APIs,
registered Resource/Schema pins and actual adapter discovery. They append distinct
contents with the same identity and nanosecond timestamp, traverse one-row pages,
retry through a new Core runtime, deny atomic/unbounded/oversized operations,
verify tenant isolation, and read again after merges, restart and backup/restore.
Independent release evidence additionally uses normally installed public wheels,
TLS authentication and a public Constructs-generated configuration to rerun the
original failure. Exact artifact and server selections belong to those reports.
