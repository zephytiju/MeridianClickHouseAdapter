<!-- SPDX-License-Identifier: Apache-2.0 -->

# Migrations

Migrations are explicit deployment artifacts. Adapter runtime startup never issues DDL.

`plan_initial_migration(migration_id, compilations)` converts deterministic `SchemaCompilation`
values into a `MigrationBundle` containing the metadata-table DDL, Resource-table DDL, and
metadata rows. The caller supplies the versioned migration ID; bundle equality and all layout
fingerprints make review and execution reproducible.

`ClickHouseMigrator.apply(bundle)` is intended only for a Platform/Vangu IaC migration job. It:

1. executes the ordered DDL statements;
2. writes the immutable Resource/schema/layout mapping into `_meridian_resources`; and
3. optimizes the replacing metadata table so subsequent verification sees the selected rows.

Operators must apply additive schema and retention changes through separately reviewed,
versioned migration bundles. Provisioning, credentials, ACLs, replication, rollback, backup,
restore, and recovery remain outside this library and under deployment authority.

After a migration, generate the authenticated physical verification through the adapter and pin
its fingerprint in the Binding before allowing workload startup. A mismatch fails closed.
