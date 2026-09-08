# SPDX-License-Identifier: Apache-2.0
import pytest

from meridian_storage.adapters.clickhouse import Topology
from tests.evidence_fixtures import append_and_assert, core_fixture
from tests.integration.test_real_clickhouse import real_engine as real_engine
from tests.recovery import backup_restore, compose

pytestmark = pytest.mark.integration


def test_public_core_evidence_append_merge_restart_and_backup(real_engine):
    endpoint, client, _ = real_engine
    runtime, layout = core_fixture((client,), (endpoint,), Topology.STANDALONE)
    append_and_assert(runtime, layout, seed=True)
    client.command(f"OPTIMIZE TABLE {layout.qualified_table('meridian_adapter_test')} FINAL")
    append_and_assert(runtime, layout, seed=False)
    compose("restart", "clickhouse")
    compose("up", "--wait", "clickhouse")
    append_and_assert(runtime, layout, seed=False)
    backup_restore(client, layout, "public_append")


def test_initial_migration_cannot_relabel_an_existing_legacy_table(real_engine):
    from dataclasses import replace

    from meridian_storage.errors import CompatibilityError

    from meridian_storage import ResourceRef
    from meridian_storage.adapters.clickhouse import (
        ClickHouseAdapterFactory,
        ClickHouseMigrator,
        ClickHouseSchemaCompiler,
        ClickHouseSettings,
        plan_initial_migration,
    )
    from meridian_storage.adapters.clickhouse.schema import create_table_ddl
    from tests.conftest import RESOURCE_FINGERPRINT, build_create_context, build_schema

    endpoint, client, _ = real_engine
    schema = build_schema()
    schema = replace(schema, ref=replace(schema.ref, name="legacy_append"))
    new = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=ResourceRef("evidence", "observability", "legacy_append"),
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=schema,
        record_profile="metric",
    )
    legacy_layout = replace(new.layout, append_only=False)
    legacy = replace(
        new,
        layout=legacy_layout,
        create_table_sql=create_table_ddl("meridian_adapter_test", legacy_layout),
    )

    def context(layout):
        return build_create_context(
            layout, endpoint=endpoint, username="meridian", password="meridian-test"
        )

    old_settings = ClickHouseSettings.from_binding(context(legacy.layout).binding)
    new_settings = ClickHouseSettings.from_binding(context(new.layout).binding)
    ClickHouseMigrator(client, old_settings).apply(plan_initial_migration("legacy", (legacy,)))
    with pytest.raises(CompatibilityError, match="append-only sorting key"):
        ClickHouseMigrator(client, new_settings).apply(plan_initial_migration("new", (new,)))
    metadata = client.query(
        "SELECT layout_fingerprint FROM meridian_adapter_test._meridian_resources FINAL "
        "WHERE resource_ref = {resource:String}",
        {"resource": legacy.layout.resource.canonical},
    )
    actual = metadata.result_rows[0][0]
    assert (
        actual.decode("ascii") if isinstance(actual, bytes) else actual
    ) == legacy.layout.layout_fingerprint
    runtime = ClickHouseAdapterFactory().create(context(new.layout))
    with pytest.raises(CompatibilityError, match="append-only sorting key"):
        runtime.open()
    old_runtime = ClickHouseAdapterFactory().create(context(legacy.layout))
    old_runtime.open()
    old_runtime.close()
