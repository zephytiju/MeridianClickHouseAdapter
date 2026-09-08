# SPDX-License-Identifier: Apache-2.0
from importlib.metadata import requires, version

import pytest
from packaging.requirements import Requirement

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import ClickHouseSchemaCompiler, ResourceLayout, Topology
from meridian_storage.adapters.clickhouse.schema.layout import HIDDEN_SCOPE
from tests.conftest import RESOURCE_FINGERPRINT
from tests.usage_fixtures import usage_schema


def test_released_shared_set() -> None:
    # Validate installed dependency API bounds, not a historical release recipe.
    for raw in requires("meridian-storage-clickhouse") or ():
        requirement = Requirement(raw)
        if requirement.name.startswith("meridian-storage-"):
            assert version(requirement.name) in requirement.specifier


@pytest.mark.parametrize("name", ["events", "aggregates"])
@pytest.mark.parametrize("topology", list(Topology))
def test_usage_layout_precision_scope_and_logical_identity(name: str, topology: Topology) -> None:
    schema = usage_schema(name)
    resource = ResourceRef("structured", "usage", name)
    compiled = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=resource,
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=schema,
        record_profile="usage",
        topology=topology,
    )
    layout = compiled.layout
    assert layout.resource == resource
    assert layout.schema_fingerprint == schema.fingerprint
    assert ResourceLayout.from_mapping(layout.to_dict()) == layout
    assert layout.timestamp_field == "windowStart"
    assert "scopeFingerprint" in layout.dimension_fields
    assert f"`{HIDDEN_SCOPE}` FixedString(64)" in compiled.create_table_sql
    assert f"ORDER BY (`{HIDDEN_SCOPE}`," in compiled.create_table_sql
    decimal_columns = {
        item.logical_name: item.clickhouse_type
        for item in layout.columns
        if item.logical_name in {"value", "originalValue", "total"}
    }
    assert decimal_columns == (
        {"value": "Decimal(76, 18)", "originalValue": "Nullable(Decimal(76, 18))"}
        if name == "events"
        else {"total": "Decimal(76, 18)"}
    )
