# SPDX-License-Identifier: Apache-2.0
from importlib.metadata import version

import pytest

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import ClickHouseSchemaCompiler, ResourceLayout, Topology
from meridian_storage.adapters.clickhouse.schema.layout import HIDDEN_SCOPE
from tests.conftest import RESOURCE_FINGERPRINT
from tests.usage_fixtures import usage_schema


def test_released_shared_set() -> None:
    assert version("meridian-storage-core") == "1.0.1"
    assert version("meridian-storage-semantics") == "2.0.0"
    assert version("meridian-storage-query") == "1.0.2"


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
