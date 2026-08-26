# SPDX-License-Identifier: Apache-2.0
"""ClickHouse physical Schema and layout contracts."""

from .compiler import (
    MAX_DECIMAL_PRECISION,
    METADATA_TABLE,
    ClickHouseSchemaCompiler,
    SchemaCompilation,
    create_table_ddl,
    logical_type_to_clickhouse,
    metadata_table_ddl,
    physical_name,
)
from .layout import (
    HIDDEN_BATCH,
    HIDDEN_COLUMNS,
    HIDDEN_INGESTED,
    HIDDEN_RESOURCE,
    HIDDEN_ROW,
    HIDDEN_SCHEMA,
    HIDDEN_SCOPE,
    HIDDEN_TENANT,
    ColumnLayout,
    RecordProfile,
    ResourceLayout,
    Topology,
)

__all__ = [
    "HIDDEN_BATCH",
    "HIDDEN_COLUMNS",
    "HIDDEN_INGESTED",
    "HIDDEN_RESOURCE",
    "HIDDEN_ROW",
    "HIDDEN_SCHEMA",
    "HIDDEN_SCOPE",
    "HIDDEN_TENANT",
    "MAX_DECIMAL_PRECISION",
    "METADATA_TABLE",
    "ClickHouseSchemaCompiler",
    "ColumnLayout",
    "RecordProfile",
    "ResourceLayout",
    "SchemaCompilation",
    "Topology",
    "create_table_ddl",
    "logical_type_to_clickhouse",
    "metadata_table_ddl",
    "physical_name",
]
