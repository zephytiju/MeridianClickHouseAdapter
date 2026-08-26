# SPDX-License-Identifier: Apache-2.0
"""Public ClickHouse Adapter SPI and deployment-time contracts."""

from ._version import __version__
from .configuration import ADAPTER_CONTRACT_VERSION, ADAPTER_ID, ClickHouseSettings
from .descriptor import adapter_descriptor, capability_manifest, query_capabilities
from .migrations import ClickHouseMigrator, MigrationBundle, plan_initial_migration
from .query import ClickHouseQueryTranslator
from .runtime import ClickHouseAdapterFactory, ClickHouseAdapterRuntime, ClickHouseAdapterSession
from .schema import (
    ClickHouseSchemaCompiler,
    ColumnLayout,
    RecordProfile,
    ResourceLayout,
    SchemaCompilation,
    Topology,
)

__all__ = [
    "ADAPTER_CONTRACT_VERSION",
    "ADAPTER_ID",
    "ClickHouseAdapterFactory",
    "ClickHouseAdapterRuntime",
    "ClickHouseAdapterSession",
    "ClickHouseMigrator",
    "ClickHouseQueryTranslator",
    "ClickHouseSchemaCompiler",
    "ClickHouseSettings",
    "ColumnLayout",
    "MigrationBundle",
    "RecordProfile",
    "ResourceLayout",
    "SchemaCompilation",
    "Topology",
    "__version__",
    "adapter_descriptor",
    "capability_manifest",
    "plan_initial_migration",
    "query_capabilities",
]
