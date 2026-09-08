# SPDX-License-Identifier: Apache-2.0
"""Real primary timestamp fidelity through adapter and released Observability queries."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from meridian_storage.plugins.observability import EvidenceResources, TelemetryQueries
from meridian_storage.query import (
    CursorSigner,
    Field,
    Literal,
    PageSpec,
    Projection,
    QueryOperation,
    QueryTarget,
    ResultSpec,
    Sort,
    TimestampRange,
    TranslationContext,
)
from meridian_storage.semantics import (
    CatalogName,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
    canonical_json_bytes,
)
from meridian_storage.spi import PhysicalResource

from meridian_storage import OperationResult, ResourceRef
from meridian_storage.adapters.clickhouse import (
    ClickHouseAdapterFactory,
    ClickHouseMigrator,
    ClickHouseQueryTranslator,
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    plan_initial_migration,
)
from meridian_storage.adapters.clickhouse._canonical import scope_fingerprint
from meridian_storage.adapters.clickhouse.ingestion import prepare_batch
from tests.conftest import (
    REGISTRY_FINGERPRINT,
    RESOURCE_FINGERPRINT,
    build_create_context,
    build_request,
)
from tests.integration.test_real_clickhouse import real_engine as real_engine

pytestmark = pytest.mark.integration
TIMES = tuple(f"2026-08-25T12:00:00.123456{n:03d}Z" for n in (789, 790, 791, 791))
NANOS = (1787659200123456789, 1787659200123456790, 1787659200123456791, 1787659200123456791)


@pytest.mark.parametrize("profile", ["log", "span", "metric"])
def test_exact_primary_timestamps_through_public_reads(real_engine, profile):
    endpoint, client, _ = real_engine
    resource = ResourceRef("evidence", "observability", profile + "_nanoseconds")
    schema = SchemaDocument(
        ref=SchemaReference(CatalogName("evidence"), resource.namespace, resource.name, "1.0.0"),
        semantic_kind=SemanticKind.DOCUMENT,
        fields=tuple(
            FieldDefinition(name, LogicalType(kind))
            for name, kind in (
                ("evidenceId", LogicalKind.STRING),
                ("observedTime", LogicalKind.UTC_TIMESTAMP),
                ("startTime", LogicalKind.UTC_TIMESTAMP),
                ("endTime", LogicalKind.UTC_TIMESTAMP),
                ("severity", LogicalKind.STRING),
                ("name", LogicalKind.STRING),
                ("value", LogicalKind.FLOAT64),
                ("binary", LogicalKind.BYTES),
                ("payload", LogicalKind.JSON),
            )
        ),
        identity=("evidenceId",),
        consistency="eventual",
    )
    compilation = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=resource,
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=schema,
        record_profile=profile,
        timestamp_field="observedTime",
    )
    layout = compilation.layout
    context = build_create_context(
        layout, endpoint=endpoint, username="meridian", password="meridian-test"
    )
    settings = ClickHouseSettings.from_binding(context.binding)
    ClickHouseMigrator(client, settings).apply(
        plan_initial_migration(profile + "-nano", (compilation,))
    )
    records = [
        {
            "evidenceId": f"row-{i}",
            "observedTime": t,
            "startTime": t,
            "endTime": t,
            "severity": "INFO",
            "name": "nano",
            "value": float(i),
            "binary": "/wAB/g==",
            "payload": {"nestedTime": t, "typed": [True, i, None]},
        }
        for i, t in enumerate(TIMES)
    ]
    request = build_request(layout, records=records, scope={"suite": profile + "-nano"})
    prepared = prepare_batch(request, layout, records, settings)
    offset = layout.insert_columns.index(layout.physical_column("observedTime"))
    assert tuple(row[offset] for row in prepared.rows) == NANOS
    runtime = ClickHouseAdapterFactory().create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    try:
        runtime.verify_physical(
            (PhysicalResource(resource, RESOURCE_FINGERPRINT, schema.fingerprint, profile),)
        )
        first = session.execute(request)
        assert session.execute(request).data["batchId"] == first.data["batchId"]
        physical = client.query(
            f"SELECT toUnixTimestamp64Nano(`{layout.physical_column('observedTime')}`) "
            f"FROM {layout.qualified_table(settings.database)} FINAL "
            f"ORDER BY `{layout.physical_column('evidenceId')}`"
        ).result_rows
        assert tuple(row[0] for row in physical) == NANOS

        def execute_query(arguments):
            return session.execute(
                replace(
                    request,
                    operation=replace(
                        request.operation,
                        operation_contract="meridian.evidence.query",
                        read_only=True,
                        input=arguments,
                    ),
                )
            )

        class Executor:
            def execute(self, expression):
                result = execute_query(expression.arguments)
                return OperationResult(
                    result.data,
                    "evidence",
                    "meridian.evidence.query",
                    "1.0.0",
                    (resource,),
                    request.request_id,
                    request.execution_id,
                    request.operation.request_fingerprint,
                    REGISTRY_FINGERPRINT,
                    context.binding.required_capability_fingerprint,
                    result.provenance,
                )

        queries = TelemetryQueries(Executor(), EvidenceResources(resource, resource, resource))
        bounds = {
            "start": datetime(2026, 8, 25, 12, tzinfo=UTC),
            "end": datetime(2026, 8, 25, 12, tzinfo=UTC) + timedelta(seconds=1),
        }
        query = (
            queries.logs(**bounds)
            if profile == "log"
            else queries.spans(**bounds)
            if profile == "span"
            else queries.metric_series("nano", **bounds)
        )
        assert canonical_json_bytes(query.page(limit=4).execute().items) == canonical_json_bytes(
            records
        )
        cursor = None
        seen = []
        for _ in range(5):
            page = query.page(limit=1, cursor=cursor).execute()
            seen.extend(page.items)
            cursor = page.cursor
            if cursor is None:
                break
        assert cursor is None, "public pagination must terminate"
        assert canonical_json_bytes(seen) == canonical_json_bytes(records)

        # Adjacent boundaries must neither exclude a valid nanosecond nor widen to a microsecond.
        for operators, expected in (
            ({"gte": TIMES[0], "lt": TIMES[1]}, records[:1]),
            ({"gt": TIMES[0], "lte": TIMES[1]}, records[1:2]),
        ):
            result = execute_query({"where": {"observedTime": operators}, "limit": 10})
            assert canonical_json_bytes(result.data["items"]) == canonical_json_bytes(expected)

        # Wire queries also exercise aliases and mixed sort directions.
        signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
        translator = ClickHouseQueryTranslator(settings, signer)
        cursor = None
        seen = []
        for _ in range(5):
            operation = QueryOperation(
                catalog="evidence",
                targets=(QueryTarget(resource),),
                operation="scan",
                filter=TimestampRange(
                    Field("observedTime"),
                    Literal("2026-08-25T12:00:00Z", "utcTimestamp"),
                    Literal("2026-08-25T12:00:01Z", "utcTimestamp"),
                ),
                order=(Sort(Field("observedTime"), "desc"), Sort(Field("evidenceId"), "asc")),
                result=ResultSpec(
                    projection=(
                        Projection(Field("observedTime"), "at"),
                        Projection(Field("evidenceId"), "id"),
                    )
                ),
                page=PageSpec(1, cursor),
                consistency="eventual",
            )
            compiled = translator.compile_wire(
                operation,
                TranslationContext(
                    "clickhouse-test",
                    operation.fingerprint,
                    REGISTRY_FINGERPRINT,
                    {resource.canonical: layout.schema_fingerprint},
                    scope_fingerprint(request.context),
                    30_000,
                ),
            )
            raw = client.query(
                compiled.command["sql"],
                parameters=dict(compiled.parameters),
                column_formats=dict(compiled.command["columnFormats"]),
            )
            page = translator.normalize_result(compiled, raw)
            seen.extend(page.data)
            cursor = page.cursor
            if cursor is None:
                break
        assert cursor is None
        assert seen == [{"at": TIMES[i], "id": f"row-{i}"} for i in (2, 3, 1, 0)]

        aggregate = execute_query(
            {
                "where": {"observedTime": {"gte": TIMES[0], "lte": TIMES[2]}},
                "metrics": [
                    {"name": "earliest", "function": "min", "field": "observedTime"},
                    {"name": "latest", "function": "max", "field": "observedTime"},
                ],
            }
        )
        assert dict(aggregate.data["items"][0]) == {"earliest": TIMES[0], "latest": TIMES[2]}
    finally:
        session.close()
        runtime.close()
