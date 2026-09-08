# SPDX-License-Identifier: Apache-2.0
"""Real FixedString cursor boundaries for every stored telemetry profile."""

from dataclasses import replace
from hashlib import sha256

import pytest
from meridian_storage.query import CursorSigner
from meridian_storage.semantics import (
    FieldDefinition,
    LogicalKind,
    LogicalType,
    canonical_json_bytes,
)

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import (
    ClickHouseAdapterFactory,
    ClickHouseMigrator,
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    plan_initial_migration,
)
from meridian_storage.adapters.clickhouse._canonical import scope_fingerprint
from tests.conftest import (
    RESOURCE_FINGERPRINT,
    build_create_context,
    build_request,
    build_schema,
    sample_record,
)
from tests.integration.test_real_clickhouse import real_engine as real_engine
from tests.unit.test_cursor_fingerprint import compile_page, issue_legacy_cursor

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("tied", [False, True])
@pytest.mark.parametrize("profile", ["log", "span", "metric"])
@pytest.mark.parametrize("wire", [False, True])
@pytest.mark.parametrize("prefix", ["0", "f"])
def test_telemetry_pagination_has_no_repeated_or_omitted_boundaries(
    real_engine, profile, wire, prefix, tied
):
    endpoint, client, _ = real_engine
    schema = build_schema()
    schema = replace(
        schema,
        fields=(
            *schema.fields,
            FieldDefinition("binary", LogicalType(LogicalKind.BYTES)),
            FieldDefinition("payload", LogicalType(LogicalKind.JSON)),
        ),
    )
    name = f"{profile}_cursor_{prefix}_{int(wire)}_{int(tied)}"
    schema = replace(schema, ref=replace(schema.ref, name=name))
    compilation = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=ResourceRef("evidence", "observability", name),
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=schema,
        record_profile=profile,
        query_final=not tied,
    )
    layout = compilation.layout
    context = build_create_context(
        layout, endpoint=endpoint, username="meridian", password="meridian-test"
    )
    settings = ClickHouseSettings.from_binding(context.binding)
    ClickHouseMigrator(client, settings).apply(plan_initial_migration(name, (compilation,)))
    # Keep both timestamp and identity tied, forcing the physical fingerprint
    # to decide every boundary. Prefix f repeats under 1.1.0; prefix 0 omits.
    if tied:
        client.command(f"SYSTEM STOP MERGES {layout.qualified_table(settings.database)}")
    by_fingerprint = {}
    for index in range(10000):
        record = {
            **sample_record(
                series_id="same-series" if tied else f"series-{index:05}", value=float(index)
            ),
            "binary": "/wAB/g==",
            "payload": {"typed": [True, index, None], "signal": profile},
        }
        fingerprint = sha256(canonical_json_bytes(record)).hexdigest()
        if fingerprint.startswith(prefix):
            by_fingerprint[fingerprint] = record
            if len(by_fingerprint) == 7:
                break
    assert len(by_fingerprint) == 7
    request = build_request(layout, records=list(by_fingerprint.values()), scope={"suite": name})
    runtime = ClickHouseAdapterFactory().create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    try:
        if tied:
            # Separate parts retain tied live rows; a single ReplacingMergeTree
            # insert block can collapse them before pagination is exercised.
            for index, record in enumerate(by_fingerprint.values()):
                single = build_request(
                    layout, records=[record], scope={"suite": name}, request_id=f"cursor-{index}"
                )
                assert session.execute(single).data["acceptedRows"] == 1
        else:
            assert session.execute(request).data["acceptedRows"] == 7
    finally:
        session.close()
        runtime.close()

    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    contexts = {"scope_fingerprint": scope_fingerprint(request.context)}
    cursor = None
    pages = []
    fingerprints = sorted(by_fingerprint, key=lambda key: (by_fingerprint[key]["series_id"], key))
    expected = [by_fingerprint[key] for key in fingerprints]
    first_compiled = None
    for _ in range(5):
        translator, compiled = compile_page(
            layout, signer, wire=wire, cursor=cursor, context_changes=contexts
        )
        if first_compiled is None:
            first_compiled = compiled
        raw = client.query(
            compiled.command["sql"],
            parameters=dict(compiled.parameters),
            tz_mode="aware",
            column_formats=dict(compiled.command["columnFormats"]),
        )
        assert isinstance(raw.result_rows[0][-1], bytes), "exercise native FixedString decoding"
        page = translator.normalize_result(compiled, raw)
        pages.extend(page.data)
        cursor = page.cursor
        if cursor is None:
            break
    assert cursor is None, "pagination must terminate"
    assert canonical_json_bytes(pages) == canonical_json_bytes(expected)
    assert len({item["value"] for item in pages}) == 7

    # Both historical driver representations resume from the exact same row.
    from base64 import b64encode

    for boundary in (fingerprints[1], b64encode(fingerprints[1].encode()).decode()):
        cursor = issue_legacy_cursor(signer, first_compiled, boundary, expected[1]["series_id"])
        translator, compiled = compile_page(
            layout, signer, wire=wire, cursor=cursor, context_changes=contexts
        )
        raw = client.query(
            compiled.command["sql"],
            parameters=dict(compiled.parameters),
            tz_mode="aware",
            column_formats=dict(compiled.command["columnFormats"]),
        )
        page = translator.normalize_result(compiled, raw)
        assert canonical_json_bytes(page.data) == canonical_json_bytes(expected[2:4])
