# SPDX-License-Identifier: Apache-2.0
"""Physical fingerprint values must round trip independently of logical bytes."""

from base64 import b64encode
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from meridian_storage.query import CursorSigner, InvalidCursor, PageSpec

from meridian_storage.adapters.clickhouse import ClickHouseQueryTranslator, ClickHouseSettings
from meridian_storage.adapters.clickhouse.query import compile_simple_query
from tests.conftest import build_binding
from tests.unit.test_query import _logical_query, _translation


def compile_page(layout, signer, *, wire, cursor=None, context_changes=None, input_changes=None):
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    translator = ClickHouseQueryTranslator(settings, signer)
    operation = _logical_query(layout, cursor=cursor)
    context = _translation(layout, operation.fingerprint)
    context = replace(context, **(context_changes or {}))
    if wire:
        if input_changes:
            operation = replace(operation, **input_changes)
            context = replace(context, plan_fingerprint=operation.fingerprint)
        compiled = translator.compile_wire(operation, context)
    else:
        value = {
            "where": {"observed_at": {"gte": "2026-08-25T00:00:00Z", "lt": "2026-08-26T00:00:00Z"}},
            "limit": 2,
            **({"cursor": cursor} if cursor else {}),
            **(input_changes or {}),
        }
        compiled = compile_simple_query("query", value, layout, context, settings, signer)
    return translator, compiled


def issue_legacy_cursor(signer, compiled, fingerprint, identity="same-series"):
    # The 1.1.0 representation: _json_value(bytes) => Base64, str => unchanged.
    command = compiled.command
    return signer.issue(
        plan_fingerprint=command["cursorPlanFingerprint"],
        schema_fingerprints=command["schemaFingerprints"],
        registry_fingerprint=command["registryFingerprint"],
        scope_fingerprint=command["scopeFingerprint"],
        page_size=command["pageSize"],
        sort_tuple=("2026-08-25T12:00:00Z", identity, fingerprint),
    )


@pytest.mark.parametrize("wire", [False, True])
@pytest.mark.parametrize("prefix", ["0", "f"])
@pytest.mark.parametrize("as_bytes", [False, True])
def test_fingerprint_cursor_resumes_at_raw_storage_boundary(layout, wire, prefix, as_bytes):
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    translator, compiled = compile_page(layout, signer, wire=wire)
    fingerprint = prefix * 64
    raw_fingerprint = fingerprint.encode() if as_bytes else fingerprint
    timestamp = datetime(2026, 8, 25, 12, tzinfo=UTC)
    result = translator.normalize_result(
        compiled,
        SimpleNamespace(
            column_names=("binary", "__meridian_sort_0", "__meridian_sort_1", "__meridian_sort_2"),
            result_rows=[(b"\xff\x00", timestamp, "same-series", raw_fingerprint)] * 3,
        ),
    )
    assert result.data[0] == {"binary": "/wA="}
    assert result.cursor
    _, next_page = compile_page(layout, signer, wire=wire, cursor=result.cursor)
    assert next_page.parameters["p5"] == fingerprint

    legacy_value = b64encode(fingerprint.encode()).decode() if as_bytes else fingerprint
    old_cursor = issue_legacy_cursor(signer, compiled, legacy_value)
    _, old_page = compile_page(layout, signer, wire=wire, cursor=old_cursor)
    assert old_page.parameters == next_page.parameters


@pytest.mark.parametrize("wire", [False, True])
@pytest.mark.parametrize(
    "value",
    [
        None,
        12,
        "f" * 63,
        "g" * 64,
        "bad=",
        "é" * 88,
        b64encode(b"f" * 64).decode()[:-3] + "h==",
        b64encode(b"\xff" * 64).decode(),
        b64encode(b"g" * 64).decode(),
    ],
)
def test_invalid_signed_physical_fingerprint_is_rejected(layout, wire, value):
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    _, compiled = compile_page(layout, signer, wire=wire)
    cursor = issue_legacy_cursor(signer, compiled, value)
    with pytest.raises(ValueError, match="cursor row fingerprint"):
        compile_page(layout, signer, wire=wire, cursor=cursor)


@pytest.mark.parametrize("wire", [False, True])
@pytest.mark.parametrize("change", ["scope", "schema", "registry", "plan", "page", "signature"])
def test_legacy_cursor_retains_all_validation_gates(layout, wire, change):
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    _, compiled = compile_page(layout, signer, wire=wire)
    cursor = issue_legacy_cursor(signer, compiled, b64encode(b"f" * 64).decode())
    contexts = {
        "scope": {"scope_fingerprint": "sha256:" + "9" * 64},
        "schema": {"schema_fingerprints": {layout.resource.canonical: "sha256:" + "9" * 64}},
        "registry": {"registry_fingerprint": "sha256:" + "9" * 64},
    }
    changes = None
    if change == "plan":
        changes = {"order": ()} if wire else {"select": ["series_id"]}
        if wire:
            from meridian_storage.query import Field, Sort

            changes = {"order": (Sort(Field("series_id"), "desc"),)}
    if change == "page":
        changes = {"page": PageSpec(1, cursor)} if wire else {"limit": 1}
    if change == "signature":
        signer = CursorSigner({"k1": b"2" * 32}, active_key_id="k1")
    with pytest.raises((ValueError, InvalidCursor)):
        compile_page(
            layout,
            signer,
            wire=wire,
            cursor=cursor,
            context_changes=contexts.get(change),
            input_changes=changes,
        )
