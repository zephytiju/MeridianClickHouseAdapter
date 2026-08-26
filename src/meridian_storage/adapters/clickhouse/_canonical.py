# SPDX-License-Identifier: Apache-2.0
"""Small deterministic and injection-safe primitives used inside the adapter."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import cast

from meridian_storage.semantics import JsonValue, canonical_json_bytes

from meridian_storage import OperationContext

_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PHYSICAL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,126}$")


def fingerprint(value: JsonValue) -> str:
    """Return the Meridian sha256 fingerprint of a canonical JSON value."""

    return f"sha256:{hashlib.sha256(canonical_json_bytes(value)).hexdigest()}"


def require_fingerprint(value: object, name: str, *, optional: bool = False) -> str | None:
    """Validate a public fingerprint without accepting ambiguous bare digests."""

    if value is None and optional:
        return None
    if not isinstance(value, str) or _FINGERPRINT_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a sha256 fingerprint")
    return value


def physical_identifier(value: object, name: str) -> str:
    """Validate an IaC-generated ClickHouse identifier."""

    if not isinstance(value, str) or _PHYSICAL_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be an IaC-generated physical identifier")
    return value


def quote_identifier(value: str) -> str:
    """Quote a pre-validated physical or logical column identifier."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("ClickHouse identifier must be non-empty and contain no NUL")
    return "`" + value.replace("`", "``") + "`"


def scope_fingerprint(context: OperationContext) -> str:
    """Bind stored and queried rows to the exact tenant/scope partition."""

    payload: JsonValue = {
        "tenant": context.tenant,
        "scope": dict(sorted(context.scope.items())),
    }
    return fingerprint(payload)


def scope_digest(context: OperationContext) -> str:
    """Return the fixed-size hex form stored in ClickHouse sort keys."""

    return scope_fingerprint(context).removeprefix("sha256:")


def json_mapping(value: object, name: str) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} must be an object")
    canonical_json_bytes(value)
    return cast(Mapping[str, JsonValue], value)


__all__ = [
    "fingerprint",
    "json_mapping",
    "physical_identifier",
    "quote_identifier",
    "require_fingerprint",
    "scope_digest",
    "scope_fingerprint",
]
