# SPDX-License-Identifier: Apache-2.0
"""Lossless DateTime64(9) conversion without floating point or datetime truncation."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_NANOSECONDS = 1_000_000_000
_TIMESTAMP = re.compile(
    r"(\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|z|[+-]\d{2}:[0-5]\d)"
)


def timestamp_nanoseconds(value: object) -> int:
    """Parse an offset-qualified RFC 3339 instant into signed DateTime64(9) ticks."""
    if not isinstance(value, str):
        raise TypeError("ClickHouse timestamp must be an RFC 3339 string with an offset")
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        raise ValueError("ClickHouse timestamp requires an offset and at most 9 fractional digits")
    whole, fraction, offset = match.groups()
    parsed = datetime.fromisoformat(whole + ("+00:00" if offset in {"Z", "z"} else offset))
    delta = parsed.astimezone(UTC) - _EPOCH
    result = (delta.days * 86400 + delta.seconds) * _NANOSECONDS
    result += int((fraction or "").ljust(9, "0"))
    if not -(2**63) <= result < 2**63:
        raise ValueError("ClickHouse timestamp is outside the signed DateTime64(9) range")
    return result


def timestamp_text(value: int, *, parameter: bool = False) -> str:
    """Render exact UTC ticks; preserve the existing microsecond result spelling."""
    seconds, nanos = divmod(value, _NANOSECONDS)
    whole = _EPOCH + timedelta(seconds=seconds)
    if parameter:
        return whole.strftime("%Y-%m-%d %H:%M:%S") + f".{nanos:09d}"
    if nanos % 1000 == 0:
        return whole.replace(microsecond=nanos // 1000).isoformat().replace("+00:00", "Z")
    return whole.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"
