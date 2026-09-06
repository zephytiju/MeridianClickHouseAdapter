# SPDX-License-Identifier: Apache-2.0
"""Usage event/aggregate schema fixtures, expressed through released Semantics only."""

from meridian_storage.semantics import (
    PROFILE_EXTENSION_KEY,
    CatalogName,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
    TimeSeriesProfile,
)


def usage_schema(name: str) -> SchemaDocument:
    event = name == "events"
    identity = "eventId" if event else "aggregateVersionId"
    measurement = "value" if event else "total"
    text_fields = (
        "schemaVersion",
        "scopeFingerprint",
        "meterId",
        "dimensionFingerprint",
        "fingerprint",
        *(
            ("eventId", "idempotencyKey", "subjectId", "unit", "source")
            if event
            else ("aggregateId", "aggregateVersionId", "sourceFingerprint", "algorithm")
        ),
    )
    nullable_fields = (
        ("correctionOf", "correctionReason", "originalUnit") if event else ("supersedes",)
    )
    timestamps = (
        ("windowStart", "windowEnd", "recordedAt")
        if event
        else (
            "windowStart",
            "windowEnd",
            "watermark",
            "createdAt",
        )
    )
    fields = [
        FieldDefinition(key, LogicalType(LogicalKind.STRING), mutable=False) for key in text_fields
    ]
    fields.extend(
        FieldDefinition(key, LogicalType(LogicalKind.STRING), nullable=True, mutable=False)
        for key in nullable_fields
    )
    fields.extend(
        FieldDefinition(key, LogicalType(LogicalKind.UTC_TIMESTAMP), mutable=False)
        for key in timestamps
    )
    fields.extend(
        FieldDefinition(key, LogicalType(LogicalKind.JSON), mutable=False)
        for key in ("scope", "dimensions", "correlation", *(("provenance",) if event else ()))
    )
    fields.extend(
        FieldDefinition(key, LogicalType(LogicalKind.INT64), mutable=False)
        for key in (
            ("meterVersion",) if event else ("meterVersion", "aggregateRevision", "eventCount")
        )
    )
    fields.append(
        FieldDefinition(measurement, LogicalType(LogicalKind.DECIMAL, 76, 18), mutable=False)
    )
    if event:
        fields.append(
            FieldDefinition(
                "originalValue",
                LogicalType(LogicalKind.DECIMAL, 76, 18),
                nullable=True,
                mutable=False,
            )
        )
    profile = TimeSeriesProfile(
        timestamp_field="windowStart",
        series_identity=(identity,),
        dimensions=("scopeFingerprint", "meterId", "meterVersion", "dimensionFingerprint"),
        measurements=(measurement,) if event else (measurement, "eventCount"),
        exemplar_field="correlation",
    )
    return SchemaDocument(
        ref=SchemaReference(CatalogName.STRUCTURED, "usage", name, "1.0.0"),
        semantic_kind=SemanticKind.TIME_SERIES,
        fields=tuple(fields),
        identity=("scopeFingerprint", identity),
        consistency="eventual",
        extensions={PROFILE_EXTENSION_KEY: profile.to_dict()},
    )
