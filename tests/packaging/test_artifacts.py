# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def distributions() -> tuple[Path, Path]:
    configured = os.environ.get("MERIDIAN_DIST_DIR")
    if configured is None:
        pytest.skip("MERIDIAN_DIST_DIR selects built artifacts")
    directory = Path(configured)
    wheels = tuple(directory.glob("meridian_storage_clickhouse-1.1.0-*.whl"))
    sdists = tuple(directory.glob("meridian_storage_clickhouse-1.1.0.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1
    return wheels[0], sdists[0]


def test_wheel_has_one_typed_adapter_package_and_contract_data(
    distributions: tuple[Path, Path],
) -> None:
    wheel, _ = distributions
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    package_prefix = "meridian_storage/adapters/clickhouse/"
    assert package_prefix + "__init__.py" in names
    assert package_prefix + "py.typed" in names
    assert package_prefix + "contracts/meridian-clickhouse-layout.v1.schema.json" in names
    assert package_prefix + "contracts/conformance/valid-metric-append.json" in names
    code_roots = {
        name.split("/", 1)[0]
        for name in names
        if ".dist-info/" not in name and not name.endswith("/")
    }
    assert code_roots == {"meridian_storage"}


def test_wheel_metadata_is_release_ready(distributions: tuple[Path, Path]) -> None:
    wheel, _ = distributions
    with zipfile.ZipFile(wheel) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = Parser().parsestr(archive.read(metadata_name).decode())
        entry_points_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/entry_points.txt")
        )
        entry_points = archive.read(entry_points_name).decode()
        license_files = {
            Path(name).name for name in archive.namelist() if ".dist-info/licenses/" in name
        }
    assert metadata["Name"] == "meridian-storage-clickhouse"
    assert metadata["Version"] == "1.1.0"
    assert metadata["License-Expression"] == "Apache-2.0"
    assert metadata.get_all("Requires-Python") == ["<3.15,>=3.12"]
    requirements = set(metadata.get_all("Requires-Dist", []))
    assert "meridian-storage-core<2,>=1.1" in requirements
    assert "meridian-storage-semantics<3,>=2.0.1" in requirements
    assert "meridian-storage-query<2,>=1.0.3" in requirements
    assert "[meridian_storage.adapters]" in entry_points
    factory_entry = ":".join(
        ("clickhouse = meridian_storage.adapters.clickhouse", "ClickHouseAdapterFactory")
    )
    assert factory_entry in entry_points
    assert license_files == {"LICENSE", "NOTICE"}


def test_sdist_contains_source_license_notice_and_build_metadata(
    distributions: tuple[Path, Path],
) -> None:
    _, sdist = distributions
    with tarfile.open(sdist, "r:gz") as archive:
        names = set(archive.getnames())
    root = "meridian_storage_clickhouse-1.1.0/"
    assert root + "pyproject.toml" in names
    assert root + "LICENSE" in names
    assert root + "NOTICE" in names
    assert root + "README.md" in names
    assert root + "src/meridian_storage/adapters/clickhouse/__init__.py" in names
