# SPDX-License-Identifier: Apache-2.0
"""Record an exact successfully tested combination; never infer future support."""

import hashlib
import json
import os
import subprocess
import sys
from importlib.metadata import distributions
from pathlib import Path

profile = sys.argv[1]
root = Path(__file__).resolve().parents[1]
compose = os.environ["MERIDIAN_COMPOSE_FILE"]
images = subprocess.check_output(  # noqa: S603
    ["docker", "compose", "-f", compose, "images", "--format", "json"],  # noqa: S607
    text=True,
)
report_path = root / "build" / "evidence" / f"{profile}.json"
report = json.loads(report_path.read_text())
report["verifiedCombination"] = {
    "status": "verified-only-for-this-run",
    "selectedEngineRelease": os.environ.get("CLICKHOUSE_SELECTED_RELEASE", "25.3"),
    "selectedServerImage": os.environ.get(
        "CLICKHOUSE_IMAGE",
        "clickhouse/clickhouse-server@sha256:b627d7a9bc0e0c1bac26cdbe9d2fc6316faa29c5d8a174f28f5abd57d0fa6ba2",
    ),
    "selectedKeeperImage": (
        "clickhouse/clickhouse-keeper@sha256:"
        "2c8b97bb628999adfa597172091ce495304e0a260d791fe6e475d64a314193b4"
    )
    if profile == "replicated"
    else None,
    "observedImages": json.loads(images),
    "installedDistributions": dict(
        sorted((d.metadata["Name"], d.version) for d in distributions())
    ),
    "requirementsLockSha256": hashlib.sha256((root / "requirements.lock").read_bytes()).hexdigest(),
    "acceptance": [
        "full-engine-suite",
        "process-restart" if profile == "standalone" else "member-loss-and-catchup",
        "real-backup-restore",
    ],
    "testResults": profile + "-junit.xml",
}
report_path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
