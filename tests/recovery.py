# SPDX-License-Identifier: Apache-2.0
"""Only the disposable test deployment may exercise administrative recovery."""

import os
import subprocess


def compose(*arguments: str) -> None:
    subprocess.run(  # noqa: S603
        ["docker", "compose", "-f", os.environ["MERIDIAN_COMPOSE_FILE"], *arguments],  # noqa: S607
        check=True,
        timeout=180,
    )


def backup_restore(client, layout, suffix: str, *, peers=()) -> None:
    table = layout.qualified_table("meridian_adapter_test")
    restored = f"`meridian_adapter_test`.`restored_{suffix}`"
    expected = client.query(f"SELECT * FROM {table} FINAL ORDER BY tuple(*)").result_rows
    assert expected
    backup = client.query(f"BACKUP TABLE {table} TO Disk('backups', '{suffix}.zip')")
    assert backup.result_rows[0][1] == "BACKUP_CREATED"
    if peers:
        # Restore into the original topology after real fixture data loss. Renaming
        # a replicated table would reuse its literal Keeper path and collide.
        create_sql = client.query(f"SHOW CREATE TABLE {table}").result_rows[0][0]
        for peer in peers:
            peer.command(f"DROP TABLE {table} SYNC")
        restored = table
    result = client.query(
        f"RESTORE TABLE {table} AS {restored} FROM Disk('backups', '{suffix}.zip')"
    )
    assert result.result_rows[0][1] == "RESTORED"
    assert client.query(f"SELECT * FROM {restored} FINAL ORDER BY tuple(*)").result_rows == expected
    if peers:
        for peer in peers:
            if peer is not client:
                peer.command(create_sql)
                peer.command(f"SYSTEM SYNC REPLICA {table}")
                assert (
                    peer.query(f"SELECT * FROM {table} FINAL ORDER BY tuple(*)").result_rows
                    == expected
                )
    else:
        client.command(f"DROP TABLE {restored} SYNC")
