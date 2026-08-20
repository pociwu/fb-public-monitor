import sqlite3
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.backup_database import backup_database, integrity_check, running_job_count


def create_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("INSERT INTO items(value) VALUES('committed')")
    connection.commit()
    return connection


def test_backup_database_uses_consistent_sqlite_snapshot(tmp_path: Path):
    source = tmp_path / "monitor.sqlite3"
    live_connection = create_database(source)
    try:
        target, removed = backup_database(source, tmp_path / "backups")
    finally:
        live_connection.close()

    assert removed == []
    assert target.is_file()
    if os.name == "posix":
        assert target.stat().st_mode & 0o777 == 0o600
        assert target.parent.stat().st_mode & 0o777 == 0o700
    integrity_check(target)
    backup_connection = sqlite3.connect(target)
    try:
        assert backup_connection.execute("SELECT value FROM items").fetchall() == [("committed",)]
        assert backup_connection.execute("PRAGMA journal_mode").fetchone()[0] in {"delete", "wal"}
    finally:
        backup_connection.close()


def test_backup_database_keeps_only_five_newest_snapshots(tmp_path: Path):
    source = tmp_path / "monitor.sqlite3"
    create_database(source).close()
    output = tmp_path / "backups"
    unrelated = output / "manual.sqlite3"
    output.mkdir()
    unrelated.write_bytes(b"keep me")
    start = datetime(2026, 8, 1, tzinfo=UTC)

    for offset in range(7):
        backup_database(source, output, keep=5, now=start + timedelta(days=offset))

    backups = sorted(path.name for path in output.glob("monitor-*.sqlite3"))
    assert len(backups) == 5
    assert backups[0].startswith("monitor-20260803")
    assert backups[-1].startswith("monitor-20260807")
    assert unrelated.read_bytes() == b"keep me"
    assert not list(output.glob("*.tmp"))


def test_running_job_count_is_read_only_and_handles_missing_schema(tmp_path: Path):
    missing = tmp_path / "missing.sqlite3"
    assert running_job_count(missing) == 0
    assert not missing.exists()

    source = tmp_path / "monitor.sqlite3"
    connection = sqlite3.connect(source)
    connection.execute("CREATE TABLE jobs(id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
    connection.executemany(
        "INSERT INTO jobs(status) VALUES(?)",
        [("running",), ("pending",), ("running",), ("done",)],
    )
    connection.commit()
    connection.close()

    assert running_job_count(source) == 2


def test_corrupt_source_never_publishes_a_backup(tmp_path: Path):
    source = tmp_path / "monitor.sqlite3"
    source.write_bytes(b"not a sqlite database")
    output = tmp_path / "backups"

    with pytest.raises((sqlite3.DatabaseError, RuntimeError)):
        backup_database(source, output)

    assert not list(output.glob("monitor-*.sqlite3"))
    assert not list(output.glob("*.tmp"))


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode bits model OCI ownership")
def test_backup_writes_to_host_directory_when_application_data_is_read_only(
    tmp_path: Path,
):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    source = data_dir / "monitor.sqlite3"
    create_database(source).close()
    os.chmod(source, 0o444)
    os.chmod(data_dir, 0o555)
    output = tmp_path / "backups" / "deploy"

    try:
        target, removed = backup_database(source, output)
    finally:
        # pytest must be able to remove its temporary directory after the test.
        os.chmod(data_dir, 0o755)

    assert removed == []
    assert target.parent == output.resolve()
    assert target.is_file()
    integrity_check(target)
