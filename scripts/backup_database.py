#!/usr/bin/env python3
"""Create and verify consistent SQLite backups for safe deployments."""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


def running_job_count(source: Path) -> int:
    """Return the number of in-flight jobs without creating a missing DB."""
    source = Path(source)
    if not source.is_file():
        return 0
    connection = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True, timeout=10)
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()
        if not table:
            return 0
        row = connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE status='running'"
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        connection.close()


def integrity_check(database: Path) -> None:
    """Raise when SQLite does not report an exact, complete ``ok`` result."""
    connection = sqlite3.connect(f"file:{Path(database).as_posix()}?mode=ro", uri=True)
    try:
        results = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    finally:
        connection.close()
    if results != ["ok"]:
        detail = "; ".join(results[:10]) or "no result"
        raise RuntimeError(f"SQLite integrity_check failed: {detail}")


def prune_backups(output_dir: Path, *, keep: int = 5) -> list[Path]:
    """Keep only the newest deployment backups created by this helper."""
    if keep < 1:
        raise ValueError("keep must be greater than zero")
    backups = sorted(
        Path(output_dir).glob("monitor-*.sqlite3"),
        key=lambda path: path.name,
        reverse=True,
    )
    removed: list[Path] = []
    for path in backups[keep:]:
        path.unlink()
        removed.append(path)
    return removed


def backup_database(
    source: Path,
    output_dir: Path,
    *,
    keep: int = 5,
    now: datetime | None = None,
) -> tuple[Path, list[Path]]:
    """Use SQLite's Backup API, verify the snapshot, then apply retention."""
    source = Path(source).resolve()
    output_dir = Path(output_dir).resolve()
    if not source.is_file():
        raise FileNotFoundError(f"SQLite database does not exist: {source}")
    if keep < 1:
        raise ValueError("keep must be greater than zero")
    output_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(output_dir, 0o700)
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    suffix = timestamp.strftime("%Y%m%dT%H%M%S.%fZ")
    target = output_dir / f"monitor-{suffix}.sqlite3"
    temporary = target.with_suffix(".sqlite3.tmp")

    source_connection: sqlite3.Connection | None = None
    target_connection: sqlite3.Connection | None = None
    try:
        source_connection = sqlite3.connect(
            f"file:{source.as_posix()}?mode=ro",
            uri=True,
            timeout=30,
        )
        target_connection = sqlite3.connect(temporary)
        os.chmod(temporary, 0o600)
        source_connection.backup(target_connection)
        target_connection.commit()
        target_connection.close()
        target_connection = None
        integrity_check(temporary)
        temporary.replace(target)
    except Exception:
        if target_connection is not None:
            target_connection.close()
            target_connection = None
        if source_connection is not None:
            source_connection.close()
            source_connection = None
        if temporary.exists():
            temporary.unlink()
        raise
    finally:
        if target_connection is not None:
            target_connection.close()
        if source_connection is not None:
            source_connection.close()

    removed = prune_backups(output_dir, keep=keep)
    return target, removed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    count = subcommands.add_parser("running-jobs", help="print the running job count")
    count.add_argument("--source", required=True, type=Path)

    backup = subcommands.add_parser("backup", help="create and verify a deployment backup")
    backup.add_argument("--source", required=True, type=Path)
    backup.add_argument("--output-dir", required=True, type=Path)
    backup.add_argument("--keep", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "running-jobs":
        print(running_job_count(args.source))
        return 0
    target, removed = backup_database(args.source, args.output_dir, keep=args.keep)
    print(f"backup_path={target}")
    print("integrity_check=ok")
    print(f"pruned={len(removed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
