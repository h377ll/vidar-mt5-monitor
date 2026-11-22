"""Coherent SQLite backup and restore helpers using the SQLite backup API."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
from urllib.parse import quote

from .contracts import atomic_bytes, sha256_file
from engine.contracts import canonical_json


def _source_connection(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{quote(str(path))}?mode=ro", uri=True)


def _validate_source(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError("SQLITE_SOURCE_INVALID")


def _quick_check(path: Path) -> str:
    db = _source_connection(path)
    try:
        result = str(db.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        db.close()
    if result != "ok":
        raise RuntimeError("SQLITE_QUICK_CHECK_FAILED")
    return result


def backup_database(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> dict[str, object]:
    source_path = Path(source)
    destination_path = Path(destination)
    _validate_source(source_path)
    if destination_path.exists() or destination_path.is_symlink():
        raise FileExistsError("BACKUP_DESTINATION_MUST_BE_NEW")
    destination_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = destination_path.with_name(f".{destination_path.name}.tmp-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        raise FileExistsError("BACKUP_TEMPORARY_EXISTS")
    source_db = _source_connection(source_path)
    target_db = sqlite3.connect(temporary)
    try:
        source_db.backup(target_db, pages=256)
        target_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        target_db.commit()
    finally:
        target_db.close()
        source_db.close()
    os.chmod(temporary, 0o600)
    _quick_check(temporary)
    os.replace(temporary, destination_path)
    directory = os.open(destination_path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return {
        "source": str(source_path),
        "destination": str(destination_path),
        "quick_check": "ok",
        "bytes": destination_path.stat().st_size,
        "sha256": sha256_file(destination_path),
    }


def versioned_backup(source: str | os.PathLike[str], directory: str | os.PathLike[str]) -> dict[str, object]:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    root = Path(directory)
    result = backup_database(source, root / f"supervisor-{now}.sqlite")
    manifest = Path(str(result["destination"]) + ".manifest.json")
    atomic_bytes(manifest, canonical_json(result) + b"\n")
    return {**result, "manifest": str(manifest)}


def restore_to_new_path(backup: str | os.PathLike[str], destination: str | os.PathLike[str]) -> dict[str, object]:
    result = backup_database(backup, destination)
    return {**result, "restored_from": str(backup)}
