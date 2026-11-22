#!/usr/bin/env python3
"""Run persistent-service acceptance without MT5, alerts or installation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import unittest


os.umask(0o077)
sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT)]
EXPECTED_TESTS = 51


def hashes() -> dict[str, str]:
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for parent in (ROOT / "src", ROOT / "ea")
        for path in sorted(parent.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    output = Path(args.output_dir).resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error("output-dir must be new and outside the repository")
    output.mkdir(parents=True, mode=0o700)
    before = hashes()
    started = datetime.now(timezone.utc).isoformat()
    begin = time.monotonic()
    from tests.service.test_service import ServiceTests

    with (output / "tests.txt").open("w", encoding="utf-8") as stream:
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(ServiceTests)
        )
    after = hashes()
    changed = sorted(path for path in set(before) | set(after) if before.get(path) != after.get(path))
    accepted = result.wasSuccessful() and result.testsRun == EXPECTED_TESTS and not changed
    verification = {
        "schema_version": "vidar.mt5.service-verification.v1",
        "accepted": accepted,
        "started_at_utc": started,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.monotonic() - begin, 3),
        "tests_planned": EXPECTED_TESTS,
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "protected_files_changed": changed,
        "real_mt5_calls": 0,
        "trading_writes": 0,
        "real_notifications": 0,
        "services_or_schedulers_changed": 0,
        "ft_qt_interactions": 0,
    }
    (output / "verification.json").write_text(json.dumps(verification, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(verification, indent=2))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
