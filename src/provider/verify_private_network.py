#!/usr/bin/env python3
"""Fail closed unless the process is inside a loopback-only network namespace."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import AbstractSet


def validate_private_network(
    *, self_namespace_inode: int, host_namespace_inode: int, interfaces: AbstractSet[str]
) -> None:
    if self_namespace_inode == host_namespace_inode:
        raise PermissionError("RPYC_NETWORK_NAMESPACE_IS_HOST")
    if interfaces != {"lo"}:
        raise PermissionError("RPYC_NETWORK_INTERFACES_NOT_LOOPBACK_ONLY")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-namespace-inode-file", required=True, type=Path)
    args = parser.parse_args()
    try:
        raw_host_inode = args.host_namespace_inode_file.read_text(encoding="ascii")
        if not raw_host_inode.endswith("\n") or not raw_host_inode[:-1].isdigit():
            raise ValueError("RPYC_HOST_NAMESPACE_RECEIPT_INVALID")
        validate_private_network(
            self_namespace_inode=os.stat("/proc/self/ns/net").st_ino,
            host_namespace_inode=int(raw_host_inode),
            interfaces={path.name for path in Path("/sys/class/net").iterdir()},
        )
        return 0
    except Exception as exc:
        print(f"{type(exc).__name__}:{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
