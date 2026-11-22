#!/usr/bin/env python3
"""Offline host-adapted candidate checks, NOT MQL5 compilation/terminal tests."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "ea/phase_c/VidarObserverDemoC.mq5"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    out = args.output_dir.resolve()
    if out.exists() or out.is_relative_to(ROOT):
        parser.error("output-dir must be new and outside the repository")
    out.mkdir(parents=True)
    source = SOURCE.read_text()
    code = re.sub(r"//[^\n]*", "", source)
    forbidden = re.findall(
        r"\b(?:OrderSend(?:Async)?|CTrade|OrderOpen|OrderDelete|OrderModify|"
        r"PositionOpen|PositionClose|PositionModify|WebRequest|SocketCreate|"
        r"ShellExecute\w*|SendNotification|SendMail)\s*(?:\(|;)|"
        r"#\s*(?:import|include|resource)\b", code
    )
    assert not forbidden, forbidden
    assert 'input long ExpectedLogin=0;' in code
    assert 'input string ExpectedServer="";' in code
    deinit = code[code.index("void OnDeinit"):]
    assert not re.search(r"\b(?:FlushEvents|Append|File\w+|Snapshot)\s*\(", deinit)
    callback = code[code.index("void OnTradeTransaction"):code.index("void FlushEvents")]
    assert not re.search(r"\b(?:Append|File\w+|Snapshot)\s*\(", callback)
    # Allow only these two host adaptations; the tested function bodies remain
    # the candidate's actual code. API behavior is supplied by explicit doubles.
    adapted, properties = re.subn(r"^#property[^\n]*\n", "", source, flags=re.M)
    adapted, arrays = re.subn(r"\buchar bytes\[\];", "std::vector<uchar> bytes;", adapted)
    assert properties == 2 and arrays == 1
    with tempfile.TemporaryDirectory(prefix="vidar-c-host-") as tmp:
        tmp = Path(tmp)
        (tmp / "candidate-under-test.hpp").write_text(adapted)
        build = subprocess.run(
            ["g++", "-std=c++17", "-Wall", "-Wextra", "-Wno-unused-parameter",
             "-I", str(tmp), "-I", str(ROOT / "tests/phase_c"),
             str(ROOT / "tests/phase_c/host_checks.cpp"), "-o", str(tmp / "checks")],
            capture_output=True, text=True, timeout=60
        )
        (out / "host-build.txt").write_text(build.stdout + build.stderr)
        if build.returncode:
            raise SystemExit(build.returncode)
        run = subprocess.run([str(tmp / "checks")], capture_output=True, text=True, timeout=30)
        (out / "tests.txt").write_text(run.stdout + run.stderr)
        passed = len(re.findall(r"^PASS ", run.stdout, re.M))
        failed = len(re.findall(r"^FAIL ", run.stdout, re.M))
        result = {
            "kind": "host-adapted function bodies with MT5 API doubles",
            "not_mql5_compilation": True, "not_terminal_initialization": True,
            "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
            "passed": passed, "failed": failed, "exit_code": run.returncode,
            "static_checks": "no trading/network/imports; no I/O in callback/deinit",
            "temporary_build_removed": False,
        }
    result["temporary_build_removed"] = not tmp.exists()
    (out / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    raise SystemExit(run.returncode)


if __name__ == "__main__":
    main()
