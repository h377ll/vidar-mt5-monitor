#!/usr/bin/env python3
"""Reproduce the selected package checks without MT5 or service changes."""
from pathlib import Path
import argparse
import json
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    output = parser.parse_args().output_dir.resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error('new output directory outside repository required')
    output.mkdir(parents=True, mode=0o700)
    environment = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
                   'LANG': 'C.UTF-8', 'PYTHONDONTWRITEBYTECODE': '1'}
    results = []
    for name in ('phase_a', 'phase_b', 'engine', 'supervisor', 'service',
                 'dashboard_demo', 'phase_c_host', 'shadow_management'):
        command = [sys.executable, '-B', str(ROOT/'tests'/f'run_{name}.py'),
                   '--output-dir', str(output/name)]
        with (output/f'{name}.stdout.txt').open('w') as log:
            run = subprocess.run(command, cwd=ROOT, env=environment, stdout=log,
                                 stderr=subprocess.STDOUT, timeout=180)
        verification_path = output/name/'verification.json'
        verification = json.loads(verification_path.read_text()) if verification_path.exists() else {}
        result = {'suite':name, 'exit_code':run.returncode, 'verification':verification,
                  'passed':run.returncode == 0 and verification.get('accepted', True) is True}
        results.append(result)
        print(json.dumps({'suite':name, 'passed':result['passed'] }), flush=True)
    code = ('import sys,unittest; sys.path[:0]=["src", "."]; '
            'suite=unittest.defaultTestLoader.loadTestsFromName("tests.service.test_offline_html_backfill"); '
            'result=unittest.TextTestRunner(verbosity=2).run(suite); '
            'sys.exit(0 if result.wasSuccessful() and result.testsRun>0 and not result.skipped else 1)')
    with (output/'offline_html.txt').open('w') as log:
        run = subprocess.run([sys.executable, '-B', '-c', code], cwd=ROOT,
                             env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=120)
    results.append({'suite':'offline_html', 'exit_code':run.returncode, 'passed':run.returncode == 0})
    summary = {'release':'v0.1.0-dev', 'synthetic_only':True,
               'accepted':all(r['passed'] for r in results), 'suites':results}
    (output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps({'accepted':summary['accepted'], 'suites_completed':len(results)}))
    return 0 if summary['accepted'] else 1

if __name__ == '__main__':
    raise SystemExit(main())
