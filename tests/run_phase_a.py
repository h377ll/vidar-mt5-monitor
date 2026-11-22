#!/usr/bin/env python3
"""Run the 49 existing tests with invented fixtures and temporary-only writes.

Does not modify production modules, start services, use MCPs, or send messages.
The legacy test names still mention live samples; their input here is synthetic.
"""
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

os.umask(0o077)
sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parents[1]
import argparse
parser = argparse.ArgumentParser(description='Reproducción sintética; no MT5 ni envíos reales')
parser.add_argument('--output-dir', required=True, help='Directorio NUEVO, fuera del paquete')
args = parser.parse_args()
OUTPUT = Path(args.output_dir).expanduser().resolve()
if OUTPUT.is_relative_to(PACKAGE):
    parser.error('Los resultados deben quedar fuera del paquete y sus evidencias históricas')
OUTPUT.mkdir(parents=True, exist_ok=False)
PROJECT = PACKAGE / 'src' / 'original'
TESTS = PACKAGE / 'tests' / 'original'
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(TESTS))
import journal as j
import event_receiver as er


def bundle(count, login, source, swaps=False):
    """Independent toy history: deposit plus two executions per position."""
    deals = [{'deal_id': '1', 'action': 'balance', 'profit': 1000,
              'open_time': '2026-01-01T00:00:00'}]
    orders, positions = [], []
    for index in range(count):
        pid = str(100 + index)
        profit = 10 if index % 2 == 0 else -5
        for side in range(2):
            rid = str(2 + index * 2 + side)
            deal = dict(deal_id=rid, order_id=rid, position_id=pid,
                        symbol='SYNTHUSD', action='buy' if side == 0 else 'sell',
                        entry='in' if side == 0 else 'out', volume=1,
                        price_open=1.1 if side == 0 else 1.2,
                        open_time=f'2026-01-{index + 2:02}T0{side + 1}:00:00',
                        profit=0 if side == 0 else profit)
            if swaps:
                deal['swaps'] = -1 if side else 0
            deals.append(deal)
            orders.append(dict(order_id=rid, position_id=pid, symbol='SYNTHUSD',
                               state='filled', price_order=deal['price_open']))
        positions.append(dict(position_id=pid, open_volume=1, close_volume=1,
                              open_price=1.1, close_price=1.2, profit=profit))
    # Expected balances are toy constants, not calculated via production helpers.
    balance = {10: 1025, 24: 1036}[(count if swaps else 10)]
    account = dict(server='SYNTHETIC-PHASE-A', login=login, type='demo',
                   broker='Fictional', currency='USD', margin_mode='hedging',
                   balance=balance, equity=balance, margin_free=balance, profit=0)
    state = dict(account=account, terminal=dict(server_connected=True))
    return dict(source=source, observed_at='2026-01-28T12:00:00Z',
                before=copy.deepcopy(state), after=copy.deepcopy(state),
                history_window=['2026-01-01', '2026-01-28'],
                get_trading_open_positions=dict(positions=[], orders=[]),
                get_trading_history_positions=dict(positions=positions),
                get_trading_history_orders=dict(deals=deals, orders=orders))


def hashes():
    # File hashes only; real fixtures/ledgers are never used as test input.
    return {str(p.relative_to(PACKAGE)): hashlib.sha256(p.read_bytes()).hexdigest()
            for root in (PROJECT, TESTS) for p in sorted(root.rglob('*')) if p.is_file() and not p.is_symlink()}


def main():
    started = datetime.now(timezone.utc).isoformat()
    before = hashes()
    observations = dict(sqlite_connections=0, loopback_connections=0,
                        listeners=[], blocked_operations=[])
    active = False
    temporary_root = None
    servers = []
    endpoints = set()

    def inside(path):
        return isinstance(path, (str, bytes, os.PathLike)) and Path(
            os.fsdecode(path)).resolve().is_relative_to(temporary_root)

    def block(label):
        observations['blocked_operations'].append(label)
        raise PermissionError('PHASE_A_ISOLATION: ' + label)

    def audit(event, args):
        if not active:
            return
        if event == 'sqlite3.connect':
            if not inside(args[0]):
                block('sqlite_outside_temporary_directory')
            observations['sqlite_connections'] += 1
        elif event == 'open':
            path, mode, flags = args
            if isinstance(path, int):
                return
            resolved = Path(os.fsdecode(path)).resolve()
            write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
            if flags & write_flags and not inside(path):
                block('file_write_outside_temporary_directory')
            if resolved.is_relative_to(PROJECT / 'evidence') or '.sqlite' in resolved.name:
                if not inside(path):
                    block('real_fixture_or_ledger_read')
        elif event == 'socket.bind':
            address = args[1]
            if address != ('127.0.0.1', 0):
                block('non_ephemeral_loopback_bind')
        elif event == 'socket.connect':
            if args[1] not in endpoints:
                block('connection_outside_test_listener')
            observations['loopback_connections'] += 1
        elif event == 'socket.getaddrinfo':
            if args[0] != '127.0.0.1':
                block('external_dns')
        elif event in ('subprocess.Popen', 'os.system', 'os.exec', 'os.posix_spawn'):
            block('subprocess_execution')

    original_factory = er.make_server

    def tracked_factory(db_path, credentials, port=0):
        server = original_factory(db_path, credentials, port)
        servers.append(server)
        endpoints.add(server.server_address)
        observations['listeners'].append(dict(host=server.server_address[0],
                                               port=server.server_address[1]))
        return server

    sys.addaudithook(audit)
    started_monotonic = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='vidar-phase-a-') as root:
        temporary_root = Path(root)
        fixture_root = temporary_root / 'fixtures'
        evidence = fixture_root / 'evidence'
        evidence.mkdir(parents=True)
        (evidence / 'manjaro-data.json').write_text(json.dumps(bundle(10, '700001', 'synthetic-a')))
        (evidence / 'vps-data.json').write_text(json.dumps(bundle(24, '700002', 'synthetic-b', swaps=True)))
        with (OUTPUT / 'tests.txt').open('w') as log:
            log.write('49 existing tests; exclusively invented fixtures, no real-source reads.\n')
            log.write('Legacy names containing live/manjaro/vps do not denote real input in this run.\n\n')
            with patch.object(j, 'ROOT', fixture_root), patch.object(tempfile, 'tempdir', root), \
                    patch.object(er, 'make_server', tracked_factory), \
                    patch.dict(os.environ, {'no_proxy': '127.0.0.1', 'NO_PROXY': '127.0.0.1'}):
                active = True
                try:
                    suite = unittest.defaultTestLoader.loadTestsFromNames([
                        'test_event_receiver', 'test_journal', 'test_notifications', 'test_risk'])
                    result = unittest.TextTestRunner(stream=log, verbosity=2).run(suite)
                finally:
                    active = False
        leftovers = [str(p.relative_to(temporary_root)) for p in temporary_root.iterdir()
                     if p != fixture_root]
        observations['test_temp_directories_remaining'] = leftovers
        observations['listener_descriptors_closed'] = all(s.fileno() == -1 for s in servers)
        observations['listener_ports_closed'] = []
        for address in endpoints:
            with socket.socket() as probe:
                probe.settimeout(0.5)
                observations['listener_ports_closed'].append(probe.connect_ex(address) != 0)
    after = hashes()
    changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    accepted = (result.wasSuccessful() and result.testsRun == 49 and not result.skipped
                and not changed and not observations['blocked_operations'] and not leftovers
                and bool(servers) and observations['listener_descriptors_closed']
                and all(observations['listener_ports_closed']) and not temporary_root.exists())
    summary = dict(started_utc=started, finished_utc=datetime.now(timezone.utc).isoformat(),
                   duration_seconds=round(time.monotonic() - started_monotonic, 3),
                   phase='A', accepted=accepted, tests_run=result.testsRun,
                   failures=len(result.failures), errors=len(result.errors), skipped=len(result.skipped),
                   synthetic_fixture_override=True, real_source_calls=0, telegram_sends=0,
                   temporary_directory_removed=not temporary_root.exists(),
                   protected_files=len(before), changed_project_files=changed,
                   observations=observations,
                   limitations=['No EA compilation or installation', 'No demo/real MT5 access',
                                'No inbox-to-reconciler integration', 'No real notification delivery',
                                'Guard applies to this Python test process, not host-wide isolation'])
    (OUTPUT / 'verification.json').write_text(json.dumps(summary, indent=2) + '\n')
    (OUTPUT / 'project-hashes.json').write_text(json.dumps(dict(before=before, after=after), indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    return 0 if accepted else 1


if __name__ == '__main__':
    raise SystemExit(main())
