from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from engine.supervisor import TEST_VAULT_MARKER
from service.offline_html_backfill import run_offline_html_backfill


def synthetic_report() -> bytes:
    rows = [
        ["Cuenta de trading:", "900001 (USD, SYNTHETIC-SERVER, demo, Hedge)"],
        ["Fecha:", "2026.11.04 12:00"],
        [
            "2026.11.03 01:00:00",
            "6001",
            "SYNUSD",
            "buy",
            "",
            "1.00",
            "1.1000",
            "1.0900",
            "1.1200",
            "2026.11.03 02:00:00",
            "1.1100",
            "-1.00",
            "0.00",
            "99.00",
        ],
        [
            "2026.11.03 01:00:00",
            "6001",
            "SYNUSD",
            "buy",
            "1.00 / 1.00",
            "market",
            "",
            "",
            "2026.11.03 01:00:00",
            "filled",
            "",
        ],
        [
            "2026.11.03 02:00:00",
            "6002",
            "SYNUSD",
            "sell",
            "1.00 / 1.00",
            "1.1200",
            "",
            "",
            "2026.11.03 02:00:00",
            "filled",
            "[tp 1.1200]",
        ],
        [
            "2026.11.03 01:00:00",
            "7001",
            "SYNUSD",
            "buy",
            "in",
            "1.00",
            "1.1000",
            "6001",
            "",
            "-0.50",
            "-0.25",
            "0.00",
            "0.00",
            "10000.00",
            "",
        ],
        [
            "2026.11.03 02:00:00",
            "7002",
            "SYNUSD",
            "sell",
            "out",
            "1.00",
            "1.1100",
            "6002",
            "",
            "-0.50",
            "-0.25",
            "0.00",
            "99.00",
            "10098.00",
            "[tp 1.1200]",
        ],
    ]
    table = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    )
    return ("<html><body><table>" + table + "</table></body></html>").encode("utf-16le")


class OfflineHtmlBackfillTests(unittest.TestCase):
    def prepare(self, root: Path, *, marker: bool = True) -> tuple[Path, Path, str]:
        report = root / "report.html"
        payload = synthetic_report()
        report.write_bytes(payload)
        vault = root / "vault"
        vault.mkdir()
        if marker:
            (vault / ".vidar-mt5-test-vault").write_text(TEST_VAULT_MARKER, encoding="utf-8")
        human = vault / "Humanas/Sesiones/2026-11-03.md"
        human.parent.mkdir(parents=True)
        human.write_text("# Diario humano de ensayo\n", encoding="utf-8")
        return report, vault, hashlib.sha256(payload).hexdigest()

    def test_replay_is_idempotent_and_historical(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report, vault, digest = self.prepare(root)
            arguments = {
                "report": report,
                "expected_sha256": digest,
                "position_id": "6001",
                "database": root / "backfill.sqlite",
                "vault": vault,
                "human_note": "Humanas/Sesiones/2026-11-03.md",
                "auto_note_directory": "Automaticas/Operaciones",
                "observed_at_utc": "2026-11-04T12:00:00Z",
            }
            first = run_offline_html_backfill(**arguments)
            db = sqlite3.connect(root / "backfill.sqlite")
            account_data = json.loads(db.execute("SELECT data FROM accounts").fetchone()[0])
            account_data["baseline_marker"] = "preserve"
            db.execute(
                "UPDATE accounts SET data=?",
                (json.dumps(account_data, ensure_ascii=False, sort_keys=True, separators=(",", ":")),),
            )
            db.commit()
            db.close()
            second = run_offline_html_backfill(**arguments)

            self.assertEqual(first["changed_records"], 5)
            self.assertEqual(second["changed_records"], 0)
            self.assertEqual(first["counts"], second["counts"])
            self.assertEqual(second["counts"]["historical_backfills"], 1)
            self.assertEqual(second["counts"]["revisions"], 0)
            self.assertEqual(second["non_historical_outbox_events"], 0)
            self.assertEqual(first["sqlite_quick_check"], "ok")
            self.assertTrue(first["note_changed"])
            self.assertFalse(second["note_changed"])
            self.assertTrue(first["human_note_changed"])
            self.assertFalse(second["human_note_changed"])
            self.assertTrue(second["bidirectional_links_ok"])
            self.assertEqual(second["metric_statuses"]["costs.reported-total"], "KNOWN")
            self.assertEqual(second["metric_statuses"]["risk.initial"], "UNKNOWN")
            self.assertEqual(second["metric_statuses"]["risk.current"], "UNKNOWN")
            self.assertEqual(second["metric_statuses"]["protection.modifications"], "UNKNOWN")

            db = sqlite3.connect(root / "backfill.sqlite")
            position = json.loads(
                db.execute("SELECT data FROM records WHERE kind='position' AND id='6001'").fetchone()[0]
            )
            account_after = json.loads(db.execute("SELECT data FROM accounts").fetchone()[0])
            source_status = db.execute("SELECT status FROM sources").fetchone()[0]
            db.close()
            self.assertEqual(position["status"], "closed")
            self.assertEqual(position["lifecycle_mode"], "historical_backfill")
            self.assertEqual(account_after["baseline_marker"], "preserve")
            self.assertEqual(source_status, "historical_backfill_selected_lifecycle_complete")
            human_text = Path(second["human_note_path"]).read_text(encoding="utf-8")
            self.assertEqual(human_text.count("![[Automaticas/Operaciones/2026-11-03-SYNUSD-6001]]"), 1)

    def test_test_vault_marker_is_mandatory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report, vault, digest = self.prepare(root, marker=False)
            with self.assertRaisesRegex(ValueError, "OBSIDIAN_TEST_VAULT_MARKER_REQUIRED"):
                run_offline_html_backfill(
                    report=report,
                    expected_sha256=digest,
                    position_id="6001",
                    database=root / "backfill.sqlite",
                    vault=vault,
                    human_note="Humanas/Sesiones/2026-11-03.md",
                    auto_note_directory="Automaticas/Operaciones",
                    observed_at_utc="2026-11-04T12:00:00Z",
                )

    def test_unreported_costs_remain_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report, vault, _digest = self.prepare(root)
            payload = report.read_bytes().decode("utf-16le").replace("<td>-0.25</td>", "<td></td>")
            report.write_bytes(payload.encode("utf-16le"))
            digest = hashlib.sha256(report.read_bytes()).hexdigest()
            result = run_offline_html_backfill(
                report=report,
                expected_sha256=digest,
                position_id="6001",
                database=root / "backfill.sqlite",
                vault=vault,
                human_note="Humanas/Sesiones/2026-11-03.md",
                auto_note_directory="Automaticas/Operaciones",
                observed_at_utc="2026-11-04T12:00:00Z",
            )
            self.assertEqual(result["metric_statuses"]["costs.reported-total"], "UNKNOWN")
            self.assertEqual(result["metric_statuses"]["pnl.reported-components"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
