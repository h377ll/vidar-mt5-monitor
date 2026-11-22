from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import unittest

from engine.contracts import AccountKey, canonical_json
from engine.dashboard import build_test_dashboard_dataset
from engine.supervisor import initialize_supervisor_schema, reconcile_cycle


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests/fixtures/engine/supervisor-sequence.json"


class DashboardProjectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.account = AccountKey.from_value(self.fixture["account_key"])
        self.db = sqlite3.connect(":memory:")
        initialize_supervisor_schema(self.db, "2026-10-31T22:00:00Z")
        for row in self.fixture["cycles"]:
            reconcile_cycle(
                self.db,
                account=self.account,
                observed_at_utc=row["observed_at_utc"],
                cycle_id=row["cycle_id"],
                connection=row["connection"],
                account_state=row["account_state"],
                orders=row["orders"],
                deals=row["deals"],
                positions=row["positions"],
                symbol_specs=self.fixture["symbol_specs"],
            )

    def tearDown(self):
        self.db.close()

    def dataset(self):
        return build_test_dashboard_dataset(self.db, source_label="SYNTHETIC-MT5-DASHBOARD")

    def test_projection_is_clearly_synthetic_and_excludes_human_layers(self):
        result = self.dataset()
        self.assertEqual(result["dataset_kind"], "SYNTHETIC_TEST")
        self.assertTrue(result["synthetic"])
        self.assertFalse(result["human_observations_included"])
        self.assertFalse(result["agent_interpretations_included"])

    def test_projection_pseudonymizes_raw_account_identity(self):
        encoded = canonical_json(self.dataset())
        self.assertNotIn(self.account.login.encode(), encoded)
        self.assertNotIn(self.account.server.encode(), encoded)
        self.assertTrue(all(value.startswith("sha256:") for value in self.dataset()["account_refs"]))

    def test_projection_preserves_metric_quality_and_provenance(self):
        rows = self.dataset()["metrics"]
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row["data_class"], "CALCULATED")
            self.assertIn(row["status"], {"KNOWN", "ESTIMATED", "UNKNOWN", "UNVERIFIABLE"})
            self.assertTrue(row["algorithm_version"])
            self.assertTrue(row["provenance"].startswith("sha256:"))

    def test_projection_contains_current_dashboard_metrics(self):
        names = {row["metric_name"] for row in self.dataset()["metrics"]}
        self.assertTrue(
            {
                "initial_risk_money",
                "current_risk_money",
                "aggregate_current_risk",
                "planned_r",
                "realized_r",
                "realized_drawdown",
                "floating_drawdown",
            }
            <= names
        )
        self.assertTrue(any(name.startswith("execution_slippage:") for name in names))

    def test_projection_is_deterministic_and_read_only(self):
        changes = self.db.total_changes
        first = canonical_json(self.dataset())
        second = canonical_json(self.dataset())
        self.assertEqual(first, second)
        self.assertEqual(self.db.total_changes, changes)

    def test_projection_rejects_non_synthetic_label(self):
        with self.assertRaisesRegex(ValueError, "SYNTHETIC_SOURCE_LABEL_REQUIRED"):
            build_test_dashboard_dataset(self.db, source_label="LIVE")


if __name__ == "__main__":
    unittest.main()
