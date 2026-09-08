import copy
import unittest

from src.gold.dashboard_export import validate_dashboard_payload


def valid_payload():
    return {
        "metadata": {"source_payments": 2, "source_decisions": 2, "source_customers": 3},
        "daily_business_kpis": [{"payment_count": 2}],
        "daily_fraud_kpis": [{"total_decisions": 2}],
        "seller_risk_watchlist": [{"seller_rank": 1, "transaction_count": 2}],
        "customer_risk_segments": [
            {"customer_risk_segment": "LOW", "count": 2},
            {"customer_risk_segment": "HIGH", "count": 1},
        ],
    }


class DashboardExportTest(unittest.TestCase):
    def test_valid_aggregate_payload_is_accepted(self):
        validate_dashboard_payload(valid_payload())

    def test_entity_identifiers_are_rejected(self):
        payload = valid_payload()
        payload["seller_risk_watchlist"][0]["seller_id"] = "not-allowed"
        with self.assertRaisesRegex(ValueError, "identifier"):
            validate_dashboard_payload(payload)

    def test_unreconciled_totals_are_rejected(self):
        payload = copy.deepcopy(valid_payload())
        payload["daily_business_kpis"][0]["payment_count"] = 1
        with self.assertRaisesRegex(ValueError, "payment totals"):
            validate_dashboard_payload(payload)


if __name__ == "__main__":
    unittest.main()
