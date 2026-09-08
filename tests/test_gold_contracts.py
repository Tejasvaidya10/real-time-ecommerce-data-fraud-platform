import unittest

from src.gold.contracts import GOLD_TABLE_SPECS


class GoldContractTest(unittest.TestCase):
    def test_required_analytics_tables_are_defined(self):
        self.assertEqual(
            {
                "daily_business_kpis",
                "daily_fraud_kpis",
                "seller_performance",
                "customer_360",
            },
            {spec.name for spec in GOLD_TABLE_SPECS},
        )

    def test_table_names_are_unique(self):
        self.assertEqual(
            len(GOLD_TABLE_SPECS),
            len({spec.name for spec in GOLD_TABLE_SPECS}),
        )

    def test_every_table_declares_a_grain_and_primary_key(self):
        for spec in GOLD_TABLE_SPECS:
            self.assertTrue(spec.grain.strip())
            self.assertTrue(spec.primary_key)
            self.assertEqual(len(spec.primary_key), len(set(spec.primary_key)))


if __name__ == "__main__":
    unittest.main()
