import random
import unittest
from decimal import Decimal

from src.generator.purchase_selection import choose_normal_purchase


class PurchaseSelectionTest(unittest.TestCase):
    def test_normal_purchase_stays_near_customer_baseline(self):
        baseline = Decimal("100.00")
        prices = [Decimal("8.00"), Decimal("30.00"), Decimal("75.00"), Decimal("140.00"), Decimal("350.00")]

        for seed in range(25):
            _, _, amount = choose_normal_purchase(baseline, prices, random.Random(seed))
            self.assertGreaterEqual(amount, baseline * Decimal("0.5"))
            self.assertLessEqual(amount, baseline * Decimal("1.5"))

    def test_empty_catalog_is_rejected(self):
        with self.assertRaises(ValueError):
            choose_normal_purchase(Decimal("100.00"), [], random.Random(42))


if __name__ == "__main__":
    unittest.main()
