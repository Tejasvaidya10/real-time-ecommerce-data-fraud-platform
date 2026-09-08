import unittest

from src.generator.fraud_rules import PaymentSignals, score_payment


class FraudRulesTest(unittest.TestCase):
    def test_normal_payment_is_approved(self):
        decision = score_payment(PaymentSignals(75, 80, False, "US-NY", "US-NY", "US-NY", 0, 1))
        self.assertEqual("APPROVE", decision.action)
        self.assertEqual(0, decision.score)

    def test_amount_outlier_requires_review(self):
        decision = score_payment(PaymentSignals(400, 100, False, "US-NY", "US-NY", "US-NY", 0, 1))
        self.assertEqual("REVIEW", decision.action)
        self.assertIn("AMOUNT_OUTLIER", decision.reason_codes)

    def test_account_takeover_is_declined(self):
        decision = score_payment(PaymentSignals(500, 80, True, "US-CA", "US-NY", "US-CA", 5, 1))
        self.assertEqual("DECLINE", decision.action)
        self.assertGreaterEqual(decision.score, 60)
        self.assertIn("NEW_DEVICE", decision.reason_codes)
        self.assertIn("REPEATED_LOGIN_FAILURES", decision.reason_codes)

    def test_score_is_capped(self):
        decision = score_payment(PaymentSignals(1000, 20, True, "US-CA", "US-NY", "US-TX", 8, 7))
        self.assertEqual(100, decision.score)


if __name__ == "__main__":
    unittest.main()
