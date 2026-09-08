import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ContractTest(unittest.TestCase):
    def test_contracts_are_valid_json_records(self):
        for path in sorted((PROJECT_ROOT / "schemas").glob("*.avsc")):
            with self.subTest(path=path.name):
                schema = json.loads(path.read_text())
                self.assertEqual("record", schema["type"])
                self.assertTrue(schema["name"])
                self.assertTrue(schema["fields"])

    def test_payment_contract_has_idempotency_keys(self):
        schema = json.loads((PROJECT_ROOT / "schemas/payment-event-v1.avsc").read_text())
        fields = {field["name"] for field in schema["fields"]}
        self.assertTrue({"event_id", "transaction_id", "event_time"}.issubset(fields))


if __name__ == "__main__":
    unittest.main()
