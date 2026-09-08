import unittest

from src.silver.contracts import ENTITY_SPECS, SUPPORTED_TYPES


class CdcContractTest(unittest.TestCase):
    def test_required_silver_entities_are_defined(self):
        self.assertEqual(
            {"customers", "orders", "payments", "chargebacks"},
            {spec.name for spec in ENTITY_SPECS},
        )

    def test_topics_and_entity_names_are_unique(self):
        self.assertEqual(len(ENTITY_SPECS), len({spec.topic for spec in ENTITY_SPECS}))
        self.assertEqual(len(ENTITY_SPECS), len({spec.name for spec in ENTITY_SPECS}))

    def test_primary_keys_exist_and_field_types_are_supported(self):
        for spec in ENTITY_SPECS:
            fields = {field.name: field.data_type for field in spec.fields}
            self.assertIn(spec.primary_key, fields)
            self.assertTrue(set(fields.values()).issubset(SUPPORTED_TYPES))


if __name__ == "__main__":
    unittest.main()
