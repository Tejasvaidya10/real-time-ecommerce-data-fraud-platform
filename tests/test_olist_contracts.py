import unittest

from src.olist.contracts import (
    deterministic_uuid,
    listing_key,
    map_order_status,
    map_payment_status,
    normalize_payment_type,
)


class OlistContractTest(unittest.TestCase):
    def test_ids_are_stable_and_namespaced(self):
        self.assertEqual(deterministic_uuid("order", "abc"), deterministic_uuid("order", "abc"))
        self.assertNotEqual(deterministic_uuid("order", "abc"), deterministic_uuid("customer", "abc"))

    def test_product_listing_includes_seller(self):
        self.assertNotEqual(listing_key("product", "seller-a"), listing_key("product", "seller-b"))

    def test_all_olist_order_statuses_are_mapped(self):
        expected = {
            "created": "CREATED",
            "approved": "PAYMENT_PENDING",
            "invoiced": "PAYMENT_PENDING",
            "processing": "PAYMENT_PENDING",
            "shipped": "SHIPPED",
            "delivered": "DELIVERED",
            "canceled": "CANCELLED",
            "unavailable": "FAILED",
        }
        self.assertEqual(expected, {status: map_order_status(status) for status in expected})

    def test_cancelled_orders_have_declined_payments(self):
        self.assertEqual("DECLINED", map_payment_status("canceled"))
        self.assertEqual("AUTHORIZED", map_payment_status("delivered"))

    def test_payment_type_is_normalized(self):
        self.assertEqual("CREDIT_CARD", normalize_payment_type("credit_card"))


if __name__ == "__main__":
    unittest.main()
