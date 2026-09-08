from __future__ import annotations

import random
from decimal import Decimal
from typing import Sequence


def choose_normal_purchase(
    customer_average: Decimal,
    unit_prices: Sequence[Decimal],
    rng: random.Random,
) -> tuple[int, int, Decimal]:
    """Choose a realistic basket close to the customer's historical baseline."""
    if not unit_prices:
        raise ValueError("unit_prices cannot be empty")

    lower_bound = customer_average * Decimal("0.5")
    upper_bound = customer_average * Decimal("1.5")
    candidates = [
        (product_index, quantity, unit_price * quantity)
        for product_index, unit_price in enumerate(unit_prices)
        for quantity in (1, 2, 3)
        if lower_bound <= unit_price * quantity <= upper_bound
    ]

    if candidates:
        return rng.choice(candidates)

    # A defensive fallback for unusual product catalogs: select the basket
    # nearest to the baseline instead of manufacturing an inconsistent total.
    return min(
        (
            (product_index, quantity, unit_price * quantity)
            for product_index, unit_price in enumerate(unit_prices)
            for quantity in (1, 2, 3)
        ),
        key=lambda candidate: abs(candidate[2] - customer_average),
    )
