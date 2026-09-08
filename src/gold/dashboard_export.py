from __future__ import annotations

from collections.abc import Mapping, Sequence
from numbers import Number


REQUIRED_SECTIONS = frozenset(
    {
        "metadata",
        "daily_business_kpis",
        "daily_fraud_kpis",
        "seller_risk_watchlist",
        "customer_risk_segments",
    }
)


def _sum(rows: Sequence[Mapping], field: str) -> Number:
    return sum(row.get(field, 0) or 0 for row in rows)


def _reject_identifiers(value) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key.endswith("_id") or key in {"customer_id", "seller_id", "transaction_id"}:
                raise ValueError(f"Dashboard export must not contain identifier field {key}")
            _reject_identifiers(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for child in value:
            _reject_identifiers(child)


def validate_dashboard_payload(payload: Mapping) -> None:
    missing = REQUIRED_SECTIONS - payload.keys()
    if missing:
        raise ValueError(f"Dashboard export is missing sections: {sorted(missing)}")
    _reject_identifiers(payload)

    metadata = payload["metadata"]
    if _sum(payload["daily_business_kpis"], "payment_count") != metadata[
        "source_payments"
    ]:
        raise ValueError("Dashboard payment totals do not reconcile")
    if _sum(payload["daily_fraud_kpis"], "total_decisions") != metadata[
        "source_decisions"
    ]:
        raise ValueError("Dashboard decision totals do not reconcile")
    if _sum(payload["customer_risk_segments"], "count") != metadata[
        "source_customers"
    ]:
        raise ValueError("Dashboard customer segments do not reconcile")
