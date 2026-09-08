from __future__ import annotations

from dataclasses import dataclass


RULES_VERSION = "rules-v1"


@dataclass(frozen=True)
class PaymentSignals:
    amount: float
    customer_average_amount: float
    is_new_device: bool
    ip_region: str
    home_region: str
    shipping_region: str
    login_failures: int
    attempt_number: int


@dataclass(frozen=True)
class RiskDecision:
    score: int
    action: str
    reason_codes: tuple[str, ...]


def score_payment(signals: PaymentSignals) -> RiskDecision:
    """Apply deterministic, explainable milestone-one payment rules."""
    score = 0
    reasons: list[str] = []

    baseline = max(signals.customer_average_amount, 1.0)
    if signals.amount >= max(250.0, baseline * 3.0):
        score += 30
        reasons.append("AMOUNT_OUTLIER")
    if signals.is_new_device:
        score += 20
        reasons.append("NEW_DEVICE")
    if signals.ip_region != signals.home_region:
        score += 20
        reasons.append("IP_HOME_MISMATCH")
    if signals.shipping_region != signals.home_region:
        score += 10
        reasons.append("SHIPPING_HOME_MISMATCH")
    if signals.login_failures >= 3:
        score += 20
        reasons.append("REPEATED_LOGIN_FAILURES")
    if signals.attempt_number >= 4:
        score += 15
        reasons.append("REPEATED_PAYMENT_ATTEMPTS")

    if score >= 60:
        action = "DECLINE"
    elif score >= 30:
        action = "REVIEW"
    else:
        action = "APPROVE"

    return RiskDecision(score=min(score, 100), action=action, reason_codes=tuple(reasons))
