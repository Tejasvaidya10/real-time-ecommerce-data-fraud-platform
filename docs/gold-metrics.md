# Gold analytics metric catalog

Gold is the serving layer for BI, risk operations, and portfolio demonstrations. It is
rebuilt from current-state Silver Delta tables with a transactional overwrite, so every
successful refresh represents one internally consistent snapshot.

## Daily business KPIs

Grain: one row per payment event date and currency.

- `payment_count`: current payment transactions.
- `unique_customers`: distinct customers who paid.
- `active_sellers`: distinct sellers receiving payments.
- `gross_payment_value`: sum of payment amount before chargebacks.
- `average_payment_value`: mean payment amount.
- `chargeback_count` and `chargeback_amount`: confirmed delayed fraud outcomes.
- `chargeback_rate_pct`: chargebacks divided by payments, expressed as a percentage.

## Daily fraud KPIs

Grain: one row per payment event date and fraud-rules version.

- `total_decisions`: scored payment transactions.
- `approved_count`, `review_count`, and `declined_count`: operational action funnel.
- `average_risk_score`: mean explainable rules score.
- `review_amount` and `declined_amount`: payment value affected by risk controls.
- `confirmed_fraud_count` and `confirmed_fraud_amount`: later chargeback outcomes.
- `flagged_rate_pct`: review plus decline decisions divided by total decisions.
- `confirmed_fraud_rate_pct`: confirmed chargebacks divided by total decisions.

## Seller performance

Grain: one row per seller with at least one current payment.

This table combines transaction volume and GMV with distinct customers, average risk,
flagged decisions, confirmed chargebacks, and the most recent payment timestamp. It is
suited to seller-risk ranking and marketplace operations dashboards.

## Customer 360

Grain: one row per current customer, including customers without payments.

Order and payment lifetime value, latest activity, average risk, review/decline counts,
and confirmed chargebacks are joined to customer attributes. `customer_risk_segment` is
`HIGH` for a decline or confirmed chargeback, `MEDIUM` for a review or elevated average
risk, and `LOW` otherwise.

## Reconciliation gates

`make verify-pipeline` fails if any Gold grain is duplicated or if:

- business payment totals differ from Silver payments;
- fraud decision totals differ from Silver decisions;
- confirmed fraud differs from Silver chargebacks;
- customer or seller rollups do not reconcile to Silver payments; or
- customer 360 and seller performance do not contain the expected entities.
