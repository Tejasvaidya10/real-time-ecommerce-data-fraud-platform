const byId = (id) => document.getElementById(id);
const sum = (rows, field) => rows.reduce((total, row) => total + Number(row[field] || 0), 0);
const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const percent = (value) => `${Number(value || 0).toFixed(2)}%`;

function moneyFormatter(currency) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: currency || "USD",
    maximumFractionDigits: 0,
  });
}

function renderDashboard(data) {
  const business = data.daily_business_kpis || [];
  const fraud = data.daily_fraud_kpis || [];
  const sellers = data.seller_risk_watchlist || [];
  const riskSegments = data.customer_risk_segments || [];
  const metadata = data.metadata || {};
  const currencies = [...new Set(business.map((row) => row.currency))];
  const currency = currencies[0] || "USD";
  const money = moneyFormatter(currency);

  const payments = sum(business, "payment_count");
  const gmv = sum(business, "gross_payment_value");
  const currentCustomers = sum(riskSegments, "count");
  const activeSellers = Number(metadata.gold_row_counts?.seller_performance || 0);
  const chargebacks = sum(business, "chargeback_count");
  const approved = sum(fraud, "approved_count");
  const reviewed = sum(fraud, "review_count");
  const declined = sum(fraud, "declined_count");
  const decisions = sum(fraud, "total_decisions");
  const flaggedRate = decisions ? ((reviewed + declined) * 100) / decisions : 0;
  const chargebackRate = payments ? (chargebacks * 100) / payments : 0;

  byId("grossPaymentValue").textContent = money.format(gmv);
  byId("paymentCount").textContent = integer.format(payments);
  byId("uniqueCustomers").textContent = integer.format(currentCustomers);
  byId("activeSellers").textContent = integer.format(activeSellers);
  byId("flaggedRate").textContent = percent(flaggedRate);
  byId("chargebackRate").textContent = percent(chargebackRate);
  byId("chargebackCount").textContent = integer.format(chargebacks);
  byId("currencyContext").textContent = currencies.length === 1
    ? `${currency} across the current snapshot`
    : `${currencies.length} currencies combined`;

  byId("decisionTotal").textContent = integer.format(decisions);
  byId("approvedCount").textContent = integer.format(approved);
  byId("reviewCount").textContent = integer.format(reviewed);
  byId("declinedCount").textContent = integer.format(declined);
  byId("rulesVersion").textContent = fraud[0]?.rules_version || "—";
  const approveEnd = decisions ? (approved * 100) / decisions : 0;
  const reviewEnd = decisions ? approveEnd + (reviewed * 100) / decisions : approveEnd;
  const donut = byId("decisionDonut");
  donut.style.background = `conic-gradient(var(--green) 0 ${approveEnd}%, var(--amber) ${approveEnd}% ${reviewEnd}%, var(--red) ${reviewEnd}% 100%)`;
  donut.setAttribute("aria-label", `${approved} approved, ${reviewed} reviewed, and ${declined} declined fraud decisions`);

  byId("declinedAmount").textContent = money.format(sum(fraud, "declined_amount"));
  byId("reviewAmount").textContent = money.format(sum(fraud, "review_amount"));
  byId("confirmedFraudAmount").textContent = money.format(sum(fraud, "confirmed_fraud_amount"));
  const weightedRisk = decisions
    ? fraud.reduce(
      (total, row) => total + Number(row.average_risk_score || 0) * Number(row.total_decisions || 0),
      0,
    ) / decisions
    : 0;
  byId("averageRiskScore").textContent = weightedRisk.toFixed(2);

  byId("sellerRows").innerHTML = sellers.map((seller) => `
    <tr>
      <td>Seller ${integer.format(seller.seller_rank)}</td>
      <td>${integer.format(seller.transaction_count)}</td>
      <td>${money.format(Number(seller.gross_payment_value || 0))}</td>
      <td>${percent(seller.flagged_rate_pct)}</td>
      <td class="${Number(seller.confirmed_chargeback_count) ? "risk-number" : ""}">${integer.format(seller.confirmed_chargeback_count)}</td>
    </tr>
  `).join("");

  const segmentCounts = { LOW: 0, MEDIUM: 0, HIGH: 0 };
  riskSegments.forEach((row) => { segmentCounts[row.customer_risk_segment] = Number(row.count || 0); });
  byId("customerTotal").textContent = `${integer.format(currentCustomers)} customers`;
  byId("riskSegments").innerHTML = ["LOW", "MEDIUM", "HIGH"].map((name) => {
    const count = segmentCounts[name] || 0;
    const share = currentCustomers ? (count * 100) / currentCustomers : 0;
    return `<div class="segment ${name.toLowerCase()}">
      <div class="segment-head"><span>${name} risk</span><strong>${integer.format(count)}</strong></div>
      <div class="track" role="img" aria-label="${name} risk: ${count} customers"><i style="width:${share}%"></i></div>
    </div>`;
  }).join("");

  const refreshed = metadata.refreshed_at ? new Date(metadata.refreshed_at) : null;
  byId("refreshedAt").textContent = refreshed && !Number.isNaN(refreshed.valueOf())
    ? `Refreshed ${refreshed.toLocaleString()}`
    : "Refresh time unavailable";
  byId("bronzeEvents").textContent = `${integer.format(metadata.bronze_records || 0)} CDC events`;
  const quarantined = Number(metadata.quarantine_records || 0);
  byId("qualityStatus").textContent = quarantined === 0
    ? "0 quarantined records"
    : `${integer.format(quarantined)} quarantined records`;
}

async function loadDashboard() {
  try {
    const response = await fetch("data/dashboard.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`Dashboard data returned ${response.status}`);
    renderDashboard(await response.json());
  } catch (error) {
    byId("errorState").hidden = false;
    byId("refreshedAt").textContent = "Snapshot unavailable";
    console.error(error);
  }
}

loadDashboard();
