# Orders Analytics Pipeline

Welcome to the **Orders Analytics Pipeline** repository! 🚀

This project delivers an end-to-end analytical engineering solution on BigQuery: raw CSV ingestion, a modular 5-step cleaning pipeline, and a customer-grain analytical model supporting retention, cohort, and profitability analysis.


## 🏗️ Architecture

<img width="1703" height="912" alt="data_architecture - analytics_engineering drawio" src="https://github.com/user-attachments/assets/90d7b47e-cd93-44d1-9ace-d5cd2025f058" />


| Resource | Link |
|---|---|
| BigQuery Project | [analytical-engineer-project](https://console.cloud.google.com/bigquery?project=analytical-engineer-project) |
| Looker Studio Dashboard | https://datastudio.google.com/reporting/6069a66f-2aeb-4216-aa70-f1c7c4f3e64f |

---

## 🗂️ Repository Structure

```
orders-analytics-pipeline/
├── pipeline/               # Steps 00–05: union → cast → standardise → clean → dedupe → publish
├── models/                 # step_06_master_customer.sql
├── analysis/               # q1_revenue_mix_retention.sql 
├── validation/             # validation_all.sql 
├── assets/                 # data_architecture.png
└── docs/                   # README.md · architecture.drawio
```

---

## Assignment 1 · Data Cleaning & Preparation

The pipeline was intentionally designed as five independent scripts rather than a single monolithic query. Each step has one responsibility, reads from the previous step's output, and can be rerun in isolation on failure.

**Key decisions:**
- **Deduplication** was included even though no duplicates existed in the source data. Boundary overlaps between incremental loads are a common production failure mode — having the logic in place avoids a late-stage incident.
- **Platform standardisation** was applied proactively. Inconsistent casing is one of the most frequent issues when new data sources are onboarded, and the cost of adding it now is near zero.
- **Native BigQuery SQL over dbt** — chosen to minimise toolchain complexity while maintaining full modularity. No additional dependencies, no new infrastructure.

All validation queries were executed manually after each step. The final output is a single clean, integrated table in the analytics layer with column-level descriptions embedded via `OPTIONS(description=...)`, visible natively in BigQuery and Looker.

---

## Assignment 2 · SQL Analysis

#### Part A — Master Customer Table

`analytics.master_customer` aggregates the event-level table to one row per `user_uuid` across five column groups: identity, cohort & lifecycle, activity, financials (USD), and retention signals.

**Validation approach:** beyond aggregate checks, a specific user with 11 transactions was traced end-to-end through the model to verify that customer grain, order counts, and financial totals were consistent with the raw event data. Record-level spot checks are a practical complement to automated validation.

#### Part B — Business Questions

Business logic is encapsulated in BigQuery views. Looker Studio connects to those views rather than raw tables — this keeps the dashboard consistent and maintainable as the underlying data evolves.

- **Q1 · Revenue Mix & Retention** — gross bookings by activation, reactivation, and retained regulars, tracked monthly across the full history.
- **Q2 · Platform Performance** — app vs web across average order value, purchase frequency, and gross profit per customer, plus platform share over time.

> 📊 [View the live dashboard]([https://lookerstudio.google.com/your-link-here](https://datastudio.google.com/reporting/6069a66f-2aeb-4216-aa70-f1c7c4f3e64f))

---

## Assignment 3 · Engineering Thinking

**Financial column conventions**
The `_operational` suffix flags columns that must be currency-normalised before any cross-country aggregation. Without applying `fx_rate_loc_to_usd_fxn`, financial metrics are summed across different local currencies and the result is meaningless — equivalent to adding euros, pounds, and dollars as the same unit.

**Inflated customer count**
If a stakeholder reports ~15% more customers than expected, I would investigate in this order: first, inspect the aggregation logic in `master_customer` and compare distinct `user_uuid` counts before and after grouping; second, check whether `analytics.orders` itself contains duplicates that should have been caught earlier in the pipeline; third, if both layers look clean, trace the issue back to the source files to identify whether the same customer appears under multiple identifiers at ingestion.

**Making the model trustworthy**
Trust is built incrementally and in collaboration with the team, not through documentation alone. Three concrete practices: define what constitutes a unique customer together with stakeholders and encode those rules as explicit SQL validation checks — creating a shared, traceable definition. Maintain a lightweight trend report showing how key metrics evolve month-over-month so anomalies surface before they reach stakeholders. Document every field with its valid values, business rule, and edge case behaviour directly in BigQuery via `OPTIONS(description=...)` so any analyst can understand the model without reverse-engineering the SQL.

---

## 📢 License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share it with proper attribution.

---

## 🙌 About Me

Hi! I'm **Erika Olaya**, a data professional passionate about building pipelines that are clean, well-documented, and trusted by the people who use them.

This is my submission for Analytical Engineer case study — feedback very welcome! 😊


