# Orders Analytics Pipeline — Groupon Case Study

Welcome to the **Orders Analytics Pipeline** repository! 🚀

This project demonstrates an end-to-end analytical engineering solution built on **BigQuery**: from raw CSV ingestion and data quality resolution, through a modular cleaning pipeline, to a customer-grain analytical model ready for retention, cohort, and profitability analysis.

Designed as a case study for Groupon's Analytical Engineer interview process, it reflects industry best practices in data pipeline design, SQL modelling, and documentation.

> 🤖 **AI usage note:** Claude (Anthropic) was used throughout this project as a coding assistant. A full breakdown of what was AI-generated, what was reviewed, and what decisions were made independently is documented in [`docs/ai_usage.md`](docs/ai_usage.md).

---

## 🗂️ Repository Structure

```
orders-analytics-pipeline/
│
├── pipeline/                        # Assignment 1 · Data cleaning pipeline
│   ├── step_00_raw_union.sql        # Union of both source CSVs, zero transformations
│   ├── step_01_type_casting.sql     # SAFE_CAST all columns to correct types
│   ├── step_02_standardisation.sql  # Trim, casing, platform normalisation
│   ├── step_03_cleaning_missing_values.sql  # Resolve null customer_country
│   ├── step_04_deduplication.sql    # Remove duplicate order_uuid rows
│   └── step_05_publish_analytics.sql        # Publish to analytics layer with column docs
│
├── models/                          # Assignment 2A · Customer analytical model
│   └── assignment_2a_master_customer.sql
│
├── analysis/                        # Assignment 2B · Business questions
│   ├── q1_revenue_mix_retention.sql
│   └── q2_platform_performance.sql
│
├── validation/                      # Data quality checks
│   ├── data_quality_audit.sql       # Pre/post pipeline profiling
│   └── validation_financial_columns.sql     # 12 business-rule checks on financial fields
│
└── docs/
    ├── assignment1_summary.md       # Written summary: issues found and fixed
    ├── assignment2_interpretation.md
    ├── assignment3_written.md       # Written answers to engineering questions
    └── ai_usage.md                  # How AI tools were used at each stage
```

---

## 🏗️ Data Architecture

The pipeline follows a three-layer architecture on BigQuery:

```
Source CSVs
    │
    ▼
┌──────────────────────────────────────┐
│  raw layer                           │
│  Unmodified union of both files      │
│  Table: raw.orders_union             │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│  staging layer                       │
│  Type casting → Standardisation      │
│  → Cleaning → Deduplication          │
│  Tables: staging.orders_typed        │
│          staging.orders_standardised │
│          staging.orders_clean        │
│          staging.orders_deduped      │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│  analytics layer                     │
│  Analysis-ready tables               │
│  Tables: analytics.orders            │
│          analytics.master_customer   │
│          analytics.orders_quality_log│
└──────────────────────────────────────┘
```

Each pipeline step is a **self-contained script** with a single responsibility. If a step fails, only that step needs to be rerun — not the full pipeline.

---

## 📦 Data Sources

| File | Period | Rows |
|---|---|---|
| `orders_historical.csv` | Jan 2021 – Jun 2023 | — |
| `orders_2024_2025.csv` | Jul 2023 – Feb 2025 | — |

Both files share an identical schema and together form a single logical dataset. See [`docs/assignment1_summary.md`](docs/assignment1_summary.md) for a full description of the fields.

---

## 🔧 Assignment 1 · Data Cleaning & Preparation

**Objective:** Merge both files into a single analysis-ready dataset, resolve data quality issues, and standardise columns.

### Issues found and fixed

| # | Issue | Fix applied |
|---|---|---|
| 1 | Mixed-case `platform` values (`App`, `APP`, `web`) | Normalised to lowercase; unknown values flagged in quality log |
| 2 | Missing `customer_country` on a subset of rows | Three-level waterfall: original value → same-user backfill → city lookup |
| 3 | Leading/trailing whitespace on string columns | `TRIM()` applied at ingestion |
| 4 | Inconsistent city casing (`NEW YORK`, `new york`) | `INITCAP(LOWER(...))` applied |
| 5 | Duplicate `order_uuid` rows near file boundary | Deduplicated, keeping historical file as authoritative source |
| 6 | Numeric columns loaded as strings from CSV | `SAFE_CAST(...AS FLOAT64)` — failures become NULL and are surfaced in audit |

Full write-up: [`docs/assignment1_summary.md`](docs/assignment1_summary.md)

---

## 📊 Assignment 2 · SQL Analysis

### Part A — Master Customer Table

`analytics.master_customer` aggregates the event-level orders table to **one row per customer**, with five column groups designed to support the requested analyses out of the box:

| Group | Key columns |
|---|---|
| Identity | `user_uuid`, `customer_country`, `primary_platform` |
| Cohort & lifecycle | `first_order_date`, `cohort_month`, `last_order_date`, `days_since_last_order` |
| Activity | `total_orders`, `total_sessions`, `is_multi_platform` |
| Financials (USD) | `gross_bookings_usd`, `margin_1_usd`, `avg_order_value_usd`, `promo_usage_rate` |
| Retention signals | `customer_segment`, `total_reactivations`, `refund_rate`, `is_active_last_12m` |

Reactivation definition follows the assignment spec: any order where the gap since the previous order exceeds 365 days.

### Part B — Business Questions

**Q1 · Customer Revenue Mix & Retention**
Revenue segmented by activation, reactivation, and retained regulars — tracked monthly to show how the mix has shifted over the full dataset history.

**Q2 · Platform Performance & Strategy**
App vs web comparison across average order value, purchase frequency, and gross profit per customer, plus app share of gross bookings over time.

Full SQL and written interpretations: [`docs/assignment2_interpretation.md`](docs/assignment2_interpretation.md)

---

## ✅ Data Quality Framework

Beyond the pipeline cleaning steps, a dedicated validation layer runs **12 business-rule checks** on the financial columns:

| Check | Rule |
|---|---|
| F01 | `fx_rate` is positive and within plausible range |
| F02 | No NULLs on any financial column |
| F03–F04 | `list_price` and `deal_discount` are non-negative |
| F05 | Discount does not exceed list price |
| F06 | `gross_bookings = list_price − discount` (within tolerance) |
| F07–F08 | Sign of `gross_bookings` is consistent with `last_status` |
| F09 | `margin_1` and `vfm` are negative on refunded rows |
| F10–F11 | `vfm ≤ margin_1 ≤ gross_bookings` hierarchy is respected |
| F12 | FX rate is consistent within the same country and date |

All checks return zero rows on a clean dataset. A summary dashboard query gives a single-glance PASS/FAIL view across all checks.

---

## 🛠️ How to Run

### Prerequisites
- BigQuery project with datasets: `source`, `raw`, `staging`, `analytics`
- Both CSV files loaded into `source.orders_historical` and `source.orders_2024_2025`

### Full pipeline run
Execute scripts in order:

```
pipeline/step_00 → step_01 → step_02 → step_03 → step_04 → step_05
```

Then build the analytical model:

```
models/assignment_2a_master_customer.sql
```

### Partial rerun after a failure
Each step reads from the previous step's output. If step 03 fails, rerun from step 03 onwards — steps 00, 01, and 02 do not need to be rerun.

### Replace project references
All scripts use `your_project` as a placeholder. Replace with your actual BigQuery project ID before running.

---

## 📋 Assignment 3 · Engineering Thinking

Written answers to three data quality and engineering questions:

- Financial column conventions and USD conversion
- Investigating an inflated customer count
- Making a data model trustworthy for other analysts

Full answers: [`docs/assignment3_written.md`](docs/assignment3_written.md)

---

## 📢 License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share it with proper attribution.

---

## 🙌 About Me

Hi! I'm **Erika Olaya**, an analytically-minded data professional passionate about building pipelines that are clean, well-documented, and actually trusted by the people who use them.

This is my submission for Groupon's Analytical Engineer case study — feedback very welcome! 😊

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/your-profile)
