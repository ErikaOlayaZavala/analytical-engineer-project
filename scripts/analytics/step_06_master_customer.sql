-- ============================================================
-- ASSIGNMENT 2A · MASTER CUSTOMER TABLE
-- Output : analytics.master_customer
-- Grain  : one row per user_uuid
-- Purpose: Customer-level analytical foundation for retention,
--          cohort, and profitability analysis.
--
-- Column groups:
--   1. Identity        – who is the customer
--   2. Cohort & lifecycle – when did they join, where are they now
--   3. Activity        – how much and how often they buy
--   4. Financials (USD)– revenue and profitability in a single currency
--   5. Retention signals – reactivations, refunds, current segment
--
-- Dependencies: analytics.orders  (output of pipeline step 05)
--
-- Reactivation logic (as per assignment spec):
--   - First order ever          → activation
--   - Gap since previous order > 365 days → reactivation
--   - All other returning orders → retained regular
-- ============================================================

CREATE OR REPLACE TABLE `analytical-engineer-project.analytics.master_customer` AS

-- ── CTE 1: base orders ────────────────────────────────────────
-- Keep only non-refunded orders for activity/financial metrics.
-- Refunds are counted separately as a quality signal.
WITH orders AS (

  SELECT
    user_uuid,
    order_uuid,
    parent_order_uuid,
    operational_view_date,
    customer_city,
    customer_country,
    platform,
    gross_bookings_operational * fx_rate_loc_to_usd_fxn  AS gross_bookings_usd,
    margin_1_operational       * fx_rate_loc_to_usd_fxn  AS margin_1_usd,
    vfm_operational            * fx_rate_loc_to_usd_fxn  AS vfm_usd,
    incentive_promo_code,
    last_status
  FROM `analytical-engineer-project.analytics.orders`

),

-- ── CTE 2: order-level gaps for reactivation logic ────────────
-- For every order, compute how many days since the previous
-- order from the same customer (NULL on the first order).
order_gaps AS (

  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    last_status,
    gross_bookings_usd,
    margin_1_usd,
    vfm_usd,
    platform,
    incentive_promo_code,

    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid   -- order_uuid breaks ties deterministically
    ) AS prev_order_date,

    DATE_DIFF(
      operational_view_date,
      LAG(operational_view_date) OVER (
        PARTITION BY user_uuid
        ORDER BY operational_view_date, order_uuid
      ),
      DAY
    ) AS days_since_prev_order

  FROM orders

),

-- ── CTE 3: classify each order ────────────────────────────────
-- Three mutually exclusive categories per the spec.
order_classified AS (

  SELECT
    *,
    CASE
      WHEN prev_order_date IS NULL              THEN 'activation'
      WHEN days_since_prev_order > 365          THEN 'reactivation'
      ELSE                                           'retained'
    END AS order_type

  FROM order_gaps

),

-- ── CTE 4: identity columns ───────────────────────────────────
-- Most recent non-null city/country wins (LAST_VALUE over date).
-- Primary platform = the platform used most across all orders.
customer_identity AS (

  SELECT DISTINCT
    user_uuid,

    LAST_VALUE(customer_city IGNORE NULLS) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS customer_city,

    LAST_VALUE(customer_country IGNORE NULLS) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS customer_country

  FROM orders

),

-- ── CTE 5: platform preference ────────────────────────────────
-- First platform used (acquisition channel) and primary platform
-- (most frequent across all orders).
customer_platform AS (

  SELECT
    user_uuid,

    -- Platform used on very first order
    FIRST_VALUE(platform) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_platform,

    -- Most frequently used platform
    -- Uses a subquery-style aggregation via ARRAY trick in BigQuery
    -- (see aggregation CTE below for the final mode computation)
    platform

  FROM orders

),

platform_mode AS (

  -- Mode = platform with the highest order count per user
  SELECT
    user_uuid,
    platform AS primary_platform
  FROM (
    SELECT
      user_uuid,
      platform,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (PARTITION BY user_uuid ORDER BY COUNT(*) DESC, platform) AS rn
    FROM orders
    GROUP BY user_uuid, platform
  )
  WHERE rn = 1

),

first_platform AS (

  SELECT DISTINCT
    user_uuid,
    FIRST_VALUE(platform) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_platform
  FROM orders

),

-- ── CTE 6: financial aggregates ───────────────────────────────
-- All monetary values in USD.
-- Refunds (negative gross_bookings_usd) are included in totals
-- so net revenue is correctly reflected.
-- Promo usage: % of orders where a promo code was applied.
customer_financials AS (

  SELECT
    user_uuid,
    SUM(gross_bookings_usd)   AS gross_bookings_usd,
    SUM(margin_1_usd)         AS margin_1_usd,
    SUM(vfm_usd)              AS vfm_usd,
    AVG(
      CASE WHEN last_status != 'refunded' THEN gross_bookings_usd END
    )                         AS avg_order_value_usd,

    -- Promo usage rate: share of non-refunded orders with a promo code
    COUNTIF(incentive_promo_code IS NOT NULL AND last_status != 'refunded')
      / NULLIF(COUNTIF(last_status != 'refunded'), 0)
                              AS promo_usage_rate

  FROM orders
  GROUP BY user_uuid

),

-- ── CTE 7: activity & lifecycle aggregates ────────────────────
customer_activity AS (

  SELECT
    user_uuid,
    MIN(operational_view_date)                          AS first_order_date,
    MAX(operational_view_date)                          AS last_order_date,
    DATE_DIFF(CURRENT_DATE(), MAX(operational_view_date), DAY)
                                                        AS days_since_last_order,
    COUNT(*)                                            AS total_orders,
    COUNT(DISTINCT parent_order_uuid)                   AS total_sessions,

    -- Reactivation & refund counters (from classified orders)
    COUNTIF(last_status = 'refunded')                   AS total_refunds,

    -- Multi-platform flag
    COUNT(DISTINCT platform) > 1                        AS is_multi_platform

  FROM orders
  GROUP BY user_uuid

),

-- ── CTE 8: retention signals from classified orders ───────────
customer_retention AS (

  SELECT
    user_uuid,
    COUNTIF(order_type = 'reactivation') AS total_reactivations
  FROM order_classified
  GROUP BY user_uuid

),

-- ── CTE 9: FINAL ASSEMBLY ─────────────────────────────────────
assembled AS (

  SELECT
    -- ── 1. Identity ───────────────────────────────────────────
    id.user_uuid,
    id.customer_city,
    id.customer_country,
    pm.primary_platform,

    -- ── 2. Cohort & lifecycle ─────────────────────────────────
    ac.first_order_date,
    DATE_TRUNC(ac.first_order_date, MONTH)              AS cohort_month,
    ac.last_order_date,
    ac.days_since_last_order,

    -- ── 3. Activity ───────────────────────────────────────────
    ac.total_orders,
    ac.total_sessions,
    fp.first_platform,
    ac.is_multi_platform,

    -- ── 4. Financials (USD) ───────────────────────────────────
    ROUND(fin.gross_bookings_usd, 2)                    AS gross_bookings_usd,
    ROUND(fin.margin_1_usd, 2)                          AS margin_1_usd,
    ROUND(fin.vfm_usd, 2)                               AS vfm_usd,
    ROUND(fin.avg_order_value_usd, 2)                   AS avg_order_value_usd,
    ROUND(fin.promo_usage_rate, 4)                      AS promo_usage_rate,

    -- ── 5. Retention signals ──────────────────────────────────
    ac.total_refunds,
    ROUND(ac.total_refunds / NULLIF(ac.total_orders, 0), 4)
                                                        AS refund_rate,
    ret.total_reactivations,
    ac.days_since_last_order <= 365                     AS is_active_last_12m,

    -- ── 6. Derived customer segment ───────────────────────────
    -- Segments are mutually exclusive and priority-ordered:
    --   churned    → last order > 365 days ago
    --   new        → first order within the last 90 days
    --   reactivated→ came back after >365 days gap (at least once)
    --   regular    → active within 365 days, no reactivation
    CASE
      WHEN ac.days_since_last_order > 365  THEN 'churned'
      WHEN ac.first_order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                                           THEN 'new'
      WHEN ret.total_reactivations > 0     THEN 'reactivated'
      ELSE                                      'regular'
    END                                                 AS customer_segment

  FROM customer_identity  id
  JOIN customer_activity  ac  USING (user_uuid)
  JOIN customer_financials fin USING (user_uuid)
  JOIN customer_retention ret USING (user_uuid)
  JOIN platform_mode      pm  USING (user_uuid)
  JOIN first_platform     fp  USING (user_uuid)

)

SELECT * FROM assembled
;
