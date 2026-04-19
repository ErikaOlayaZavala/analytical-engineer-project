-- ============================================================
-- ASSIGNMENT 2B · QUESTION 1
-- Customer Revenue Mix & Retention
--
-- Business question:
--   What share of gross bookings in the last 6 months came from
--   newly activated customers, reactivated customers, and retained
--   regulars? How has this mix shifted over the available history?
--
-- Sources:
--   analytics.orders          → event-level, for monthly time series
--   analytics.master_customer → customer segment and first_order_date
--
-- Reactivation definitions (per assignment spec):
--   activation   → customer's very first order ever
--   reactivation → order where gap since previous order > 365 days
--   retained     → all other returning orders
-- ============================================================


-- ------------------------------------------------------------
-- VIEW 1 · Monthly revenue mix by order type
-- Joins event-level orders back to the classified order type
-- computed in master_customer's internal logic.
-- We re-derive order_type here at event level so the time series
-- reflects when each order occurred, not when the customer joined.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW `analytical-engineer-project.analytics.v_q1_monthly_revenue_mix` AS

WITH order_gaps AS (

  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    gross_bookings_operational * fx_rate_loc_to_usd_fxn AS gross_bookings_usd,
    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
    ) AS prev_order_date
  FROM `analytical-engineer-project.analytics.orders`
  WHERE last_status != 'refunded'

),

order_classified AS (

  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    gross_bookings_usd,
    CASE
      WHEN prev_order_date IS NULL                                          THEN 'activation'
      WHEN DATE_DIFF(operational_view_date, prev_order_date, DAY) > 365   THEN 'reactivation'
      ELSE                                                                       'retained'
    END AS order_type
  FROM order_gaps

)

SELECT
  DATE_TRUNC(operational_view_date, MONTH)        AS order_month,
  order_type,
  COUNT(DISTINCT user_uuid)                        AS customers,
  COUNT(*)                                         AS orders,
  ROUND(SUM(gross_bookings_usd), 2)                AS gross_bookings_usd,
  ROUND(
    SUM(gross_bookings_usd) * 100.0
    / SUM(SUM(gross_bookings_usd)) OVER (PARTITION BY DATE_TRUNC(operational_view_date, MONTH))
  , 2)                                             AS share_pct
FROM order_classified
GROUP BY 1, 2
ORDER BY 1, 2
;


-- ------------------------------------------------------------
-- VIEW 2 · Last 6 months summary (snapshot for the headline KPI)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW `analytical-engineer-project.analytics.v_q1_last6m_revenue_mix` AS

WITH order_gaps AS (

  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    gross_bookings_operational * fx_rate_loc_to_usd_fxn AS gross_bookings_usd,
    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
    ) AS prev_order_date
  FROM `analytical-engineer-project.analytics.orders`
  WHERE last_status != 'refunded'

),

order_classified AS (

  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    gross_bookings_usd,
    CASE
      WHEN prev_order_date IS NULL                                          THEN 'activation'
      WHEN DATE_DIFF(operational_view_date, prev_order_date, DAY) > 365   THEN 'reactivation'
      ELSE                                                                       'retained'
    END AS order_type
  FROM order_gaps

)

SELECT
  order_type,
  COUNT(DISTINCT user_uuid)                        AS customers,
  COUNT(*)                                         AS orders,
  ROUND(SUM(gross_bookings_usd), 2)                AS gross_bookings_usd,
  ROUND(
    SUM(gross_bookings_usd) * 100.0
    / SUM(SUM(gross_bookings_usd)) OVER ()
  , 2)                                             AS share_pct
FROM order_classified
WHERE operational_view_date >= DATE_SUB(
  (SELECT MAX(operational_view_date) FROM `analytical-engineer-project.analytics.orders`),
  INTERVAL 6 MONTH
)
GROUP BY 1
ORDER BY 3 DESC
;


-- ------------------------------------------------------------
-- QUICK CHECKS — run after creating the views
-- ------------------------------------------------------------

-- Check 1: share_pct should sum to 100 for each month
SELECT
  order_month,
  ROUND(SUM(share_pct), 1) AS total_share   -- should be 100.0
FROM `analytical-engineer-project.analytics.v_q1_monthly_revenue_mix`
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10
;

-- Check 2: last 6 months total share should be 100
SELECT
  ROUND(SUM(share_pct), 1) AS total_share   -- should be 100.0
FROM `analytical-engineer-project.analytics.v_q1_last6m_revenue_mix`
;
