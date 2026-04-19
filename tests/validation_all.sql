-- ============================================================
-- CONSOLIDATED VALIDATION SCRIPT
-- Project : analytical-engineer-project
--
-- Covers every layer of the pipeline in order:
--   SECTION 1 · staging.orders_raw_union
--   SECTION 2 · staging.orders_typed
--   SECTION 3 · staging.orders_standardised
--   SECTION 4 · staging.orders_clean
--   SECTION 5 · staging.orders_deduped
--   SECTION 6 · analytics.orders
--   SECTION 7 · analytics.master_customer
--   SECTION 8 · Financial business rules (12 checks)
--   SECTION 9 · End-to-end summary dashboard (PASS / FAIL)
--
-- How to use:
--   Run each section independently after its corresponding
--   pipeline step. Run Section 9 last for a full overview.
--   A clean dataset returns 0 violations on every check.
-- ============================================================


-- ============================================================
-- SECTION 1 · raw.orders_union
-- Expected: row count = sum of both source tables
-- ============================================================

SELECT
  source_file,
  COUNT(*)                   AS row_count,
  MIN(operational_view_date) AS min_date,
  MAX(operational_view_date) AS max_date
FROM `analytical-engineer-project.staging.orders_raw_union`
GROUP BY source_file
ORDER BY source_file
;


-- ============================================================
-- SECTION 2 · staging.orders_typed
-- Expected: zero rows where SAFE_CAST returned NULL
-- ============================================================

SELECT
  COUNTIF(operational_view_date IS NULL)        AS null_date,
  COUNTIF(fx_rate_loc_to_usd_fxn IS NULL)       AS null_fx_rate,
  COUNTIF(list_price_operational IS NULL)       AS null_list_price,
  COUNTIF(deal_discount_operational IS NULL)    AS null_deal_discount,
  COUNTIF(gross_bookings_operational IS NULL)   AS null_gross_bookings,
  COUNTIF(margin_1_operational IS NULL)         AS null_margin_1,
  COUNTIF(vfm_operational IS NULL)              AS null_vfm,
  COUNT(*)                                      AS total_rows
FROM `analytical-engineer-project.staging.orders_typed`
;


-- ============================================================
-- SECTION 3 · staging.orders_standardised
-- Expected: platform values only app / web / touch / NULL
-- Expected: last_status values only redeemed / unredeemed / refunded / expired
-- ============================================================

-- Platform distribution after standardisation
SELECT
  platform,
  platform_unknown,
  COUNT(*) AS row_count
FROM `analytical-engineer-project.staging.orders_standardised`
GROUP BY 1, 2
ORDER BY 3 DESC
;

-- last_status distribution
SELECT
  last_status,
  COUNT(*) AS row_count
FROM `analytical-engineer-project.staging.orders_standardised`
GROUP BY 1
ORDER BY 2 DESC
;


-- ============================================================
-- SECTION 4 · staging.orders_clean
-- Expected: country resolution breakdown — most nulls filled
-- ============================================================

SELECT
  CASE
    WHEN country_was_null = FALSE                          THEN 'a · original value'
    WHEN country_was_null = TRUE AND customer_country IS NOT NULL THEN 'b · backfilled'
    ELSE                                                        'c · still null'
  END                        AS country_resolution,
  COUNT(*)                   AS row_count
FROM `analytical-engineer-project.staging.orders_clean`
GROUP BY 1
ORDER BY 1
;


-- ============================================================
-- SECTION 5 · staging.orders_deduped
-- Expected: zero duplicate order_uuid values
-- ============================================================

SELECT
  'before dedup' AS stage,
  COUNT(*)                   AS total_rows,
  COUNT(DISTINCT order_uuid) AS distinct_orders,
  COUNT(*) - COUNT(DISTINCT order_uuid) AS duplicates_removed
FROM `analytical-engineer-project.staging.orders_clean`

UNION ALL

SELECT
  'after dedup',
  COUNT(*),
  COUNT(DISTINCT order_uuid),
  COUNT(*) - COUNT(DISTINCT order_uuid)
FROM `analytical-engineer-project.staging.orders_deduped`
;


-- ============================================================
-- SECTION 6 · analytics.orders
-- Expected: full date range, no nulls on key columns
-- ============================================================

SELECT
  COUNT(*)                          AS total_rows,
  COUNT(DISTINCT user_uuid)         AS distinct_users,
  COUNT(DISTINCT order_uuid)        AS distinct_orders,
  MIN(operational_view_date)        AS min_date,
  MAX(operational_view_date)        AS max_date,
  COUNTIF(customer_country IS NULL) AS null_country,
  COUNTIF(platform IS NULL)         AS null_platform
FROM `analytical-engineer-project.analytics.orders`
;


-- ============================================================
-- SECTION 7 · analytics.master_customer
-- Expected: one row per user_uuid, no nulls on key fields
-- ============================================================

-- Row count and basic sanity
SELECT
  COUNT(*)                            AS total_customers,
  COUNT(DISTINCT user_uuid)           AS distinct_uuids,   -- must equal total_customers
  COUNTIF(gross_bookings_usd IS NULL) AS null_bookings,
  COUNTIF(first_order_date IS NULL)   AS null_first_date,
  COUNTIF(customer_segment IS NULL)   AS null_segment,
  MIN(first_order_date)               AS earliest_customer,
  MAX(first_order_date)               AS latest_customer
FROM `analytical-engineer-project.analytics.master_customer`
;

-- Segment distribution
SELECT
  customer_segment,
  COUNT(*) AS customers,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM `analytical-engineer-project.analytics.master_customer`
GROUP BY 1
ORDER BY 2 DESC
;

-- Financials sanity
SELECT
  MIN(total_orders)          AS min_orders,
  MAX(total_orders)          AS max_orders,
  ROUND(AVG(total_orders), 2) AS avg_orders,
  MIN(gross_bookings_usd)    AS min_bookings_usd,
  MAX(gross_bookings_usd)    AS max_bookings_usd
FROM `analytical-engineer-project.analytics.master_customer`
;


-- ============================================================
-- SECTION 8 · Financial business rules on analytics.orders
-- Each check returns rows only when a violation exists.
-- A clean dataset returns zero rows on every check.
-- ============================================================

-- F01 · fx_rate must be positive and within plausible range
SELECT 'F01 - fx_rate out of range' AS check_name, order_uuid,
  customer_country, fx_rate_loc_to_usd_fxn AS failing_value
FROM `analytical-engineer-project.analytics.orders`
WHERE fx_rate_loc_to_usd_fxn <= 0 OR fx_rate_loc_to_usd_fxn > 10000
;

-- F02 · No NULLs on any financial column
SELECT 'F02 - NULL financial column' AS check_name, order_uuid,
  CASE
    WHEN fx_rate_loc_to_usd_fxn IS NULL    THEN 'fx_rate_loc_to_usd_fxn'
    WHEN list_price_operational IS NULL    THEN 'list_price_operational'
    WHEN deal_discount_operational IS NULL THEN 'deal_discount_operational'
    WHEN gross_bookings_operational IS NULL THEN 'gross_bookings_operational'
    WHEN margin_1_operational IS NULL      THEN 'margin_1_operational'
    WHEN vfm_operational IS NULL           THEN 'vfm_operational'
  END AS failing_column
FROM `analytical-engineer-project.analytics.orders`
WHERE fx_rate_loc_to_usd_fxn IS NULL OR list_price_operational IS NULL
   OR deal_discount_operational IS NULL OR gross_bookings_operational IS NULL
   OR margin_1_operational IS NULL OR vfm_operational IS NULL
;

-- F03 · list_price must be >= 0
SELECT 'F03 - negative list_price' AS check_name,
  order_uuid, list_price_operational AS failing_value
FROM `analytical-engineer-project.analytics.orders`
WHERE list_price_operational < 0
;

-- F04 · deal_discount must be >= 0
SELECT 'F04 - negative discount' AS check_name,
  order_uuid, deal_discount_operational AS failing_value
FROM `analytical-engineer-project.analytics.orders`
WHERE deal_discount_operational < 0
;

-- F05 · discount cannot exceed list_price
SELECT 'F05 - discount exceeds list_price' AS check_name,
  order_uuid, list_price_operational, deal_discount_operational
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status != 'refunded'
  AND deal_discount_operational > list_price_operational + 0.01
;

-- F06 · gross_bookings = list_price - discount (within tolerance)
SELECT 'F06 - gross_bookings formula mismatch' AS check_name,
  order_uuid, list_price_operational, deal_discount_operational,
  gross_bookings_operational,
  list_price_operational - deal_discount_operational AS expected_gross
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status != 'refunded'
  AND ABS(gross_bookings_operational - (list_price_operational - deal_discount_operational))
      > GREATEST(0.10, 0.01 * ABS(list_price_operational))
;

-- F07 · refunded rows must have negative gross_bookings
SELECT 'F07 - refund with non-negative gross_bookings' AS check_name,
  order_uuid, gross_bookings_operational
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status = 'refunded' AND gross_bookings_operational >= 0
;

-- F08 · non-refunded rows must have non-negative gross_bookings
SELECT 'F08 - non-refund with negative gross_bookings' AS check_name,
  order_uuid, last_status, gross_bookings_operational
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status != 'refunded' AND gross_bookings_operational < 0
;

-- F09 · margin_1 and vfm must be negative on refunded rows
SELECT 'F09 - positive margin/vfm on refunded row' AS check_name,
  order_uuid, margin_1_operational, vfm_operational
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status = 'refunded'
  AND (margin_1_operational > 0 OR vfm_operational > 0)
;

-- F10 · margin_1 cannot exceed gross_bookings
SELECT 'F10 - margin_1 exceeds gross_bookings' AS check_name,
  order_uuid, gross_bookings_operational, margin_1_operational
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status != 'refunded'
  AND margin_1_operational > gross_bookings_operational + 0.01
;

-- F11 · vfm cannot exceed margin_1
SELECT 'F11 - vfm exceeds margin_1' AS check_name,
  order_uuid, margin_1_operational, vfm_operational
FROM `analytical-engineer-project.analytics.orders`
WHERE last_status != 'refunded'
  AND vfm_operational > margin_1_operational + 0.01
;

-- F12 · FX rate consistent within same country and date
SELECT 'F12 - inconsistent fx_rate for same country + date' AS check_name,
  customer_country, operational_view_date,
  COUNT(DISTINCT ROUND(fx_rate_loc_to_usd_fxn, 6)) AS distinct_rates,
  MIN(fx_rate_loc_to_usd_fxn) AS min_rate,
  MAX(fx_rate_loc_to_usd_fxn) AS max_rate
FROM `analytical-engineer-project.analytics.orders`
WHERE customer_country IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(DISTINCT ROUND(fx_rate_loc_to_usd_fxn, 6)) > 1
;


-- ============================================================
-- SECTION 9 · END-TO-END SUMMARY DASHBOARD
-- Run this last. All counts should be 0 in a clean dataset.
-- ============================================================

WITH checks AS (

  SELECT 'S1 · raw union: missing source_file'             AS check_name,
    COUNTIF(source_file IS NULL) AS violations
  FROM `analytical-engineer-project.raw.orders_union`

  UNION ALL SELECT 'S2 · typed: NULL from SAFE_CAST',
    COUNTIF(fx_rate_loc_to_usd_fxn IS NULL OR list_price_operational IS NULL
         OR deal_discount_operational IS NULL OR gross_bookings_operational IS NULL
         OR margin_1_operational IS NULL OR vfm_operational IS NULL)
  FROM `analytical-engineer-project.staging.orders_typed`

  UNION ALL SELECT 'S3 · standardised: unknown platform values',
    COUNTIF(platform_unknown = TRUE)
  FROM `analytical-engineer-project.staging.orders_standardised`

  UNION ALL SELECT 'S4 · clean: country still null after backfill',
    COUNTIF(country_was_null = TRUE AND customer_country IS NULL)
  FROM `analytical-engineer-project.staging.orders_clean`

  UNION ALL SELECT 'S5 · deduped: duplicate order_uuid remaining',
    COUNT(*) - COUNT(DISTINCT order_uuid)
  FROM `analytical-engineer-project.staging.orders_deduped`

  UNION ALL SELECT 'S6 · analytics.orders: null platform',
    COUNTIF(platform IS NULL)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'S7 · master_customer: duplicate user_uuid',
    COUNT(*) - COUNT(DISTINCT user_uuid)
  FROM `analytical-engineer-project.analytics.master_customer`

  UNION ALL SELECT 'S7 · master_customer: null segment',
    COUNTIF(customer_segment IS NULL)
  FROM `analytical-engineer-project.analytics.master_customer`

  UNION ALL SELECT 'F01 · fx_rate out of range',
    COUNTIF(fx_rate_loc_to_usd_fxn <= 0 OR fx_rate_loc_to_usd_fxn > 10000)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F02 · NULL financial column',
    COUNTIF(fx_rate_loc_to_usd_fxn IS NULL OR list_price_operational IS NULL
         OR deal_discount_operational IS NULL OR gross_bookings_operational IS NULL
         OR margin_1_operational IS NULL OR vfm_operational IS NULL)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F03 · negative list_price',
    COUNTIF(list_price_operational < 0)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F04 · negative discount',
    COUNTIF(deal_discount_operational < 0)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F05 · discount exceeds list_price',
    COUNTIF(last_status != 'refunded'
      AND deal_discount_operational > list_price_operational + 0.01)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F06 · gross_bookings formula mismatch',
    COUNTIF(last_status != 'refunded'
      AND ABS(gross_bookings_operational - (list_price_operational - deal_discount_operational))
          > GREATEST(0.10, 0.01 * ABS(list_price_operational)))
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F07 · refund with non-negative gross_bookings',
    COUNTIF(last_status = 'refunded' AND gross_bookings_operational >= 0)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F08 · non-refund with negative gross_bookings',
    COUNTIF(last_status != 'refunded' AND gross_bookings_operational < 0)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F09 · positive margin/vfm on refunded row',
    COUNTIF(last_status = 'refunded'
      AND (margin_1_operational > 0 OR vfm_operational > 0))
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F10 · margin_1 exceeds gross_bookings',
    COUNTIF(last_status != 'refunded'
      AND margin_1_operational > gross_bookings_operational + 0.01)
  FROM `analytical-engineer-project.analytics.orders`

  UNION ALL SELECT 'F11 · vfm exceeds margin_1',
    COUNTIF(last_status != 'refunded'
      AND vfm_operational > margin_1_operational + 0.01)
  FROM `analytical-engineer-project.analytics.orders`

)

SELECT
  check_name,
  violations,
  CASE WHEN violations = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM checks
ORDER BY check_name
;
