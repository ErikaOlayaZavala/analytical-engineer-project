-- ============================================================
-- STEP 01 · TYPE CASTING
-- Layer  : staging
-- Input  : staging.orders_raw_union
-- Output : staging.orders_typed
-- Purpose: Cast every column to its correct data type.
--          SAFE_CAST is used for all numeric and date columns
--          so that malformed values become NULL instead of
--          crashing the pipeline.  No business logic here –
--          only type enforcement.
-- ============================================================

CREATE OR REPLACE TABLE `analytical-engineer-project.staging.orders_typed` AS

SELECT
  -- Dates
  SAFE_CAST(operational_view_date AS DATE)          AS operational_view_date,

  -- String identifiers (kept as-is; trimming happens in step 02)
  user_uuid,
  customer_city,
  customer_country,
  order_uuid,
  parent_order_uuid,
  platform,
  incentive_promo_code,
  last_status,

  -- Numeric / financial columns
  SAFE_CAST(fx_rate_loc_to_usd_fxn    AS FLOAT64)  AS fx_rate_loc_to_usd_fxn,
  SAFE_CAST(list_price_operational     AS FLOAT64)  AS list_price_operational,
  SAFE_CAST(deal_discount_operational  AS FLOAT64)  AS deal_discount_operational,
  SAFE_CAST(gross_bookings_operational AS FLOAT64)  AS gross_bookings_operational,
  SAFE_CAST(margin_1_operational       AS FLOAT64)  AS margin_1_operational,
  SAFE_CAST(vfm_operational            AS FLOAT64)  AS vfm_operational,

  -- Lineage (pass-through from step 00)
  source_file

FROM `analytical-engineer-project.staging.orders_raw_union`
;
