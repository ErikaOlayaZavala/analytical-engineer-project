-- ============================================================
-- STEP 04 · DEDUPLICATION
-- Layer  : staging
-- Input  : staging.orders_clean
-- Output : staging.orders_deduped
-- Purpose: Ensure each order_uuid appears exactly once.
--          Duplicates can occur when a row falls near the file
--          boundary (Jun/Jul 2023) or due to upstream pipeline
--          replays.
--          Strategy: keep the row from the historical file
--          first; if both rows come from the same file, keep
--          the one with the earliest operational_view_date.
-- ============================================================

CREATE OR REPLACE TABLE `analytical-engineer-project.staging.orders_deduped` AS

WITH ranked AS (

  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_uuid
      ORDER BY
        -- Prefer the historical file as the authoritative source
        CASE WHEN source_file = 'historical' THEN 0 ELSE 1 END,
        -- Tiebreak: earliest date
        operational_view_date ASC
    ) AS rn

  FROM `analytical-engineer-project.staging.orders_clean`

)

SELECT
  operational_view_date,
  user_uuid,
  customer_city,
  customer_country,
  order_uuid,
  parent_order_uuid,
  platform,
  fx_rate_loc_to_usd_fxn,
  list_price_operational,
  deal_discount_operational,
  gross_bookings_operational,
  margin_1_operational,
  vfm_operational,
  incentive_promo_code,
  last_status,
  -- Audit columns (carried forward)
  source_file,
  country_was_null,
  platform_unknown,
  platform_raw
FROM ranked
WHERE rn = 1
;
