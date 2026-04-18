-- ============================================================
-- STEP 00 · RAW UNION
-- Layer  : staging
-- Input  : orders_historical  (Jan 2021 – Jun 2023)
--          orders_2024_2025   (Jul 2023 – Feb 2025)
-- Output : raw.orders_union
-- Purpose: Combine both source files into a single table with
--          zero transformations.  Every original value is kept
--          exactly as it arrived.  A source_file lineage column
--          is added so downstream steps can trace each row back
--          to its origin.
-- ============================================================

CREATE OR REPLACE TABLE `analytical-engineer-project.staging.orders_raw_union` AS

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
  'historical' AS source_file           -- lineage tag
FROM `analytical-engineer-project.source_data.orders_historical`


UNION ALL

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
  '2024_2025' AS source_file
FROM `analytical-engineer-project.source_data.orders_2024_2025`
;
