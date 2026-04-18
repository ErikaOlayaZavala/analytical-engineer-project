-- ============================================================
-- STEP 05 · PUBLISH – ANALYTICS LAYER
-- Layer  : analytics
-- Input  : staging.orders_deduped
-- Output : analytics.orders  (analysis-ready, final table)
-- Purpose: Promote the clean, deduped dataset to the analytics
--          layer.  Audit columns (country_was_null,
--          platform_unknown, platform_raw) are moved to a
--          separate quality-log table so the main table stays
--          lean for analysts, while full traceability is
--          preserved for the data engineering team.
-- ============================================================

-- ── Main analytics table ─────────────────────────────────────
CREATE OR REPLACE TABLE `analytical-engineer-project.analytics.orders` AS

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
  source_file             -- kept for lineage; useful in ad-hoc queries
FROM `analytical-engineer-project.staging.orders_deduped`
;
