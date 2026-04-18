-- ============================================================
-- STEP 02 · STANDARDISATION
-- Layer  : staging
-- Input  : staging.orders_typed
-- Output : staging.orders_standardised
-- Purpose: Apply formatting rules to string columns so that
--          values are consistent across the whole dataset.
--          Three sub-tasks in this step:
--            a) Trim whitespace from all string fields
--            b) Normalise platform to lowercase allowed values
--            c) Normalise customer_city to title-case
--          No nulls are filled here – that is step 03.
-- ============================================================

CREATE OR REPLACE TABLE `analytical-engineer-project.staging.orders_standardised` AS

SELECT
  -- ── Dates & numerics pass through unchanged ─────────────
  operational_view_date,
  fx_rate_loc_to_usd_fxn,
  list_price_operational,
  deal_discount_operational,
  gross_bookings_operational,
  margin_1_operational,
  vfm_operational,

  -- ── Identifiers: trim only ───────────────────────────────
  TRIM(user_uuid)            AS user_uuid,
  TRIM(order_uuid)           AS order_uuid,
  TRIM(parent_order_uuid)    AS parent_order_uuid,

  -- ── customer_city: trim + title-case ────────────────────
  INITCAP(LOWER(TRIM(customer_city)))    AS customer_city,

  -- ── customer_country: trim + uppercase ISO-2 ────────────
  -- ISO codes must be uppercase (GB, US, DE …)
  UPPER(TRIM(customer_country))          AS customer_country,

  -- ── platform: map all variants to canonical lowercase ───
  -- Allowed output values: app | web | touch
  -- Unknown values → NULL, flagged via platform_unknown
  CASE
    WHEN LOWER(TRIM(platform)) IN ('app', 'web', 'touch')  THEN LOWER(TRIM(platform))
    WHEN LOWER(TRIM(platform)) IN ('mobile', 'mobile app') THEN 'app'
    WHEN LOWER(TRIM(platform)) IN ('desktop')              THEN 'web'
    ELSE NULL
  END                                     AS platform,

  -- Audit flag: TRUE when the original value was not recognised
  CASE
    WHEN LOWER(TRIM(platform)) NOT IN ('app', 'web', 'touch', 'mobile', 'mobile app', 'desktop')
    THEN TRUE
    ELSE FALSE
  END                                     AS platform_unknown,

  -- Keep original platform value for auditability
  TRIM(platform)                          AS platform_raw,

  -- ── Promo code: trim, empty string → NULL ───────────────
  NULLIF(TRIM(incentive_promo_code), '')  AS incentive_promo_code,

  -- ── last_status: trim + lowercase ───────────────────────
  LOWER(TRIM(last_status))               AS last_status,

  -- ── Lineage pass-through ─────────────────────────────────
  source_file

FROM `analytical-engineer-project.staging.orders_typed`
;
