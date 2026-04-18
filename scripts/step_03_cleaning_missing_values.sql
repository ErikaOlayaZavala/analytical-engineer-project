-- ============================================================
-- STEP 03 · CLEANING – MISSING VALUES
-- Layer  : staging
-- Input  : staging.orders_standardised
-- Output : staging.orders_clean
-- Purpose: Resolve NULL values.  Only customer_country has a
--          documented null problem; this step applies a
--          three-level resolution waterfall:
--
--   Level 1 – Keep original value (already present).
--   Level 2 – Backfill from the same user_uuid:
--              for users with ≥1 row that has a country,
--              apply that country to rows where it is missing.
--              Safe because user_uuid is stable across sessions.
--   Level 3 – City lookup:
--              match customer_city against a curated table of
--              cities that belong unambiguously to one country.
--              Extend the lookup as new cities are discovered.
--   Fallback – Leave NULL and flag with country_was_null = TRUE
--              so analysts can handle them explicitly.
--
-- No deduplication in this step – that is step 04.
-- ============================================================

CREATE OR REPLACE TABLE `analytical-engineer-project.staging.orders_clean` AS

-- ── Level 2 helper: one country per user (most recent known) ─
WITH country_per_user AS (

  SELECT DISTINCT
    user_uuid,
    LAST_VALUE(customer_country IGNORE NULLS) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS inferred_country
  FROM `analytical-engineer-project.staging.orders_standardised`
  WHERE customer_country IS NOT NULL

),

-- ── Level 3 helper: unambiguous city → ISO-2 lookup ──────────
-- Add rows as you discover new cities in the data.
-- Do NOT add cities that exist in multiple countries.
city_country_lookup AS (

  SELECT city, country
  FROM UNNEST([
    STRUCT('New York'     AS city, 'US' AS country),
    STRUCT('Los Angeles'  AS city, 'US'),
    STRUCT('Chicago'      AS city, 'US'),
    STRUCT('Houston'      AS city, 'US'),
    STRUCT('London'       AS city, 'GB'),
    STRUCT('Manchester'   AS city, 'GB'),
    STRUCT('Birmingham'   AS city, 'GB'),
    STRUCT('Berlin'       AS city, 'DE'),
    STRUCT('Munich'       AS city, 'DE'),
    STRUCT('Hamburg'      AS city, 'DE'),
    STRUCT('Paris'        AS city, 'FR'),
    STRUCT('Lyon'         AS city, 'FR'),
    STRUCT('Madrid'       AS city, 'ES'),
    STRUCT('Barcelona'    AS city, 'ES'),
    STRUCT('Rome'         AS city, 'IT'),
    STRUCT('Milan'        AS city, 'IT'),
    STRUCT('Amsterdam'    AS city, 'NL'),
    STRUCT('Rotterdam'    AS city, 'NL'),
    STRUCT('Sydney'       AS city, 'AU'),
    STRUCT('Melbourne'    AS city, 'AU'),
    STRUCT('Toronto'      AS city, 'CA'),
    STRUCT('Vancouver'    AS city, 'CA')
  ])

),

-- ── Apply waterfall ───────────────────────────────────────────
resolved AS (

  SELECT
    s.*,

    -- Flag rows where country was originally missing
    CASE
      WHEN s.customer_country IS NULL THEN TRUE
      ELSE FALSE
    END AS country_was_null,

    -- Resolution waterfall
    COALESCE(
      s.customer_country,           -- Level 1: original value
      cpu.inferred_country,         -- Level 2: same-user backfill
      ccl.country                   -- Level 3: city lookup
    ) AS customer_country_resolved

  FROM `analytical-engineer-project.staging.orders_standardised` s
  LEFT JOIN country_per_user cpu
         ON s.user_uuid = cpu.user_uuid
  LEFT JOIN city_country_lookup ccl
         ON s.customer_city = ccl.city   -- city is already title-cased (step 02)

)

-- ── Final output: replace customer_country with resolved value ─
SELECT
  operational_view_date,
  user_uuid,
  customer_city,
  customer_country_resolved          AS customer_country,
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
  -- Audit columns
  source_file,
  country_was_null,
  platform_unknown,
  platform_raw
FROM resolved
;
