-- ============================================================================
-- ROUTE 1 (declarative) — Auto CDC bronze -> silver, in Lakeflow SQL.
--
-- This is the whole CDC engine: dedupe-by-key, order-by-sequence, apply inserts/
-- updates/deletes. In Informatica you'd hand-build all of that; here it is a few
-- declarative lines. Compare with src/route1_cdc/streaming_silver_merge.py, which
-- does the SAME thing by hand in ~40 lines of Structured Streaming + MERGE.
--
-- ${demo.catalog} / ${demo.schema} are injected from the pipeline `configuration`.
-- Bronze tables are external (written by the generator) so they are fully qualified;
-- silver tables are pipeline datasets, referenced by bare name.
-- ============================================================================

-- ---- silver_customers : current state (SCD Type 1) --------------------------
CREATE OR REFRESH STREAMING TABLE silver_customers
  COMMENT "Current-state customers (SCD1) via Auto CDC.";

CREATE FLOW customers_scd1 AS AUTO CDC INTO silver_customers
FROM STREAM(${demo.catalog}.${demo.schema}.bronze_customers_raw)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 1;

-- ---- silver_customers_history : full history (SCD Type 2) -------------------
-- Same source, same keys — one extra word (TYPE 2) gives you slowly-changing
-- history with __START_AT / __END_AT validity columns maintained automatically.
CREATE OR REFRESH STREAMING TABLE silver_customers_history
  COMMENT "Customer history (SCD2) via Auto CDC.";

CREATE FLOW customers_scd2 AS AUTO CDC INTO silver_customers_history
FROM STREAM(${demo.catalog}.${demo.schema}.bronze_customers_raw)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2;

-- ---- silver_orders : current state (SCD Type 1), CDF ENABLED ----------------
-- CDF is turned on here so routes 2 & 3 can read row-level changes (table_changes /
-- readChangeFeed) and update gold incrementally.
CREATE OR REFRESH STREAMING TABLE silver_orders
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT "Current-state orders (SCD1) via Auto CDC. CDF on for incremental gold.";

CREATE FLOW orders_scd1 AS AUTO CDC INTO silver_orders
FROM STREAM(${demo.catalog}.${demo.schema}.bronze_orders_raw)
KEYS (order_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 1;
