-- ============================================================================
-- ROUTE 1 gold (non-aggregated) — enriched orders.
--
-- A MATERIALIZED VIEW that joins current-state orders to their customer's region.
-- The engine refreshes it INCREMENTALLY (Enzyme): when only a few silver rows
-- changed, only the affected output rows are recomputed — not the whole join.
-- This correctly reflects updates AND deletes flowing out of the SCD1 silver tables
-- (a plain streaming read of an updated/deleted source would fail; the MV handles it).
--
-- silver_orders / silver_customers are pipeline datasets => referenced by bare name.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_orders_enriched
  COMMENT "Orders enriched with customer region; incrementally maintained."
AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.status,
  o.amount,
  c.region,
  c.name AS customer_name
FROM silver_orders o
LEFT JOIN silver_customers c
  ON o.customer_id = c.customer_id;


-- ============================================================================
-- ROUTE 3 (declarative aggregate) — daily revenue by region as a MATERIALIZED VIEW.
--
-- This is the customer's own GROUP BY, wrapped in CREATE MATERIALIZED VIEW and nothing
-- else. No CDF read, no MERGE, no affected-key logic, no watermark to manage — the engine
-- (Enzyme) maintains it INCREMENTALLY from the base-table changes on refresh, provided the
-- base tables have CDF / row tracking (silver_orders has CDF on). It is the declarative
-- counterpart to the hand-written CDF+MERGE aggregate: same result, far less code, at the
-- cost of letting the engine decide when it can refresh incrementally vs. fully.
--
-- NOTE: materialized views are created/refreshed in Lakeflow or Databricks SQL — not from a
-- generic serverless notebook — which is why this route lives in the pipeline, not the
-- walkthrough notebook's Run-All path.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue_by_region_mv
  COMMENT "Daily revenue by region; declarative incremental aggregate (Enzyme-maintained)."
AS
SELECT
  c.region,
  o.order_date,
  SUM(o.amount) AS revenue,
  COUNT(*)      AS orders
FROM silver_orders o
JOIN silver_customers c
  ON o.customer_id = c.customer_id
GROUP BY c.region, o.order_date;


-- ============================================================================
-- GOLD via AUTO CDC — when gold is ROW-LEVEL current state or history (NOT an aggregate).
--
-- Use AUTO CDC into a gold STREAMING TABLE when the gold dataset itself represents row-level
-- current state or history sourced from a change feed:
--   * the source emits inserts, updates, and deletes,
--   * you need SCD Type 1 (current state) or Type 2 (history),
--   * you want low-latency, incremental, exactly-once-style processing,
--   * you want to preserve history and/or expose a downstream change data feed.
--
-- This is the SAME feature used for silver (lakeflow_silver_auto_cdc.sql) — the layer name
-- doesn't change the tool. It is the row-level counterpart to the aggregate gold above:
--   aggregate gold  -> Materialized View / CDF+MERGE  (SUM/COUNT can't be CDC-applied)
--   row-level gold  -> AUTO CDC streaming table       (SCD1/SCD2 current state or history)
--
-- Example: gold_orders_history — a business-ready SCD Type 2 record of every order state
-- change, with CDF exposed for downstream consumers (BI, reverse ETL, auditing). Sourced
-- from the bronze orders change feed; the engine applies I/U/D, orders by sequence, and
-- maintains the __START_AT / __END_AT validity windows.
--
-- NOTE: like all AUTO CDC, this runs in the Lakeflow pipeline, not a serverless notebook.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE gold_orders_history
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT "Gold SCD2 history of order state changes via Auto CDC; CDF exposed downstream.";

CREATE FLOW gold_orders_history_scd2 AS AUTO CDC INTO gold_orders_history
FROM STREAM(${demo.catalog}.${demo.schema}.bronze_orders_raw)
KEYS (order_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2;
