-- Migrated from Dataiku recipe: compute_product_metrics
-- Source: PostgreSQL (postgresql_dw)
-- Target: T-SQL (Fabric Warehouse)
-- Review flags: Contains INTERVAL literal — use DATEADD in T-SQL

/* Product performance metrics from PostgreSQL data warehouse */
WITH recent_orders AS (
  SELECT
    oi.product_id AS product_id,
    oi.quantity AS quantity,
    oi.unit_price AS unit_price,
    oi.quantity * oi.unit_price AS line_total,
    CAST(oi.order_date AS DATE) AS order_day,
    CAST(oi.discount_pct AS NUMERIC(5, 2)) AS discount_applied
  FROM order_items AS oi
  WHERE
    oi.order_date >= GETDATE() - INTERVAL '90' DAYS
), product_stats AS (
  SELECT
    product_id AS product_id,
    COUNT_BIG(*) AS order_count,
    SUM(quantity) AS total_units_sold,
    SUM(line_total) AS gross_revenue,
    AVG(line_total) AS avg_order_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
      CASE WHEN line_total IS NULL THEN 1 ELSE 0 END, line_total) AS median_order_value,
    MAX(order_day) AS last_order_date
  FROM recent_orders
  GROUP BY
    product_id
), inventory_current AS (
  SELECT
    product_id,
    quantity_on_hand,
    reorder_point,
    warehouse_location
  FROM (
    SELECT
      product_id AS product_id,
      quantity_on_hand AS quantity_on_hand,
      reorder_point AS reorder_point,
      warehouse_location AS warehouse_location,
      ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY CASE WHEN product_id IS NULL THEN 1 ELSE 0 END, product_id, CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END DESC, snapshot_date DESC
      ) AS _row_number
    FROM inventory_snapshots
  ) AS _t
  WHERE
    _row_number = 1
)
SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  CAST(p.unit_cost AS NUMERIC(10, 2)) AS unit_cost,
  COALESCE(ps.order_count, 0) AS orders_90d,
  COALESCE(ps.total_units_sold, 0) AS units_sold_90d,
  CAST(COALESCE(ps.gross_revenue, 0) AS NUMERIC(12, 2)) AS revenue_90d,
  CAST(COALESCE(ps.avg_order_value, 0) AS NUMERIC(10, 2)) AS avg_order_value,
  CAST(COALESCE(ps.median_order_value, 0) AS NUMERIC(10, 2)) AS median_order_value,
  COALESCE(ic.quantity_on_hand, 0) AS current_stock,
  COALESCE(ic.reorder_point, 0) AS reorder_point,
  ic.warehouse_location,
  CASE
    WHEN ic.quantity_on_hand <= ic.reorder_point
    THEN 'REORDER'
    WHEN ic.quantity_on_hand <= ic.reorder_point * 2
    THEN 'LOW'
    ELSE 'OK'
  END AS stock_status,
  p.tags + ARRAY('computed') AS enriched_tags,
  GETDATE() AS computed_at
FROM products_catalog AS p
LEFT JOIN product_stats AS ps
  USING (product_id)
LEFT JOIN inventory_current AS ic
  USING (product_id)
WHERE
  p.is_active = 1
  AND LOWER(p.product_name) LIKE LOWER('%widget%')
  OR LOWER(p.category) LIKE LOWER('%electronics%')
ORDER BY
  revenue_90d DESC