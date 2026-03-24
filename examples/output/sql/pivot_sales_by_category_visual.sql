-- Migrated from Dataiku visual recipe: pivot_sales_by_category
-- Recipe type: Pivot
-- Target: T-SQL (Fabric Warehouse)

SELECT [REGION], [Electronics], [Clothing], [Food], [Home]
FROM (
  SELECT [REGION], [PRODUCT_CATEGORY], [total_revenue]
  FROM [regional_sales_summary]
) src
PIVOT (
  SUM([total_revenue])
  FOR [PRODUCT_CATEGORY] IN ([Electronics], [Clothing], [Food], [Home])
) AS pvt
