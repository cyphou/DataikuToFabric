-- Migrated from Dataiku visual recipe: rank_customers_by_region
-- Recipe type: Window
-- Target: T-SQL (Fabric Warehouse)

SELECT *,
  RANK() OVER (PARTITION BY [REGION] ORDER BY [LIFETIME_REVENUE]) AS [revenue_rank]
FROM [CUSTOMER_360_VIEW]
