-- Migrated from Dataiku visual recipe: filter_high_value_customers
-- Recipe type: Filter
-- Target: T-SQL (Fabric Warehouse)

SELECT *
FROM [CUSTOMER_360_VIEW]
WHERE [STATUS_LABEL] = 'Active' AND [TOTAL_ORDERS] >= '5' AND [LIFETIME_REVENUE] > '1000'
