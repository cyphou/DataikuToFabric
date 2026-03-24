-- Migrated from Dataiku visual recipe: join_customer_orders
-- Recipe type: Join
-- Target: T-SQL (Fabric Warehouse)

SELECT a.*, b.*
FROM [CRM_CUSTOMERS] a
LEFT JOIN [ERP_ORDERS] b
  ON a.[CUSTOMER_ID] = b.[CUSTOMER_ID]
