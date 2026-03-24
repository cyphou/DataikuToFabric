-- Migrated from Dataiku visual recipe: aggregate_regional_sales
-- Recipe type: Group By
-- Target: T-SQL (Fabric Warehouse)

SELECT [REGION], [PRODUCT_CATEGORY], SUM([TOTAL_AMOUNT]) AS [total_revenue], AVG([TOTAL_AMOUNT]) AS [avg_order_value], COUNT([ORDER_ID]) AS [order_count], MAX([TOTAL_AMOUNT]) AS [max_order], MIN([TOTAL_AMOUNT]) AS [min_order]
FROM [customer_orders_joined]
GROUP BY [REGION], [PRODUCT_CATEGORY]
