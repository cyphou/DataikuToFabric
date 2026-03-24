-- Migrated from Dataiku visual recipe: clean_customer_data
-- Recipe type: Prepare
-- Target: T-SQL (Fabric Warehouse)

-- SPLIT [full_address] on ',' — requires manual CHARINDEX/SUBSTRING
-- NOTE: Rename columns [cust_nm]→[customer_name], [eml_addr]→[email_address]
-- NOTE: Drop columns [internal_id] from result
SELECT *, UPPER([country_code]) AS [country_code], LOWER([email_address]) AS [email_address], ISNULL([phone_number], 'N/A') AS [phone_number], REPLACE([address_line_1], '  ', ' ') AS [address_line_1], CAST([zip_code] AS VARCHAR(10)) AS [zip_code], ([first_name] + ' ' + [last_name]) AS [full_name]
FROM [raw_customer_import]
WHERE [status] != 'DELETED'
