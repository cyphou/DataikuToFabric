-- Dataset: products_catalog
-- Source type: PostgreSQL
-- Target: warehouse

CREATE TABLE [products_catalog] (
  [product_id] INT NULL,
  [product_name] NVARCHAR(200) NULL,
  [category] NVARCHAR(50) NULL,
  [subcategory] NVARCHAR(50) NULL,
  [brand] NVARCHAR(100) NULL,
  [unit_cost] DECIMAL(10,2) NULL,
  [unit_price] DECIMAL(10,2) NULL,
  [is_active] BIT NULL,
  [tags] NVARCHAR(MAX) NULL,
  [attributes] NVARCHAR(MAX) NULL,
  [created_at] DATETIME2 NULL,
  [updated_at] DATETIME2 NULL
)
