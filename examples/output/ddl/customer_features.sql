-- Dataset: customer_features
-- Source type: Filesystem
-- Target: lakehouse

CREATE TABLE IF NOT EXISTS customer_features (
  [CUSTOMER_ID] BIGINT NULL,
  [FULL_NAME] STRING NULL,
  [EMAIL] STRING NULL,
  [TOTAL_ORDERS] INT NULL,
  [LIFETIME_REVENUE] DOUBLE NULL,
  [account_age_days] INT NULL,
  [avg_order_value] DOUBLE NULL,
  [orders_per_month] DOUBLE NULL,
  [rfm_score] INT NULL,
  [segment] STRING NULL
) USING DELTA
