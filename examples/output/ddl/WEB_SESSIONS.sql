-- Dataset: WEB_SESSIONS
-- Source type: Filesystem
-- Target: lakehouse

CREATE TABLE IF NOT EXISTS WEB_SESSIONS (
  [session_id] STRING NULL,
  [USER_ID] BIGINT NULL,
  [SESSION_DATE] DATE NULL,
  [page_views] INT NULL,
  [duration_seconds] INT NULL,
  [device_type] STRING NULL,
  [browser] STRING NULL,
  [referrer_url] STRING NULL,
  [is_converted] BOOLEAN NULL
) USING DELTA
PARTITIONED BY (SESSION_DATE)
