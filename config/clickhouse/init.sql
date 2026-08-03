CREATE DATABASE IF NOT EXISTS uba_analytics;
CREATE TABLE IF NOT EXISTS uba_analytics.user_events_enriched (
    event_id String,
    user_id String,
    username Nullable(String),
    email Nullable(String),
    country Nullable(String),
    city Nullable(String),
    device_type Nullable(String),
    is_premium Nullable(UInt8),
    session_id String,
    event_type String,
    page_url String,
    referrer Nullable(String),
    ip_address String,
    event_time DateTime,
    item_category Nullable(String),
    price Nullable(Float64),
    search_query Nullable(String),
    processed_at DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, event_type, user_id);