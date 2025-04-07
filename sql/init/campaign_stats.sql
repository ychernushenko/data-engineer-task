CREATE TABLE IF NOT EXISTS campaign_stats (
    campaign_id UInt32,
    campaign_name String,
    impressions UInt64,
    clicks UInt64,
) ENGINE = MergeTree()
ORDER BY campaign_id;