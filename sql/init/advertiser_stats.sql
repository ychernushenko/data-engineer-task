CREATE TABLE IF NOT EXISTS advertiser_stats (
    advertiser_id UInt32,
    advertiser_name String,
    impressions UInt64,
    clicks UInt64,
) ENGINE = MergeTree()
ORDER BY advertiser_id;