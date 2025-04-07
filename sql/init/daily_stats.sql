CREATE TABLE IF NOT EXISTS daily_stats (
    day Date,
    impressions UInt64,
    clicks UInt64,
) ENGINE = MergeTree()
ORDER BY day;