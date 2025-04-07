CREATE TABLE IF NOT EXISTS clicks (
    id UInt32,
    campaign_id UInt32,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY id