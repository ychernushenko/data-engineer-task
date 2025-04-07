CREATE TABLE IF NOT EXISTS advertiser (
    id UInt32,
    name String,
    updated_at DateTime,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY id