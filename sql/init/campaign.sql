CREATE TABLE IF NOT EXISTS campaign (
    id UInt32,
    name String,
    bid Float32,
    budget Float32,
    start_date Date,
    end_date Date,
    advertiser_id UInt32,
    updated_at DateTime,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY id