INSERT INTO daily_stats
SELECT
    day,
    SUM(DISTINCT impressions) AS impressions,
    SUM(DISTINCT clicks) AS clicks
FROM (
    SELECT
        toDate(created_at) AS day,
        count() AS impressions,
        0 AS clicks
    FROM impressions
    GROUP BY day

    UNION ALL

    SELECT
        toDate(created_at) AS day,
        0 AS impressions,
        count() AS clicks
    FROM clicks
    GROUP BY day
)
GROUP BY day;