SELECT
    day,
    impressions,
    clicks,
    if(impressions > 0, least(1.0, clicks / impressions), 0.0) AS ctr
FROM daily_stats
ORDER BY day;