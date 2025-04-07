SELECT
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    if(impressions > 0, least(1.0, clicks / impressions), 0.0) AS ctr
FROM campaign_stats
ORDER BY campaign_id;