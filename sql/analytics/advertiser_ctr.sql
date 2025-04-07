SELECT
    advertiser_id,
    advertiser_name,
    impressions,
    clicks,
    if(impressions > 0, least(1.0, clicks / impressions), 0.0) AS ctr
FROM advertiser_stats
WHERE impressions > 0
ORDER BY ctr DESC;