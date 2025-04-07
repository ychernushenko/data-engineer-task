INSERT INTO campaign_stats
SELECT
    c.id AS campaign_id,
    c.name AS campaign_name,
    countDistinct(i.id) AS impressions,
    countDistinct(cl.id) AS clicks
FROM campaign c
LEFT JOIN impressions i ON c.id = i.campaign_id
LEFT JOIN clicks cl ON c.id = cl.campaign_id
GROUP BY c.id, c.name;