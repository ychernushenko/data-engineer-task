INSERT INTO advertiser_stats
SELECT
    a.id AS advertiser_id,
    a.name AS advertiser_name,
    COUNT(DISTINCT i.id) AS impressions,
    COUNT(DISTINCT cl.id) AS clicks
FROM advertiser a
LEFT JOIN campaign c ON a.id = c.advertiser_id
LEFT JOIN impressions i ON c.id = i.campaign_id
LEFT JOIN clicks cl ON c.id = cl.campaign_id
GROUP BY a.id, a.name;