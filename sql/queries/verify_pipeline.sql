--B. Join tables and Check connection & relationship accuracy between tables in the database
SELECT
    pc.company_name,
    b.brand_name,
    cam.campaign_name,
    cr.creator_name,
    p.platform_name,
    pm.impressions,
    pm.likes
FROM performance_metrics pm
JOIN collaborations        col ON pm.collaboration_id = col.collaboration_id
JOIN campaigns             cam ON col.campaign_id     = cam.campaign_id
JOIN brands                b   ON cam.brand_id        = b.brand_id
JOIN parent_companies      pc  ON b.company_id        = pc.company_id
JOIN creators              cr  ON col.creator_id      = cr.creator_id
JOIN platforms             p   ON pm.platform_id      = p.platform_id
LIMIT 10;