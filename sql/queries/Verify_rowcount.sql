-- A. verify successful generation of data and row count
SELECT 'platforms'                 AS table_name, COUNT(*) AS row_count FROM platforms
UNION ALL SELECT 'parent_companies',              COUNT(*) FROM parent_companies
UNION ALL SELECT 'brands',                        COUNT(*) FROM brands
UNION ALL SELECT 'products',                      COUNT(*) FROM products
UNION ALL SELECT 'creators',                      COUNT(*) FROM creators
UNION ALL SELECT 'creator_platforms',             COUNT(*) FROM creator_platforms
UNION ALL SELECT 'campaigns',                     COUNT(*) FROM campaigns
UNION ALL SELECT 'collaborations',                COUNT(*) FROM collaborations
UNION ALL SELECT 'collaboration_platforms',       COUNT(*) FROM collaboration_platforms
UNION ALL SELECT 'performance_metrics',           COUNT(*) FROM performance_metrics
