-- check leftover performance metrics after cleaning
SELECT 'performance_metrics' AS table_name, COUNT(*) AS row_count 
FROM performance_metrics;

-- Check duplicate creator names
SELECT creator_name, COUNT(*) 
FROM creators
GROUP BY creator_name
HAVING COUNT(*) > 1;

-- Check duplicate brand names within same company
SELECT brand_name, company_id, COUNT(*)
FROM brands
GROUP BY brand_name, company_id
HAVING COUNT(*) > 1;

-- Check duplicate campaign names within same brand
SELECT campaign_name, brand_id, COUNT(*)
FROM campaigns
GROUP BY campaign_name, brand_id
HAVING COUNT(*) > 1;

-- Check duplicate company names
SELECT company_name, COUNT(*)
FROM parent_companies
GROUP BY company_name
HAVING COUNT(*) > 1;

-- See both Johnson and Sons rows
SELECT * FROM parent_companies
WHERE company_name = 'Johnson and Sons';

-- See the duplicate creators
SELECT * FROM creators
WHERE creator_name IN (
    SELECT creator_name
    FROM creators
    GROUP BY creator_name
    HAVING COUNT(*) > 1
)
ORDER BY creator_name;


-- Check if company_id 3 or 87 have brands attached
SELECT company_id, COUNT(*) as brand_count
FROM brands
WHERE company_id IN (3, 87)
GROUP BY company_id;

-- Check if duplicate creators have collaborations attached
SELECT creator_id, COUNT(*) as collab_count
FROM collaborations
WHERE creator_id IN (58, 222, 23, 100, 76, 433)
GROUP BY creator_id;

-- See the brands attached to each Johnson and Sons
SELECT b.brand_id, b.brand_name, b.company_id, b.industry
FROM brands b
WHERE b.company_id IN (3, 87);

-- See full details of duplicate creators and their collab counts
SELECT c.creator_id, c.creator_name, c.niche, c.follower_count,
       COUNT(col.collaboration_id) as collab_count
FROM creators c
LEFT JOIN collaborations col ON c.creator_id = col.creator_id
WHERE c.creator_id IN (58, 222, 23, 100, 76, 433)
GROUP BY c.creator_id, c.creator_name, c.niche, c.follower_count
ORDER BY c.creator_name;

-- Remove creator_handle from creators
ALTER TABLE creators 
DROP COLUMN IF EXISTS creator_handle;

-- Remove registration_number from parent_companies
ALTER TABLE parent_companies 
DROP COLUMN IF EXISTS registration_number;

-- solution 
-- What the creators table could have
-- ALTER TABLE creators ADD COLUMN creator_handle VARCHAR(100) UNIQUE;

-- -- What parent_companies could have  
-- ALTER TABLE parent_companies ADD COLUMN registration_number VARCHAR(50) UNIQUE;

