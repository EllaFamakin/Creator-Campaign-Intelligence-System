-- 1. platforms
CREATE TABLE platforms (
    platform_id   SERIAL PRIMARY KEY,
    platform_name VARCHAR(50) NOT NULL UNIQUE
);

-- 2. parent_companies
CREATE TABLE parent_companies (
    company_id    SERIAL PRIMARY KEY,
    company_name  VARCHAR(100) NOT NULL,
    founding_date DATE
);

-- 3. brands
CREATE TABLE brands (
    brand_id   SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL,
    company_id INT NOT NULL REFERENCES parent_companies(company_id),
    industry   VARCHAR(80),
    UNIQUE (brand_name, company_id)
);

-- 4. products
CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    brand_id     INT NOT NULL REFERENCES brands(brand_id)
);


-- 5. creators
CREATE TABLE creators (
    creator_id     SERIAL PRIMARY KEY,
    creator_name   VARCHAR(100), --made nullable to mimic more realistic messy data
    niche          VARCHAR(80), -- made nullable to mimic more realistic messy dataset
    follower_count INT,
    post_count     INT
);

-- 6. creator_platforms (bridge)
CREATE TABLE creator_platforms (
    creator_id  INT REFERENCES creators(creator_id),
    platform_id INT REFERENCES platforms(platform_id),
    PRIMARY KEY (creator_id, platform_id)
);

-- 7. campaigns
CREATE TABLE campaigns (
    campaign_id   SERIAL PRIMARY KEY,
    brand_id      INT NOT NULL REFERENCES brands(brand_id),
    product_id    INT REFERENCES products(product_id), -- nullable
    campaign_name VARCHAR(150) NOT NULL,
    campaign_type VARCHAR(80),
    campaign_goal VARCHAR(80),
    start_date    DATE,
    end_date      DATE,
    budget        NUMERIC(12, 2),
    CONSTRAINT chk_campaign_dates CHECK (end_date >= start_date)
);

-- 8. collaborations (bridge)
CREATE TABLE collaborations (
    collaboration_id SERIAL PRIMARY KEY,
    campaign_id      INT NOT NULL REFERENCES campaigns(campaign_id),
    creator_id       INT NOT NULL REFERENCES creators(creator_id),
    content_type     VARCHAR(80),
    payment_amount   NUMERIC(10, 2)
);

-- 9. collaboration_platforms (bridge)
CREATE TABLE collaboration_platforms (
    collaboration_id INT REFERENCES collaborations(collaboration_id),
    platform_id      INT REFERENCES platforms(platform_id),
    PRIMARY KEY (collaboration_id, platform_id)
);

-- 10. performance_metrics
CREATE TABLE performance_metrics (
    collaboration_id INT  REFERENCES collaborations(collaboration_id),
    platform_id      INT  REFERENCES platforms(platform_id),
    metric_date      DATE NOT NULL,

    -- core engagement metrics
    impressions      INT,
    unique_reach     INT,  -- always <= impressions
    clicks           INT,  -- always <= impressions
    likes            INT,  -- always <= impressions
    shares           INT,
    comments         INT,

    -- campaign goal- specific metrics (made NULL when not applicable)
    leads              INT,          -- Lead Generation campaigns only
    conversions        INT,          -- Conversion campaigns only
    revenue_generated  NUMERIC(12,2),-- Conversion campaigns only

    -- Data quality
    is_verified        BOOLEAN DEFAULT TRUE,
    lead_quality_score INT,          -- 0-100, Lead Generation only
    
    PRIMARY KEY (collaboration_id, platform_id, metric_date),

   -- Constraints to prevent impossible values
    CONSTRAINT chk_reach      CHECK (unique_reach <= impressions),
    CONSTRAINT chk_clicks     CHECK (clicks <= impressions),
    CONSTRAINT chk_likes      CHECK (likes <= impressions),
    CONSTRAINT chk_lead_score CHECK (lead_quality_score BETWEEN 0 AND 100)
);

-- Indexes for query performance
CREATE INDEX idx_campaigns_brand      ON campaigns(brand_id);
CREATE INDEX idx_campaigns_goal       ON campaigns(campaign_goal);
CREATE INDEX idx_collaborations_camp  ON collaborations(campaign_id);
CREATE INDEX idx_collaborations_cr    ON collaborations(creator_id);
CREATE INDEX idx_metrics_date         ON performance_metrics(metric_date);
CREATE INDEX idx_metrics_verified     ON performance_metrics(is_verified);
CREATE INDEX idx_brands_company       ON brands(company_id);



-- v2.0 Changes:
-- Added unique_reach, leads, conversions, revenue_generated to performance_metrics
-- Added is_verified and lead_quality_score data quality flags
-- Added CHECK constraints for logical metric validation
-- Added UNIQUE constraints on company and brand names
-- creator_name and niche made nullable for data quality simulation