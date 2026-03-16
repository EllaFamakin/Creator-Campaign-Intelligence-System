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
    industry   VARCHAR(80)
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
    creator_name   VARCHAR(100) NOT NULL,
    niche          VARCHAR(80),
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
    budget        NUMERIC(12, 2)
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
    impressions      INT,
    clicks           INT,
    likes            INT,
    shares           INT,
    comments         INT,
    PRIMARY KEY (collaboration_id, platform_id, metric_date)
);