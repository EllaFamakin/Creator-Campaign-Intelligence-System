-- ============================================================
-- CREATOR INTELLIGENCE PLATFORM
-- Analytical Queries
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- SECTION 1: CREATOR ANALYSIS
-- ════════════════════════════════════════════════════════════

-- ── Query 1: Niche effectiveness by campaign goal and industry ────────────
-- Answers: "For a Health brand running an Awareness campaign,
-- which creator niche delivers the best results?"
SELECT
    industry,
    campaign_goal,
    niche,
    COUNT(DISTINCT creator_id)       AS creator_count,
    ROUND(AVG(engagement_rate), 2)   AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)               AS avg_ctr,
    SUM(impressions)                 AS total_impressions
FROM kpi_summary
GROUP BY industry, campaign_goal, niche
HAVING COUNT(DISTINCT creator_id) >= 3
ORDER BY industry, campaign_goal, avg_engagement_rate DESC;


-- ── Query 2: Creator versatility ────────────────────────────
-- How do creators perform across different platforms?
-- Identifies creators who perform consistently well everywhere
-- vs those who are platform-specific
SELECT
    creator_name,
    niche,
    COUNT(DISTINCT platform_name)        AS platforms_active_on,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    ROUND(MAX(engagement_rate) -
          MIN(engagement_rate), 2)       AS engagement_variance,
    STRING_AGG(DISTINCT platform_name,
               ', ')                     AS platforms
FROM kpi_summary
GROUP BY creator_name, niche
HAVING COUNT(DISTINCT platform_name) > 1
ORDER BY engagement_variance ASC,
         avg_engagement_rate DESC;


-- ── Query 3: Red flag detection ──────────────────────────────
-- Creators with high budget utilisation but
-- low engagement — potential poor ROI
SELECT
    creator_name,
    niche,
    brand_name,
    campaign_name,
    MAX(budget_utilisation_pct)          AS budget_utilisation_pct,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions,
    col.payment_amount,
    -- Red flag score: high budget % + low engagement
    ROUND(MAX(budget_utilisation_pct) /
          NULLIF(AVG(engagement_rate), 0), 2) AS red_flag_score
FROM kpi_summary
JOIN collaborations col
  ON kpi_summary.collaboration_id = col.collaboration_id
GROUP BY creator_name, niche, brand_name,
         campaign_name, col.payment_amount
HAVING MAX(budget_utilisation_pct) > 20
   AND AVG(engagement_rate) < 10
ORDER BY red_flag_score DESC
LIMIT 20;


-- ════════════════════════════════════════════════════════════
-- SECTION 2: CAMPAIGN ANALYSIS
-- ════════════════════════════════════════════════════════════

-- ── Query 4: Campaign type vs goal alignment ─────────────────
-- Does the campaign type actually affect performance
-- within each campaign goal?
SELECT
    campaign_goal,
    campaign_type,
    COUNT(DISTINCT campaign_id)          AS campaign_count,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions,
    ROUND(AVG(budget_utilisation_pct), 2)AS avg_budget_utilisation
FROM kpi_summary
GROUP BY campaign_goal, campaign_type
ORDER BY campaign_goal, avg_engagement_rate DESC;


-- ── Query 5: Campaign duration vs performance ────────────────
-- Do longer campaigns outperform shorter ones?
-- Bucketed into duration tiers for comparison
SELECT
    CASE
        WHEN campaign_duration_days <= 30  THEN '1. Short (≤30 days)'
        WHEN campaign_duration_days <= 90  THEN '2. Medium (31-90 days)'
        WHEN campaign_duration_days <= 180 THEN '3. Long (91-180 days)'
        WHEN campaign_duration_days <= 365 THEN '4. Extended (181-365 days)'
        ELSE                                    '5. Multi-Year (365+ days)'
    END AS duration_tier,
    COUNT(DISTINCT campaign_id)          AS campaign_count,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    ROUND(AVG(impressions_per_day), 2)   AS avg_impressions_per_day,
    SUM(impressions)                     AS total_impressions
FROM kpi_summary
GROUP BY duration_tier
ORDER BY duration_tier;


-- ── Query 6: Budget efficiency ───────────────────────────────
-- Which campaigns delivered the most impressions
-- and engagement per dollar spent?
SELECT
    campaign_name,
    campaign_goal,
    brand_name,
    budget,
    SUM(impressions)                     AS total_impressions,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    -- Impressions per dollar spent
    ROUND(SUM(impressions) /
          NULLIF(budget, 0), 2)          AS impressions_per_dollar,
    -- Engagement per dollar spent
    ROUND(AVG(engagement_rate) /
          NULLIF(budget, 0) * 1000, 4)   AS engagement_per_1000_dollars
FROM kpi_summary
GROUP BY campaign_name, campaign_goal,
         brand_name, budget
ORDER BY impressions_per_dollar DESC
LIMIT 20;


-- ════════════════════════════════════════════════════════════
-- SECTION 3: BRAND & COMPANY ANALYSIS
-- ════════════════════════════════════════════════════════════

-- ── Query 7: Industry comparison ─────────────────────────────
-- Which industries get the best results
-- from creator campaigns?
SELECT
    industry,
    COUNT(DISTINCT brand_id)             AS brand_count,
    COUNT(DISTINCT campaign_id)          AS campaign_count,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions,
    ROUND(AVG(budget), 2)                AS avg_campaign_budget
FROM kpi_summary
GROUP BY industry
ORDER BY avg_engagement_rate DESC;


-- ── Query 8: Company portfolio analysis ──────────────────────
-- How do brands within the same company compare?
-- Helps identify which brands are strongest performers
SELECT
    company_name,
    brand_name,
    industry,
    COUNT(DISTINCT campaign_id)          AS campaign_count,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions,
    SUM(budget)                          AS total_budget_spent
FROM kpi_summary
GROUP BY company_name, brand_name, industry
ORDER BY company_name, avg_engagement_rate DESC;


-- ── Query 9: Budget allocation patterns ──────────────────────
-- How much do different industries spend on creators
-- and how does spend correlate with performance?
SELECT
    industry,
    CASE
        WHEN budget < 10000  THEN '1. Small (<$10K)'
        WHEN budget <= 50000 THEN '2. Medium ($10K-$50K)'
        ELSE                      '3. Large (>$50K)'
    END AS budget_tier,
    COUNT(DISTINCT campaign_id)          AS campaign_count,
    ROUND(AVG(budget), 2)                AS avg_budget,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions
FROM kpi_summary
GROUP BY industry, budget_tier
ORDER BY industry, budget_tier;


-- ════════════════════════════════════════════════════════════
-- SECTION 4: PLATFORM ANALYSIS
-- ════════════════════════════════════════════════════════════

-- ── Query 10: Platform engagement comparison ─────────────────
-- Which platform drives the most engagement overall?
SELECT
    platform_name,
    COUNT(DISTINCT collaboration_id)     AS total_collaborations,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions,
    SUM(unique_reach)                    AS total_unique_reach,
    ROUND(AVG(impressions_per_day), 2)   AS avg_impressions_per_day
FROM kpi_summary
GROUP BY platform_name
ORDER BY avg_engagement_rate DESC;


-- ── Query 11: Platform niche fit ─────────────────────────────
-- Which creator niches perform best on each platform?
-- Helps brands choose the right platform-niche combination
SELECT
    platform_name,
    niche,
    COUNT(DISTINCT creator_id)           AS creator_count,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions
FROM kpi_summary
GROUP BY platform_name, niche
ORDER BY platform_name, avg_engagement_rate DESC;


-- ── Query 12: Platform CTR comparison ───────────────────────
-- Which platform drives the most clicks
-- relative to impressions?
SELECT
    platform_name,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    ROUND(MIN(ctr), 2)                   AS min_ctr,
    ROUND(MAX(ctr), 2)                   AS max_ctr,
    COUNT(DISTINCT collaboration_id)     AS total_collaborations,
    SUM(clicks)                          AS total_clicks,
    SUM(impressions)                     AS total_impressions
FROM kpi_summary
GROUP BY platform_name
ORDER BY avg_ctr DESC;


-- ── Query 13: Platform cost efficiency ──────────────────────
-- Which platform gives the lowest CPM
-- for Awareness and Reach campaigns?
SELECT
    platform_name,
    campaign_goal,
    COUNT(DISTINCT collaboration_id)     AS collaborations,
    ROUND(AVG(cpm), 2)                   AS avg_cpm,
    ROUND(MIN(cpm), 2)                   AS min_cpm,
    ROUND(MAX(cpm), 2)                   AS max_cpm,
    SUM(impressions)                     AS total_impressions
FROM kpi_summary
WHERE cpm IS NOT NULL
GROUP BY platform_name, campaign_goal
ORDER BY avg_cpm ASC;


-- ════════════════════════════════════════════════════════════
-- SECTION 5: GOAL-SPECIFIC DEEP DIVES
-- ════════════════════════════════════════════════════════════

-- ── Query 14: Lead quality by niche ─────────────────────────
-- Which creator niches generate the highest
-- quality leads for Lead Generation campaigns?
SELECT
    niche,
    industry,
    COUNT(DISTINCT collaboration_id)     AS collaborations,
    ROUND(AVG(lead_quality_score), 2)    AS avg_lead_quality,
    ROUND(AVG(cpl), 2)                   AS avg_cpl,
    ROUND(AVG(lead_conversion_rate), 2)  AS avg_lead_conversion_rate,
    SUM(leads)                           AS total_leads
FROM kpi_summary
WHERE campaign_goal = 'Lead Generation'
  AND lead_quality_score IS NOT NULL
GROUP BY niche, industry
ORDER BY avg_lead_quality DESC;


-- ── Query 15: ROAS distribution ─────────────────────────────
-- What is the distribution of returns on
-- ad spend across Conversion campaigns?
SELECT
    CASE
        WHEN roas < 1    THEN '1. Loss (<1x)'
        WHEN roas < 2    THEN '2. Break Even (1x-2x)'
        WHEN roas < 5    THEN '3. Good (2x-5x)'
        WHEN roas < 10   THEN '4. Strong (5x-10x)'
        ELSE                  '5. Exceptional (10x+)'
    END AS roas_tier,
    COUNT(*)                             AS row_count,
    ROUND(AVG(roas), 2)                  AS avg_roas,
    ROUND(MIN(roas), 2)                  AS min_roas,
    ROUND(MAX(roas), 2)                  AS max_roas,
    COUNT(DISTINCT creator_name)         AS creators_in_tier
FROM kpi_summary
WHERE roas IS NOT NULL
GROUP BY roas_tier
ORDER BY roas_tier;


-- ── Query 16: CPL benchmarking ───────────────────────────────
-- What is a good vs bad cost per lead
-- broken down by platform and niche?
SELECT
    platform_name,
    niche,
    COUNT(DISTINCT collaboration_id)     AS collaborations,
    ROUND(AVG(cpl), 2)                   AS avg_cpl,
    ROUND(MIN(cpl), 2)                   AS min_cpl,
    ROUND(MAX(cpl), 2)                   AS max_cpl,
    ROUND(AVG(lead_quality_score), 2)    AS avg_lead_quality
FROM kpi_summary
WHERE campaign_goal = 'Lead Generation'
  AND cpl IS NOT NULL
GROUP BY platform_name, niche
ORDER BY avg_cpl ASC;


-- ── Query 17: CPA analysis ───────────────────────────────────
-- Which combinations of platform, niche and
-- campaign type deliver the lowest cost per acquisition?
SELECT
    platform_name,
    niche,
    campaign_type,
    COUNT(DISTINCT collaboration_id)     AS collaborations,
    ROUND(AVG(cpa), 2)                   AS avg_cpa,
    ROUND(MIN(cpa), 2)                   AS min_cpa,
    ROUND(MAX(cpa), 2)                   AS max_cpa,
    ROUND(AVG(roas), 2)                  AS avg_roas,
    ROUND(AVG(conversion_rate), 2)       AS avg_conversion_rate
FROM kpi_summary
WHERE campaign_goal = 'Conversion'
  AND cpa IS NOT NULL
GROUP BY platform_name, niche, campaign_type
ORDER BY avg_cpa ASC;


-- ════════════════════════════════════════════════════════════
-- SECTION 6: TIME-BASED ANALYSIS
-- ════════════════════════════════════════════════════════════

-- ── Query 18: Performance trends over campaign duration ──────
-- How does engagement change as a campaign progresses?
-- Splits campaign into thirds to show early/mid/late performance
SELECT
    campaign_goal,
    CASE
        WHEN (metric_date - start_date)::float /
             NULLIF(campaign_duration_days, 0) <= 0.33
            THEN '1. Early (first third)'
        WHEN (metric_date - start_date)::float /
             NULLIF(campaign_duration_days, 0) <= 0.66
            THEN '2. Mid (second third)'
        ELSE
            '3. Late (final third)'
    END AS campaign_phase,
    COUNT(*)                             AS metric_rows,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    ROUND(AVG(impressions), 2)           AS avg_impressions
FROM kpi_summary
GROUP BY campaign_goal, campaign_phase
ORDER BY campaign_goal, campaign_phase;


-- ── Query 19: Best performing days within campaign window ────
-- Which day of the week performs best overall?
SELECT
    TO_CHAR(metric_date, 'Day')          AS day_of_week,
    EXTRACT(DOW FROM metric_date)        AS day_number,
    COUNT(*)                             AS metric_rows,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    ROUND(AVG(impressions), 2)           AS avg_impressions
FROM kpi_summary
GROUP BY day_of_week, day_number
ORDER BY day_number;


-- ── Query 20: Year over year performance comparison ──────────
-- How has creator campaign performance
-- trended across 2022, 2023, 2024, 2025?
SELECT
    EXTRACT(YEAR FROM metric_date)       AS year,
    COUNT(DISTINCT campaign_id)          AS campaigns_active,
    COUNT(DISTINCT creator_id)           AS creators_active,
    ROUND(AVG(engagement_rate), 2)       AS avg_engagement_rate,
    ROUND(AVG(ctr), 2)                   AS avg_ctr,
    SUM(impressions)                     AS total_impressions,
    ROUND(AVG(budget), 2)                AS avg_campaign_budget
FROM kpi_summary
GROUP BY year
ORDER BY year;