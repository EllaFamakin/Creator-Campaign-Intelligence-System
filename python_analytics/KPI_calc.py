import psycopg2
from colorama import init, Fore, Style

init(autoreset=True)

def log_success(msg): print(f"{Fore.GREEN}✅ {msg}{Style.RESET_ALL}")
def log_info(msg):    print(f"{Fore.CYAN}ℹ️  {msg}{Style.RESET_ALL}")
def log_warn(msg):    print(f"{Fore.YELLOW}⚠️  {msg}{Style.RESET_ALL}")
def log_header(msg):  print(f"\n{Fore.MAGENTA}{'='*60}\n   {msg}\n{'='*60}{Style.RESET_ALL}")

# ── Connect ───────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="ELLArocks26",
    host="localhost",
    port=5432
)
cur = conn.cursor()
log_success("Connected to influencer_marketing")

# ══════════════════════════════════════════════════════════════
# DROP EXISTING VIEW
# ══════════════════════════════════════════════════════════════
log_header("Dropping existing materialized view")
cur.execute("DROP MATERIALIZED VIEW IF EXISTS kpi_summary;")
log_success("Dropped kpi_summary (if existed)")

# ══════════════════════════════════════════════════════════════
# CREATE MATERIALIZED VIEW
# ══════════════════════════════════════════════════════════════
log_header("Creating kpi_summary materialized view")

cur.execute("""
    CREATE MATERIALIZED VIEW kpi_summary AS
    SELECT
        -- ── Identifiers ──────────────────────────────────────
        col.collaboration_id,
        cr.creator_id,
        cam.campaign_id,
        b.brand_id,
        pc.company_id,

        -- ── Creator info ─────────────────────────────────────
        cr.creator_name,
        cr.niche,
        cr.follower_count,

        -- ── Platform ─────────────────────────────────────────
        p.platform_name,

        -- ── Brand & company ──────────────────────────────────
        b.brand_name,
        b.industry,
        pc.company_name,

        -- ── Campaign info ────────────────────────────────────
        cam.campaign_name,
        cam.campaign_type,
        cam.campaign_goal,
        cam.start_date,
        cam.end_date,
        cam.budget,
        (cam.end_date - cam.start_date) AS campaign_duration_days,

        -- ── Collaboration info ───────────────────────────────
        col.content_type,
        col.payment_amount,

        -- ── Raw metrics ──────────────────────────────────────
        pm.metric_date,
        pm.impressions,
        pm.unique_reach,
        pm.clicks,
        pm.likes,
        pm.shares,
        pm.comments,
        pm.leads,
        pm.conversions,
        pm.revenue_generated,
        pm.is_verified,
        pm.lead_quality_score,

        -- ══════════════════════════════════════════════════
        -- CALCULATED KPIs
        -- ══════════════════════════════════════════════════

        -- ── Engagement Rate (%) ──────────────────────────
        -- Relevant for: ALL goals
        -- Formula: (likes + comments + shares) / impressions * 100
        CASE
            WHEN pm.impressions > 0
            THEN ROUND(
                ((COALESCE(pm.likes, 0) +
                  COALESCE(pm.comments, 0) +
                  COALESCE(pm.shares, 0))::numeric
                / pm.impressions) * 100, 2)
            ELSE NULL
        END AS engagement_rate,

        -- ── CTR: Click Through Rate (%) ──────────────────
        -- Relevant for: ALL goals
        -- Formula: clicks / impressions * 100
        CASE
            WHEN pm.impressions > 0
            THEN ROUND((pm.clicks::numeric / pm.impressions) * 100, 2)
            ELSE NULL
        END AS ctr,

        -- ── Unique Reach Rate (%) ────────────────────────
        -- Relevant for: Awareness, Reach
        -- Formula: unique_reach / impressions * 100
        CASE
            WHEN cam.campaign_goal IN ('Awareness', 'Reach')
             AND pm.impressions > 0
            THEN ROUND((pm.unique_reach::numeric / pm.impressions) * 100, 2)
            ELSE NULL
        END AS unique_reach_rate,

        -- ── CPM: Cost per 1000 Impressions ───────────────
        -- Relevant for: Awareness, Reach
        -- Formula: (payment_amount / impressions) * 1000
        CASE
            WHEN cam.campaign_goal IN ('Awareness', 'Reach')
             AND pm.impressions > 0
            THEN ROUND((col.payment_amount / pm.impressions) * 1000, 2)
            ELSE NULL
        END AS cpm,

        -- ── CPL: Cost per Lead ───────────────────────────
        -- Relevant for: Lead Generation
        -- Formula: payment_amount / leads
        CASE
            WHEN cam.campaign_goal = 'Lead Generation'
             AND pm.leads > 0
            THEN ROUND(col.payment_amount / pm.leads, 2)
            ELSE NULL
        END AS cpl,

        -- ── CPA: Cost per Acquisition ────────────────────
        -- Relevant for: Conversion
        -- Formula: payment_amount / conversions
        CASE
            WHEN cam.campaign_goal = 'Conversion'
             AND pm.conversions > 0
            THEN ROUND(col.payment_amount / pm.conversions, 2)
            ELSE NULL
        END AS cpa,

        -- ── ROAS: Return on Ad Spend ─────────────────────
        -- Relevant for: Conversion
        -- Formula: revenue_generated / payment_amount
        CASE
            WHEN cam.campaign_goal = 'Conversion'
             AND col.payment_amount > 0
             AND pm.revenue_generated IS NOT NULL
            THEN ROUND(pm.revenue_generated / col.payment_amount, 2)
            ELSE NULL
        END AS roas,

        -- ── Conversion Rate (%) ──────────────────────────
        -- Relevant for: Conversion
        -- Formula: conversions / clicks * 100
        CASE
            WHEN cam.campaign_goal = 'Conversion'
             AND pm.clicks > 0
             AND pm.conversions IS NOT NULL
            THEN ROUND((pm.conversions::numeric / pm.clicks) * 100, 2)
            ELSE NULL
        END AS conversion_rate,

        -- ── Lead Conversion Rate (%) ─────────────────────
        -- Relevant for: Lead Generation
        -- Formula: leads / clicks * 100
        CASE
            WHEN cam.campaign_goal = 'Lead Generation'
             AND pm.clicks > 0
             AND pm.leads IS NOT NULL
            THEN ROUND((pm.leads::numeric / pm.clicks) * 100, 2)
            ELSE NULL
        END AS lead_conversion_rate,

        -- ── Impressions per Day ──────────────────────────
        -- Relevant for: ALL goals
        -- Normalizes reach across campaigns of different lengths
        CASE
            WHEN (cam.end_date - cam.start_date) > 0
            THEN ROUND(pm.impressions::numeric /
                 (cam.end_date - cam.start_date), 2)
            ELSE NULL
        END AS impressions_per_day,

        -- ── Budget Utilisation (%) ───────────────────────
        -- Relevant for: ALL goals
        -- Calculated at collaboration level not daily metric level
        -- Shows what % of campaign budget went to this creator
        -- Use MAX() in Tableau to avoid summing across metric dates
        CASE
            WHEN cam.budget > 0
            THEN ROUND(
                (SELECT SUM(c2.payment_amount)
                 FROM collaborations c2
                 WHERE c2.collaboration_id = col.collaboration_id
                ) / cam.budget * 100, 2)
            ELSE NULL
        END AS budget_utilisation_pct

    FROM collaborations col
    JOIN creators            cr  ON col.creator_id       = cr.creator_id
    JOIN campaigns           cam ON col.campaign_id      = cam.campaign_id
    JOIN brands              b   ON cam.brand_id         = b.brand_id
    JOIN parent_companies    pc  ON b.company_id         = pc.company_id
    JOIN collaboration_platforms cp
                                 ON col.collaboration_id = cp.collaboration_id
    JOIN platforms           p   ON cp.platform_id       = p.platform_id
    JOIN performance_metrics pm  ON col.collaboration_id = pm.collaboration_id
                                AND cp.platform_id       = pm.platform_id

    -- ── Exclude incomplete creators ──────────────────────
    WHERE cr.creator_name IS NOT NULL
      AND cr.niche         IS NOT NULL

    -- ── Exclude unverified metrics ───────────────────────
      AND pm.is_verified = TRUE;
""")

log_success("Materialized view kpi_summary created!")

# ── Add indexes on materialized view for Tableau performance ──
cur.execute("CREATE INDEX idx_kpi_goal ON kpi_summary(campaign_goal);")
cur.execute("CREATE INDEX idx_kpi_platform ON kpi_summary(platform_name);")
cur.execute("CREATE INDEX idx_kpi_niche ON kpi_summary(niche);")
log_success("Indexes created on kpi_summary")

conn.commit()

# ══════════════════════════════════════════════════════════════
# PREVIEW RESULTS BY GOAL
# ══════════════════════════════════════════════════════════════
log_header("KPI Preview by Campaign Goal")

goals = ['Awareness', 'Reach', 'Engagement', 'Lead Generation', 'Conversion']

for goal in goals:
    cur.execute("""
        SELECT
            creator_name,
            platform_name,
            brand_name,
            engagement_rate,
            ctr,
            cpm,
            cpl,
            cpa,
            roas,
            conversion_rate,
            lead_conversion_rate,
            unique_reach_rate,
            budget_utilisation_pct
        FROM kpi_summary
        WHERE campaign_goal = %s
        LIMIT 2
    """, (goal,))
    rows = cur.fetchall()

    print(f"\n{Fore.CYAN}  ── {goal} ──{Style.RESET_ALL}")
    if not rows:
        log_warn(f"No rows found for {goal}")
        continue

    for row in rows:
        print(f"  Creator:               {row[0]}")
        print(f"  Platform:              {row[1]}")
        print(f"  Brand:                 {row[2]}")
        print(f"  Engagement Rate:       {row[3]}%")
        print(f"  CTR:                   {row[4]}%")
        print(f"  CPM:                   {row[5]}")
        print(f"  CPL:                   {row[6]}")
        print(f"  CPA:                   {row[7]}")
        print(f"  ROAS:                  {row[8]}")
        print(f"  Conversion Rate:       {row[9]}%")
        print(f"  Lead Conversion Rate:  {row[10]}%")
        print(f"  Unique Reach Rate:     {row[11]}%")
        print(f"  Budget Utilisation:    {row[12]}%")
        print(f"  {'-'*40}")

# ── Row count summary ─────────────────────────────────────────
log_header("View Summary")
cur.execute("SELECT COUNT(*) FROM kpi_summary")
total = cur.fetchone()[0]
log_info(f"Total rows in kpi_summary: {total}")

cur.execute("""
    SELECT campaign_goal, COUNT(*) as rows
    FROM kpi_summary
    GROUP BY campaign_goal
    ORDER BY rows DESC
""")
rows = cur.fetchall()
for row in rows:
    log_info(f"  {row[0]:<20} {row[1]} rows")

cur.close()
conn.close()
log_success("Done! KPI view ready for Tableau and SQL analytics.")