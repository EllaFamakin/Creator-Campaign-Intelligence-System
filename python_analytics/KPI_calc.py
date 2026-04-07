import psycopg2

conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="ELLArocks26",
    host="localhost",
    port=5432
)
cur = conn.cursor()
print("✅ Connected\n")

# ── Create materialized view with all KPIs ─────────────
cur.execute("DROP MATERIALIZED VIEW IF EXISTS kpi_summary;")

cur.execute("""
    CREATE MATERIALIZED VIEW kpi_summary AS
    SELECT
        col.collaboration_id,
        cr.creator_id,
        cr.creator_name,
        cr.niche,
        cr.follower_count,
        p.platform_name,
        b.brand_name,
        pc.company_name,
        cam.campaign_name,
        cam.campaign_type,
        cam.campaign_goal,
        cam.start_date,
        cam.end_date,
        cam.budget,
        col.content_type,
        col.payment_amount,
        pm.metric_date,
        pm.impressions,
        pm.clicks,
        pm.likes,
        pm.shares,
        pm.comments,

        -- Engagement Rate (%)
        CASE WHEN pm.impressions > 0
            THEN ROUND(
                ((COALESCE(pm.likes,0) + COALESCE(pm.comments,0) + COALESCE(pm.shares,0))::numeric
                / pm.impressions) * 100, 2)
            ELSE 0
        END AS engagement_rate,

        -- CTR: Click Through Rate (%)
        CASE WHEN pm.impressions > 0
            THEN ROUND((pm.clicks::numeric / pm.impressions) * 100, 2)
            ELSE 0
        END AS ctr,

        -- CPM: Cost per 1000 impressions
        CASE WHEN pm.impressions > 0
            THEN ROUND((col.payment_amount / pm.impressions) * 1000, 2)
            ELSE 0
        END AS cpm,

        -- Conversion Rate (%)
        CASE WHEN pm.clicks > 0
            THEN ROUND((pm.clicks::numeric / pm.impressions) * 100, 2)
            ELSE 0
        END AS conversion_rate,

        -- ROAS placeholder (revenue not tracked — flagged)
        NULL::numeric AS roas

    FROM collaborations col
    JOIN creators           cr  ON col.creator_id      = cr.creator_id
    JOIN campaigns          cam ON col.campaign_id      = cam.campaign_id
    JOIN brands             b   ON cam.brand_id         = b.brand_id
    JOIN parent_companies   pc  ON b.company_id         = pc.company_id
    JOIN collaboration_platforms cp ON col.collaboration_id = cp.collaboration_id
    JOIN platforms          p   ON cp.platform_id       = p.platform_id
    JOIN performance_metrics pm ON col.collaboration_id = pm.collaboration_id
                                AND cp.platform_id      = pm.platform_id;
""")

conn.commit()
print("✅ Materialized view created!\n")

# ── Quick preview ──────────────────────────────────────
cur.execute("""
    SELECT creator_name, platform_name, brand_name,
           engagement_rate, ctr, cpm, conversion_rate
    FROM kpi_summary
    LIMIT 5;
""")
rows = cur.fetchall()
print("📊 Sample KPI Results:")
print("-" * 60)
for row in rows:
    print(f"  Creator:         {row[0]}")
    print(f"  Platform:        {row[1]}")
    print(f"  Brand:           {row[2]}")
    print(f"  Engagement Rate: {row[3]}%")
    print(f"  CTR:             {row[4]}%")
    print(f"  CPM:             ${row[5]}")
    print(f"  Conversion Rate: {row[6]}%")
    print("-" * 60)

cur.close()
conn.close()
print("\n✅ Done! KPI view ready.")