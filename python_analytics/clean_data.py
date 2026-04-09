import psycopg2
from colorama import init, Fore, Style

init(autoreset=True)

def log_success(msg): print(f"{Fore.GREEN}  ✅ {msg}{Style.RESET_ALL}")
def log_warn(msg):    print(f"{Fore.YELLOW}  ⚠️  {msg}{Style.RESET_ALL}")
def log_info(msg):    print(f"{Fore.CYAN}  ℹ️  {msg}{Style.RESET_ALL}")
def log_header(msg):  print(f"\n{Fore.MAGENTA}{'='*60}\n   {msg}\n{'='*60}{Style.RESET_ALL}")
def log_sub(msg):     print(f"{Fore.WHITE}  {msg}{Style.RESET_ALL}")

# ── Connect ───────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="ELLArocks26",
    host="localhost",
    port=5432
)
cur = conn.cursor()
log_success("Connected to influencer_marketing\n")

# ══════════════════════════════════════════════════════════════
# 1. NULL VALUE CHECKS
# ══════════════════════════════════════════════════════════════
log_header("CHECK 1: NULL VALUES")

# Standard null checks — excludes intentionally nullable fields
null_checks = {
    # creators.creator_name and niche excluded — intentionally nullable
    "creators":         ["follower_count", "post_count"],
    "brands":           ["brand_name", "company_id", "industry"],
    "parent_companies": ["company_name"],
    # campaigns.product_id excluded — intentionally nullable
    "campaigns": [
        "campaign_name", "campaign_type", "campaign_goal",
        "start_date", "end_date", "budget"
    ],
    # collaborations.payment_amount handled separately
    "collaborations": ["campaign_id", "creator_id", "content_type"],
    # performance_metrics goal-specific columns excluded — NULL by design
    "performance_metrics": [
        "impressions", "unique_reach", "clicks",
        "likes", "shares", "comments", "is_verified"
    ]
}

total_nulls = 0
for table, columns in null_checks.items():
    for col in columns:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
        count = cur.fetchone()[0]
        if count > 0:
            log_warn(f"{table}.{col} — {count} null(s) found")
            total_nulls += count
        else:
            log_success(f"{table}.{col} — no nulls")

log_sub(f"\n  Total unexpected nulls: {total_nulls}")

# ── Intentional nullable fields — just report, don't flag ─────
log_sub("\n  Intentionally nullable fields (expected):")
cur.execute("SELECT COUNT(*) FROM creators WHERE creator_name IS NULL")
log_info(f"creators.creator_name NULL: {cur.fetchone()[0]} (intentional)")
cur.execute("SELECT COUNT(*) FROM creators WHERE niche IS NULL")
log_info(f"creators.niche NULL: {cur.fetchone()[0]} (intentional)")
cur.execute("SELECT COUNT(*) FROM campaigns WHERE product_id IS NULL")
log_info(f"campaigns.product_id NULL: {cur.fetchone()[0]} (brand-level campaigns)")
cur.execute("SELECT COUNT(*) FROM performance_metrics WHERE leads IS NULL")
log_info(f"performance_metrics.leads NULL: {cur.fetchone()[0]} (non-lead campaigns)")
cur.execute("SELECT COUNT(*) FROM performance_metrics WHERE conversions IS NULL")
log_info(f"performance_metrics.conversions NULL: {cur.fetchone()[0]} (non-conversion campaigns)")
cur.execute("SELECT COUNT(*) FROM performance_metrics WHERE revenue_generated IS NULL")
log_info(f"performance_metrics.revenue_generated NULL: {cur.fetchone()[0]} (non-conversion campaigns)")
cur.execute("SELECT COUNT(*) FROM performance_metrics WHERE lead_quality_score IS NULL")
log_info(f"performance_metrics.lead_quality_score NULL: {cur.fetchone()[0]} (non-lead campaigns)")

# ── payment_amount imputation by budget tier ──────────────────
log_sub("\n  Checking collaborations.payment_amount...")
cur.execute("SELECT COUNT(*) FROM collaborations WHERE payment_amount IS NULL")
payment_nulls = cur.fetchone()[0]

if payment_nulls > 0:
    log_warn(f"{payment_nulls} null payment_amount(s) — imputing by budget tier")
    cur.execute("""
        UPDATE collaborations c
        SET payment_amount = (
            SELECT ROUND(AVG(c2.payment_amount), 2)
            FROM collaborations c2
            JOIN campaigns cam2 ON c2.campaign_id = cam2.campaign_id
            JOIN campaigns cam  ON c.campaign_id  = cam.campaign_id
            WHERE c2.payment_amount IS NOT NULL
            AND CASE
                WHEN cam.budget < 10000  THEN 'small'
                WHEN cam.budget <= 50000 THEN 'medium'
                ELSE 'large'
            END =
            CASE
                WHEN cam2.budget < 10000  THEN 'small'
                WHEN cam2.budget <= 50000 THEN 'medium'
                ELSE 'large'
            END
        )
        WHERE c.payment_amount IS NULL
    """)
    log_success("payment_amount imputed using budget tier averages")
else:
    log_success("collaborations.payment_amount — no nulls")

# ══════════════════════════════════════════════════════════════
# 2. CREATOR DATA QUALITY AUDIT
# ══════════════════════════════════════════════════════════════
log_header("CHECK 2: CREATOR DATA QUALITY AUDIT")

# Creators missing name only
cur.execute("""
    SELECT COUNT(*) FROM creators
    WHERE creator_name IS NULL AND niche IS NOT NULL
""")
missing_name = cur.fetchone()[0]

# Creators missing niche only
cur.execute("""
    SELECT COUNT(*) FROM creators
    WHERE niche IS NULL AND creator_name IS NOT NULL
""")
missing_niche = cur.fetchone()[0]

# Creators missing both
cur.execute("""
    SELECT COUNT(*) FROM creators
    WHERE creator_name IS NULL AND niche IS NULL
""")
missing_both = cur.fetchone()[0]

# Creators with no platform linked
cur.execute("""
    SELECT COUNT(*) FROM creators c
    LEFT JOIN creator_platforms cp ON c.creator_id = cp.creator_id
    WHERE cp.platform_id IS NULL
""")
missing_platform = cur.fetchone()[0]

log_info(f"Creators missing name only:     {missing_name}")
log_info(f"Creators missing niche only:    {missing_niche}")
log_info(f"Creators missing both:          {missing_both}")
log_info(f"Creators with no platform:      {missing_platform}")

total_incomplete = missing_name + missing_niche + missing_both + missing_platform
log_sub(f"\n  Total incomplete creator records: {total_incomplete}")
log_info("These will be flagged and excluded from KPI analysis")

# ══════════════════════════════════════════════════════════════
# 3. DUPLICATE CHECKS
# ══════════════════════════════════════════════════════════════
log_header("CHECK 3: DUPLICATE ROWS")
log_info("Note: UNIQUE constraints on company_name and (brand_name, company_id)")
log_info("prevent true duplicates at the database level.\n")

duplicate_checks = {
    "platforms":               ["platform_name"],
    "creator_platforms":       ["creator_id", "platform_id"],
    "collaboration_platforms": ["collaboration_id", "platform_id"],
}

total_dupes = 0
for table, columns in duplicate_checks.items():
    col_list = ", ".join(columns)
    cur.execute(f"""
        SELECT {col_list}, COUNT(*) as count
        FROM {table}
        GROUP BY {col_list}
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()
    if dupes:
        log_warn(f"{table} — {len(dupes)} duplicate(s) found:")
        for d in dupes[:3]:
            log_sub(f"    {d}")
        total_dupes += len(dupes)
    else:
        log_success(f"{table} — no duplicates")

log_sub(f"\n  Total duplicate groups: {total_dupes}")

# ══════════════════════════════════════════════════════════════
# 4. NUMERIC RANGE VALIDATION
# ══════════════════════════════════════════════════════════════
log_header("CHECK 4: NUMERIC RANGE VALIDATION")

numeric_checks = [
    ("creators",            "follower_count",  "follower_count < 0",        "Negative follower count"),
    ("creators",            "post_count",      "post_count < 0",            "Negative post count"),
    ("campaigns",           "budget",          "budget <= 0",               "Zero or negative budget"),
    ("collaborations",      "payment_amount",  "payment_amount <= 0",       "Zero or negative payment"),
    ("performance_metrics", "impressions",     "impressions <= 0",          "Zero or negative impressions"),
    ("performance_metrics", "unique_reach",    "unique_reach < 0",          "Negative unique reach"),
    ("performance_metrics", "clicks",          "clicks < 0",                "Negative clicks"),
    ("performance_metrics", "likes",           "likes < 0",                 "Negative likes"),
    ("performance_metrics", "shares",          "shares < 0",                "Negative shares"),
    ("performance_metrics", "comments",        "comments < 0",              "Negative comments"),
    ("performance_metrics", "leads",           "leads < 0",                 "Negative leads"),
    ("performance_metrics", "conversions",     "conversions < 0",           "Negative conversions"),
    ("performance_metrics", "revenue_generated","revenue_generated < 0",    "Negative revenue"),
    ("performance_metrics", "lead_quality_score","lead_quality_score < 0 OR lead_quality_score > 100", "Invalid lead quality score"),
]

# Cross-column checks — these are already enforced by DB constraints
# but we check anyway to report on data quality
cross_checks = [
    ("clicks > impressions",     "Clicks exceed impressions"),
    ("unique_reach > impressions","Unique reach exceeds impressions"),
    ("likes > impressions",       "Likes exceed impressions"),
]

total_range_issues = 0
for table, col, condition, label in numeric_checks:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    count = cur.fetchone()[0]
    if count > 0:
        log_warn(f"{label} in {table}.{col} — {count} row(s)")
        total_range_issues += count
    else:
        log_success(f"{label} — none found")

log_sub("\n  Cross-column validation:")
for condition, label in cross_checks:
    cur.execute(f"SELECT COUNT(*) FROM performance_metrics WHERE {condition}")
    count = cur.fetchone()[0]
    if count > 0:
        log_warn(f"{label} — {count} row(s)")
        total_range_issues += count
    else:
        log_success(f"{label} — none found")

log_sub(f"\n  Total range issues: {total_range_issues}")

# ══════════════════════════════════════════════════════════════
# 5. DATE RANGE VALIDATION
# ══════════════════════════════════════════════════════════════
log_header("CHECK 5: DATE RANGE VALIDATION")

date_checks = [
    ("campaigns", "end_date < start_date",      "Campaign end before start"),
    ("campaigns", "start_date < '2022-01-01'",  "Campaign start before 2022"),
    ("campaigns", "end_date > '2025-12-31'",    "Campaign end after 2025"),
    ("performance_metrics", "metric_date < '2022-01-01'", "Metric date before 2022"),
    ("performance_metrics", "metric_date > '2025-12-31'", "Metric date after 2025"),
]

# Check metric dates fall within their campaign window
cur.execute("""
    SELECT COUNT(*)
    FROM performance_metrics pm
    JOIN collaborations col ON pm.collaboration_id = col.collaboration_id
    JOIN campaigns cam ON col.campaign_id = cam.campaign_id
    WHERE pm.metric_date < cam.start_date
       OR pm.metric_date > cam.end_date
""")
out_of_window = cur.fetchone()[0]

total_date_issues = 0
for table, condition, label in date_checks:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    count = cur.fetchone()[0]
    if count > 0:
        log_warn(f"{label} — {count} row(s)")
        total_date_issues += count
    else:
        log_success(f"{label} — none found")

if out_of_window > 0:
    log_warn(f"Metric dates outside campaign window — {out_of_window} row(s)")
    total_date_issues += out_of_window
else:
    log_success("All metric dates fall within campaign windows")

log_sub(f"\n  Total date issues: {total_date_issues}")

# ══════════════════════════════════════════════════════════════
# 6. TEXT FORMATTING
# ══════════════════════════════════════════════════════════════
log_header("CHECK 6: TEXT FORMATTING")

text_fixes = [
    ("creators",  "niche",         "creators.niche"),
    ("brands",    "industry",      "brands.industry"),
    ("platforms", "platform_name", "platforms.platform_name"),
    ("campaigns", "campaign_goal", "campaigns.campaign_goal"),
    ("campaigns", "campaign_type", "campaigns.campaign_type"),
]

total_text_fixes = 0
for table, col, label in text_fixes:
    cur.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE {col} IS NOT NULL
        AND {col} != INITCAP({col})
    """)
    count = cur.fetchone()[0]
    if count > 0:
        log_warn(f"{label} — {count} inconsistent casing found")
        cur.execute(f"""
            UPDATE {table} SET {col} = INITCAP({col})
            WHERE {col} IS NOT NULL
        """)
        log_success(f"Fixed: {label} standardized to title case")
        total_text_fixes += count
    else:
        log_success(f"{label} — casing consistent")

# ══════════════════════════════════════════════════════════════
# 7. VERIFIED DATA QUALITY CHECK
# ══════════════════════════════════════════════════════════════
log_header("CHECK 7: DATA VERIFICATION STATUS")

cur.execute("SELECT COUNT(*) FROM performance_metrics WHERE is_verified = TRUE")
verified = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM performance_metrics WHERE is_verified = FALSE")
unverified = cur.fetchone()[0]
total_pm = verified + unverified
pct = round((unverified / total_pm) * 100, 2) if total_pm > 0 else 0

log_info(f"Verified rows:   {verified}")
log_warn(f"Unverified rows: {unverified} ({pct}% of total)")
log_info("Unverified rows will be excluded from KPI calculations in the materialized view")

# ══════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════
log_header("CLEANING SUMMARY")
log_sub(f"  Unexpected nulls found:        {total_nulls}")
log_sub(f"  Payment nulls imputed:         {payment_nulls}")
log_sub(f"  Incomplete creator records:    {total_incomplete}")
log_sub(f"  Duplicate groups found:        {total_dupes}")
log_sub(f"  Numeric range issues:          {total_range_issues}")
log_sub(f"  Date issues found:             {total_date_issues}")
log_sub(f"  Text fixes applied:            {total_text_fixes}")
log_sub(f"  Unverified metric rows:        {unverified}")
log_sub(f"  Rows discarded:                0")

total_issues = total_nulls + total_dupes + total_range_issues + total_date_issues
if total_issues == 0:
    log_success("Data is clean and ready for KPI calculations!")
else:
    log_warn(f"{total_issues} issue(s) found — review above")

conn.commit()
cur.close()
conn.close()
log_success("Connection closed")