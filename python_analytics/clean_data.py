import psycopg2
import pandas as pd

# ── Connect to database ────────────────────────────────
conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="ELLArocks26",
    host="localhost",
    port=5432
)
cur = conn.cursor()
print("✅ Connected to database\n")
print("=" * 60)

# ══════════════════════════════════════════════════════
# 1. CHECK FOR NULL VALUES
# ══════════════════════════════════════════════════════
print("\n📋 CHECK 1: NULL VALUES")
print("-" * 60)

# These columns are INTENTIONALLY nullable — skip them
# campaigns.product_id → brand level campaign
# parent_companies.founding_date → optional field
# collaborations.payment_amount → handled separately below

null_checks = {
    "creators": [
        ("creator_name",   "discard"),
        ("niche",          "discard"),
        ("follower_count", "discard"),
        ("post_count",     "discard")
    ],
    "brands": [
        ("brand_name",  "discard"),
        ("company_id",  "discard"),
        ("industry",    "flag")
    ],
    "parent_companies": [
        ("company_name", "discard")
    ],
    "campaigns": [
        ("campaign_name",  "discard"),
        ("campaign_type",  "flag"),
        ("campaign_goal",  "flag"),
        ("start_date",     "discard"),
        ("end_date",       "discard"),
        ("budget",         "flag")
    ],
    "collaborations": [
        ("campaign_id",  "discard"),
        ("creator_id",   "discard"),
        ("content_type", "flag")
    ],
    "performance_metrics": [
        ("impressions", "discard"),
        ("clicks",      "flag"),
        ("likes",       "flag"),
        ("shares",      "flag"),
        ("comments",    "flag")
    ]
}

total_nulls      = 0
total_discarded  = 0
total_flagged    = 0

for table, columns in null_checks.items():
    for col, action in columns:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
        count = cur.fetchone()[0]

        if count > 0:
            total_nulls += count
            print(f"  ⚠️  {table}.{col} — {count} null(s) found → action: {action}")

            if action == "discard":
                cur.execute(f"DELETE FROM {table} WHERE {col} IS NULL")
                print(f"      → Discarded {count} row(s) from {table}")
                total_discarded += count

            elif action == "flag":
                # Add a flag column if it doesn't exist
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = '{table}'
                            AND column_name = 'data_quality_flag'
                        ) THEN
                            ALTER TABLE {table} ADD COLUMN data_quality_flag VARCHAR(100);
                        END IF;
                    END$$;
                """)
                cur.execute(f"""
                    UPDATE {table}
                    SET data_quality_flag = 'missing_{col}'
                    WHERE {col} IS NULL
                """)
                print(f"      → Flagged {count} row(s) with 'missing_{col}'")
                total_flagged += count
        else:
            print(f"  ✅ {table}.{col} — no nulls")

print(f"\n  Nulls found:     {total_nulls}")
print(f"  Rows discarded:  {total_discarded}")
print(f"  Rows flagged:    {total_flagged}")

# ══════════════════════════════════════════════════════
# 1b. IMPUTE PAYMENT_AMOUNT BY CAMPAIGN BUDGET TIER
# ══════════════════════════════════════════════════════
print("\n📋 CHECK 1b: IMPUTE MISSING PAYMENT AMOUNTS")
print("-" * 60)

# Check if any payment_amount nulls exist first
cur.execute("SELECT COUNT(*) FROM collaborations WHERE payment_amount IS NULL")
payment_nulls = cur.fetchone()[0]

if payment_nulls > 0:
    print(f"  ⚠️  {payment_nulls} missing payment_amount(s) found")
    print(f"  → Imputing using average payment within same campaign budget tier\n")

    # Tier averages — calculate average payment per budget tier
    cur.execute("""
        SELECT
            CASE
                WHEN c.budget < 10000               THEN 'small'
                WHEN c.budget BETWEEN 10000 AND 50000 THEN 'medium'
                ELSE                                      'large'
            END AS tier,
            ROUND(AVG(col.payment_amount)::NUMERIC, 2) AS avg_payment
        FROM collaborations col
        JOIN campaigns c ON col.campaign_id = c.campaign_id
        WHERE col.payment_amount IS NOT NULL
        GROUP BY tier
    """)
    tier_averages = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  Budget tier averages:")
    for tier, avg in tier_averages.items():
        print(f"    {tier.capitalize()} campaigns → avg payment: ${avg}")

    # Impute null payment amounts using the tier average
    cur.execute("""
        UPDATE collaborations col
        SET payment_amount = (
            SELECT ROUND(AVG(col2.payment_amount)::NUMERIC, 2)
            FROM collaborations col2
            JOIN campaigns c2 ON col2.campaign_id = c2.campaign_id
            WHERE col2.payment_amount IS NOT NULL
            AND CASE
                    WHEN c2.budget < 10000                THEN 'small'
                    WHEN c2.budget BETWEEN 10000 AND 50000 THEN 'medium'
                    ELSE                                       'large'
                END = CASE
                    WHEN c.budget < 10000                THEN 'small'
                    WHEN c.budget BETWEEN 10000 AND 50000 THEN 'medium'
                    ELSE                                       'large'
                END
        )
        FROM campaigns c
        WHERE col.campaign_id = c.campaign_id
        AND col.payment_amount IS NULL
    """)
    print(f"\n  ✅ Imputed {payment_nulls} payment amount(s) using tier averages")
else:
    print(f"  ✅ No missing payment amounts found")

# ══════════════════════════════════════════════════════
# 2. CHECK FOR DUPLICATE ROWS
# ══════════════════════════════════════════════════════
print("  ℹ️  Note: Name-only matches for companies and creators")
print("  are not treated as duplicates — same names can belong")
print("  to distinct real-world entities (different industries,")
print("  niches, and follower counts confirm this).\n")


print("\n📋 CHECK 2: DUPLICATE ROWS")
print("-" * 60)

duplicate_checks = {
    "platforms":        ["platform_name"],
    "campaigns":        ["campaign_name", "brand_id"],
    "creator_platforms":       ["creator_id", "platform_id"],
    "collaboration_platforms": ["collaboration_id", "platform_id"],
}

total_dupes = 0
for table, columns in duplicate_checks.items():
    col_list = ", ".join(columns)
    cur.execute(f"""
        SELECT {col_list}, COUNT(*) AS count
        FROM {table}
        GROUP BY {col_list}
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()
    if dupes:
        print(f"  ⚠️  {table} — {len(dupes)} duplicate group(s) found:")
        for d in dupes[:3]:
            print(f"      {d}")
        print(f"      → Logged for review, no auto-delete (manual inspection recommended)")
        total_dupes += len(dupes)
    else:
        print(f"  ✅ {table} — no duplicates")

print(f"\n  Total duplicate groups found: {total_dupes}")

# ══════════════════════════════════════════════════════
# 3. VALIDATE NUMERIC RANGES
# ══════════════════════════════════════════════════════
print("\n📋 CHECK 3: NUMERIC RANGE VALIDATION")
print("-" * 60)

numeric_checks = [
    ("creators",            "follower_count", "follower_count < 0",    "Negative follower count",   "discard"),
    ("creators",            "post_count",     "post_count < 0",        "Negative post count",       "discard"),
    ("campaigns",           "budget",         "budget <= 0",           "Zero or negative budget",   "flag"),
    ("collaborations",      "payment_amount", "payment_amount <= 0",   "Zero or negative payment",  "flag"),
    ("performance_metrics", "impressions",    "impressions < 0",       "Negative impressions",      "discard"),
    ("performance_metrics", "clicks",         "clicks < 0",            "Negative clicks",           "discard"),
    ("performance_metrics", "likes",          "likes < 0",             "Negative likes",            "discard"),
    ("performance_metrics", "shares",         "shares < 0",            "Negative shares",           "discard"),
    ("performance_metrics", "comments",       "comments < 0",          "Negative comments",         "discard"),
    ("performance_metrics", "clicks",         "clicks > impressions",  "Clicks exceed impressions", "discard"),
    ("performance_metrics", "likes",          "likes > impressions",   "Likes exceed impressions",  "discard"),
]

total_range_issues = 0
for table, col, condition, label, action in numeric_checks:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    count = cur.fetchone()[0]

    if count > 0:
        print(f"  ⚠️  {label} in {table} — {count} row(s) → action: {action}")
        if action == "discard":
            cur.execute(f"DELETE FROM {table} WHERE {condition}")
            print(f"      → Discarded {count} row(s)")
        elif action == "flag":
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = '{table}'
                        AND column_name = 'data_quality_flag'
                    ) THEN
                        ALTER TABLE {table} ADD COLUMN data_quality_flag VARCHAR(100);
                    END IF;
                END$$;
            """)
            cur.execute(f"""
                UPDATE {table}
                SET data_quality_flag = '{label.lower().replace(' ', '_')}'
                WHERE {condition}
            """)
            print(f"      → Flagged {count} row(s)")
        total_range_issues += count
    else:
        print(f"  ✅ {label} — none found")

print(f"\n  Total range issues found: {total_range_issues}")

# ══════════════════════════════════════════════════════
# 4. VALIDATE DATE RANGES
# ══════════════════════════════════════════════════════
print("\n📋 CHECK 4: DATE RANGE VALIDATION")
print("-" * 60)

date_checks = [
    ("campaigns", "end_date < start_date",     "Campaign end before start date",    "discard"),
    ("campaigns", "start_date < '2020-01-01'", "Campaign start date too old",       "flag"),
    ("campaigns", "end_date > '2026-12-31'",   "Campaign end date too far future",  "flag"),
    ("performance_metrics", "metric_date < '2020-01-01'", "Metric date too old",   "discard"),
]

total_date_issues = 0
for table, condition, label, action in date_checks:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
    count = cur.fetchone()[0]

    if count > 0:
        print(f"  ⚠️  {label} — {count} row(s) → action: {action}")
        if action == "discard":
            cur.execute(f"DELETE FROM {table} WHERE {condition}")
            print(f"      → Discarded {count} row(s)")
        elif action == "flag":
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = '{table}'
                        AND column_name = 'data_quality_flag'
                    ) THEN
                        ALTER TABLE {table} ADD COLUMN data_quality_flag VARCHAR(100);
                    END IF;
                END$$;
            END$$;
            """)
            cur.execute(f"""
                UPDATE {table}
                SET data_quality_flag = '{label.lower().replace(' ', '_')}'
                WHERE {condition}
            """)
            print(f"      → Flagged {count} row(s)")
        total_date_issues += count
    else:
        print(f"  ✅ {label} — none found")

print(f"\n  Total date issues found: {total_date_issues}")

# ══════════════════════════════════════════════════════
# 5. STANDARDIZE TEXT FORMATTING
# ══════════════════════════════════════════════════════
print("\n📋 CHECK 5: TEXT FORMATTING")
print("-" * 60)

text_checks = [
    ("creators", "niche",           "INITCAP(niche)"),
    ("brands",   "industry",        "INITCAP(industry)"),
    ("platforms","platform_name",   "INITCAP(platform_name)"),
    ("campaigns","campaign_type",   "INITCAP(campaign_type)"),
    ("campaigns","campaign_goal",   "INITCAP(campaign_goal)"),
    ("creators", "content_type",    "INITCAP(content_type)") if False else None,
]

# Remove None entries
text_checks = [t for t in text_checks if t is not None]

total_text_fixes = 0
for table, col, fix in text_checks:
    cur.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE {col} IS NOT NULL
        AND {col} != {fix}
    """)
    count = cur.fetchone()[0]
    if count > 0:
        cur.execute(f"UPDATE {table} SET {col} = {fix} WHERE {col} != {fix}")
        print(f"  ✅ {table}.{col} — fixed {count} casing issue(s)")
        total_text_fixes += count
    else:
        print(f"  ✅ {table}.{col} — casing is consistent")

print(f"\n  Total text fixes applied: {total_text_fixes}")

# ══════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("📊 CLEANING SUMMARY")
print("=" * 60)
print(f"  Null values found:         {total_nulls}")
print(f"  Rows discarded:            {total_discarded}")
print(f"  Rows flagged:              {total_flagged}")
print(f"  Payment amounts imputed:   {payment_nulls}")
print(f"  Duplicate groups found:    {total_dupes}")
print(f"  Numeric range issues:      {total_range_issues}")
print(f"  Date issues found:         {total_date_issues}")
print(f"  Text fixes applied:        {total_text_fixes}")

total_issues = total_nulls + total_dupes + total_range_issues + total_date_issues
if total_issues == 0:
    print("\n  ✅ Data is clean and ready for KPI calculation!")
else:
    print(f"\n  ⚠️  {total_issues} issue(s) found and handled — review above for details")

conn.commit()
cur.close()
conn.close()
print("\n✅ Connection closed")