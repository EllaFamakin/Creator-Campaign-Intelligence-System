import random
from faker import Faker
from datetime import date, timedelta
import psycopg2
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

fake = Faker('en_US')
random.seed(42)
Faker.seed(42)

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
# LOOKUP MAPS
# ══════════════════════════════════════════════════════════════

# Campaign goal → valid campaign types
GOAL_TYPE_MAP = {
    'Awareness':       ['Sponsored Post', 'Brand Ambassador', 'Paid Partnership'],
    'Reach':           ['Sponsored Post', 'Paid Partnership', 'Boosted Content'],
    'Engagement':      ['Giveaway', 'Challenge', 'Interactive Post', 'Sponsored Post'],
    'Lead Generation': ['Affiliate', 'Discount Code', 'Sign-up Drive'],
    'Conversion':      ['Affiliate', 'Product Review', 'Limited Offer', 'Discount Code'],
}

# Campaign goal → valid content types
GOAL_CONTENT_MAP = {
    'Awareness':       ['Reel', 'Story', 'Video', 'Post'],
    'Reach':           ['Reel', 'Video', 'Post', 'Story'],
    'Engagement':      ['Reel', 'Story', 'Live', 'Post', 'Challenge'],
    'Lead Generation': ['Post', 'Story', 'Blog', 'Reel'],
    'Conversion':      ['Product Review', 'Reel', 'Video', 'Blog', 'Post'],
}

# Niche → matching industries (for lead quality scoring)
NICHE_INDUSTRY_MAP = {
    'Beauty':    ['Beauty', 'Health', 'Fashion'],
    'Fitness':   ['Fitness', 'Health', 'Food & Beverage'],
    'Gaming':    ['Gaming', 'Tech'],
    'Food':      ['Food & Beverage', 'Health'],
    'Travel':    ['Travel', 'Fashion', 'Food & Beverage'],
    'Fashion':   ['Fashion', 'Beauty'],
    'Tech':      ['Tech', 'Gaming'],
    'Lifestyle': ['Beauty', 'Fashion', 'Health', 'Food & Beverage'],
}

INDUSTRIES = ['Beauty', 'Fitness', 'Food & Beverage', 'Tech',
              'Fashion', 'Gaming', 'Health', 'Travel']
NICHES     = list(NICHE_INDUSTRY_MAP.keys())
PLATFORMS  = ['Instagram', 'TikTok', 'YouTube', 'Facebook', 'X (Twitter)', 'Pinterest']
GOALS      = list(GOAL_TYPE_MAP.keys())

# Realistic campaign name templates
def make_campaign_name(brand_name, goal, product_name=None):
    year    = random.choice([2022, 2023, 2024, 2025])
    season  = random.choice(['Spring', 'Summer', 'Fall', 'Winter', 'Holiday', 'Back-to-School'])
    targets = ['Gen Z', 'Millennials', 'Young Professionals', 'Parents', 'Fitness Enthusiasts',
               'Tech Savvy Users', 'Beauty Lovers', 'Gamers']
    target  = random.choice(targets)

    if product_name:
        templates = [
            f"{product_name} Launch Campaign {year}",
            f"Introducing {product_name} — {season} {year}",
            f"{product_name} x {target} {year}",
            f"{brand_name} {product_name} {goal} Drive",
            f"{season} {year}: {product_name} Spotlight",
        ]
    else:
        templates = [
            f"{brand_name} {season} {goal} Campaign {year}",
            f"{brand_name} x {target} {year}",
            f"{brand_name} {year} Brand Awareness Push",
            f"{season} {year} {brand_name} Creator Series",
            f"{brand_name} {goal} Initiative {year}",
        ]
    return random.choice(templates)


# ══════════════════════════════════════════════════════════════
# 1. PLATFORMS
# ══════════════════════════════════════════════════════════════
log_header("1. Inserting Platforms")
for name in PLATFORMS:
    cur.execute("INSERT INTO platforms (platform_name) VALUES (%s)", (name,))
log_success(f"Inserted {len(PLATFORMS)} platforms")


# ══════════════════════════════════════════════════════════════
# 2. PARENT COMPANIES
# ══════════════════════════════════════════════════════════════
log_header("2. Inserting Parent Companies")

# Unique company name pool using fake.unique
company_names = set()
while len(company_names) < 50:
    company_names.add(fake.unique.company())
company_names = list(company_names)

companies = []
for name in company_names:
    founding = fake.date_between(
        start_date=date(1980, 1, 1),
        end_date=date(2015, 1, 1)
    )
    cur.execute(
        "INSERT INTO parent_companies (company_name, founding_date) VALUES (%s, %s) RETURNING company_id",
        (name, founding)
    )
    companies.append(cur.fetchone()[0])

log_success(f"Inserted {len(companies)} unique parent companies")


# ══════════════════════════════════════════════════════════════
# 3. BRANDS
# ══════════════════════════════════════════════════════════════
log_header("3. Inserting Brands")

# Pre-generate a large unique brand name pool
brand_name_pool = set()
while len(brand_name_pool) < 300:
    brand_name_pool.add(fake.unique.company())
brand_name_pool = list(brand_name_pool)
brand_name_iter = iter(brand_name_pool)

brands = []          # list of (brand_id, industry)
brand_industry = {}  # brand_id → industry

for company_id in companies:
    for _ in range(random.randint(1, 3)):
        brand_name = next(brand_name_iter)
        industry   = random.choice(INDUSTRIES)
        cur.execute(
            """INSERT INTO brands (brand_name, company_id, industry)
               VALUES (%s, %s, %s) RETURNING brand_id""",
            (brand_name, company_id, industry)
        )
        bid = cur.fetchone()[0]
        brands.append(bid)
        brand_industry[bid] = industry

log_success(f"Inserted {len(brands)} unique brands")


# ══════════════════════════════════════════════════════════════
# 4. PRODUCTS
# ══════════════════════════════════════════════════════════════
log_header("4. Inserting Products")

products = []  # list of (product_id, brand_id)
for brand_id in brands:
    for _ in range(random.randint(1, 4)):
        cur.execute(
            """INSERT INTO products (product_name, brand_id)
               VALUES (%s, %s) RETURNING product_id""",
            (fake.catch_phrase(), brand_id)
        )
        products.append((cur.fetchone()[0], brand_id))

log_success(f"Inserted {len(products)} products")


# ══════════════════════════════════════════════════════════════
# 5. CREATORS (with ~3% intentional data quality issues)
# ══════════════════════════════════════════════════════════════
log_header("5. Inserting Creators")

# Generate unique creator names
creator_name_pool = set()
while len(creator_name_pool) < 600:
    creator_name_pool.add(fake.unique.name())
creator_name_pool = list(creator_name_pool)

creators          = []   # list of creator_ids
creator_niche_map = {}   # creator_id → niche
quality_issue_count = 0

for i in range(500):
    name  = creator_name_pool[i]
    niche = random.choice(NICHES)

    # ~3% chance of intentional data quality issue
    roll = random.random()
    if roll < 0.01:       # 1% — missing name
        name  = None
        quality_issue_count += 1
    elif roll < 0.02:     # 1% — missing niche
        niche = None
        quality_issue_count += 1
    elif roll < 0.03:     # 1% — missing both
        name  = None
        niche = None
        quality_issue_count += 1

    cur.execute(
        """INSERT INTO creators (creator_name, niche, follower_count, post_count)
           VALUES (%s, %s, %s, %s) RETURNING creator_id""",
        (name, niche,
         random.randint(5000, 2_000_000),
         random.randint(20, 3000))
    )
    cid = cur.fetchone()[0]
    creators.append(cid)
    creator_niche_map[cid] = niche

log_success(f"Inserted 500 creators ({quality_issue_count} with intentional data quality issues)")


# ══════════════════════════════════════════════════════════════
# 6. CREATOR PLATFORMS
# ══════════════════════════════════════════════════════════════
log_header("6. Inserting Creator Platforms")

platform_ids = list(range(1, len(PLATFORMS) + 1))
total_cp = 0

for creator_id in creators:
    chosen = random.sample(platform_ids, k=random.randint(1, 3))
    for pid in chosen:
        cur.execute(
            "INSERT INTO creator_platforms (creator_id, platform_id) VALUES (%s, %s)",
            (creator_id, pid)
        )
        total_cp += 1

log_success(f"Inserted {total_cp} creator-platform links")


# ══════════════════════════════════════════════════════════════
# 7. CAMPAIGNS
# ══════════════════════════════════════════════════════════════
log_header("7. Inserting Campaigns")

campaign_ids      = []
campaign_meta     = {}  # campaign_id → {goal, budget, start, end}

for brand_id in brands:
    brand_products = [p[0] for p in products if p[1] == brand_id]

    for _ in range(random.randint(1, 3)):
        goal       = random.choice(GOALS)
        camp_type  = random.choice(GOAL_TYPE_MAP[goal])
        # Brand Ambassador campaigns run longer than other types
        if camp_type == 'Brand Ambassador':
            duration = random.randint(90, 1095)  # 3 months to 3 years
            start_date = fake.date_between(
                start_date=date(2022, 1, 1),
                end_date=date(2023, 1, 1)  # ensures end stays within 2025
            )
        else:
            duration = random.randint(14, 90)    # 2 weeks to 3 months
            start_date = fake.date_between(
                start_date=date(2022, 1, 1),
                end_date=date(2025, 6, 1)
            )
        end_date = start_date + timedelta(days=duration)
        budget     = round(random.uniform(5000, 150000), 2)
        product_id = random.choice(brand_products + [None])

        # Get product name for campaign naming if applicable
        product_name = None
        if product_id:
            cur.execute("SELECT product_name FROM products WHERE product_id = %s", (product_id,))
            row = cur.fetchone()
            if row:
                product_name = row[0]

        # Get brand name for campaign naming
        cur.execute("SELECT brand_name FROM brands WHERE brand_id = %s", (brand_id,))
        brand_name = cur.fetchone()[0]

        camp_name = make_campaign_name(brand_name, goal, product_name)

        cur.execute(
            """INSERT INTO campaigns
               (brand_id, product_id, campaign_name, campaign_type,
                campaign_goal, start_date, end_date, budget)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING campaign_id""",
            (brand_id, product_id, camp_name, camp_type,
             goal, start_date, end_date, budget)
        )
        cid = cur.fetchone()[0]
        campaign_ids.append(cid)
        campaign_meta[cid] = {
            'goal': goal, 'budget': budget,
            'start': start_date, 'end': end_date
        }

log_success(f"Inserted {len(campaign_ids)} campaigns")


# ══════════════════════════════════════════════════════════════
# 8. COLLABORATIONS
# ══════════════════════════════════════════════════════════════
log_header("8. Inserting Collaborations")

collab_ids   = []
collab_meta  = {}  # collaboration_id → {campaign_id, creator_id, payment}

for campaign_id in campaign_ids:
    meta          = campaign_meta[campaign_id]
    goal          = meta['goal']
    budget        = meta['budget']
    content_type  = random.choice(GOAL_CONTENT_MAP[goal])

    num_creators  = random.randint(1, 5)
    chosen        = random.sample(creators, k=num_creators)

    # Total payments capped at 60-85% of campaign budget
    total_payment_pool = budget * random.uniform(0.60, 0.85)
    # Split pool across creators with some variation
    weights = [random.uniform(0.5, 1.5) for _ in chosen]
    total_w = sum(weights)

    for i, creator_id in enumerate(chosen):
        payment = round((weights[i] / total_w) * total_payment_pool, 2)

        cur.execute(
            """INSERT INTO collaborations
               (campaign_id, creator_id, content_type, payment_amount)
               VALUES (%s, %s, %s, %s) RETURNING collaboration_id""",
            (campaign_id, creator_id, content_type, payment)
        )
        collab_id = cur.fetchone()[0]
        collab_ids.append(collab_id)
        collab_meta[collab_id] = {
            'campaign_id': campaign_id,
            'creator_id':  creator_id,
            'payment':     payment,
            'goal':        goal
        }

log_success(f"Inserted {len(collab_ids)} collaborations")


# ══════════════════════════════════════════════════════════════
# 9. COLLABORATION PLATFORMS
# ══════════════════════════════════════════════════════════════
log_header("9. Inserting Collaboration Platforms")

collab_platform_map = {}  # collab_id → [platform_ids]
total_cp_links = 0

for collab_id in collab_ids:
    chosen = random.sample(platform_ids, k=random.randint(1, 2))
    collab_platform_map[collab_id] = chosen
    for pid in chosen:
        cur.execute(
            "INSERT INTO collaboration_platforms (collaboration_id, platform_id) VALUES (%s, %s)",
            (collab_id, pid)
        )
        total_cp_links += 1

log_success(f"Inserted {total_cp_links} collaboration-platform links")


# ══════════════════════════════════════════════════════════════
# 10. PERFORMANCE METRICS
# ══════════════════════════════════════════════════════════════
log_header("10. Inserting Performance Metrics")

total_metrics = 0
skipped       = 0

for collab_id, pid_list in collab_platform_map.items():
    meta        = collab_meta[collab_id]
    goal        = meta['goal']
    payment     = meta['payment']
    creator_id  = meta['creator_id']
    campaign_id = meta['campaign_id']
    camp        = campaign_meta[campaign_id]
    start_date  = camp['start']
    end_date    = camp['end']
    duration    = (end_date - start_date).days

    # Get brand industry for lead quality scoring
    cur.execute("""
        SELECT b.industry FROM campaigns cam
        JOIN brands b ON cam.brand_id = b.brand_id
        WHERE cam.campaign_id = %s
    """, (campaign_id,))
    brand_industry_val = cur.fetchone()[0]
    creator_niche_val  = creator_niche_map.get(creator_id)

    for pid in pid_list:
        # Generate 3-7 metric dates within campaign window
        num_days = random.randint(3, min(7, duration)) if duration >= 3 else 1
        # Pick unique dates within campaign window
        possible_offsets = list(range(duration + 1))
        if len(possible_offsets) < num_days:
            num_days = len(possible_offsets)
        chosen_offsets = random.sample(possible_offsets, k=num_days)

        for offset in chosen_offsets:
            metric_date = start_date + timedelta(days=offset)

            # ── Core metrics (proportional) ──────────────────
            impressions  = random.randint(500, 500_000)
            unique_reach = int(impressions * random.uniform(0.55, 0.85))
            clicks       = int(impressions * random.uniform(0.01, 0.15))
            likes        = int(impressions * random.uniform(0.01, 0.20))
            shares       = int(impressions * random.uniform(0.001, 0.05))
            comments     = int(impressions * random.uniform(0.001, 0.03))

            # ── Goal-specific metrics ─────────────────────────
            leads              = None
            conversions        = None
            revenue_generated  = None
            lead_quality_score = None

            if goal == 'Lead Generation':
                leads = int(clicks * random.uniform(0.05, 0.30))
                # Lead quality score based on niche-industry match + engagement
                niche_match      = 30 if (
                    creator_niche_val and
                    brand_industry_val in NICHE_INDUSTRY_MAP.get(creator_niche_val, [])
                ) else 0
                engagement_rate  = (likes + comments + shares) / impressions if impressions > 0 else 0
                engagement_score = min(int(engagement_rate * 200), 50)
                noise            = random.randint(0, 20)
                lead_quality_score = min(niche_match + engagement_score + noise, 100)

            elif goal == 'Conversion':
                conversions       = int(clicks * random.uniform(0.01, 0.10))
                revenue_generated = round(
                    conversions * random.uniform(20, 500), 2
                )

            # ── is_verified flag (~5% unverified) ────────────
            is_verified = random.random() > 0.05

            try:
                cur.execute(
                    """INSERT INTO performance_metrics
                       (collaboration_id, platform_id, metric_date,
                        impressions, unique_reach, clicks, likes, shares, comments,
                        leads, conversions, revenue_generated,
                        is_verified, lead_quality_score)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT DO NOTHING""",
                    (collab_id, pid, metric_date,
                     impressions, unique_reach, clicks, likes, shares, comments,
                     leads, conversions, revenue_generated,
                     is_verified, lead_quality_score)
                )
                total_metrics += 1
            except Exception as e:
                skipped += 1

conn.commit()
log_success(f"Inserted {total_metrics} performance metric rows")
if skipped > 0:
    log_warn(f"Skipped {skipped} rows due to constraint violations")


# ══════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════
log_header("GENERATION COMPLETE")

cur.execute("SELECT COUNT(*) FROM platforms")
log_info(f"Platforms:               {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM parent_companies")
log_info(f"Parent Companies:        {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM brands")
log_info(f"Brands:                  {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM products")
log_info(f"Products:                {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM creators")
log_info(f"Creators:                {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM creator_platforms")
log_info(f"Creator-Platform Links:  {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM campaigns")
log_info(f"Campaigns:               {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM collaborations")
log_info(f"Collaborations:          {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM collaboration_platforms")
log_info(f"Collab-Platform Links:   {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM performance_metrics")
log_info(f"Performance Metrics:     {cur.fetchone()[0]}")

cur.close()
conn.close()
log_success("Connection closed. Data generation complete!")