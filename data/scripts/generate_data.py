import random
from faker import Faker
from datetime import date, timedelta
import psycopg2

fake = Faker('en_US')
random.seed(42)
Faker.seed(42)

conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="ELLArocks26",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# ── 1. platforms ──────────────────────────────────────
platforms = ['Instagram', 'TikTok', 'YouTube', 'Facebook', 'X (Twitter)', 'Pinterest']
for name in platforms:
    cur.execute("INSERT INTO platforms (platform_name) VALUES (%s)", (name,))

# ── 2. parent_companies ───────────────────────────────
companies = []
for _ in range(200):
    name = fake.company()
    date_ = fake.date_between(start_date=date(1980, 1, 1), end_date=date(2015, 1, 1))
    cur.execute(
        "INSERT INTO parent_companies (company_name, founding_date) VALUES (%s, %s) RETURNING company_id",
        (name, date_)
    )
    companies.append(cur.fetchone()[0])

# ── 3. brands ─────────────────────────────────────────
industries = ['Beauty', 'Fitness', 'Food & Beverage', 'Tech', 'Fashion', 'Gaming', 'Health']
brands = []
for company_id in companies:
    for _ in range(random.randint(1, 3)):  # 1–3 brands per company
        cur.execute(
            "INSERT INTO brands (brand_name, company_id, industry) VALUES (%s, %s, %s) RETURNING brand_id",
            (fake.company(), company_id, random.choice(industries))
        )
        brands.append(cur.fetchone()[0])

# ── 4. products ───────────────────────────────────────
products = []
for brand_id in brands:
    for _ in range(random.randint(1, 4)):
        cur.execute(
            "INSERT INTO products (product_name, brand_id) VALUES (%s, %s) RETURNING product_id",
            (fake.catch_phrase(), brand_id)
        )
        products.append((cur.fetchone()[0], brand_id))

# ── 5. creators ───────────────────────────────────────
niches = ['Beauty', 'Fitness', 'Gaming', 'Food', 'Travel', 'Fashion', 'Tech', 'Lifestyle']
creators = []
for _ in range(500):
    cur.execute(
        """INSERT INTO creators (creator_name, niche, follower_count, post_count)
           VALUES (%s, %s, %s, %s) RETURNING creator_id""",
        (fake.name(), random.choice(niches),
         random.randint(5000, 2_000_000),
         random.randint(20, 3000))
    )
    creators.append(cur.fetchone()[0])

# ── 6. creator_platforms ──────────────────────────────
platform_ids = list(range(1, len(platforms) + 1))
for creator_id in creators:
    chosen = random.sample(platform_ids, k=random.randint(1, 3))
    for pid in chosen:
        cur.execute(
            "INSERT INTO creator_platforms (creator_id, platform_id) VALUES (%s, %s)",
            (creator_id, pid)
        )

# ── 7. campaigns ──────────────────────────────────────
types  = ['Sponsored Post', 'Giveaway', 'Brand Ambassador', 'Product Review', 'Affiliate']
goals  = ['Awareness', 'Engagement', 'Conversions', 'Reach', 'Lead Generation']
campaign_ids = []
for brand_id in brands:
    brand_products = [p[0] for p in products if p[1] == brand_id]
    for _ in range(random.randint(1, 3)):
        start = fake.date_between(start_date=date(2023, 1, 1), end_date=date(2024, 6, 1))
        end   = start + timedelta(days=random.randint(14, 90))
        product_id = random.choice(brand_products + [None])  # nullable
        cur.execute(
            """INSERT INTO campaigns
               (brand_id, product_id, campaign_name, campaign_type, campaign_goal, start_date, end_date, budget)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING campaign_id""",
            (brand_id, product_id, fake.catch_phrase(),
             random.choice(types), random.choice(goals),
             start, end, round(random.uniform(1000, 100000), 2))
        )
        campaign_ids.append(cur.fetchone()[0])

# ── 8. collaborations ─────────────────────────────────
content_types = ['Reel', 'Story', 'Video', 'Post', 'Live', 'Blog']
collab_ids = []
for campaign_id in campaign_ids:
    chosen_creators = random.sample(creators, k=random.randint(1, 5))
    for creator_id in chosen_creators:
        cur.execute(
            """INSERT INTO collaborations (campaign_id, creator_id, content_type, payment_amount)
               VALUES (%s, %s, %s, %s) RETURNING collaboration_id""",
            (campaign_id, creator_id,
             random.choice(content_types),
             round(random.uniform(100, 15000), 2))
        )
        collab_ids.append(cur.fetchone()[0])

# ── 9. collaboration_platforms ────────────────────────
for collab_id in collab_ids:
    chosen = random.sample(platform_ids, k=random.randint(1, 2))
    for pid in chosen:
        cur.execute(
            "INSERT INTO collaboration_platforms (collaboration_id, platform_id) VALUES (%s, %s)",
            (collab_id, pid)
        )

# ── 10. performance_metrics ───────────────────────────
# Fetch what platforms each collab is on
cur.execute("SELECT collaboration_id, platform_id FROM collaboration_platforms")
collab_platform_pairs = cur.fetchall()

for (collab_id, pid) in collab_platform_pairs:
    # 3–7 days of metrics per collab/platform
    for day_offset in range(random.randint(3, 7)):
        metric_date = date(2024, 1, 1) + timedelta(days=day_offset + random.randint(0, 365))
        cur.execute(
            """INSERT INTO performance_metrics
               (collaboration_id, platform_id, metric_date, impressions, clicks, likes, shares, comments)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (collab_id, pid, metric_date,
             random.randint(500, 500_000),
             random.randint(50, 20_000),
             random.randint(30, 50_000),
             random.randint(5, 5_000),
             random.randint(5, 3_000))
        )

conn.commit()
cur.close()
conn.close()
print("Data loaded successfully.")