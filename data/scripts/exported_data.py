import psycopg2
import csv
import os

conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="ELLArocks26",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# Create a data folder if it doesn't exist
os.makedirs("data", exist_ok=True)

tables = [
    "platforms",
    "parent_companies",
    "brands",
    "products",
    "creators",
    "creator_platforms",
    "campaigns",
    "collaborations",
    "collaboration_platforms",
    "performance_metrics"
]

for table in tables:
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    with open(f"data/{table}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)   # header row
        writer.writerows(rows)

    print(f" Exported {table}.csv — {len(rows)} rows")

cur.close()
conn.close()
print("\n All tables exported to /data folder")