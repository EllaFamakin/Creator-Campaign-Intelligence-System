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

cur.execute("SELECT * FROM kpi_summary")
rows = cur.fetchall()
col_names = [desc[0] for desc in cur.description]

# Save to data folder
os.makedirs("../../data", exist_ok=True)

with open("../../data/kpi_summary.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(col_names)
    writer.writerows(rows)

print(f"✅ Exported kpi_summary.csv — {len(rows)} rows")
cur.close()
conn.close()