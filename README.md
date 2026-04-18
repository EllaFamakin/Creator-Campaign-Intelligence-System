---

## Setup Instructions

### Prerequisites
- PostgreSQL 18 installed — [Download here](https://www.postgresql.org/download/windows/)
- Python 3.x installed — [Download here](https://www.python.org/downloads/)
- VS Code installed — [Download here](https://code.visualstudio.com/)

### Step 1 — Clone the repository
```bash
git clone https://github.com/EllaFamakin/Creator-Campaign-Intelligence-System.git
cd Creator-Campaign-Intelligence-System
```

### Step 2 — Create and activate virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
```

### Step 3 — Install dependencies
```bash
pip install psycopg2-binary faker colorama
```

### Step 4 — Create the database
```bash
psql -U postgres
```
```sql
CREATE DATABASE influencer_marketing;
\c influencer_marketing
```

### Step 5 — Create all tables
Run `Database.sql` in VS Code using the SQLTools extension connected to `influencer_marketing`.

### Step 6 — Update database credentials
In each Python script update the connection block with your PostgreSQL password:
```python
conn = psycopg2.connect(
    dbname="influencer_marketing",
    user="postgres",
    password="your_password_here",  # ← update this
    host="localhost",
    port=5432
)
```

---

## How to Run the Project

Run scripts in this exact order:

### 1. Generate synthetic data
```bash
python data/scripts/generate_data.py
```
Generates 50 companies, 500 creators, 194 campaigns, and 4,339 performance metric rows.

### 2. Clean and validate data
```bash
python python_analytics/clean_data.py
```
Runs 7 validation checks and auto-fixes text formatting issues.

### 3. Calculate KPIs
```bash
python python_analytics/kpi_calculations.py
```
Creates the `kpi_summary` materialized view with 11 goal-aware KPIs.

### 4. Export data to CSV
```bash
python data/scripts/export_csv.py
python python_analytics/export_kpi_summary.py
```
Exports all tables and the KPI summary to the `/data` folder.

---

## Example Usage

### Query top performing creators by engagement rate
```sql
SELECT
    creator_name,
    niche,
    platform_name,
    ROUND(AVG(engagement_rate), 2) AS avg_engagement_rate,
    ROUND(AVG(ctr), 2) AS avg_ctr
FROM kpi_summary
GROUP BY creator_name, niche, platform_name
ORDER BY avg_engagement_rate DESC
LIMIT 10;
```

### Identify red flag creators
```sql
SELECT
    creator_name,
    campaign_name,
    ROUND(AVG(budget_utilisation_pct), 2) AS avg_budget_utilisation,
    ROUND(AVG(engagement_rate), 2) AS avg_engagement_rate
FROM kpi_summary
WHERE budget_utilisation_pct > 40
  AND engagement_rate < 9.75
GROUP BY creator_name, campaign_name
ORDER BY avg_budget_utilisation DESC;
```

---

## Dashboard

The interactive Tableau dashboard is published on Tableau Public and covers three pages:

| Page | Focus |
|---|---|
| Executive Overview | High level KPIs, campaign distribution, platform reach, year over year trends |
| Creator Intelligence | Niche effectiveness, creator tier analysis, red flag detection |
| Campaign & Platform Performance | Campaign type vs goal, platform comparison, CPM analysis, duration vs performance |

**[View Live Dashboard](https://public.tableau.com/app/profile/daniella.famakin/vizzes)**

---

## AI Tool Acknowledgement

Claude (Anthropic) was used as a learning and debugging assistant throughout this project. Full disclosure is included in the project report.

---

## Author

**Daniella Famakin**
CSCI 412 — Senior Project