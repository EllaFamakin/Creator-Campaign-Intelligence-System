# CREATOR CAMPAIGN INTELLIGENCE SYSTEM

This project is a structured advertising analytics system that stores and analyzes data consisting of the historical campaign collaboration between brands and creator across social media platforms in a PostgreSQL relational database. 

The system uses various tools to perform its tasks
+ **PostgreSQL** database to store all data regarding the Campaign, Creator, brand, and performance.
+  **SQL** and **Python** to analyze campaign performance metrics
+ **Tableau** pto provide interactive dashboards that explore creator effectiveness, platform performance, and campaign outcomes.

---

## Schema Design
For full database schema design see: 
[Database Schema](./Database%20Schema.md)

### Tables and Relationships

| Table | Description | Key Relationships |
|---|---|---|
| `platforms` | Social media platforms | Referenced by creator_platforms, collaboration_platforms, performance_metrics |
| `parent_companies` | Organizations owning brands | One-to-many with brands |
| `brands` | Brand identities | Belongs to one parent company, has many campaigns |
| `products` | Products offered by brands | Belongs to one brand, optionally referenced by campaigns |
| `creators` | Influencer profiles | Many-to-many with platforms via creator_platforms |
| `campaigns` | Marketing initiatives | Belongs to one brand, optionally promotes one product |
| `collaborations` | Campaign-creator partnerships | Bridge table between campaigns and creators |
| `collaboration_platforms` | Platforms where collaboration is published | Bridge table between collaborations and platforms |
| `performance_metrics` | Daily engagement data | Composite PK: collaboration_id + platform_id + metric_date |

---

## KPI Definitions

| KPI | Formula | Relevant Goals |
|---|---|---|
| Engagement Rate | (likes + comments + shares) / impressions × 100 | All |
| CTR (Click Through Rate) | clicks / impressions × 100 | All |
| Impressions per Day | impressions / campaign_duration_days | All |
| Budget Utilisation | payment_amount / budget × 100 | All |
| CPM (Cost per 1000 Impressions) | (payment_amount / impressions) × 1000 | Awareness, Reach |
| Unique Reach Rate | unique_reach / impressions × 100 | Awareness, Reach |
| CPL (Cost per Lead) | payment_amount / leads | Lead Generation |
| Lead Conversion Rate | leads / clicks × 100 | Lead Generation |
| CPA (Cost per Acquisition) | payment_amount / conversions | Conversion |
| ROAS (Return on Ad Spend) | revenue_generated / payment_amount | Conversion |
| Conversion Rate | conversions / clicks × 100 | Conversion |

---

## Abbreviations

| Abbreviation | Full Term |
|---|---|
| KPI | Key Performance Indicator |
| CTR | Click Through Rate |
| CPM | Cost per Mille (Cost per 1000 Impressions) |
| CPL | Cost per Lead |
| CPA | Cost per Acquisition |
| ROAS | Return on Ad Spend |
| DDL | Data Definition Language |
| FK | Foreign Key |
| PK | Primary Key |
| LOD | Level of Detail (Tableau expression type) |
| ERD | Entity Relationship Diagram |

---

## Campaign Goals and Metrics

| Goal | Success Metric | KPIs Used |
|---|---|---|
| Awareness | High reach, low CPM | CPM, Unique Reach Rate, Impressions per Day |
| Reach | Maximum unique viewers | CPM, Unique Reach Rate, Impressions per Day |
| Engagement | High interaction rate | Engagement Rate, CTR |
| Lead Generation | Leads at acceptable cost | CPL, Lead Conversion Rate |
| Conversion | Revenue and ROI | ROAS, CPA, Conversion Rate |

---

## Data Quality Notes

- 3% of creators have intentional missing data (name or niche) to simulate real-world incomplete records
- 5% of performance metric rows are marked `is_verified = FALSE` and excluded from KPI calculations
- All engagement metrics are generated proportionally from impressions — logically impossible values cannot exist
- Brand Ambassador campaigns run 3 months to 3 years; all other campaign types run 2 weeks to 3 months
- Creator payments are capped at 60-85% of total campaign budget

---

## Red Flag Detection Thresholds

Thresholds are based on the distribution of the dataset:

| Tier | Budget Utilisation | Engagement Rate | Basis |
|---|---|---|---|
| High Risk | > 40% | < 9.75% | Top 10% spend + bottom 25% engagement |
| Medium Risk | > 28% | < 9.75% | Above P75 spend + bottom 25% engagement |
| Low Risk | > 28% | < 14.59% | Above P75 spend + below average engagement |
| No Flag | Everything else | — | Acceptable performance |

Note: Thresholds would be refined with real-world industry benchmark data in a production environment.

---

## Future Improvements

1. Real-world data via social media APIs
2. Audience demographic data
3. Reusable framework for any brand's data
4. Live PostgreSQL connection instead of CSV export
5. Benchmark calibration against industry standards
6. Competitor benchmarking against industry averages