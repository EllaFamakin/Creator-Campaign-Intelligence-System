# DATABASE SCHEMA DESIGN

## creators
- creator_id (Primary Key)
- creator_name
- main_platform (TikTok, Instagram, Facebook, Youtube, etc.)
- niche (Beauty, fitness, gaming, food, etc.)
- followers
- avg_page_engagement_rate
- post_count

## brands
- brand_id (Primary Key)
- brand_name
- parent_company
- industry

## campaigns
- campaign_id (Primary Key)
- brand_id (Foreign Key)
- platform
- campaign_name
- campaign_product
- start_date
- end_date
- budget

## campaign_collaborations
- collaboration_id (Primary Key)
- campaign_id (Foreign Key)
- creator_id (Foreign Key)
- content_type
- payment_amount

## performance_metrics
- performance_id (Primary Key)
- collaboration_id (Foreign Key)
- impressions
- clicks
- likes
- shares
- comments
- conversions
- revenue_generated