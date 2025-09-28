-- SQL transformations for analytics views and indexes

-- View aggregating daily campaign performance metrics
CREATE VIEW IF NOT EXISTS vw_campaign_daily AS
SELECT
    date,
    campaign_id,
    source,
    medium,
    spend,
    impressions,
    clicks,
    conversions,
    revenue,
    CAST(clicks AS FLOAT) / NULLIF(impressions, 0) AS ctr,
    spend / NULLIF(clicks, 0) AS cpc,
    spend / NULLIF(conversions, 0) AS cpa,
    (revenue - spend) / NULLIF(spend, 0) AS roi
FROM fact_campaigns;

-- View summarising performance by channel (source, medium)
CREATE VIEW IF NOT EXISTS vw_channel_summary AS
SELECT
    source,
    medium,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(revenue) AS revenue,
    CAST(SUM(clicks) AS FLOAT) / NULLIF(SUM(impressions), 0) AS ctr,
    SUM(spend) / NULLIF(SUM(clicks), 0) AS cpc,
    SUM(spend) / NULLIF(SUM(conversions), 0) AS cpa,
    (SUM(revenue) - SUM(spend)) / NULLIF(SUM(spend), 0) AS roi
FROM fact_campaigns
GROUP BY source, medium;

-- Indexes to speed up lookups by date, source, and medium
CREATE INDEX IF NOT EXISTS idx_fact_campaigns_date ON fact_campaigns(date);
CREATE INDEX IF NOT EXISTS idx_fact_campaigns_source ON fact_campaigns(source);
CREATE INDEX IF NOT EXISTS idx_fact_campaigns_medium ON fact_campaigns(medium);
