SELECT COUNT(*) FROM dwh.fact_delivery
WHERE delivery_lead_time_days IS NOT NULL AND delivery_lead_time_days < 0;
