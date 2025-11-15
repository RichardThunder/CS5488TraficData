-- Evening Congestion Analysis Query (17:00-21:00)
-- Find top 10 most congested roads with detailed metrics

SELECT Road_EN, District,
       SUM(volume) as total_volume,
       AVG(occupancy) as avg_occupancy,
       AVG(speed) as avg_speed,
       COUNT(*) as observation_count,
       SUM(CASE WHEN occupancy > 50 AND speed < 40 THEN 1 ELSE 0 END) as severe_congestion_count,
       SUM(CASE WHEN occupancy > 30 AND speed < 60 THEN 1 ELSE 0 END) as moderate_congestion_count,
       (AVG(occupancy) * 0.4 +
        (100 - AVG(speed)) * 0.3 +
        (SUM(CASE WHEN occupancy > 50 AND speed < 40 THEN 1 ELSE 0 END) / COUNT(*) * 100) * 0.3)
        as congestion_score
FROM traffic_data
WHERE Road_EN IS NOT NULL
  AND HOUR(period_from) >= 17 AND HOUR(period_from) < 21
GROUP BY Road_EN, District
ORDER BY congestion_score DESC
LIMIT 10
