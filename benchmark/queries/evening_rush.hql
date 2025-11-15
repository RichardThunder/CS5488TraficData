-- Evening Rush Hour Query (17:00-21:00)
-- Find top 10 busiest roads during evening rush hour

SELECT Road_EN, District,
       SUM(volume) as total_volume,
       SUM(occupancy) as total_occupancy
FROM traffic_data
WHERE Road_EN IS NOT NULL
  AND HOUR(period_from) >= 17 AND HOUR(period_from) < 21
GROUP BY Road_EN, District
ORDER BY total_volume DESC
LIMIT 10
