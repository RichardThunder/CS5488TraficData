-- Morning Rush Hour Query (6:00-10:00)
-- Find top 10 busiest roads during morning rush hour

SELECT Road_EN, District,
       SUM(volume) as total_volume,
       SUM(occupancy) as total_occupancy
FROM traffic_data
WHERE Road_EN IS NOT NULL
  AND HOUR(period_from) >= 6 AND HOUR(period_from) < 10
GROUP BY Road_EN, District
ORDER BY total_volume DESC
LIMIT 10
