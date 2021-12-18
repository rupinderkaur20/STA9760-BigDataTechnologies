-- Query: highest hourly stock "high" per company

SELECT name, hour, MAX(high) AS max_high_per_hour
FROM (SELECT SUBSTRING(ts, 12, 2) AS hour, high, name
      FROM stocks
      GROUP BY SUBSTRING(ts, 12, 2), high, name )
WHERE hour != ' ' 
GROUP BY name, hour
ORDER BY hour, name