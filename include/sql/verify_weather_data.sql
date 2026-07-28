-- Verification queries for the ETLWeather pipeline.
--
-- Run after a DAG run to prove data actually landed:
--     docker exec -it weather_db psql -U postgres -d weather -f /dev/stdin < include/sql/verify_weather_data.sql
-- or paste individual queries into any SQL client on localhost:5434.

\echo '=== 1. Row count and load window ==='
SELECT
    COUNT(*)                AS total_rows,
    MIN(timestamp)          AS first_loaded_at,
    MAX(timestamp)          AS last_loaded_at
FROM weather_data;

\echo ''
\echo '=== 2. Ten most recent observations ==='
SELECT
    timestamp,
    latitude,
    longitude,
    temperature,
    windspeed,
    winddirection,
    weathercode
FROM weather_data
ORDER BY timestamp DESC
LIMIT 10;

\echo ''
\echo '=== 3. Decoded WMO weather codes ==='
-- Reference: WMO weather interpretation codes as published by Open-Meteo.
SELECT
    weathercode,
    CASE weathercode
        WHEN 0  THEN 'Clear sky'
        WHEN 1  THEN 'Mainly clear'
        WHEN 2  THEN 'Partly cloudy'
        WHEN 3  THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 48 THEN 'Depositing rime fog'
        WHEN 51 THEN 'Light drizzle'
        WHEN 53 THEN 'Moderate drizzle'
        WHEN 55 THEN 'Dense drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 71 THEN 'Slight snow fall'
        WHEN 73 THEN 'Moderate snow fall'
        WHEN 75 THEN 'Heavy snow fall'
        WHEN 80 THEN 'Slight rain showers'
        WHEN 81 THEN 'Moderate rain showers'
        WHEN 82 THEN 'Violent rain showers'
        WHEN 95 THEN 'Thunderstorm'
        ELSE 'Other / unmapped code'
    END                     AS condition,
    COUNT(*)                AS observations
FROM weather_data
GROUP BY weathercode
ORDER BY observations DESC;

\echo ''
\echo '=== 4. Duplicate detection (evidence the pipeline is append-only) ==='
-- The DAG has no idempotency key, so re-running the same logical date inserts
-- another row. Any group with n > 1 is a duplicate load of identical readings.
SELECT
    latitude, longitude, temperature, windspeed, winddirection, weathercode,
    COUNT(*)                     AS times_inserted,
    MIN(timestamp)               AS first_seen,
    MAX(timestamp)               AS last_seen
FROM weather_data
GROUP BY latitude, longitude, temperature, windspeed, winddirection, weathercode
HAVING COUNT(*) > 1
ORDER BY times_inserted DESC;

\echo ''
\echo '=== 5. Basic data-quality assertions (0 rows = healthy) ==='
SELECT 'temperature out of plausible range' AS check_name, *
FROM weather_data WHERE temperature < -90 OR temperature > 60
UNION ALL
SELECT 'negative windspeed', *
FROM weather_data WHERE windspeed < 0
UNION ALL
SELECT 'winddirection outside 0-360', *
FROM weather_data WHERE winddirection < 0 OR winddirection > 360
UNION ALL
SELECT 'null measure present', *
FROM weather_data WHERE temperature IS NULL OR windspeed IS NULL OR weathercode IS NULL;
