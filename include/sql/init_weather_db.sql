-- Bootstrap schema for the ETLWeather target warehouse.
--
-- Mounted into the `weather_db` container at
-- /docker-entrypoint-initdb.d/, so Postgres executes it automatically the
-- FIRST time the data volume is created. Re-running `astro dev restart` does
-- not re-run it; delete the volume to force a fresh bootstrap:
--     docker volume rm etlweather_weather_db_data
--
-- The DAG's load task also issues CREATE TABLE IF NOT EXISTS, so this file is
-- not strictly required. It exists so the table (and its comments) are present
-- for inspection before the first DAG run.

CREATE TABLE IF NOT EXISTS weather_data (
    latitude      FLOAT,
    longitude     FLOAT,
    temperature   FLOAT,
    windspeed     FLOAT,
    winddirection FLOAT,
    weathercode   INT,
    timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  weather_data               IS 'Append-only current-weather observations from the Open-Meteo API.';
COMMENT ON COLUMN weather_data.latitude      IS 'Requested WGS84 latitude (echoed from the DAG constant, not the API response).';
COMMENT ON COLUMN weather_data.longitude     IS 'Requested WGS84 longitude (echoed from the DAG constant, not the API response).';
COMMENT ON COLUMN weather_data.temperature   IS 'Air temperature at 2 m, degrees Celsius.';
COMMENT ON COLUMN weather_data.windspeed     IS 'Wind speed at 10 m, km/h.';
COMMENT ON COLUMN weather_data.winddirection IS 'Wind direction at 10 m, degrees clockwise from north.';
COMMENT ON COLUMN weather_data.weathercode   IS 'WMO weather interpretation code (0 = clear sky, 3 = overcast, 61 = slight rain, ...).';
COMMENT ON COLUMN weather_data.timestamp     IS 'Row insert time, set by the database default — NOT the observation time from the API.';
