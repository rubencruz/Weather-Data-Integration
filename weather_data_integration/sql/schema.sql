CREATE TABLE IF NOT EXISTS weather_observation (
    city VARCHAR(120) NOT NULL,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    observation_time TIMESTAMP NOT NULL,
    temperature_c NUMERIC(8,3),
    humidity_pct NUMERIC(6,2),
    wind_speed_kmh NUMERIC(8,3),
    precipitation_mm NUMERIC(8,3),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, observation_time)
);

CREATE TABLE IF NOT EXISTS weather_daily_gold (
    city VARCHAR(120) NOT NULL,
    observation_date DATE NOT NULL,
    avg_temperature_c NUMERIC(8,3),
    min_temperature_c NUMERIC(8,3),
    max_temperature_c NUMERIC(8,3),
    avg_humidity_pct NUMERIC(8,3),
    total_precipitation_mm NUMERIC(10,3),
    avg_wind_speed_kmh NUMERIC(8,3),
    record_count BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, observation_date)
);
