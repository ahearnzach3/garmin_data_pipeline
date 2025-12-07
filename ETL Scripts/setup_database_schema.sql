-- =========================================================
-- Garmin Data Schema Setup for Azure PostgreSQL
-- =========================================================
-- This script creates a separate schema for Garmin running data
-- while keeping your Where2Run app data in the public schema

-- Create the garmin schema
CREATE SCHEMA IF NOT EXISTS garmin;

-- Grant permissions to your user
GRANT ALL PRIVILEGES ON SCHEMA garmin TO where2runadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA garmin TO where2runadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA garmin TO where2runadmin;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA garmin
GRANT ALL PRIVILEGES ON TABLES TO where2runadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA garmin
GRANT ALL PRIVILEGES ON SEQUENCES TO where2runadmin;

-- =========================================================
-- Table Definitions
-- =========================================================

-- Running Data Table
CREATE TABLE IF NOT EXISTS garmin.running_data (
    id SERIAL PRIMARY KEY,
    
    -- Activity Info
    activity_type VARCHAR(50),
    date TIMESTAMP,
    month_numeric INTEGER,
    month VARCHAR(10),
    year INTEGER,
    week_of_year INTEGER,
    
    -- Distance & Time
    distance NUMERIC(10, 2),
    distance_group VARCHAR(20),
    distance_group_id INTEGER,
    time INTERVAL,
    moving_time INTERVAL,
    elapsed_time INTERVAL,
    idle_time INTERVAL,
    
    -- Pace
    avg_pace INTERVAL,
    best_pace INTERVAL,
    
    -- Heart Rate
    avg_hr NUMERIC(5, 2),
    max_hr NUMERIC(5, 2),
    
    -- Elevation
    elev_gain NUMERIC(10, 2),
    elev_loss NUMERIC(10, 2),
    
    -- Cadence
    avg_run_cadence NUMERIC(5, 2),
    max_run_cadence NUMERIC(5, 2),
    
    -- Calories
    calories NUMERIC(10, 2),
    
    -- Training Effect
    aerobic_te NUMERIC(3, 1),
    anaerobic_te NUMERIC(3, 1),
    
    -- Cumulative Metrics
    weekly_cumulative_mins INTERVAL,
    weekly_mins_prior_to_run NUMERIC(10, 2),
    monthly_cumulative_mins INTERVAL,
    monthly_mins_prior_to_run NUMERIC(10, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_running_data_date ON garmin.running_data(date);
CREATE INDEX IF NOT EXISTS idx_running_data_year_week ON garmin.running_data(year, week_of_year);
CREATE INDEX IF NOT EXISTS idx_running_data_year_month ON garmin.running_data(year, month);
CREATE INDEX IF NOT EXISTS idx_running_data_distance_group ON garmin.running_data(distance_group_id);

-- =========================================================
-- Future Tables (Placeholders)
-- =========================================================

-- Sleep Data Table
CREATE TABLE IF NOT EXISTS garmin.sleep_data (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add sleep-specific columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Training History Table
CREATE TABLE IF NOT EXISTS garmin.training_history (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add training history columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ATL Data Table
CREATE TABLE IF NOT EXISTS garmin.atl_data (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add ATL columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MaxMet Data Table
CREATE TABLE IF NOT EXISTS garmin.maxmet_data (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add MaxMet columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Race Predictions Table
CREATE TABLE IF NOT EXISTS garmin.race_predictions (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add race prediction columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Training Plan Table
CREATE TABLE IF NOT EXISTS garmin.training_plan (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add training plan columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- UDS Data Table
CREATE TABLE IF NOT EXISTS garmin.uds_data (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    -- Add UDS columns as needed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- Views for Combined Analysis
-- =========================================================

-- Example view to combine running data with date dimensions
CREATE OR REPLACE VIEW garmin.running_summary AS
SELECT 
    year,
    month,
    week_of_year,
    COUNT(*) as total_runs,
    SUM(distance) as total_distance,
    AVG(distance) as avg_distance,
    SUM(EXTRACT(EPOCH FROM time)/60) as total_minutes,
    AVG(EXTRACT(EPOCH FROM avg_pace)/60) as avg_pace_minutes
FROM garmin.running_data
GROUP BY year, month, week_of_year
ORDER BY year DESC, week_of_year DESC;

-- =========================================================
-- Comments
-- =========================================================

COMMENT ON SCHEMA garmin IS 'Schema for Garmin fitness data including running, sleep, and training metrics';
COMMENT ON TABLE garmin.running_data IS 'Main table for Garmin running activity data';
COMMENT ON TABLE garmin.sleep_data IS 'Sleep tracking data from Garmin';
COMMENT ON TABLE garmin.training_history IS 'Historical training load and metrics';

-- =========================================================
-- Verification Queries
-- =========================================================

-- List all tables in garmin schema
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'garmin';

-- Check row count
-- SELECT COUNT(*) FROM garmin.running_data;

-- View recent activities
-- SELECT date, distance, time, avg_pace FROM garmin.running_data ORDER BY date DESC LIMIT 10;
