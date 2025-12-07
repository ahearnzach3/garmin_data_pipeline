-- =====================================================
-- Create summarized_activities table
-- Contains ALL activity types from Garmin export
-- =====================================================

-- Drop table if exists (for fresh start)
DROP TABLE IF EXISTS garmin.summarized_activities CASCADE;

-- Create summarized_activities table
CREATE TABLE garmin.summarized_activities (
    -- Primary identifiers
    activityId BIGINT PRIMARY KEY,
    activityType VARCHAR(100),
    name VARCHAR(500),
    
    -- Timestamps
    beginTimestamp TIMESTAMP,
    startTimeGmt TIMESTAMP,
    startTimeLocal TIMESTAMP,
    
    -- Distance and duration metrics
    distance NUMERIC(10, 3),  -- kilometers
    duration NUMERIC(10, 2),  -- seconds
    elapsedDuration NUMERIC(10, 2),  -- seconds
    movingDuration NUMERIC(10, 2),  -- seconds
    
    -- Calories
    calories NUMERIC(10, 2),
    bmrCalories NUMERIC(10, 2),
    
    -- Speed metrics
    avgSpeed NUMERIC(10, 4),  -- m/s
    maxSpeed NUMERIC(10, 4),  -- m/s
    
    -- Heart rate
    avgHr INTEGER,
    maxHr INTEGER,
    
    -- Cadence (running-specific)
    avgRunCadence INTEGER,
    maxRunCadence INTEGER,
    steps INTEGER,
    
    -- Elevation
    elevationGain NUMERIC(10, 2),  -- meters
    elevationLoss NUMERIC(10, 2),  -- meters
    minElevation NUMERIC(10, 2),  -- meters
    maxElevation NUMERIC(10, 2),  -- meters
    
    -- Training effects
    aerobicTrainingEffect NUMERIC(4, 2),
    anaerobicTrainingEffect NUMERIC(4, 2),
    trainingEffectLabel VARCHAR(100),
    activityTrainingLoad NUMERIC(10, 2),
    
    -- Running metrics
    avgStrideLength NUMERIC(10, 2),
    
    -- Power metrics
    avgPower INTEGER,
    maxPower INTEGER,
    normPower INTEGER,
    
    -- Activity metadata
    lapCount INTEGER,
    favorite BOOLEAN,
    sportType VARCHAR(100),
    
    -- Location data
    startLatitude NUMERIC(12, 8),
    startLongitude NUMERIC(12, 8),
    endLatitude NUMERIC(12, 8),
    endLongitude NUMERIC(12, 8),
    locationName VARCHAR(200),
    
    -- Device information
    deviceId BIGINT,
    manufacturer VARCHAR(100),
    
    -- Physiological metrics
    vO2MaxValue INTEGER,
    waterEstimated INTEGER,
    moderateIntensityMinutes INTEGER,
    vigorousIntensityMinutes INTEGER,
    
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_summarized_activities_type ON garmin.summarized_activities(activityType);
CREATE INDEX idx_summarized_activities_start_time ON garmin.summarized_activities(startTimeLocal);
CREATE INDEX idx_summarized_activities_sport ON garmin.summarized_activities(sportType);
CREATE INDEX idx_summarized_activities_location ON garmin.summarized_activities(locationName);

-- Add comments to table
COMMENT ON TABLE garmin.summarized_activities IS 'All activity types from Garmin summarizedActivities export';
COMMENT ON COLUMN garmin.summarized_activities.activityId IS 'Unique activity identifier from Garmin';
COMMENT ON COLUMN garmin.summarized_activities.distance IS 'Distance in kilometers';
COMMENT ON COLUMN garmin.summarized_activities.duration IS 'Activity duration in seconds';
COMMENT ON COLUMN garmin.summarized_activities.avgSpeed IS 'Average speed in meters per second';

-- Grant permissions (adjust as needed)
GRANT SELECT, INSERT, UPDATE, DELETE ON garmin.summarized_activities TO where2runadmin;
