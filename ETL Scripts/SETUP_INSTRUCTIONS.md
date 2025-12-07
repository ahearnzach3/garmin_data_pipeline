# Setup Instructions for Summarized Activities Table

## What Changed

You now have **TWO datasets** for activity data:

1. **`running_data`** - Filtered to ONLY running activities (existing)
2. **`summarized_activities`** - ALL activity types (NEW)
   - Running, Walking, HIIT, Cycling, Stair Climbing, Cardio, etc.

## Step 1: Create the Database Table

Before running the pipeline, you need to create the new `summarized_activities` table in your Azure PostgreSQL database.

### Option A: Using Azure Portal Query Editor

1. Go to Azure Portal → Your PostgreSQL database
2. Open "Query Editor"
3. Copy and paste the contents of `create_summarized_activities_table.sql`
4. Execute the script

### Option B: Using psql Command Line

```bash
psql -h where2run-db2.postgres.database.azure.com -U where2runadmin -d Where2run_db2 -f create_summarized_activities_table.sql
```

### Option C: Using DBeaver or pgAdmin

1. Open your database connection
2. Open `create_summarized_activities_table.sql`
3. Execute the script

## Step 2: Update Your config.yaml (Already Done)

Your `config.yaml` has already been updated with:

```yaml
tables:
  summarized_activities: "summarized_activities"

dataset_patterns:
  summarized_activities: "**/DI-Connect-Fitness/*summarizedActivities*.json"

datasets_to_process:
  - running_data
  - summarized_activities
  - sleep_data
  # ... etc
```

## Step 3: Run the Pipeline

Process just the new dataset:

```bash
cd "ETL Scripts"
python etl_pipeline.py --datasets summarized_activities
```

Or run the full pipeline (all datasets):

```bash
python etl_pipeline.py
```

## What Gets Loaded

### `running_data` Table
- **Filtered**: Only activities where `activityType` contains "running" or "run"
- **Examples**: Running, Treadmill Running
- **Use for**: Running-specific analysis, pace tracking, marathon training

### `summarized_activities` Table
- **ALL Activities**: Every workout type from your Garmin
- **Examples**: 
  - Running
  - Walking
  - HIIT
  - Cycling
  - Stair Climbing
  - Indoor Cardio
  - Swimming
  - Strength Training
- **Use for**: Complete workout history, cross-training analysis, total activity overview

## Database Schema

The `summarized_activities` table includes:

- **Identifiers**: activityId, activityType, sportType
- **Timestamps**: beginTimestamp, startTimeGmt, startTimeLocal
- **Metrics**: distance (km), duration (sec), calories, speed (m/s)
- **Heart Rate**: avgHr, maxHr
- **Elevation**: elevationGain, elevationLoss (meters)
- **Training Effects**: aerobicTrainingEffect, anaerobicTrainingEffect
- **Location**: latitude/longitude, locationName
- **Device**: deviceId, manufacturer
- **And more**: 40+ columns total

## Verification

After running the pipeline, verify the data was loaded:

```sql
-- Check row count
SELECT COUNT(*) FROM garmin.summarized_activities;

-- See activity type breakdown
SELECT activityType, COUNT(*) 
FROM garmin.summarized_activities 
GROUP BY activityType 
ORDER BY COUNT(*) DESC;

-- Compare with running_data
SELECT 
    (SELECT COUNT(*) FROM garmin.running_data) as running_only,
    (SELECT COUNT(*) FROM garmin.summarized_activities) as all_activities;
```

## Troubleshooting

### "Table does not exist" error
→ Run `create_summarized_activities_table.sql` first

### No data loaded
→ Check that summarizedActivities.json exists in your export:
```bash
find "PBI Python Scripts/Raw Data" -name "*summarizedActivities*.json"
```

### Duplicates in summarized_activities
→ The pipeline uses `replace` strategy - it truncates before loading
→ No duplicates should occur

## What's Next?

You can now:
1. Connect Power BI to both tables
2. Create visualizations comparing running vs. total activity
3. Analyze cross-training patterns
4. Track all workout types in one place
