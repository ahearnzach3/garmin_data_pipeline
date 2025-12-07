# Garmin Data ETL Pipeline

Comprehensive automated ETL pipeline to extract, transform, and load **all** Garmin datasets from mass exports to Azure PostgreSQL database.

## Overview

This pipeline fully automates the data processing workflow from Garmin data exports to your Azure PostgreSQL database. It handles:

- ✅ **Running Data** - Filtered running activities (pace, distance, heart rate, etc.)
- ✅ **Summarized Activities** - ALL activity types (running, walking, HIIT, cycling, etc.)
- ✅ **Sleep Data** - Sleep stages, duration, quality metrics
- ✅ **ATL (Acute Training Load)** - Training load and recovery metrics
- ✅ **MaxMet Data** - Max metabolic rate metrics
- ✅ **Race Predictions** - Predicted race times by distance
- ✅ **Training History** - Historical training load and metrics
- ✅ **UDS (User Daily Summary)** - Daily activity summaries

### Key Features

- 🔍 **Auto-Discovery**: Automatically finds and aggregates date-stamped JSON files
- 🔄 **Truncate & Reload**: Fresh data load every time (no duplicates)
- 📊 **Multiple Datasets**: Processes 7+ datasets in one run
- 🛡️ **Error Handling**: Continues processing even if one dataset fails
- 📝 **Comprehensive Logging**: Detailed logs for troubleshooting

## Project Structure

```
ETL Scripts/
├── config.template.yaml           # Configuration template (for reference)
├── config.yaml                   # Your actual config with Azure credentials
├── requirements.txt              # Python dependencies
├── db_utils.py                  # Database connection utilities
├── aggregate_json_files.py      # JSON file discovery and aggregation
├── transform_all_datasets.py    # Transformation logic for all datasets
├── extract_json_data.py         # Legacy: Running data JSON extraction
├── transform_running_data.py    # Legacy: Running data transformations
├── load_final_datasets.py       # Direct CSV to database loader
├── etl_pipeline.py             # Main ETL orchestrator (USE THIS!)
└── README.md                   # This file
```

## Quick Start Guide

### 1. Install Dependencies

```bash
cd "ETL Scripts"
pip install -r requirements.txt
```

### 2. Configure Your Setup

Edit `config.yaml` to point to your Garmin data export folder:

```yaml
data_paths:
  raw_data: "path/to/your/garmin/export"  # Change this!
```

### 3. Run the Full Pipeline

Process all datasets at once:

```bash
python etl_pipeline.py
```

Or process specific datasets only:

```bash
python etl_pipeline.py --datasets running_data sleep_data
```

### 4. Test Database Connection

Before running the full pipeline:

```bash
python etl_pipeline.py --test-connection
```

## How The Pipeline Works

### Automated Workflow

The pipeline follows this sequence for **each dataset**:

1. **EXTRACT** 📥
   - Scans your Garmin export folder for JSON files matching the dataset pattern
   - Example: `**/DI-Connect-Wellness/*sleepData.json`
   - Automatically aggregates all date-stamped files
   - Combines into a single DataFrame

2. **TRANSFORM** ⚙️
   - Cleans and standardizes data (date formats, null handling, etc.)
   - Applies business rules specific to each dataset
   - Creates derived columns (e.g., Distance_Group, sleepDurationHours)
   - Removes duplicates

3. **LOAD** 📤
   - Truncates existing table in PostgreSQL (fresh start every time)
   - Bulk inserts transformed data
   - Verifies row counts

### Dataset Patterns

The pipeline automatically finds files using these patterns:

| Dataset | JSON Pattern |
|---------|--------------|
| Running Data | `**/DI-Connect-Fitness/*summarizedActivities*.json` |
| Sleep Data | `**/DI-Connect-Wellness/*sleepData.json` |
| ATL Data | `**/DI-Connect-Metrics/MetricsAcuteTrainingLoad_*.json` |
| MaxMet Data | `**/DI-Connect-Metrics/MetricsMaxMetData_*.json` |
| Race Predictions | `**/DI-Connect-Metrics/RunRacePredictions_*.json` |
| Training History | `**/DI-Connect-Metrics/TrainingHistory_*.json` |
| UDS Data | `**/DI-Connect-Aggregator/UDSFile_*.json` |

**No matter how many date-stamped files you have, the pipeline finds and aggregates them all automatically!**

**Important**: 
- Your Garmin data will be stored in the `garmin` schema
- Your Where2Run app data remains in the `public` schema
- This keeps data organized and allows you to combine them later

⚠️ **Security**: `config.yaml` is gitignored to protect your credentials. Never commit this file!

### 3. Set Up Database Schema

Run the setup script to create the `garmin` schema and all tables:

```bash
python setup_database.py
```

This will:
- Create the `garmin` schema
- Set up all tables (running_data, sleep_data, etc.)
- Create indexes for performance
- Verify the setup was successful

### 4. Test Database Connection

```bash
python etl_pipeline.py --test-connection
```

If successful, you should see:
```
Connection successful. PostgreSQL version: PostgreSQL 14.x ...
```

## Usage

### Run the Pipeline

Process the latest running data export:

```bash
python etl_pipeline.py
```

### Process Specific Dataset

```bash
python etl_pipeline.py --datasets running_data
```

### Verbose Logging

```bash
python etl_pipeline.py --verbose
```

### Command-Line Options

```
--config PATH           Path to config file (default: config.yaml)
--datasets NAMES        Datasets to process: running_data, sleep_data, all
--test-connection       Test database connection and exit
--verbose              Enable detailed logging
```

## How It Works

### Database Schema Organization

Your Azure PostgreSQL database (`Where2run_db2`) now has two schemas:

- **`public` schema**: Your Where2Run app tables (routes, users, etc.)
- **`garmin` schema**: All Garmin fitness data (running, sleep, training)

This separation allows you to:
- Keep concerns organized
- Easily combine data with SQL joins when needed
- Manage permissions separately if needed
- Query each schema independently

Example join for future analysis:
```sql
SELECT 
    r.date,
    r.distance,
    r.avg_pace,
    w.route_name
FROM garmin.running_data r
LEFT JOIN public.routes w ON ST_Contains(w.geom, r.location)
WHERE r.date >= '2024-01-01';
```

### 1. Data Extraction
- Pipeline searches for the latest Garmin export file in the `Raw Data` directory
- Supports pattern matching (e.g., `Running_Data_20250120.csv`)

### 2. Data Transformation
The pipeline applies all transformations from your PBI notebook:
- ✅ Clean column names (spaces → underscores, remove special characters)
- ✅ Drop null columns
- ✅ Create distance groups (0-3 miles, 3-5 miles, etc.)
- ✅ Extract date features (week, month, year)
- ✅ Standardize time formats (pace, duration, idle time)
- ✅ Calculate cumulative metrics (weekly/monthly minutes)
- ✅ Drop unnecessary columns

### 3. Data Loading
- Connects to Azure PostgreSQL using secure SSL connection
- Loads transformed data using efficient bulk insert
- Supports multiple load strategies:
  - `replace`: Drop and recreate table (default)
  - `append`: Add new rows to existing table
  - `fail`: Error if table already exists

### 4. Verification
- Validates row counts after load
- Logs detailed execution summary
- Provides success/failure report

## Adding New Datasets

To add transformation logic for other datasets (Sleep, Training History, etc.):

1. Create a new transformation module (e.g., `transform_sleep_data.py`)
2. Follow the pattern from `transform_running_data.py`
3. Add processor method in `etl_pipeline.py`
4. Update the `available_datasets` dictionary

Example structure:

```python
# transform_sleep_data.py
class SleepDataTransformer:
    def load_data(self, file_path):
        # Load logic
        
    def transform(self):
        # Transformation logic
        
    def get_dataframe(self):
        return self.df
```

## Configuration Options

### Load Strategies

Edit `config.yaml` to change behavior:

```yaml
etl_settings:
  load_strategy: "replace"  # Options: replace, append, upsert
  batch_size: 1000         # Rows per batch insert
  verbose: true            # Enable detailed logging
```

### Table Names

Customize PostgreSQL table names:

```yaml
tables:
  running_data: "running_data"
  sleep_data: "sleep_data"
  # Add more as needed
```

## Logging

All pipeline execution details are logged to:
- Console output (stdout)
- Log file: `etl_pipeline.log`

Log includes:
- Timestamps
- Data transformation steps
- Row counts
- Success/failure status
- Execution duration

## Scheduling (Future Enhancement)

To run automatically on a schedule, you can use:

- **Windows Task Scheduler**
- **Cron jobs** (Linux/Mac)
- **Azure Functions** (cloud-based)
- **Apache Airflow** (enterprise orchestration)

Example cron job (daily at 2 AM):
```bash
0 2 * * * cd /path/to/ETL\ Scripts && python etl_pipeline.py
```

## Troubleshooting

### Connection Issues

**Error**: `connection refused` or `timeout`

- Check firewall rules in Azure PostgreSQL
- Verify your IP is whitelisted
- Confirm SSL is enabled

**Error**: `authentication failed`

- Double-check username/password in `config.yaml`
- Ensure user has proper database permissions

### Data Issues

**Error**: `File not found`

- Verify Raw Data directory path in config
- Check file naming pattern matches

**Error**: `Column not found`

- Garmin export format may have changed
- Update transformation logic accordingly

### Permission Issues

**Error**: `permission denied for table`

- Grant necessary permissions:
```sql
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE running_data TO your_username;
```

## Security Best Practices

1. ✅ Never commit `config.yaml` to version control
2. ✅ Use environment variables for sensitive data
3. ✅ Keep Azure PostgreSQL firewall rules restrictive
4. ✅ Use strong passwords
5. ✅ Enable SSL/TLS connections
6. ✅ Regularly rotate database credentials

## Performance Tips

- Increase `batch_size` for faster loads (1000-5000)
- Use `append` strategy when adding incremental data
- Create indexes on frequently queried columns
- Consider partitioning large tables by date

## Support

For issues or questions:
1. Check the logs: `etl_pipeline.log`
2. Review error messages carefully
3. Test connection first: `python etl_pipeline.py --test-connection`

## Next Steps

1. ✅ Complete Running Data automation
2. 🔜 Add Sleep Data transformations
3. 🔜 Add Training History transformations
4. 🔜 Implement incremental loading (only new data)
5. 🔜 Add data quality checks
6. 🔜 Set up automated scheduling
7. 🔜 Create data validation tests

---

**Last Updated**: December 6, 2025
