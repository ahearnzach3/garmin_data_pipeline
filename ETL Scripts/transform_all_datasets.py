"""
Transformation Functions for All Garmin Datasets
Based on the 2024 Marathon Training PBI Data Wrangling notebook.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional


logger = logging.getLogger(__name__)


def clean_timedelta_to_string(val):
    """
    Convert timedelta to string format (HH:MM:SS) for PBI compatibility.
    
    Args:
        val: Value to convert
        
    Returns:
        String formatted as HH:MM:SS or original value
    """
    if isinstance(val, pd.Timedelta):
        # Convert timedelta to string and remove '0 days' part
        clean_value = str(val).split(' ')[-1]  # Keep only the 'hh:mm:ss' part
        return clean_value
    return val


def transform_running_data_full(df: pd.DataFrame) -> pd.DataFrame:
    """
    Complete transformation for running data (extends existing transform_running_data.py).
    
    Args:
        df: Raw running data DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming running data...")
    df_cleaned = df.copy()
    
    # Drop columns that are entirely null
    logger.info(f"Original shape: {df_cleaned.shape}")
    df_cleaned = df_cleaned.dropna(axis=1, how='all')
    logger.info(f"After dropping null columns: {df_cleaned.shape}")
    
    # Clean column headers
    df_cleaned.columns = df_cleaned.columns.str.replace(' ', '_')
    df_cleaned.columns = df_cleaned.columns.str.replace(r'[^A-Za-z0-9_]+', '', regex=True)
    
    # Convert Date column
    if 'Date' in df_cleaned.columns:
        df_cleaned['Date'] = pd.to_datetime(df_cleaned['Date'])
    
    # Create Distance_Group if not exists
    if 'Distance' in df_cleaned.columns and 'Distance_Group' not in df_cleaned.columns:
        bins = [0, 3, 5, 7, 10, 13, float('inf')]
        labels = ['0-3 miles', '3-5 miles', '5-7 miles', '7-10 miles', '10-13 miles', '13+ miles']
        df_cleaned['Distance_Group'] = pd.cut(df_cleaned['Distance'], bins=bins, labels=labels, right=False)
        
        # Create DistanceGroupId for sorting
        distance_group_mapping = {
            "0-3 miles": 1, "3-5 miles": 2, "5-7 miles": 3,
            "7-10 miles": 4, "10-13 miles": 5, "13+ miles": 6
        }
        df_cleaned['DistanceGroupId'] = df_cleaned['Distance_Group'].map(distance_group_mapping)
    
    # Clean timedelta columns for PBI
    time_cols = ['Time', 'Avg_Pace', 'Weekly_Cumulative_Mins', 'Monthly_Cumulative_Mins', 
                 'Best_Pace', 'Moving_Time', 'Elapsed_Time', 'Idle_Time']
    for col in time_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_timedelta_to_string)
    
    logger.info(f"Running data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_sleep_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform sleep data from Garmin export.
    
    Args:
        df: Raw sleep data DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming sleep data...")
    df_cleaned = df.copy()
    
    # Convert calendarDate to datetime
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['calendarDate'])
    
    # Expand sleepScores dictionary into separate columns
    if 'sleepScores' in df_cleaned.columns:
        df_cleaned = df_cleaned.join(df_cleaned['sleepScores'].apply(pd.Series))
        df_cleaned = df_cleaned.drop('sleepScores', axis=1)
    
    # Calculate sleep duration
    if 'sleepStartTimestampGMT' in df_cleaned.columns and 'sleepEndTimestampGMT' in df_cleaned.columns:
        df_cleaned['sleepStartTimestampGMT'] = pd.to_datetime(df_cleaned['sleepStartTimestampGMT'])
        df_cleaned['sleepEndTimestampGMT'] = pd.to_datetime(df_cleaned['sleepEndTimestampGMT'])
        df_cleaned['sleepDuration'] = df_cleaned['sleepEndTimestampGMT'] - df_cleaned['sleepStartTimestampGMT']
        df_cleaned['sleepDurationHours'] = (df_cleaned['sleepDuration'].dt.total_seconds() / 3600).round(1)
        
        # Drop timestamp columns
        df_cleaned = df_cleaned.drop(['sleepStartTimestampGMT', 'sleepEndTimestampGMT'], axis=1)
    
    # Fill null values in float columns with mean
    float_columns = [col for col in df_cleaned.columns if df_cleaned[col].dtype == 'float64']
    for col in float_columns:
        if df_cleaned[col].isna().sum() > 0:
            mean_value = df_cleaned[col].mean()
            df_cleaned[col] = df_cleaned[col].fillna(mean_value)
    
    # Format sleepDuration for PBI
    if 'sleepDuration' in df_cleaned.columns:
        df_cleaned['sleepDuration'] = df_cleaned['sleepDuration'].dt.round('s')
        df_cleaned['sleepDurationFormatted'] = df_cleaned['sleepDuration'].apply(lambda x: str(x).split(' ')[-1])
    
    # Fill text columns
    if 'insight' in df_cleaned.columns:
        df_cleaned['insight'] = df_cleaned['insight'].fillna("NONE")
    if 'feedback' in df_cleaned.columns:
        df_cleaned['feedback'] = df_cleaned['feedback'].fillna("NONE")
    
    # Convert seconds columns to hours
    columns_to_convert = [col for col in df_cleaned.columns if 'Seconds' in col]
    for col in columns_to_convert:
        df_cleaned[col] = (df_cleaned[col] / 3600).round(1)
    
    # Rename columns from Seconds to Hours
    df_cleaned = df_cleaned.rename(columns={col: col.replace('Seconds', 'Hours') 
                                            for col in df_cleaned.columns if 'Seconds' in col})
    
    # Drop unnecessary columns
    cols_to_drop = [col for col in ['sleepWindowConfirmationType', 'retro', 'napList'] 
                    if col in df_cleaned.columns]
    if cols_to_drop:
        df_cleaned = df_cleaned.drop(cols_to_drop, axis=1)
    
    logger.info(f"Sleep data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_atl_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Acute Training Load data.
    
    Args:
        df: Raw ATL DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming ATL data...")
    df_cleaned = df.copy()
    
    # Filter out NONE status records
    if 'acwrStatus' in df_cleaned.columns:
        initial_count = len(df_cleaned)
        df_cleaned = df_cleaned[df_cleaned['acwrStatus'] != "NONE"]
        logger.info(f"Filtered out {initial_count - len(df_cleaned)} NONE status records")
    
    # Convert calendarDate using timestamp
    if 'timestamp' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['timestamp']).dt.date
    
    # Drop unnecessary columns
    cols_to_drop = [col for col in ['deviceId'] if col in df_cleaned.columns]
    if cols_to_drop:
        df_cleaned = df_cleaned.drop(cols_to_drop, axis=1)
    
    # Drop rows with null critical values
    if 'dailyAcuteChronicWorkloadRatio' in df_cleaned.columns:
        df_cleaned = df_cleaned[df_cleaned['dailyAcuteChronicWorkloadRatio'].notna()]
    
    # Remove duplicates, keeping most recent record per date
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned = df_cleaned.sort_values('timestamp', ascending=False)
        df_cleaned = df_cleaned.drop_duplicates(subset='calendarDate', keep='first')
    
    logger.info(f"ATL data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_maxmet_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform MaxMet data.
    
    Args:
        df: Raw MaxMet DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming MaxMet data...")
    df_cleaned = df.copy()
    
    # Convert calendarDate
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['calendarDate']).dt.date
    elif 'timestamp' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['timestamp']).dt.date
    
    # Remove duplicates
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop_duplicates(subset='calendarDate', keep='first')
    
    logger.info(f"MaxMet data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_race_predictions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Race Predictions data.
    
    Args:
        df: Raw Race Predictions DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming Race Predictions data...")
    df_cleaned = df.copy()
    
    # Convert calendarDate
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['calendarDate']).dt.date
    elif 'timestamp' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['timestamp']).dt.date
    
    # Remove duplicates, keeping minimum race time per date/distance
    if 'calendarDate' in df_cleaned.columns and 'raceDistance' in df_cleaned.columns:
        df_cleaned = df_cleaned.sort_values(['calendarDate', 'raceDistance', 'raceTime'])
        df_cleaned = df_cleaned.drop_duplicates(subset=['calendarDate', 'raceDistance'], keep='first')
    
    logger.info(f"Race Predictions data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_training_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Training History data.
    
    Args:
        df: Raw Training History DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming Training History data...")
    df_cleaned = df.copy()
    
    # Convert calendarDate
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['calendarDate']).dt.date
    elif 'timestamp' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['timestamp']).dt.date
    
    # Remove duplicates
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop_duplicates(subset='calendarDate', keep='first')
    
    logger.info(f"Training History data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_uds_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform UDS (User Daily Summary) data.
    
    Args:
        df: Raw UDS DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming UDS data...")
    df_cleaned = df.copy()
    
    # Convert calendarDate
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned['calendarDate'] = pd.to_datetime(df_cleaned['calendarDate'])
    
    # Remove duplicates
    if 'calendarDate' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop_duplicates(subset='calendarDate', keep='first')
    
    logger.info(f"UDS data transformed: {len(df_cleaned)} records")
    return df_cleaned


def transform_summarized_activities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform summarized activities data (ALL activity types).
    
    Args:
        df: Raw summarized activities DataFrame
        
    Returns:
        Transformed DataFrame
    """
    logger.info("Transforming summarized activities...")
    df_cleaned = df.copy()
    
    # Convert timestamps
    date_columns = ['beginTimestamp', 'startTimeGmt', 'startTimeLocal']
    for col in date_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = pd.to_datetime(df_cleaned[col], unit='ms', errors='coerce')
    
    # Convert distances from centimeters to kilometers
    distance_columns = ['distance']
    for col in distance_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col] / 100000  # cm to km
    
    # Convert durations from milliseconds to seconds
    duration_columns = ['duration', 'elapsedDuration', 'movingDuration']
    for col in duration_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col] / 1000  # ms to seconds
    
    # Convert speeds from cm/ms to m/s
    speed_columns = ['avgSpeed', 'maxSpeed']
    for col in speed_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col] * 10  # cm/ms to m/s
    
    # Convert elevations from centimeters to meters
    elevation_columns = ['elevationGain', 'elevationLoss', 'minElevation', 'maxElevation']
    for col in elevation_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col] / 100  # cm to meters
    
    # Remove duplicates by activityId
    if 'activityId' in df_cleaned.columns:
        df_cleaned = df_cleaned.drop_duplicates(subset='activityId', keep='first')
    
    logger.info(f"Summarized activities transformed: {len(df_cleaned)} records")
    return df_cleaned


# Mapping of dataset names to transformation functions
TRANSFORM_FUNCTIONS = {
    'running_data': transform_running_data_full,
    'summarized_activities': transform_summarized_activities,
    'sleep_data': transform_sleep_data,
    'atl_data': transform_atl_data,
    'maxmet_data': transform_maxmet_data,
    'race_predictions': transform_race_predictions,
    'training_history': transform_training_history,
    'uds_data': transform_uds_data
}


def transform_dataset(dataset_name: str, df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Transform a dataset using the appropriate transformation function.
    
    Args:
        dataset_name: Name of the dataset
        df: Raw DataFrame
        
    Returns:
        Transformed DataFrame or None if transformation fails
    """
    if dataset_name not in TRANSFORM_FUNCTIONS:
        logger.warning(f"No transformation function found for {dataset_name}")
        return df
    
    try:
        transform_func = TRANSFORM_FUNCTIONS[dataset_name]
        return transform_func(df)
    except Exception as e:
        logger.error(f"Failed to transform {dataset_name}: {e}")
        return None
