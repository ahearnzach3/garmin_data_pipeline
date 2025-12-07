"""
Data transformation module for Running Data ETL pipeline.
Contains all transformation logic from the PBI Data Wrangling notebook.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional


logger = logging.getLogger(__name__)


class RunningDataTransformer:
    """Handles all transformations for Running Data."""
    
    def __init__(self):
        """Initialize the transformer."""
        self.df = None
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load running data from CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Loaded DataFrame
        """
        try:
            logger.info(f"Loading data from {file_path}")
            self.df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(self.df)} rows with {len(self.df.columns)} columns")
            return self.df
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def clean_columns(self) -> None:
        """Remove null columns and clean column names."""
        logger.info("Cleaning columns...")
        
        # Drop columns that are entirely null
        original_cols = len(self.df.columns)
        self.df = self.df.dropna(axis=1, how='all')
        dropped_cols = original_cols - len(self.df.columns)
        logger.info(f"Dropped {dropped_cols} null columns")
        
        # Add underscores and remove special characters
        self.df.columns = self.df.columns.str.replace(' ', '_')
        self.df.columns = self.df.columns.str.replace(r'[^A-Za-z0-9_]+', '', regex=True)
        
        logger.info("Column names cleaned")
    
    def create_distance_group(self) -> None:
        """Create distance group categorization."""
        logger.info("Creating distance groups...")
        
        bins = [0, 3, 5, 7, 10, 13, float('inf')]
        labels = ['0-3 miles', '3-5 miles', '5-7 miles', '7-10 miles', '10-13 miles', '13+ miles']
        
        self.df['Distance_Group'] = pd.cut(
            self.df['Distance'], 
            bins=bins, 
            labels=labels, 
            right=False
        )
        
        # Create DistanceGroupId for sorting
        distance_group_mapping = {
            "0-3 miles": 1,
            "3-5 miles": 2,
            "5-7 miles": 3,
            "7-10 miles": 4,
            "10-13 miles": 5,
            "13+ miles": 6
        }
        self.df['DistanceGroupId'] = self.df['Distance_Group'].map(distance_group_mapping)
        
        # Reorder columns to place Distance_Group and DistanceGroupId after Distance
        columns = self.df.columns.to_list()
        distance_index = columns.index('Distance')
        columns.insert(distance_index + 1, columns.pop(columns.index('Distance_Group')))
        columns.insert(distance_index + 2, columns.pop(columns.index('DistanceGroupId')))
        self.df = self.df[columns]
        
        logger.info("Distance groups created")
    
    def process_date_features(self) -> None:
        """Create date-related features."""
        logger.info("Processing date features...")
        
        # Convert Date to datetime and normalize
        self.df['Date'] = pd.to_datetime(self.df['Date']).dt.normalize()
        
        # Create temporal features
        self.df['Week_of_Year'] = self.df['Date'].dt.isocalendar().week
        self.df['Month_Numeric'] = self.df['Date'].dt.month
        self.df['Month'] = self.df['Date'].dt.strftime('%b')
        self.df['Year'] = self.df['Date'].dt.year
        
        # Reorder columns
        columns = self.df.columns.to_list()
        date_index = columns.index('Date')
        columns.insert(date_index + 1, columns.pop(columns.index('Month_Numeric')))
        columns.insert(date_index + 2, columns.pop(columns.index('Month')))
        columns.insert(date_index + 3, columns.pop(columns.index('Year')))
        columns.insert(date_index + 4, columns.pop(columns.index('Week_of_Year')))
        self.df = self.df[columns]
        
        logger.info("Date features created")
    
    def drop_unnecessary_columns(self) -> None:
        """Drop columns not needed for analysis."""
        logger.info("Dropping unnecessary columns...")
        
        cols_to_drop = []
        
        # Check which columns exist before dropping
        if 'Favorite' in self.df.columns:
            cols_to_drop.append('Favorite')
        if 'Best_Lap_Time' in self.df.columns:
            cols_to_drop.append('Best_Lap_Time')
        if 'Number_of_Laps' in self.df.columns:
            cols_to_drop.append('Number_of_Laps')
        if 'Avg_GAP' in self.df.columns:
            cols_to_drop.append('Avg_GAP')
        
        if cols_to_drop:
            self.df = self.df.drop(cols_to_drop, axis=1)
            logger.info(f"Dropped columns: {', '.join(cols_to_drop)}")
    
    def clean_time_formats(self) -> None:
        """Clean and standardize time format columns."""
        logger.info("Cleaning time formats...")
        
        def drop_milliseconds(val):
            """Drop milliseconds from time strings."""
            if isinstance(val, str) and '.' in val:
                minutes, _ = val.split('.')
                return minutes
            return val
        
        def convert_mmss_to_seconds(val):
            """Convert mm:ss format to seconds."""
            if isinstance(val, str) and ':' in val:
                parts = val.split(':')
                if len(parts) == 2:
                    minutes, seconds = parts
                    return int(minutes) * 60 + int(seconds)
            return val
        
        def standardize_time_format(val):
            """Standardize time format to h:mm:ss."""
            if isinstance(val, str) and ':' in val:
                parts = val.split(':')
                if len(parts) == 2:  # mm:ss format
                    return f'0:{parts[0]}:{parts[1]}'
                elif len(parts) == 3:  # Already h:mm:ss
                    return val
            return val
        
        # Process pace columns
        pace_cols = ['Avg_Pace', 'Best_Pace']
        for col in pace_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(drop_milliseconds)
                self.df[col] = self.df[col].apply(convert_mmss_to_seconds)
                self.df[col] = pd.to_timedelta(self.df[col], unit='s')
        
        # Process duration columns
        time_cols = ['Time', 'Moving_Time', 'Elapsed_Time']
        for col in time_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(drop_milliseconds)
                self.df[col] = self.df[col].apply(standardize_time_format)
                self.df[col] = pd.to_timedelta(self.df[col], errors='coerce')
        
        # Calculate Idle_Time
        if all(col in self.df.columns for col in ['Elapsed_Time', 'Moving_Time']):
            self.df['Idle_Time'] = self.df['Elapsed_Time'] - self.df['Moving_Time']
        
        logger.info("Time formats cleaned")
    
    def create_cumulative_features(self) -> None:
        """Create weekly and monthly cumulative minutes features."""
        logger.info("Creating cumulative features...")
        
        if 'Time' in self.df.columns:
            # Weekly cumulative minutes
            self.df['Weekly_Cumulative_Mins'] = self.df.groupby(['Year', 'Week_of_Year'])['Time'].cumsum()
            self.df['Weekly_Mins_Prior_to_Run'] = self.df.groupby(['Year', 'Week_of_Year'])['Weekly_Cumulative_Mins'].shift(1, fill_value=pd.Timedelta(0))
            self.df['Weekly_Mins_Prior_to_Run'] = (self.df['Weekly_Mins_Prior_to_Run'].dt.total_seconds() / 60).round(2)
            
            # Monthly cumulative minutes
            self.df['Monthly_Cumulative_Mins'] = self.df.groupby(['Year', 'Month'])['Time'].cumsum()
            self.df['Monthly_Mins_Prior_to_Run'] = self.df.groupby(['Year', 'Month'])['Monthly_Cumulative_Mins'].shift(1, fill_value=pd.Timedelta(0))
            self.df['Monthly_Mins_Prior_to_Run'] = (self.df['Monthly_Mins_Prior_to_Run'].dt.total_seconds() / 60).round(2)
            
            logger.info("Cumulative features created")
    
    def transform(self, file_path: str) -> pd.DataFrame:
        """
        Execute full transformation pipeline.
        
        Args:
            file_path: Path to input CSV file
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting Running Data transformation pipeline...")
        
        # Execute transformation steps
        self.load_data(file_path)
        self.clean_columns()
        self.create_distance_group()
        self.process_date_features()
        self.drop_unnecessary_columns()
        self.clean_time_formats()
        self.create_cumulative_features()
        
        logger.info("Transformation pipeline completed successfully")
        logger.info(f"Final shape: {self.df.shape}")
        
        return self.df


def transform_running_data(input_file: str, output_file: Optional[str] = None) -> pd.DataFrame:
    """
    Convenience function to transform running data.
    
    Args:
        input_file: Path to input CSV file
        output_file: Optional path to save transformed data
        
    Returns:
        Transformed DataFrame
    """
    transformer = RunningDataTransformer()
    df = transformer.transform(input_file)
    
    if output_file:
        logger.info(f"Saving transformed data to {output_file}")
        df.to_csv(output_file, index=False)
    
    return df
