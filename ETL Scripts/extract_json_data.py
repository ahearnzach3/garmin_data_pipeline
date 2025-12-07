"""
JSON data extraction module for Garmin summarizedActivities.json
Converts JSON export to CSV format for the ETL pipeline.
"""

import json
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime


logger = logging.getLogger(__name__)


class GarminJSONExtractor:
    """Extract running data from Garmin JSON export."""
    
    def __init__(self):
        """Initialize the extractor."""
        self.data = None
        self.df = None
    
    def load_json(self, json_path: str) -> Dict[Any, Any]:
        """
        Load JSON file from Garmin export.
        
        Args:
            json_path: Path to summarizedActivities.json file
            
        Returns:
            Parsed JSON data
        """
        try:
            logger.info(f"Loading JSON from: {json_path}")
            with open(json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            logger.info(f"Loaded {len(self.data)} activities from JSON")
            return self.data
            
        except Exception as e:
            logger.error(f"Failed to load JSON: {e}")
            raise
    
    def extract_running_activities(self) -> pd.DataFrame:
        """
        Extract running activities from JSON and convert to DataFrame.
        
        Returns:
            DataFrame with running activities
        """
        if self.data is None:
            raise ValueError("No data loaded. Call load_json() first.")
        
        logger.info("Extracting running activities...")
        
        running_activities = []
        
        for activity in self.data:
            # Filter for running activities only
            activity_type = activity.get('activityType', {}).get('typeKey', '')
            
            if 'running' in activity_type.lower() or 'run' in activity_type.lower():
                # Extract relevant fields
                record = {
                    'Activity_Type': activity.get('activityType', {}).get('typeKey', ''),
                    'Date': activity.get('startTimeLocal', ''),
                    'Favorite': activity.get('favorite', False),
                    'Title': activity.get('activityName', ''),
                    'Distance': activity.get('distance', 0) / 1000 if activity.get('distance') else 0,  # meters to km
                    'Calories': activity.get('calories', 0),
                    'Time': self._format_duration(activity.get('duration', 0)),
                    'Avg_HR': activity.get('averageHR', None),
                    'Max_HR': activity.get('maxHR', None),
                    'Aerobic_TE': activity.get('aerobicTrainingEffect', None),
                    'Anaerobic_TE': activity.get('anaerobicTrainingEffect', None),
                    'Avg_Run_Cadence': activity.get('averageRunningCadenceInStepsPerMinute', None),
                    'Max_Run_Cadence': activity.get('maxRunningCadenceInStepsPerMinute', None),
                    'Avg_Pace': self._format_pace(activity.get('averageSpeed', 0)),
                    'Best_Pace': self._format_pace(activity.get('maxSpeed', 0)),
                    'Elev_Gain': activity.get('elevationGain', 0),
                    'Elev_Loss': activity.get('elevationLoss', 0),
                    'Avg_Stride_Length': activity.get('avgStrideLength', None),
                    'Moving_Time': self._format_duration(activity.get('movingDuration', 0)),
                    'Elapsed_Time': self._format_duration(activity.get('elapsedDuration', 0)),
                }
                
                running_activities.append(record)
        
        self.df = pd.DataFrame(running_activities)
        
        logger.info(f"Extracted {len(self.df)} running activities")
        return self.df
    
    def _format_duration(self, seconds: float) -> str:
        """
        Convert seconds to HH:MM:SS format.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted time string
        """
        if not seconds or seconds == 0:
            return "0:00:00"
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        return f"{hours}:{minutes:02d}:{secs:02d}"
    
    def _format_pace(self, speed_mps: float) -> str:
        """
        Convert speed (m/s) to pace (min/km).
        
        Args:
            speed_mps: Speed in meters per second
            
        Returns:
            Pace as MM:SS string
        """
        if not speed_mps or speed_mps == 0:
            return "0:00"
        
        # Convert m/s to min/km
        pace_seconds = 1000 / speed_mps  # seconds per km
        minutes = int(pace_seconds // 60)
        seconds = int(pace_seconds % 60)
        
        return f"{minutes}:{seconds:02d}"
    
    def save_to_csv(self, output_path: str) -> None:
        """
        Save extracted data to CSV file.
        
        Args:
            output_path: Path to save CSV file
        """
        if self.df is None:
            raise ValueError("No data to save. Call extract_running_activities() first.")
        
        logger.info(f"Saving {len(self.df)} records to: {output_path}")
        self.df.to_csv(output_path, index=False)
        logger.info("CSV saved successfully")
    
    def extract_and_save(self, json_path: str, output_path: str) -> pd.DataFrame:
        """
        Complete extraction workflow: load JSON, extract running data, save to CSV.
        
        Args:
            json_path: Path to summarizedActivities.json
            output_path: Path to save CSV
            
        Returns:
            Extracted DataFrame
        """
        self.load_json(json_path)
        self.extract_running_activities()
        self.save_to_csv(output_path)
        return self.df


def find_summarized_activities_json(base_path: str) -> Optional[Path]:
    """
    Search for summarizedActivities.json file in directory structure.
    
    Args:
        base_path: Base directory to search
        
    Returns:
        Path to JSON file if found, None otherwise
    """
    logger.info(f"Searching for summarizedActivities.json in: {base_path}")
    
    search_dir = Path(base_path)
    if not search_dir.exists():
        logger.error(f"Directory not found: {base_path}")
        return None
    
    # Search recursively for the JSON file
    json_files = list(search_dir.rglob("*summarizedActivities.json"))
    
    if not json_files:
        logger.warning("No summarizedActivities.json file found")
        return None
    
    # Return the most recent file if multiple found
    latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"Found JSON file: {latest_file}")
    
    return latest_file


def extract_running_data_from_json(
    base_path: str, 
    output_csv: Optional[str] = None
) -> pd.DataFrame:
    """
    Convenience function to extract running data from JSON export.
    
    Args:
        base_path: Base directory containing Garmin export
        output_csv: Optional path to save CSV
        
    Returns:
        DataFrame with running data
    """
    # Find JSON file
    json_path = find_summarized_activities_json(base_path)
    
    if json_path is None:
        raise FileNotFoundError("No summarizedActivities.json file found")
    
    # Extract data
    extractor = GarminJSONExtractor()
    
    if output_csv:
        df = extractor.extract_and_save(str(json_path), output_csv)
    else:
        extractor.load_json(str(json_path))
        df = extractor.extract_running_activities()
    
    return df
