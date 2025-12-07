"""
JSON File Aggregator for Garmin Data Exports
Finds and combines date-stamped JSON files from Garmin mass exports.
"""

import json
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional, Dict
import glob


logger = logging.getLogger(__name__)


class JSONAggregator:
    """Aggregate multiple date-stamped JSON files into a single DataFrame."""
    
    def __init__(self, base_path: str):
        """
        Initialize aggregator.
        
        Args:
            base_path: Base path to raw data folder (e.g., 'Raw Data' or extracted zip location)
        """
        self.base_path = Path(base_path)
        
    def find_files(self, pattern: str) -> List[Path]:
        """
        Find all JSON files matching a pattern.
        
        Args:
            pattern: Glob pattern to search for files
                    Example: '**/DI-Connect-Wellness/*sleepData.json'
                    
        Returns:
            List of Path objects for matching files
        """
        search_pattern = str(self.base_path / pattern)
        files = glob.glob(search_pattern, recursive=True)
        file_paths = [Path(f) for f in files]
        
        logger.info(f"Found {len(file_paths)} files matching pattern: {pattern}")
        for file_path in file_paths:
            logger.info(f"  - {file_path.name}")
            
        return sorted(file_paths)
    
    def aggregate_json_files(
        self, 
        pattern: str,
        dataset_name: str = "dataset"
    ) -> Optional[pd.DataFrame]:
        """
        Find and aggregate all JSON files matching a pattern into one DataFrame.
        
        Args:
            pattern: Glob pattern to search for files
            dataset_name: Name of dataset for logging purposes
            
        Returns:
            Combined DataFrame or None if no files found
        """
        logger.info(f"Aggregating {dataset_name} files...")
        
        # Find all matching files
        files = self.find_files(pattern)
        
        if not files:
            logger.warning(f"No files found for {dataset_name} with pattern: {pattern}")
            return None
        
        # Load and combine all JSON files
        dataframes = []
        for file_path in files:
            try:
                logger.info(f"Loading: {file_path.name}")
                df = pd.read_json(file_path)
                logger.info(f"  Loaded {len(df)} records")
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Failed to load {file_path.name}: {e}")
                continue
        
        if not dataframes:
            logger.error(f"No dataframes successfully loaded for {dataset_name}")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined {dataset_name}: {len(combined_df)} total records from {len(dataframes)} files")
        
        return combined_df
    
    def aggregate_multiple_datasets(
        self, 
        dataset_patterns: Dict[str, str]
    ) -> Dict[str, pd.DataFrame]:
        """
        Aggregate multiple datasets at once.
        
        Args:
            dataset_patterns: Dictionary mapping dataset names to glob patterns
            
        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        results = {}
        
        for dataset_name, pattern in dataset_patterns.items():
            df = self.aggregate_json_files(pattern, dataset_name)
            if df is not None:
                results[dataset_name] = df
            else:
                logger.warning(f"Skipping {dataset_name} - no data found")
        
        return results


def aggregate_garmin_data(raw_data_path: str) -> Dict[str, pd.DataFrame]:
    """
    Aggregate all Garmin datasets from a mass export.
    
    Args:
        raw_data_path: Path to raw data folder containing Garmin export
        
    Returns:
        Dictionary of DataFrames for each dataset
    """
    aggregator = JSONAggregator(raw_data_path)
    
    # Define all dataset patterns
    dataset_patterns = {
        'running_data': '**/DI-Connect-Fitness/*summarizedActivities*.json',
        'sleep_data': '**/DI-Connect-Wellness/*sleepData.json',
        'atl_data': '**/DI-Connect-Metrics/MetricsAcuteTrainingLoad_*.json',
        'maxmet_data': '**/DI-Connect-Metrics/MetricsMaxMetData_*.json',
        'race_predictions': '**/DI-Connect-Metrics/RunRacePredictions_*.json',
        'training_history': '**/DI-Connect-Metrics/TrainingHistory_*.json',
        'uds_data': '**/DI-Connect-Aggregator/UDSFile_*.json'
    }
    
    return aggregator.aggregate_multiple_datasets(dataset_patterns)


if __name__ == '__main__':
    # Test the aggregator
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) > 1:
        raw_data_path = sys.argv[1]
    else:
        raw_data_path = 'Raw Data'
    
    logger.info(f"Testing aggregation from: {raw_data_path}")
    results = aggregate_garmin_data(raw_data_path)
    
    logger.info("\n" + "="*60)
    logger.info("AGGREGATION SUMMARY")
    logger.info("="*60)
    for dataset_name, df in results.items():
        logger.info(f"{dataset_name}: {len(df)} records, {len(df.columns)} columns")
