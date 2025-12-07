"""
Main ETL Pipeline Orchestrator
Automates the extraction, transformation, and loading of Garmin data to Azure PostgreSQL.
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import sys

from db_utils import DatabaseManager, setup_logging
from transform_running_data import transform_running_data
from extract_json_data import extract_running_data_from_json


class GarminETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize ETL pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseManager(config_path)
        self.config = self.db_manager.config
        self.results = {
            'success': [],
            'failed': [],
            'start_time': datetime.now()
        }
    
    def test_database_connection(self) -> bool:
        """
        Test database connectivity before running pipeline.
        
        Returns:
            True if connection successful, False otherwise
        """
        self.logger.info("Testing database connection...")
        return self.db_manager.test_connection()
    
    def find_latest_file(self, directory: str, pattern: str) -> Optional[Path]:
        """
        Find the most recent file matching a pattern in a directory.
        
        Args:
            directory: Directory to search
            pattern: File pattern (e.g., 'Running_Data_*.csv')
            
        Returns:
            Path to most recent file, or None if not found
        """
        search_dir = Path(directory)
        if not search_dir.exists():
            self.logger.error(f"Directory not found: {directory}")
            return None
        
        files = list(search_dir.glob(pattern))
        if not files:
            self.logger.warning(f"No files matching '{pattern}' found in {directory}")
            return None
        
        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        self.logger.info(f"Found latest file: {latest_file.name}")
        return latest_file
    
    def process_running_data(self, input_file: Optional[str] = None) -> bool:
        """
        Process and load running data to database.
        
        Args:
            input_file: Path to input file. If None, searches for JSON or CSV file.
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Processing Running Data")
            self.logger.info("=" * 60)
            
            df = None
            
            # Find input file if not specified
            if input_file is None:
                raw_data_dir = self.config['data_paths']['raw_data']
                
                # First, try to find JSON file (preferred method)
                self.logger.info("Looking for Garmin JSON export...")
                try:
                    df = extract_running_data_from_json(raw_data_dir)
                    self.logger.info("Successfully extracted data from JSON")
                except Exception as json_error:
                    self.logger.warning(f"JSON extraction failed: {json_error}")
                    
                    # Fallback to CSV file
                    self.logger.info("Falling back to CSV file search...")
                    input_file = self.find_latest_file(raw_data_dir, 'Running_Data_*.csv')
                    
                    if input_file is None:
                        raise FileNotFoundError(
                            "No JSON or CSV running data file found. "
                            "Expected: summarizedActivities.json or Running_Data_*.csv"
                        )
                    
                    # Transform CSV data
                    self.logger.info(f"Transforming data from CSV: {input_file}")
                    df = transform_running_data(str(input_file))
            else:
                # Use specified file
                self.logger.info(f"Transforming data from: {input_file}")
                df = transform_running_data(str(input_file))
            
            # If we extracted from JSON, still need to apply transformations
            if df is not None and 'Distance_Group' not in df.columns:
                self.logger.info("Applying transformations to extracted data...")
                # Save to temp CSV and transform
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
                    df.to_csv(tmp.name, index=False)
                    df = transform_running_data(tmp.name)
            
            # Load to database
            table_name = self.config['tables']['running_data']
            load_strategy = self.config['etl_settings']['load_strategy']
            
            self.logger.info(f"Loading to table: {table_name}")
            self.db_manager.load_dataframe(df, table_name, if_exists=load_strategy)
            
            # Verify load
            row_count = self.db_manager.get_row_count(table_name)
            self.logger.info(f"Verification: {row_count} rows in {table_name}")
            
            self.results['success'].append({
                'dataset': 'running_data',
                'rows': len(df),
                'table': table_name
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process running data: {e}")
            self.results['failed'].append({
                'dataset': 'running_data',
                'error': str(e)
            })
            return False
    
    def process_sleep_data(self, input_file: Optional[str] = None) -> bool:
        """
        Process and load sleep data to database.
        
        Args:
            input_file: Path to input file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Processing Sleep Data")
            self.logger.info("=" * 60)
            
            # TODO: Implement sleep data transformation
            # This is a placeholder for future implementation
            self.logger.warning("Sleep data processing not yet implemented")
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to process sleep data: {e}")
            self.results['failed'].append({
                'dataset': 'sleep_data',
                'error': str(e)
            })
            return False
    
    def run_full_pipeline(self, datasets: Optional[List[str]] = None) -> Dict:
        """
        Run the complete ETL pipeline for all or specified datasets.
        
        Args:
            datasets: List of dataset names to process. If None, processes all.
            
        Returns:
            Dictionary with pipeline results
        """
        self.logger.info("=" * 60)
        self.logger.info("GARMIN DATA ETL PIPELINE STARTED")
        self.logger.info("=" * 60)
        
        # Test database connection first
        if not self.test_database_connection():
            self.logger.error("Database connection failed. Aborting pipeline.")
            return self.results
        
        # Define available datasets
        available_datasets = {
            'running_data': self.process_running_data,
            'sleep_data': self.process_sleep_data,
            # Add more datasets as they are implemented
        }
        
        # Determine which datasets to process
        if datasets is None:
            datasets = ['running_data']  # Default to running_data only for now
        
        # Process each dataset
        for dataset_name in datasets:
            if dataset_name in available_datasets:
                processor = available_datasets[dataset_name]
                processor()
            else:
                self.logger.warning(f"Unknown dataset: {dataset_name}")
        
        # Calculate summary
        self.results['end_time'] = datetime.now()
        self.results['duration'] = (
            self.results['end_time'] - self.results['start_time']
        ).total_seconds()
        
        # Print summary
        self._print_summary()
        
        return self.results
    
    def _print_summary(self) -> None:
        """Print pipeline execution summary."""
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        
        self.logger.info(f"Duration: {self.results['duration']:.2f} seconds")
        self.logger.info(f"Successful: {len(self.results['success'])}")
        self.logger.info(f"Failed: {len(self.results['failed'])}")
        
        if self.results['success']:
            self.logger.info("\nSuccessfully processed:")
            for item in self.results['success']:
                self.logger.info(
                    f"  - {item['dataset']}: {item['rows']} rows → {item['table']}"
                )
        
        if self.results['failed']:
            self.logger.error("\nFailed datasets:")
            for item in self.results['failed']:
                self.logger.error(f"  - {item['dataset']}: {item['error']}")
        
        self.logger.info("=" * 60)


def main():
    """Main entry point for ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Garmin Data ETL Pipeline - Load data to Azure PostgreSQL'
    )
    
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        choices=['running_data', 'sleep_data', 'all'],
        default=['running_data'],
        help='Datasets to process (default: running_data)'
    )
    
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Test database connection and exit'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose or True)
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize pipeline
        pipeline = GarminETLPipeline(config_path=args.config)
        
        # Test connection only
        if args.test_connection:
            success = pipeline.test_database_connection()
            sys.exit(0 if success else 1)
        
        # Handle 'all' datasets option
        datasets = None if 'all' in args.datasets else args.datasets
        
        # Run pipeline
        results = pipeline.run_full_pipeline(datasets=datasets)
        
        # Exit with appropriate code
        sys.exit(0 if not results['failed'] else 1)
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Make sure config.yaml exists (copy from config.template.yaml)")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
