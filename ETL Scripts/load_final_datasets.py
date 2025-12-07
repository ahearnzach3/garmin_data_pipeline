"""
Load Final Dataset CSVs directly to PostgreSQL.
This script loads pre-processed CSV files from the Final Datasets folder.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from db_utils import DatabaseManager, setup_logging


logger = logging.getLogger(__name__)


class FinalDatasetLoader:
    """Load final processed CSV files to PostgreSQL."""
    
    def __init__(self, db_manager: DatabaseManager, final_datasets_path: str):
        """
        Initialize loader.
        
        Args:
            db_manager: DatabaseManager instance
            final_datasets_path: Path to Final Datasets folder
        """
        self.db_manager = db_manager
        self.final_datasets_path = Path(final_datasets_path)
        self.schema = db_manager.config['database'].get('schema', 'garmin')
    
    def load_csv_to_table(
        self, 
        csv_file: str, 
        table_name: str,
        if_exists: str = 'replace'
    ) -> bool:
        """
        Load a single CSV file to PostgreSQL table.
        
        Args:
            csv_file: Name of CSV file in Final Datasets folder
            table_name: Target table name
            if_exists: How to handle existing table ('replace', 'append', 'fail')
            
        Returns:
            True if successful, False otherwise
        """
        csv_path = self.final_datasets_path / csv_file
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return False
        
        try:
            logger.info(f"Loading {csv_file} to {self.schema}.{table_name}")
            
            # Read CSV
            df = pd.read_csv(csv_path)
            logger.info(f"Read {len(df)} rows, {len(df.columns)} columns from {csv_file}")
            
            # For running_data, drop the view first if it exists
            if table_name == 'running_data' and if_exists == 'replace':
                try:
                    self.db_manager.execute_query('DROP VIEW IF EXISTS garmin.running_summary CASCADE')
                    logger.info("Dropped dependent view garmin.running_summary")
                except Exception as e:
                    logger.warning(f"Could not drop view: {e}")
            
            # Load to database
            self.db_manager.load_dataframe(df, table_name, if_exists=if_exists)
            
            # Verify
            row_count = self.db_manager.get_row_count(table_name)
            logger.info(f"✓ Loaded {row_count} rows to {self.schema}.{table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {csv_file}: {e}")
            return False
    
    def load_all_datasets(self, if_exists: str = 'replace') -> Dict[str, bool]:
        """
        Load all final datasets to PostgreSQL.
        
        Args:
            if_exists: How to handle existing tables
            
        Returns:
            Dictionary mapping dataset name to success status
        """
        # Map CSV files to table names
        datasets = {
            'Running_Data_Cleaned_PBI_Final.csv': 'running_data',
            'Sleep_Cleaned_PBI_Final.csv': 'sleep_data',
            'ATL_Cleaned_PBI_Final.csv': 'atl_data',
            'MaxMet_Cleaned_PBI_Final.csv': 'maxmet_data',
            'RacePredictions_Cleaned_PBI_Final.csv': 'race_predictions',
            'TrainingHistory_Cleaned_PBI_Final.csv': 'training_history',
            'Training_Plan_PBI_Final.csv': 'training_plan',
            'UDS_Cleaned_PBI_Final.csv': 'uds_data',
        }
        
        results = {}
        
        logger.info("=" * 60)
        logger.info("LOADING FINAL DATASETS TO POSTGRESQL")
        logger.info("=" * 60)
        
        for csv_file, table_name in datasets.items():
            csv_path = self.final_datasets_path / csv_file
            
            if not csv_path.exists():
                logger.warning(f"Skipping {csv_file} - file not found")
                results[table_name] = False
                continue
            
            success = self.load_csv_to_table(csv_file, table_name, if_exists)
            results[table_name] = success
        
        # Summary
        logger.info("=" * 60)
        logger.info("LOAD SUMMARY")
        logger.info("=" * 60)
        successful = sum(1 for v in results.values() if v)
        total = len(results)
        logger.info(f"Successfully loaded: {successful}/{total} datasets")
        
        for table_name, success in results.items():
            status = "✓" if success else "✗"
            logger.info(f"  {status} {table_name}")
        
        return results


def main():
    """Load all final datasets to PostgreSQL."""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Load Final Dataset CSVs to PostgreSQL'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--if-exists',
        choices=['replace', 'append', 'fail'],
        default='replace',
        help='How to handle existing tables'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=True)
    
    try:
        # Initialize database manager
        logger.info("Connecting to database...")
        db_manager = DatabaseManager(args.config)
        
        # Test connection
        if not db_manager.test_connection():
            logger.error("Database connection failed")
            sys.exit(1)
        
        # Get Final Datasets path from config
        final_datasets_path = db_manager.config['data_paths']['final_datasets']
        
        # Load datasets
        loader = FinalDatasetLoader(db_manager, final_datasets_path)
        results = loader.load_all_datasets(if_exists=args.if_exists)
        
        # Exit with appropriate code
        all_success = all(results.values())
        sys.exit(0 if all_success else 1)
        
    except Exception as e:
        logger.error(f"Load failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
