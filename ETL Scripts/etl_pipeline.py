"""
Comprehensive Garmin Data ETL Pipeline
Automates extraction, transformation, and loading of all Garmin datasets to PostgreSQL.

This pipeline:
1. Finds and aggregates date-stamped JSON files from Garmin exports
2. Transforms data according to business rules
3. Loads data to Azure PostgreSQL database (truncate and reload)
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import sys

from db_utils import DatabaseManager, setup_logging
from aggregate_json_files import JSONAggregator
from transform_all_datasets import transform_dataset, TRANSFORM_FUNCTIONS


class GarminETLPipeline:
    """Comprehensive ETL pipeline orchestrator for all Garmin datasets."""
    
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
    
    def process_dataset(
        self, 
        dataset_name: str,
        force_pattern: Optional[str] = None
    ) -> bool:
        """
        Process a single dataset: Extract → Transform → Load.
        
        Args:
            dataset_name: Name of the dataset to process
            force_pattern: Optional pattern override for testing
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info(f"Processing {dataset_name.upper()}")
            self.logger.info("=" * 60)
            
            # Get configuration
            raw_data_path = self.config['data_paths']['raw_data']
            table_name = self.config['tables'].get(dataset_name)
            pattern = force_pattern or self.config['dataset_patterns'].get(dataset_name)
            load_strategy = self.config['etl_settings']['load_strategy']
            
            if not table_name:
                raise ValueError(f"No table configured for {dataset_name}")
            if not pattern:
                raise ValueError(f"No pattern configured for {dataset_name}")
            
            # EXTRACT: Aggregate JSON files
            self.logger.info(f"Step 1: Extracting from pattern: {pattern}")
            aggregator = JSONAggregator(raw_data_path)
            df = aggregator.aggregate_json_files(pattern, dataset_name)
            
            if df is None or len(df) == 0:
                raise ValueError(f"No data found for {dataset_name}")
            
            self.logger.info(f"Extracted {len(df)} records")
            
            # TRANSFORM: Apply business rules
            self.logger.info(f"Step 2: Transforming {dataset_name}")
            df_transformed = transform_dataset(dataset_name, df)
            
            if df_transformed is None:
                raise ValueError(f"Transformation failed for {dataset_name}")
            
            self.logger.info(f"Transformed: {len(df_transformed)} records, {len(df_transformed.columns)} columns")
            
            # LOAD: Insert into database
            self.logger.info(f"Step 3: Loading to table '{table_name}' (strategy: {load_strategy})")
            self.db_manager.load_dataframe(df_transformed, table_name, if_exists=load_strategy)
            
            # Verify load
            row_count = self.db_manager.get_row_count(table_name)
            self.logger.info(f"✓ Verification: {row_count} rows in {table_name}")
            
            # Record success
            self.results['success'].append({
                'dataset': dataset_name,
                'rows_extracted': len(df),
                'rows_loaded': len(df_transformed),
                'table': table_name
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Failed to process {dataset_name}: {e}")
            self.results['failed'].append({
                'dataset': dataset_name,
                'error': str(e)
            })
            return False
    
    def run_full_pipeline(self, datasets: Optional[List[str]] = None) -> Dict:
        """
        Run the complete ETL pipeline for all or specified datasets.
        
        Args:
            datasets: List of dataset names to process. If None, processes all configured datasets.
            
        Returns:
            Dictionary with pipeline results
        """
        self.logger.info("=" * 60)
        self.logger.info("GARMIN DATA ETL PIPELINE STARTED")
        self.logger.info("=" * 60)
        self.logger.info(f"Strategy: {self.config['etl_settings']['load_strategy'].upper()}")
        self.logger.info("=" * 60)
        
        # Test database connection first
        if not self.test_database_connection():
            self.logger.error("✗ Database connection failed. Aborting pipeline.")
            return self.results
        
        self.logger.info("✓ Database connection successful")
        self.logger.info("=" * 60)
        
        # Determine which datasets to process
        if datasets is None:
            datasets = self.config['etl_settings'].get('datasets_to_process', [])
        
        if not datasets:
            self.logger.warning("No datasets specified to process")
            return self.results
        
        self.logger.info(f"Datasets to process: {', '.join(datasets)}")
        self.logger.info("=" * 60)
        
        # Process each dataset
        for dataset_name in datasets:
            self.process_dataset(dataset_name)
        
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
        self.logger.info("\n" + "=" * 60)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        
        self.logger.info(f"Duration: {self.results['duration']:.2f} seconds")
        self.logger.info(f"Successful: {len(self.results['success'])}")
        self.logger.info(f"Failed: {len(self.results['failed'])}")
        
        if self.results['success']:
            self.logger.info("\n✓ Successfully processed:")
            for item in self.results['success']:
                self.logger.info(
                    f"  • {item['dataset']}: "
                    f"{item['rows_extracted']} extracted → "
                    f"{item['rows_loaded']} loaded → "
                    f"{item['table']}"
                )
        
        if self.results['failed']:
            self.logger.error("\n✗ Failed datasets:")
            for item in self.results['failed']:
                self.logger.error(f"  • {item['dataset']}: {item['error']}")
        
        self.logger.info("=" * 60)


def main():
    """Main entry point for ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Garmin Data ETL Pipeline - Automated data processing to Azure PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (all datasets)
  python etl_pipeline.py

  # Process specific datasets only
  python etl_pipeline.py --datasets running_data sleep_data

  # Test database connection
  python etl_pipeline.py --test-connection

  # Run with custom config
  python etl_pipeline.py --config my_config.yaml
        """
    )
    
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        help='Specific datasets to process (default: all configured datasets)'
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
            if success:
                logger.info("✓ Database connection test passed")
            else:
                logger.error("✗ Database connection test failed")
            sys.exit(0 if success else 1)
        
        # Run pipeline
        results = pipeline.run_full_pipeline(datasets=args.datasets)
        
        # Exit with appropriate code
        exit_code = 0 if not results['failed'] else 1
        if exit_code == 0:
            logger.info("\n✓ Pipeline completed successfully!")
        else:
            logger.error("\n✗ Pipeline completed with errors")
        
        sys.exit(exit_code)
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Make sure config.yaml exists (copy from config.template.yaml)")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
