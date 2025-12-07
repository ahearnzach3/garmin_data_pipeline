"""
Database utility module for Azure PostgreSQL connections.
Handles connection management and data loading operations.
"""

import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
import yaml


class DatabaseManager:
    """Manages PostgreSQL database connections and operations."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize database manager with configuration.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.db_config = self.config['database']
        self.engine = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Config file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing config file: {e}")
            raise
    
    def get_connection_string(self) -> str:
        """
        Build PostgreSQL connection string for SQLAlchemy.
        
        Returns:
            Connection string in format: postgresql://user:password@host:port/database
        """
        return (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}"
            f"/{self.db_config['database']}?sslmode={self.db_config['sslmode']}"
        )
    
    def get_engine(self):
        """
        Get or create SQLAlchemy engine.
        
        Returns:
            SQLAlchemy engine instance
        """
        if self.engine is None:
            connection_string = self.get_connection_string()
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            self.logger.info("Database engine created")
        return self.engine
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                sslmode=self.db_config['sslmode']
            )
            self.logger.info("Database connection established")
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
                self.logger.info("Database connection closed")
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                self.logger.info(f"Connection successful. PostgreSQL version: {version[0]}")
                cursor.close()
                return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def load_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        if_exists: str = 'replace',
        method: str = 'multi'
    ) -> None:
        """
        Load pandas DataFrame to PostgreSQL table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            method: Method to use for insertion ('multi' is faster)
        """
        try:
            engine = self.get_engine()
            
            # Get schema from config (default to 'public' if not specified)
            schema = self.db_config.get('schema', 'public')
            
            self.logger.info(f"Loading {len(df)} rows to table '{schema}.{table_name}'...")
            
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method=method,
                chunksize=self.config['etl_settings']['batch_size']
            )
            
            self.logger.info(f"Successfully loaded {len(df)} rows to '{schema}.{table_name}'")
            
        except Exception as e:
            self.logger.error(f"Failed to load data to '{table_name}': {e}")
            raise
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query and return results as DataFrame.
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame with query results, or None for non-SELECT queries
        """
        try:
            engine = self.get_engine()
            
            if query.strip().upper().startswith('SELECT'):
                df = pd.read_sql(query, engine)
                self.logger.info(f"Query executed successfully, returned {len(df)} rows")
                return df
            else:
                with engine.connect() as conn:
                    conn.execute(text(query))
                    conn.commit()
                self.logger.info("Query executed successfully")
                return None
                
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            schema: Schema name (uses config schema if not specified)
            
        Returns:
            True if table exists, False otherwise
        """
        if schema is None:
            schema = self.db_config.get('schema', 'public')
            
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s
                AND table_name = %s
            );
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (schema, table_name))
                exists = cursor.fetchone()[0]
                cursor.close()
                return exists
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False
    
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses config schema if not specified)
            
        Returns:
            Number of rows in the table
        """
        if schema is None:
            schema = self.db_config.get('schema', 'public')
            
        query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}";'
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                count = cursor.fetchone()[0]
                cursor.close()
                return count
        except Exception as e:
            self.logger.error(f"Error getting row count: {e}")
            return 0


def setup_logging(verbose: bool = True) -> None:
    """
    Configure logging for the ETL pipeline.
    
    Args:
        verbose: If True, set log level to INFO, otherwise WARNING
    """
    log_level = logging.INFO if verbose else logging.WARNING
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('etl_pipeline.log'),
            logging.StreamHandler()
        ]
    )
