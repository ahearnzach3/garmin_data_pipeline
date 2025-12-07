"""
Database setup script - Creates the garmin schema and all tables
Run this once before using the ETL pipeline for the first time
"""

import sys
import logging
from pathlib import Path
from db_utils import DatabaseManager, setup_logging


def run_sql_file(db_manager: DatabaseManager, sql_file_path: str) -> bool:
    """
    Execute SQL commands from a file.
    
    Args:
        db_manager: DatabaseManager instance
        sql_file_path: Path to SQL file
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Read SQL file
        logger.info(f"Reading SQL file: {sql_file_path}")
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        # Remove comment blocks and parse statements properly
        lines = []
        for line in sql_content.split('\n'):
            stripped = line.strip()
            # Skip comment-only lines
            if stripped.startswith('--') or not stripped:
                continue
            lines.append(line)
        
        # Join lines and split by semicolon
        clean_sql = '\n'.join(lines)
        statements = [stmt.strip() for stmt in clean_sql.split(';') if stmt.strip()]
        
        logger.info(f"Found {len(statements)} SQL statements to execute")
        
        # Log first few statements for debugging
        for i, stmt in enumerate(statements[:3], 1):
            logger.debug(f"Statement {i}: {stmt[:100]}...")
        
        # Execute each statement in its own transaction to avoid rollback issues
        success_count = 0
        for i, statement in enumerate(statements, 1):
            try:
                with db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    logger.info(f"Executing statement {i}/{len(statements)}...")
                    logger.debug(f"SQL: {statement[:200]}")
                    cursor.execute(statement)
                    cursor.close()
                    success_count += 1
                    logger.info(f"Statement {i} executed successfully")
                    
            except Exception as e:
                # Some errors are expected (e.g., "already exists")
                error_msg = str(e).lower()
                if "already exists" in error_msg or "duplicate" in error_msg:
                    logger.info(f"Statement {i} skipped (object already exists)")
                    success_count += 1
                else:
                    logger.warning(f"Statement {i} failed: {e}")
                continue
        
        logger.info(f"SQL file executed: {success_count}/{len(statements)} statements successful")
        return True
        
    except Exception as e:
        logger.error(f"Failed to execute SQL file: {e}")
        return False


def verify_setup(db_manager: DatabaseManager) -> bool:
    """
    Verify that schema and tables were created successfully.
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        True if verification passed, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        schema = db_manager.config['database'].get('schema', 'garmin')
        
        # Check if schema exists
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.schemata 
                WHERE schema_name = %s
            );
        """
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Verify schema
            cursor.execute(query, (schema,))
            schema_exists = cursor.fetchone()[0]
            
            if not schema_exists:
                logger.error(f"Schema '{schema}' was not created")
                return False
            
            logger.info(f"✓ Schema '{schema}' exists")
            
            # List tables in schema
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name;
            """, (schema,))
            
            tables = cursor.fetchall()
            
            if not tables:
                logger.warning(f"No tables found in schema '{schema}'")
                return False
            
            logger.info(f"✓ Found {len(tables)} tables in schema '{schema}':")
            for table in tables:
                logger.info(f"  - {table[0]}")
            
            cursor.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False


def main():
    """Main setup function."""
    # Setup logging
    setup_logging(verbose=True)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("GARMIN DATA PIPELINE - DATABASE SETUP")
    logger.info("=" * 60)
    
    try:
        # Initialize database manager
        logger.info("Initializing database connection...")
        db_manager = DatabaseManager('config.yaml')
        
        # Test connection
        logger.info("Testing database connection...")
        if not db_manager.test_connection():
            logger.error("Database connection failed. Check your config.yaml file.")
            sys.exit(1)
        
        # Get SQL file path
        sql_file = Path(__file__).parent / 'setup_database_schema.sql'
        
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            sys.exit(1)
        
        # Execute SQL setup
        logger.info("Creating schema and tables...")
        if not run_sql_file(db_manager, str(sql_file)):
            logger.error("Failed to execute SQL setup")
            sys.exit(1)
        
        # Verify setup
        logger.info("Verifying setup...")
        if not verify_setup(db_manager):
            logger.error("Setup verification failed")
            sys.exit(1)
        
        # Success
        logger.info("=" * 60)
        logger.info("✓ DATABASE SETUP COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Verify schema in Azure Portal or pgAdmin")
        logger.info("2. Run the ETL pipeline: python etl_pipeline.py")
        logger.info("")
        
        sys.exit(0)
        
    except FileNotFoundError:
        logger.error("config.yaml not found. Copy config.template.yaml to config.yaml and fill in your credentials.")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
