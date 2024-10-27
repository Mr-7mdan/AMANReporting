from sqlalchemy import create_engine, inspect, text, types as sqlalchemy_types
from urllib.parse import quote_plus
import logging
import time
import pandas as pd
import uuid
import numpy as np

logger = logging.getLogger('ATMForecast')

def setup_database_connections(config):
    db_user = config['db_username']
    db_password = quote_plus(config['db_password'])
    db_host = config['host']
    db_port = config['port']
    db_name = config['db_name']

    logger.info(f"Connecting to database: {db_name} on host: {db_host}:{db_port}")

    # Use pymssql for SQL Server
    source_db_url = f"mssql+pymssql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    logger.debug(f"Database URL: {source_db_url.replace(db_password, '********')}")

    logger.info("Creating database engine with extended timeout")
    try:
        source_engine = create_engine(source_db_url, pool_timeout=30, pool_pre_ping=True)
        # Test the connection
        with source_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
        logger.info("Database engine created successfully")
        
    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
        raise

    # Create local database engine
    local_db_url = "sqlite:///atm_forecasting.db"
    local_engine = create_engine(local_db_url)

    return source_engine, local_engine

def get_last_record_id(engine, table_name, update_column):
    try:
        with engine.connect() as connection:
            logger.debug(f"Executing SQL query to get max {update_column} for table {table_name}")
            result = connection.execute(text(f"SELECT MAX({update_column}) FROM {table_name}"))
            logger.debug("SQL query executed successfully")
            
            row = result.fetchone()
            logger.debug(f"Fetched row: {row}")
            
            if row is not None and row[0] is not None:
                last_record_id = row[0]
                logger.info(f"Last record {update_column} for table {table_name}: {last_record_id}")
            else:
                logger.info(f"No records found in the local database for table {table_name}")
                last_record_id = None
        
        return last_record_id
    except Exception as e:
        logger.error(f"Error getting last record {update_column} for table {table_name}: {str(e)}")
        return None

def refresh_data(source_engine, local_engine, app_config_engine):
    from data_fetcher import ATMDataFetcher
    logger.info("Initializing ATMDataFetcher")
    fetcher = ATMDataFetcher(source_engine, local_engine)

    try:
        logger.info("Starting data fetch and store process")
        
        with app_config_engine.connect() as connection:
            result = connection.execute(text("SELECT name, update_column FROM ConfigTables"))
            added_tables = result.fetchall()
        
        logger.info(f"Found {len(added_tables)} tables to update")
        
        for table_name, update_column in added_tables:
            logger.info(f"Processing table: {table_name} with update column: {update_column}")
            try:
                # Get the last record ID from the local database
                last_record_id = get_last_record_id(local_engine, table_name, update_column)
                logger.info(f"Last record ID for {table_name}: {last_record_id}")

                # Fetch new data
                fetched_data = fetcher.fetch_and_store_data(table_name, update_column, last_record_id)
                
                if not isinstance(fetched_data, pd.DataFrame):
                    fetched_data = pd.DataFrame(fetched_data)

                if not fetched_data.empty:
                    # Convert UUID columns to string
                    for col in fetched_data.columns:
                        if fetched_data[col].dtype == 'object':
                            fetched_data[col] = fetched_data[col].apply(lambda x: str(x) if isinstance(x, uuid.UUID) else x)
                    
                    # Replace NaN with None
                    fetched_data = fetched_data.replace({np.nan: None})

                    max_retries = 5
                    retry_delay = 1.0  # seconds

                    for attempt in range(max_retries):
                        try:
                            with local_engine.begin() as connection:
                                # Append new data to the existing table
                                fetched_data.to_sql(table_name, connection, if_exists='append', index=False, dtype={
                                    col_name: sqlalchemy_types.String() for col_name in fetched_data.columns
                                })
                                logger.info(f"Appended {len(fetched_data)} new records to table {table_name}")
                            break  # If successful, break the retry loop
                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(f"Attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                            else:
                                raise
                else:
                    logger.info(f"No new data fetched for table {table_name}")

            except Exception as e:
                logger.error(f"Error processing table {table_name}: {str(e)}")

        logger.info("Data fetch and store process completed for all tables")

    except Exception as e:
        logger.error(f"An error occurred during data refresh: {str(e)}")
        raise
    finally:
        logger.info("Closing database connection")
        fetcher.close()

def test_connection(engine):
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False
