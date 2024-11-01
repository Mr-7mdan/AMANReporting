import pandas as pd
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
import logging
import time
from sqlalchemy import inspect

class ATMDataFetcher:
    def __init__(self, source_engine, local_engine):
        self.source_engine = source_engine
        self.local_engine = local_engine
        self.logger = logging.getLogger('ATMForecast')

    def fetch_and_store_data(self, table_name, update_column, last_record_id, progress_callback=None):
        """
        Fetch and store data from source to local database
        
        Args:
            table_name (str): Name of the table to fetch
            update_column (str): Column to track updates
            last_record_id: Last record ID in local database
            progress_callback (callable): Optional callback function to report progress
        """
        try:
            query = self.build_query(table_name, update_column, last_record_id)
            self.logger.info(f"Query for table {table_name}: {query}")
            
            self.logger.info(f"Starting data fetch process for table {table_name}...")
            start_time = time.time()
            
            with self.source_engine.connect() as connection:
                # First get total count for progress tracking
                count_query = f"SELECT COUNT(*) FROM ({query}) as subquery"
                total_count = connection.execute(text(count_query)).scalar()
                
                # Fetch data in chunks for better memory management and progress tracking
                chunk_size = 1000
                chunks = []
                rows_fetched = 0
                
                result = connection.execution_options(stream_results=True).execute(text(query))
                while True:
                    chunk = result.fetchmany(chunk_size)
                    if not chunk:
                        break
                    
                    chunks.append(chunk)
                    rows_fetched += len(chunk)
                    
                    # Report progress if callback provided
                    if progress_callback:
                        progress_callback(rows_fetched)
                
                # Convert to DataFrame
                if chunks:
                    df = pd.DataFrame([dict(row._mapping) for chunk in chunks for row in chunk])
                    
                    # Store in local database
                    with self.local_engine.begin() as local_conn:
                        df.to_sql(table_name, local_conn, if_exists='replace', index=False)
                    
                    elapsed_time = time.time() - start_time
                    self.logger.info(f"Fetched {len(df)} rows for table {table_name} in {elapsed_time:.2f} seconds")
                    return df
                else:
                    self.logger.info(f"No new data fetched for table {table_name}")
                    return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error in fetch_and_store_data for table {table_name}: {str(e)}")
            raise

    def get_last_record_id(self, table_name, update_column):
        try:
            with self.local_engine.connect() as connection:
                self.logger.debug(f"Executing SQL query to get max {update_column} for table {table_name}")
                result = connection.execute(text(f"SELECT MAX({update_column}) FROM {table_name}"))
                self.logger.debug("SQL query executed successfully")
                
                row = result.fetchone()
                self.logger.debug(f"Fetched row: {row}")
                
                if row is not None and row[0] is not None:
                    last_record = row[0]
                    self.logger.info(f"Last record {update_column} for table {table_name}: {last_record}")
                else:
                    self.logger.info(f"No records found in the local database for table {table_name}")
                    last_record = None
            
            return last_record
        except Exception as e:
            self.logger.error(f"Error getting last record {update_column} for table {table_name}: {str(e)}")
            return None

    def build_query(self, table_name, update_column, last_record_id):
        # First, let's check the counts in both databases
        source_count_query = "SELECT COUNT(*) as total_count, MAX({}) as max_id FROM {}".format(update_column, table_name)
        local_count_query = "SELECT COUNT(*) as local_count FROM {}".format(table_name)
        
        with self.source_engine.connect() as connection:
            result = connection.execute(text(source_count_query))
            row = result.fetchone()
            source_total = row[0]
            max_id = row[1]
            self.logger.info(f"Source table {table_name} has {source_total} total rows and max {update_column} is {max_id}")
        
        local_total = 0
        if inspect(self.local_engine).has_table(table_name):
            with self.local_engine.connect() as connection:
                result = connection.execute(text(local_count_query))
                local_total = result.fetchone()[0]
                self.logger.info(f"Local table {table_name} has {local_total} rows")
        
        missing_records = source_total - local_total
        self.logger.info(f"Missing {missing_records} records from local table {table_name}")

        query = f"SELECT * FROM {table_name}"
        if last_record_id is not None:
            # Convert last_record_id to integer for comparison
            last_record_id = int(last_record_id)
            query += f" WHERE {update_column} > {last_record_id}"
            query += f" ORDER BY {update_column} ASC"
            self.logger.info(f"Fetching records after {update_column}={last_record_id}")
        else:
            self.logger.info(f"Fetching all records (total: {source_total})")
        
        return query

    def close(self):
        self.source_engine.dispose()
        self.local_engine.dispose()
