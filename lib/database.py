import os
from dotenv import load_dotenv, set_key
from sqlalchemy import create_engine, text, Table, Column, String, MetaData, DateTime, Integer, Float, Boolean, Date
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from urllib.parse import quote_plus
import logging
import pyodbc
import pymssql
from sqlalchemy import inspect

logger = logging.getLogger(__name__)
# Set logging level to warning
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


class DatabaseManager:
    def __init__(self):
        self.metadata = MetaData()  # For app_config.db
        self.data_metadata = MetaData()  # For AMANReporting.db
        self.setup_tables()
        self.local_engine = self.create_local_engine()  # AMANReporting.db
        self.app_config_engine = self.create_app_config_engine()  # app_config.db
        self.create_session_factories()
        self.create_tables()

    def setup_tables(self):
        # Config tables (app_config.db)
        self.LastUpdated = Table('LastUpdated', self.metadata,
            Column('id', String, primary_key=True),
            Column('timestamp', DateTime)
        )

        self.ConfigTables = Table('ConfigTables', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String, nullable=False),
            Column('update_column', String, nullable=False)
        )

        self.CustomQueries = Table('CustomQueries', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String, nullable=False),
            Column('sql_query', String, nullable=False),
            Column('update_column', String, nullable=False)
        )

        self.KPIConfigurations = Table('KPIConfigurations', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String, nullable=False),
            Column('table_name', String, nullable=False),
            Column('date_column', String, nullable=False),
            Column('time_spans', String, nullable=False),
            Column('conditions', String, nullable=False),
            Column('calculation_steps', String, nullable=False),
            Column('dimensions', String, nullable=True)
        )

        self.ChartConfigurations = Table('ChartConfigurations', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String, nullable=False),
            Column('table_name', String, nullable=False),
            Column('x_axis', String, nullable=False),
            Column('y_axis', String, nullable=False),
            Column('chart_type', String, nullable=False),
            Column('time_spans', String),
            Column('is_enabled', Boolean, default=True),
            Column('created_at', DateTime),
            Column('updated_at', DateTime)
        )

        self.UserLayouts = Table('UserLayouts', self.metadata,
            Column('id', String, primary_key=True),
            Column('layout_data', String, nullable=False),
            Column('created_at', DateTime),
            Column('updated_at', DateTime)
        )

        # Data tables (AMANReporting.db) will be created dynamically

        # Update TableSyncStatus table to include progress column
        self.TableSyncStatus = Table('TableSyncStatus', self.metadata,
            Column('table_id', Integer, primary_key=True),
            Column('status', String, nullable=False),  # pending, syncing, completed, error
            Column('rows_fetched', Integer, default=0),
            Column('error_message', String),
            Column('last_sync', DateTime),
            Column('progress', Float)  # Add progress column for tracking percentage
        )

        # Update CustomQuerySyncStatus table to include progress
        self.CustomQuerySyncStatus = Table('CustomQuerySyncStatus', self.metadata,
            Column('query_id', Integer, primary_key=True),
            Column('status', String, nullable=False),  # pending, syncing, completed, error
            Column('rows_fetched', Integer, default=0),
            Column('error_message', String),
            Column('last_sync', DateTime),
            Column('execution_time', Float),  # Store query execution time in seconds
            Column('progress', Float),  # Add progress column for tracking percentage
            Column('total_rows', Integer)  # Add this column
        )

        # Add RefreshHistory table
        self.RefreshHistory = Table('RefreshHistory', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('timestamp', DateTime, nullable=False),
            Column('total_time', Float),
            Column('total_rows', Integer),
            Column('items', String),  # JSON string of refresh items
            Column('status', String)  # completed, error
        )

        # Add this to the setup_tables method
        self.UserPreferences = Table('UserPreferences', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('grid_size', Integer, default=48),  # 48, 72, 96, or 144
            Column('created_at', DateTime),
            Column('updated_at', DateTime)
        )

    def create_local_engine(self):
        """Create engine for AMANReporting.db (data storage)"""
        return create_engine(
            'sqlite:///AMANReporting.db',
            connect_args={
                'check_same_thread': False,
                'timeout': 30
            },
            pool_pre_ping=True,
            pool_recycle=3600
        )

    def create_app_config_engine(self):
        """Create engine for app_config.db (configuration storage)"""
        try:
            db_path = 'app_config.db'
            engine = create_engine(
                f'sqlite:///{db_path}',
                echo=True,
                connect_args={
                    'check_same_thread': False,
                    'timeout': 30
                },
                pool_pre_ping=True,
                pool_recycle=3600
            )
            logger.info(f"Created app config database at {db_path}")
            return engine
        except Exception as e:
            logger.error(f"Error creating app config database: {str(e)}")
            raise

    def create_session_factories(self):
        """Create separate session factories for each database"""
        self.app_config_session_factory = sessionmaker(bind=self.app_config_engine)
        self.app_config_session = scoped_session(self.app_config_session_factory)
        
        self.local_session_factory = sessionmaker(bind=self.local_engine)
        self.local_session = scoped_session(self.local_session_factory)

    def create_tables(self):
        """Create tables only if they don't exist"""
        try:
            logger.info("Checking and creating missing tables")
            
            # Get list of existing tables
            with self.app_config_engine.connect() as conn:
                inspector = inspect(self.app_config_engine)
                existing_tables = inspector.get_table_names()
                logger.info(f"Found existing tables: {existing_tables}")
                
                # Create only missing tables
                for table_name, table in self.metadata.tables.items():
                    if table_name not in existing_tables:
                        logger.info(f"Creating missing table: {table_name}")
                        table.create(self.app_config_engine)
                    else:
                        logger.info(f"Table already exists: {table_name}")
                
            logger.info("Table check/creation completed successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    def get_remote_engine(self):
        """Create and return a remote database engine"""
        try:
            logger.info("Creating remote database engine")
            config = self.get_config()
            db_user = config['db_username']
            db_password = quote_plus(config['db_password'])  # Properly escape password
            db_host = config['host']
            db_port = config['port']
            db_name = config['db_name']

            # Test network connectivity first
            import socket
            try:
                sock = socket.create_connection((db_host, int(db_port)), timeout=5)
                sock.close()
                logger.info(f"Network connectivity test successful to {db_host}:{db_port}")
            except Exception as e:
                logger.error(f"Network connectivity test failed: {str(e)}")
                raise Exception(f"Cannot connect to {db_host}:{db_port}. Please check if the server is accessible and the port is open.")

            # Try pymssql first with properly escaped password
            try:
                logger.info("Trying pymssql connection...")
                pymssql_url = f"mssql+pymssql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
                engine = create_engine(
                    pymssql_url,
                    connect_args={
                        'charset': 'UTF-8',
                        'timeout': 30,
                        'login_timeout': 30,
                        'tds_version': '7.4'
                    }
                )
                
                # Test the connection
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    logger.info("Successfully connected using pymssql")
                    return engine
            except Exception as e:
                logger.warning(f"pymssql connection failed: {str(e)}")

            # If pymssql fails, try ODBC with properly escaped password
            try:
                logger.info("Trying ODBC connection...")
                connection_string = (
                    "DRIVER={ODBC Driver 18 for SQL Server};"
                    f"SERVER={db_host},{db_port};"
                    f"DATABASE={db_name};"
                    f"UID={db_user};"
                    f"PWD={db_password};"  # Using escaped password
                    "TrustServerCertificate=yes;"
                    "Encrypt=yes;"
                )
                
                engine = create_engine(
                    f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}",
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    pool_timeout=30,
                    fast_executemany=True
                )

                # Test the connection
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    logger.info("Successfully connected using ODBC")
                    return engine
            except Exception as e:
                logger.error(f"ODBC connection failed: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"Failed to create remote database engine: {str(e)}")
            raise

    def get_config(self):
        logger.info("Fetching configuration from environment variables")
        config = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'db_name': os.environ.get('DB_NAME'),
            'db_username': os.environ.get('DB_USERNAME'),
            'db_password': os.environ.get('DB_PASSWORD'),
            'flask_port': os.environ.get('FLASK_PORT', '5000')
        }
        safe_config = {k: v if k != 'db_password' else '********' for k, v in config.items()}
        logger.info(f"Configuration fetched: {safe_config}")
        return config

    def test_connection(self, engine):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def recreate_tables(self):
        """
        Only recreate tables that need schema updates
        """
        try:
            logger.info("Checking for table schema updates")
            
            with self.app_config_engine.begin() as conn:
                inspector = inspect(self.app_config_engine)
                existing_tables = inspector.get_table_names()
                
                for table_name, table in self.metadata.tables.items():
                    if table_name in existing_tables:
                        # Compare existing columns with metadata columns
                        existing_columns = {col['name']: col for col in inspector.get_columns(table_name)}
                        metadata_columns = {col.name: col for col in table.columns}
                        
                        # Check if we need to update this table
                        needs_update = False
                        missing_columns = []
                        
                        for col_name, col in metadata_columns.items():
                            if col_name not in existing_columns:
                                needs_update = True
                                missing_columns.append(col_name)
                        
                        if needs_update:
                            logger.info(f"Table {table_name} needs schema update. Missing columns: {missing_columns}")
                            
                            # Get existing data
                            result = conn.execute(text(f"SELECT * FROM {table_name}"))
                            existing_data = result.fetchall()
                            
                            # Drop and recreate table
                            conn.execute(text(f"DROP TABLE {table_name}"))
                            table.create(self.app_config_engine)
                            
                            # Restore data for existing columns
                            if existing_data:
                                existing_column_names = list(existing_columns.keys())
                                placeholders = ','.join(['?' for _ in existing_column_names])
                                insert_stmt = f"INSERT INTO {table_name} ({','.join(existing_column_names)}) VALUES ({placeholders})"
                                
                                insert_data = []
                                for row in existing_data:
                                    row_data = []
                                    for col in existing_column_names:
                                        row_data.append(getattr(row, col))
                                    insert_data.append(row_data)
                                
                                if insert_data:
                                    conn.execute(text(insert_stmt), insert_data)
                                    logger.info(f"Restored {len(insert_data)} rows to {table_name}")
                        else:
                            logger.info(f"Table {table_name} schema is up to date")
                            
            logger.info("Schema update check completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error checking/updating table schemas: {str(e)}")
            return False





