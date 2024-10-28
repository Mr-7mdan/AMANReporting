import os
from dotenv import load_dotenv, set_key
from sqlalchemy import create_engine, text, Table, Column, String, MetaData, DateTime, Integer, Float, Boolean, Date
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from urllib.parse import quote_plus
import logging
import pyodbc
import pymssql

logger = logging.getLogger(__name__)

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
        """Create tables in their respective databases"""
        # Create config tables in app_config.db
        self.metadata.create_all(self.app_config_engine)
        
        # Data tables in AMANReporting.db are created dynamically when data is fetched

    def get_remote_engine(self):
        """Create and return a remote database engine using pymssql"""
        try:
            logger.info("Creating remote database engine")
            config = self.get_config()
            db_user = config['db_username']
            db_password = config['db_password']
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

            # Try connection with different settings
            connection_methods = [
                {
                    'server': db_host,
                    'port': int(db_port),
                    'user': db_user,
                    'password': db_password,
                    'database': db_name,
                    'timeout': 30,
                    'login_timeout': 30,
                    'charset': 'UTF-8',
                    'as_dict': True,
                    'tds_version': '7.4'  # Add TDS version
                },
                {
                    'server': f"{db_host}:{db_port}",
                    'user': db_user,
                    'password': db_password,
                    'database': db_name,
                    'timeout': 30,
                    'login_timeout': 30,
                    'charset': 'UTF-8',
                    'as_dict': True
                }
            ]

            last_error = None
            for method in connection_methods:
                try:
                    logger.info(f"Trying connection with settings: {str({k:v for k,v in method.items() if k != 'password'})}")
                    
                    # Test direct connection first
                    import pymssql
                    conn = pymssql.connect(**method)
                    conn.close()
                    logger.info("Direct connection test successful")

                    # Create SQLAlchemy engine
                    engine = create_engine(
                        f"mssql+pymssql://",
                        creator=lambda: pymssql.connect(**method),
                        pool_pre_ping=True,
                        pool_recycle=3600,
                        pool_timeout=30
                    )

                    # Test the engine
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                        logger.info("Successfully created and tested database engine")
                        return engine

                except Exception as e:
                    last_error = e
                    logger.warning(f"Connection attempt failed: {str(e)}")
                    continue

            if last_error:
                raise Exception(f"All connection attempts failed. Last error: {str(last_error)}")
            raise Exception("Could not establish connection with any method")

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
        Recreate all tables with proper transaction handling
        """
        try:
            # Get existing data first
            with self.app_config_engine.connect() as conn:
                # Commit any pending transaction
                conn.execute(text("COMMIT"))
                
                # Start new transaction
                with conn.begin():
                    # Get existing data
                    existing_data = {}
                    for table in self.metadata.tables.values():
                        try:
                            result = conn.execute(text(f"SELECT * FROM {table.name}"))
                            existing_data[table.name] = result.fetchall()
                        except:
                            existing_data[table.name] = []

                    # Drop and recreate tables
                    self.metadata.drop_all(self.app_config_engine)
                    self.metadata.create_all(self.app_config_engine)

                    # Restore data
                    for table_name, data in existing_data.items():
                        if data:
                            columns = self.metadata.tables[table_name].columns.keys()
                            insert_stmt = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['?' for _ in columns])})"
                            conn.execute(text(insert_stmt), data)

            logger.info("Tables recreated successfully")
            return True
        except Exception as e:
            logger.error(f"Error recreating tables: {str(e)}")
            return False