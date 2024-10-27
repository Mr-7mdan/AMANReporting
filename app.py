import os
from dotenv import load_dotenv, set_key
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from sqlalchemy import create_engine, text, Table, Column, String, MetaData, DateTime, Integer, Float, Boolean, Date
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
import logging
from db_utils import setup_database_connections, get_last_record_id, refresh_data, test_connection
from urllib.parse import quote_plus
import uuid
import time
import calendar
import json
import sqlite3
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import scoped_session, sessionmaker

# Configure logging first
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create Flask app with explicit template folder
app = Flask(__name__, 
           template_folder='templates',
           static_folder='static')

# Set secret key
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'fallback_secret_key')

# Add error handlers
@app.errorhandler(404)
def not_found_error(error):
    logger.error(f"404 Error: {error}")
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"500 Error: {error}")
    return render_template('500.html'), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled Exception: {str(e)}", exc_info=True)
    return render_template('500.html'), 500

# Add a debug route
@app.route('/debug')
def debug():
    try:
        # List all registered routes
        routes = []
        for rule in app.url_map.iter_rules():
            routes.append({
                'endpoint': rule.endpoint,
                'methods': list(rule.methods),
                'path': str(rule)
            })
        
        # Get template folder location
        template_folder = app.template_folder
        
        # List available templates
        templates = []
        template_path = os.path.join(os.path.dirname(__file__), 'templates')
        if os.path.exists(template_path):
            templates = os.listdir(template_path)
        
        debug_info = {
            'routes': routes,
            'template_folder': template_folder,
            'templates': templates,
            'config': {k: v for k, v in app.config.items() if not k.startswith('_')},
            'template_path_exists': os.path.exists(template_path)
        }
        
        return jsonify(debug_info)
    except Exception as e:
        logger.error(f"Error in debug route: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# Initialize local SQLite database
local_engine = create_engine(
    'sqlite:///AMANReporting.db',
    connect_args={
        'check_same_thread': False,
        'timeout': 30
    },
    pool_pre_ping=True,
    pool_recycle=3600
)
metadata = MetaData()

# Create LastUpdated table if it doesn't exist
LastUpdated = Table('LastUpdated', metadata,
    Column('id', String, primary_key=True),
    Column('timestamp', DateTime)
)

# Create all tables
metadata.create_all(local_engine)

# Create a separate engine for app configuration
def create_app_config_db():
    try:
        db_path = 'app_config.db'
        
        # Create the engine with thread-safe settings
        engine = create_engine(
            f'sqlite:///{db_path}',
            echo=True,
            connect_args={
                'check_same_thread': False,  # Allow multi-threading
                'timeout': 30  # Add timeout for busy database
            },
            pool_pre_ping=True,  # Check connection before using
            pool_recycle=3600  # Recycle connections after an hour
        )
        
        logger.info(f"Created app config database at {db_path}")
        return engine
    except Exception as e:
        logger.error(f"Error creating app config database: {str(e)}")
        raise

# Create the engine using the new function
app_config_engine = create_app_config_db()

# Create thread-local session factories
app_config_session_factory = sessionmaker(bind=app_config_engine)
app_config_session = scoped_session(app_config_session_factory)

local_session_factory = sessionmaker(bind=local_engine)
local_session = scoped_session(local_session_factory)

# Create the tables in the app configuration database
app_config_metadata = MetaData()

# Define the Tables table in the app configuration database
ConfigTables = Table('ConfigTables', app_config_metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('update_column', String, nullable=False)
)

# Add this new table definition
CustomQueries = Table('CustomQueries', app_config_metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('sql_query', String, nullable=False),
    Column('update_column', String, nullable=False)
)

# Add this new table definition after CustomQueries
KPIConfigurations = Table('KPIConfigurations', app_config_metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('table_name', String, nullable=False),
    Column('date_column', String, nullable=False),
    Column('time_spans', String, nullable=False),
    Column('conditions', String, nullable=False),
    Column('calculation_steps', String, nullable=False)  # JSON array of calculation steps
)

# Create the tables in the app configuration database
app_config_metadata.create_all(app_config_engine)

# Then update the recreate_kpi_table function to only recreate if needed
def recreate_kpi_table():
    logger.info("Checking KPI Configurations table")
    try:
        with app_config_engine.begin() as conn:
            # Check if table exists and has the correct schema
            try:
                result = conn.execute(text("SELECT * FROM KPIConfigurations LIMIT 1"))
                columns = result.keys()
                required_columns = {'id', 'name', 'table_name', 'date_column', 'time_spans', 'conditions', 'calculation_steps'}
                
                if required_columns.issubset(set(columns)):
                    logger.info("KPIConfigurations table exists with correct schema")
                    return
                
                logger.info("KPIConfigurations table needs to be updated")
            except Exception as e:
                logger.info("KPIConfigurations table does not exist or has incorrect schema")

            # Get existing KPIs if table exists
            try:
                existing_kpis = conn.execute(text("SELECT * FROM KPIConfigurations")).fetchall()
                logger.info(f"Found {len(existing_kpis)} existing KPIs")
            except Exception as e:
                logger.info("No existing KPIs found or table doesn't exist")
                existing_kpis = []

            # Drop the existing table
            conn.execute(text("DROP TABLE IF EXISTS KPIConfigurations"))
            logger.info("Dropped existing KPIConfigurations table")

            # Create the table with the new schema
            conn.execute(text("""
                CREATE TABLE KPIConfigurations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    date_column TEXT NOT NULL,
                    time_spans TEXT NOT NULL,
                    conditions TEXT NOT NULL,
                    calculation_steps TEXT NOT NULL
                )
            """))
            logger.info("Created new KPIConfigurations table with updated schema")

            # Restore existing KPIs if any
            for kpi in existing_kpis:
                # Convert old format to new calculation steps format if needed
                if not hasattr(kpi, 'calculation_steps'):
                    calculation_steps = [{
                        'type': 'field',
                        'field': kpi.calculation_field if hasattr(kpi, 'calculation_field') else '',
                        'method': kpi.calculation_method if hasattr(kpi, 'calculation_method') else 'Count',
                        'operator': None
                    }]
                else:
                    calculation_steps = json.loads(kpi.calculation_steps)

                conn.execute(
                    text("""
                        INSERT INTO KPIConfigurations 
                        (name, table_name, date_column, time_spans, conditions, calculation_steps)
                        VALUES (:name, :table_name, :date_column, :time_spans, :conditions, :calculation_steps)
                    """),
                    {
                        'name': kpi.name,
                        'table_name': kpi.table_name,
                        'date_column': kpi.date_column if hasattr(kpi, 'date_column') else 'OrderDate',
                        'time_spans': kpi.time_spans if hasattr(kpi, 'time_spans') else '[]',
                        'conditions': kpi.conditions if hasattr(kpi, 'conditions') else '[]',
                        'calculation_steps': json.dumps(calculation_steps)
                    }
                )
            logger.info("Restored existing KPIs with new schema")

    except Exception as e:
        logger.error(f"Error recreating KPI table: {str(e)}", exc_info=True)
        raise

# Add this line after your app_config_metadata.create_all(app_config_engine) call
recreate_kpi_table()

def get_config():
    logger.info("Fetching configuration from environment variables")
    config = {
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
        'db_name': os.environ.get('DB_NAME'),
        'db_username': os.environ.get('DB_USERNAME'),
        'db_password': os.environ.get('DB_PASSWORD'),
        'flask_port': os.environ.get('FLASK_PORT', '5000')  # Add this line
    }
    # Log the config without the password
    safe_config = {k: v if k != 'db_password' else '********' for k, v in config.items()}
    logger.info(f"Configuration fetched: {safe_config}")
    return config

def get_remote_engine():
    logger.info("Creating remote database engine")
    config = get_config()
    db_user = config['db_username']
    db_password = quote_plus(config['db_password'])
    db_host = config['host']
    db_port = config['port']
    db_name = config['db_name']

    # Use pymssql for SQL Server
    db_url = f"mssql+pymssql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    logger.debug(f"Database URL: {db_url.replace(db_password, '********')}")

    try:
        engine = create_engine(db_url, pool_timeout=30, pool_pre_ping=True)
        logger.info("Remote database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create remote database engine: {str(e)}")
        raise

def get_last_updated():
    logger.info("Fetching last update timestamp")
    try:
        with app_config_engine.connect() as conn:
            result = conn.execute(text("SELECT timestamp FROM LastUpdated WHERE id = 'last_refresh'")).scalar()
            if result:
                logger.info(f"Last update timestamp: {result}")
                return result
            logger.info("No last update timestamp found")
            return "Never"
    except Exception as e:
        logger.warning(f"Error getting last update timestamp: {str(e)}")
        return "Never"

def update_last_updated():
    logger.info("Updating last update timestamp")
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT OR REPLACE INTO LastUpdated (id, timestamp) 
                    VALUES ('last_refresh', :timestamp)
                """),
                {"timestamp": now}
            )
        logger.info(f"Last update timestamp set to: {now}")
    except Exception as e:
        logger.error(f"Error updating last update timestamp: {str(e)}")

@app.context_processor
def inject_last_updated():
    return dict(last_updated=get_last_updated())

def is_db_configured():
    logger.info("Checking if database is configured")
    config = get_config()
    required_keys = ['host', 'port', 'db_name', 'db_username', 'db_password']
    is_configured = all(config.get(key) for key in required_keys)
    logger.info(f"Database configured: {is_configured}")
    return is_configured

# Add a basic health check route
@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"}), 200

# Modify the main route to include error handling
@app.route('/')
def index():
    try:
        logger.info("Accessing landing page")
        return render_template('index.html')
    except Exception as e:
        logger.error(f"Error accessing landing page: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/settings', methods=['GET'])
def settings():
    logger.info("Accessing settings page")
    config = get_config()
    
    with app_config_engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM ConfigTables"))
        added_tables = result.fetchall()
        
        result = connection.execute(text("SELECT * FROM CustomQueries"))
        custom_queries = result.fetchall()
    
    return render_template('settings.html', config=config, added_tables=added_tables, custom_queries=custom_queries)

@app.route('/update_flask_settings', methods=['POST'])
def update_flask_settings():
    try:
        flask_port = request.form['flask_port']
        # Validate port number
        port_num = int(flask_port)
        if not (1024 <= port_num <= 65535):
            raise ValueError("Port must be between 1024 and 65535")
        
        # Save to environment variable and .env file
        os.environ['FLASK_PORT'] = flask_port
        set_key('.env', 'FLASK_PORT', flask_port)
        
        logger.info(f"Flask port updated to: {flask_port}")
        flash('Flask settings updated successfully. Restart the server for changes to take effect.', 'success')
    except ValueError as e:
        logger.error(f"Invalid port number: {str(e)}")
        flash(f'Invalid port number: {str(e)}', 'error')
    except Exception as e:
        logger.error(f"Error updating Flask settings: {str(e)}")
        flash(f'Error updating settings: {str(e)}', 'error')
    
    return redirect(url_for('settings'))

@app.route('/update_db_settings', methods=['POST'])
def update_db_settings():
    logger.info("Processing database settings update")
    config = {
        'host': request.form['host'],
        'port': request.form['port'],
        'db_name': request.form['db_name'],
        'db_username': request.form['db_username'],
        'db_password': request.form['db_password']
    }
    
    for key, value in config.items():
        os.environ[f'DB_{key.upper()}'] = value
        set_key('.env', f'DB_{key.upper()}', value)
    
    logger.info("Database configuration updated")
    flash('Database settings updated successfully', 'success')
    return redirect(url_for('settings'))

@app.route('/test_db_connection')
def test_db_connection_route():
    logger.info("Testing database connection")
    try:
        engine = get_remote_engine()
        if test_connection(engine):
            return jsonify({"message": "Connection successful!"}), 200
        else:
            return jsonify({"error": "Connection failed"}), 500
    except Exception as e:
        logger.error(f"Error testing database connection: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/get_db_structure')
def get_db_structure():
    logger.info("Fetching database structure")
    try:
        engine = get_remote_engine()
        inspector = inspect(engine)
        
        structure = []
        for table_name in inspector.get_table_names():
            columns = [column['name'] for column in inspector.get_columns(table_name)]
            structure.append({
                "id": table_name,
                "text": table_name,
                "children": [{"id": f"{table_name}.{col}", "text": col} for col in columns]
            })
        
        return jsonify(structure)
    except Exception as e:
        logger.error(f"Error fetching database structure: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/add_table', methods=['POST'])
def add_table():
    logger.info("Adding new table")
    table_name = request.form['table_name']
    update_column = request.form['update_column']
    
    try:
        with app_config_engine.begin() as conn:
            conn.execute(
                text("INSERT INTO ConfigTables (name, update_column) VALUES (:name, :update_column)"),
                {"name": table_name, "update_column": update_column}
            )
        flash(f'Table {table_name} added successfully', 'success')
    except IntegrityError:
        flash(f'Table {table_name} already exists', 'error')
    except Exception as e:
        logger.error(f"Error adding table: {str(e)}")
        flash(f'Error adding table: {str(e)}', 'error')
    
    return redirect(url_for('settings'))

@app.route('/delete_table/<int:table_id>', methods=['POST'])
def delete_table(table_id):
    logger.info(f"Deleting table with ID: {table_id}")
    max_retries = 5
    retry_delay = 1.0  # seconds

    for attempt in range(max_retries):
        try:
            with app_config_engine.begin() as conn:
                result = conn.execute(
                    text("SELECT name FROM ConfigTables WHERE id = :id"),
                    {"id": table_id}
                ).fetchone()
                
                if result is None:
                    flash('Table not found', 'error')
                    return redirect(url_for('settings'))
                
                table_name = result[0]
                
                conn.execute(
                    text("DELETE FROM ConfigTables WHERE id = :id"),
                    {"id": table_id}
                )
            
            flash(f'Table {table_name} deleted successfully', 'success')
            return redirect(url_for('settings'))

        except OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logger.warning(f"Database locked, retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                logger.exception(f"Error deleting table after {attempt + 1} attempts: {str(e)}")
                flash(f'Error deleting table: {str(e)}', 'error')
                return redirect(url_for('settings'))

        except Exception as e:
            logger.exception(f"Error deleting table: {str(e)}")
            flash(f'Error deleting table: {str(e)}', 'error')
            return redirect(url_for('settings'))

    flash('Failed to delete table due to database lock', 'error')
    return redirect(url_for('settings'))

@app.route('/add_custom_query', methods=['POST'])
def add_custom_query():
    logger.info("Adding new custom query")
    query_name = request.form['query_name']
    sql_query = request.form['sql_query']
    update_column = request.form['update_column']
    
    try:
        # Test the query first
        source_engine = get_remote_engine()
        with source_engine.connect() as conn:
            result = conn.execute(text(sql_query))
            # Fetch a few rows to make sure it works
            rows = result.fetchmany(5)
        
        # If we get here, the query executed successfully
        with app_config_engine.begin() as conn:
            conn.execute(
                text("INSERT INTO CustomQueries (name, sql_query, update_column) VALUES (:name, :sql_query, :update_column)"),
                {"name": query_name, "sql_query": sql_query, "update_column": update_column}
            )
        flash(f'Custom query "{query_name}" added successfully', 'success')
    except IntegrityError:
        flash(f'Custom query "{query_name}" already exists', 'error')
    except Exception as e:
        logger.error(f"Error adding custom query: {str(e)}")
        flash(f'Error adding custom query: {str(e)}', 'error')
    
    return redirect(url_for('settings'))

@app.route('/delete_custom_query/<int:query_id>', methods=['POST'])
def delete_custom_query(query_id):
    logger.info(f"Deleting custom query with ID: {query_id}")
    try:
        with app_config_engine.begin() as conn:
            result = conn.execute(
                text("SELECT name FROM CustomQueries WHERE id = :id"),
                {"id": query_id}
            ).fetchone()
            
            if result is None:
                flash('Custom query not found', 'error')
                return redirect(url_for('settings'))
            
            query_name = result[0]
            
            conn.execute(
                text("DELETE FROM CustomQueries WHERE id = :id"),
                {"id": query_id}
            )
        
        flash(f'Custom query "{query_name}" deleted successfully', 'success')
    except Exception as e:
        logger.error(f"Error deleting custom query: {str(e)}")
        flash(f'Error deleting custom query: {str(e)}', 'error')
    
    return redirect(url_for('settings'))

@app.route('/refresh_data', methods=['POST'])
def refresh_data_route():
    logger.info("Starting data refresh process")
    try:
        source_engine = get_remote_engine()
        
        # Fetch custom queries
        with app_config_engine.connect() as conn:
            custom_queries = conn.execute(text("SELECT * FROM CustomQueries")).fetchall()
        
        # Refresh data for regular tables
        refresh_data(source_engine, local_engine, app_config_engine)
        
        # Refresh data for custom queries
        for query in custom_queries:
            try:
                logger.info(f"Executing custom query: {query.name}")
                with source_engine.connect() as conn:
                    df = pd.read_sql(query.sql_query, conn)
                
                # Save the result to the local database
                df.to_sql(query.name, local_engine, if_exists='replace', index=False)
                logger.info(f"Custom query {query.name} executed and saved successfully")
            except Exception as e:
                logger.error(f"Error executing custom query {query.name}: {str(e)}")
        
        update_last_updated()
        flash('Data refreshed successfully', 'success')
    except Exception as e:
        logger.error(f"Error refreshing data: {str(e)}")
        flash(f'Error refreshing data: {str(e)}', 'error')
    return redirect(url_for('index'))

# Add this near the top of app.py, after creating the Flask app
@app.template_filter('datetime')
def format_datetime(value, format='%Y-%m-%d %H:%M:%S'):
    if value is None:
        return ''
    if isinstance(value, str):
        try:
            value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return value
    return value.strftime(format)

@app.route('/stats')
def stats():
    return render_template('stats.html')

@app.route('/stats/config', methods=['GET', 'POST'])
def stats_config():
    # Define time span pairs at the start of the function
    time_span_pairs = [
        ['Today', 'Yesterday'],
        ['Month to Date', 'Last Month to Date'],
        ['Year to Date', 'Last Year to Date']
    ]

    if request.method == 'POST':
        try:
            logger.info("=================== START KPI CONFIGURATION SAVING ===================")
            
            # Get the KPI ID if editing
            kpi_id = request.form.get('kpi_id')
            is_update = bool(kpi_id)
            logger.info(f"{'Updating' if is_update else 'Creating new'} KPI configuration")
            
            # Get form data
            form_data = dict(request.form)
            logger.info(f"Raw form data: {form_data}")
            
            # Process time spans
            selected_time_spans = []
            for i in range(len(time_span_pairs)):
                span_key = f'time_span_{i}'
                if span_key in form_data:
                    try:
                        span_pair = json.loads(form_data[span_key])
                        selected_time_spans.append(span_pair)
                        logger.info(f"Added time span: {span_pair}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing time span {span_key}: {e}")
            
            logger.info(f"Selected time spans: {selected_time_spans}")
            
            # Get conditions
            conditions = []
            condition_fields = request.form.getlist('conditions[]')
            
            # Process conditions in groups of three
            for i in range(0, len(condition_fields), 3):
                if i + 2 < len(condition_fields):  # Make sure we have all three parts
                    field = condition_fields[i].strip()
                    operator = condition_fields[i + 1].strip()
                    value = condition_fields[i + 2].strip()
                    if all([field, operator, value]):  # Check if all parts are non-empty
                        conditions.append({
                            'field': field,
                            'operator': operator,
                            'value': value
                        })
            
            logger.info(f"Raw condition data:")
            logger.info(f"Fields: {condition_fields}")
            logger.info(f"Processed conditions: {conditions}")
            
            # Get calculation steps
            calculation_steps = request.form.get('calculation_steps')
            if not calculation_steps:
                # Create default calculation step if none provided
                calculation_steps = json.dumps([{
                    'type': 'field',
                    'field': request.form.get('calculation_field', ''),
                    'method': request.form.get('calculation_method', 'Count'),
                    'operator': None
                }])
            
            # Prepare KPI data
            kpi_data = {
                'name': request.form['kpi_name'].strip(),
                'table_name': request.form['table_name'].strip(),
                'date_column': request.form['date_column'].strip(),
                'time_spans': json.dumps(selected_time_spans),
                'conditions': json.dumps(conditions),
                'calculation_steps': calculation_steps  # Add this line
            }
            
            logger.info(f"Final KPI data being saved: {kpi_data}")
            
            # Save to database
            with app_config_engine.begin() as conn:
                if is_update:
                    conn.execute(
                        text("""
                            UPDATE KPIConfigurations 
                            SET name=:name, table_name=:table_name, 
                                date_column=:date_column,
                                time_spans=:time_spans, 
                                conditions=:conditions,
                                calculation_steps=:calculation_steps
                            WHERE id=:id
                        """),
                        {**kpi_data, 'id': kpi_id}
                    )
                else:
                    result = conn.execute(
                        text("""
                            INSERT INTO KPIConfigurations 
                            (name, table_name, date_column, time_spans, conditions, calculation_steps) 
                            VALUES (:name, :table_name, :date_column, :time_spans, :conditions, :calculation_steps)
                            RETURNING id
                        """),
                        kpi_data
                    )
                    new_id = result.scalar()
                    logger.info(f"Inserted new KPI with ID {new_id}")
                
            logger.info("=================== END KPI CONFIGURATION SAVING ===================")
            
        except Exception as e:
            logger.error("Error in KPI configuration saving:", exc_info=True)
            flash(f'Error {"updating" if is_update else "adding"} KPI configuration: {str(e)}', 'error')
        return redirect(url_for('stats_config'))
    
    # Get existing KPI configurations
    with app_config_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM KPIConfigurations"))
        # Convert Row objects to dictionaries
        kpis = [dict(row._mapping) for row in result.fetchall()]
        logger.info(f"Retrieved KPIs: {kpis}")
    
    # Get available tables from local database
    inspector = inspect(local_engine)
    available_tables = inspector.get_table_names()
    
    # For GET requests, pass the time span pairs to the template
    return render_template('stats_config.html', 
                         time_span_pairs=time_span_pairs,
                         calculation_methods=['Count', 'Distinct Count', 'Sum', 'Average', 'Maximum', 'Minimum'],
                         available_tables=available_tables,
                         kpis=kpis)

@app.route('/get_table_columns/<table_name>')
def get_table_columns(table_name):
    try:
        logger.info(f"Getting columns for table: {table_name}")
        inspector = inspect(local_engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        logger.info(f"Found columns: {columns}")
        return jsonify(columns)
    except Exception as e:
        logger.error(f"Error getting table columns: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/delete_kpi/<int:kpi_id>', methods=['POST'])
def delete_kpi(kpi_id):
    try:
        with app_config_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM KPIConfigurations WHERE id = :id"),
                {"id": kpi_id}
            )
        flash('KPI configuration deleted successfully', 'success')
    except Exception as e:
        flash(f'Error deleting KPI configuration: {str(e)}', 'error')
    return redirect(url_for('stats_config'))

# Add this near the top of app.py with the other imports
import json

# Add this with your other template filters
@app.template_filter('from_json')
def from_json_filter(value):
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return []

@app.route('/calculate_kpis')
def calculate_kpis():
    try:
        logger.info("Starting KPI calculations")
        with app_config_engine.connect() as conn:
            kpis = conn.execute(text("SELECT * FROM KPIConfigurations")).fetchall()
            logger.info(f"Found {len(kpis)} KPI configurations")
        
        results = []
        for kpi in kpis:
            logger.info(f"Processing KPI: {kpi.name}")
            time_spans = json.loads(kpi.time_spans)
            conditions = json.loads(kpi.conditions) if kpi.conditions else []
            logger.info(f"Time spans: {time_spans}")
            logger.info(f"Conditions: {conditions}")
            
            # Skip KPIs with no time spans
            if not time_spans:
                logger.info(f"Skipping KPI {kpi.name} - no time spans configured")
                continue
            
            for current_period, comparison_period in time_spans:
                logger.info(f"Calculating for periods: {current_period} vs {comparison_period}")
                # Calculate the date ranges
                current_range = get_date_range(current_period)
                comparison_range = get_date_range(comparison_period)
                logger.info(f"Current range: {current_range}")
                logger.info(f"Comparison range: {comparison_range}")
                
                # Build and execute the query for both periods
                current_value = calculate_kpi_value(kpi, current_range, conditions)
                logger.info(f"Current value: {current_value}")
                comparison_value = calculate_kpi_value(kpi, comparison_range, conditions)
                logger.info(f"Comparison value: {comparison_value}")
                
                # Calculate the change percentage
                if comparison_value and comparison_value != 0:
                    change = ((current_value - comparison_value) / comparison_value) * 100
                else:
                    change = 0
                logger.info(f"Change percentage: {change}%")
                
                results.append({
                    'name': f"{kpi.name} ({current_period})",
                    'current_value': format_value(current_value),
                    'comparison_period': comparison_period,
                    'change': round(change, 1)
                })
        
        logger.info(f"Final results: {results}")
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error calculating KPIs: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def calculate_kpi_value(kpi, date_range, conditions):
    try:
        logger.info(f"Calculating KPI value for:")
        logger.info(f"KPI: {kpi}")
        logger.info(f"Date range: {date_range}")
        logger.info(f"Conditions: {conditions}")
        
        calculation_steps = json.loads(kpi.calculation_steps)
        logger.info(f"Calculation steps: {calculation_steps}")
        
        # Build the base query for the first step
        first_step = calculation_steps[0]
        if first_step['type'] == 'field':
            calculation_sql = get_calculation_sql(first_step['method'], f'"{first_step["field"]}"')
            query = f"SELECT {calculation_sql} as step_result FROM {kpi.table_name} "
            query += f"WHERE DATE({kpi.date_column}) BETWEEN :start_date AND :end_date"
        else:  # constant value
            query = f"SELECT {first_step['value']} as step_result"
        
        params = {
            'start_date': date_range[0],
            'end_date': date_range[1]
        }
        
        # Add conditions to the query
        if conditions:
            for i, condition in enumerate(conditions):
                field = f'"{condition["field"]}"' if ' ' in condition["field"] else condition["field"]
                query += f" AND {field} {condition['operator']} :value_{i}"
                params[f'value_{i}'] = condition['value']
        
        logger.info(f"Base query: {query}")
        logger.info(f"Parameters: {params}")
        
        # Execute the first step
        with local_engine.connect() as conn:
            # Get initial result
            result = float(conn.execute(text(query), params).scalar() or 0)
            logger.info(f"Initial result: {result}")
            
            # Process subsequent steps
            for step in calculation_steps[1:]:
                if step['type'] == 'field':
                    # Get the value from the database
                    step_query = get_calculation_sql(step['method'], f'"{step["field"]}"')
                    step_query = f"SELECT {step_query} FROM {kpi.table_name} "
                    step_query += f"WHERE DATE({kpi.date_column}) BETWEEN :start_date AND :end_date"
                    if conditions:
                        for i, condition in enumerate(conditions):
                            field = f'"{condition["field"]}"' if ' ' in condition["field"] else condition["field"]
                            step_query += f" AND {field} {condition['operator']} :value_{i}"
                    
                    step_value = float(conn.execute(text(step_query), params).scalar() or 0)
                else:
                    # Use constant value
                    step_value = float(step['value'])
                
                logger.info(f"Step value: {step_value} (operator: {step['operator']})")
                
                # Apply the operator
                if step['operator'] == '+':
                    result += step_value
                elif step['operator'] == '-':
                    result -= step_value
                elif step['operator'] == '*':
                    result *= step_value
                elif step['operator'] == '/':
                    if step_value != 0:  # Prevent division by zero
                        result /= step_value
                
                logger.info(f"After step {step}: {result}")
            
            return result
            
    except Exception as e:
        logger.error(f"Error calculating KPI value: {str(e)}", exc_info=True)
        return 0

def get_date_range(period):
    logger.info(f"Calculating date range for period: {period}")
    today = datetime.now().date()
    
    if period == 'Today':
        result = (today, today)
    elif period == 'Yesterday':
        yesterday = today - timedelta(days=1)
        result = (yesterday, yesterday)
    elif period == 'Month to Date':
        result = (today.replace(day=1), today)
    elif period == 'Last Month to Date':
        last_month = today.replace(day=1) - timedelta(days=1)
        result = (last_month.replace(day=1), last_month)
    elif period == 'Year to Date':
        result = (today.replace(month=1, day=1), today)
    elif period == 'Last Year to Date':
        last_year = today.replace(year=today.year-1)
        result = (last_year.replace(month=1, day=1), last_year)
    else:
        result = (today, today)
    
    logger.info(f"Calculated date range for {period}: {result}")
    return result

def get_calculation_sql(method, field):
    logger.info(f"Getting SQL for calculation method: {method}, field: {field}")
    if method == 'Count':
        sql = 'COUNT(*)'
    elif method == 'Distinct Count':
        sql = f'COUNT(DISTINCT {field})'
    elif method == 'Sum':
        sql = f'SUM({field})'
    elif method == 'Average':
        sql = f'AVG({field})'
    elif method == 'Maximum':
        sql = f'MAX({field})'
    elif method == 'Minimum':
        sql = f'MIN({field})'
    else:
        sql = 'COUNT(*)'
    
    logger.info(f"Generated SQL: {sql}")
    return sql

def format_value(value):
    if isinstance(value, (int, float)):
        return '{:,.0f}'.format(value)
    return str(value)

@app.route('/get_kpi_config/<int:kpi_id>')
def get_kpi_config(kpi_id):
    try:
        with app_config_engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM KPIConfigurations WHERE id = :id"),
                {"id": kpi_id}
            ).fetchone()
            if result:
                return jsonify({
                    'id': result.id,
                    'name': result.name,
                    'conditions': result.conditions,
                    'time_spans': result.time_spans
                })
            return jsonify({'error': 'KPI not found'}), 404
    except Exception as e:
        logger.error(f"Error getting KPI config: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_kpi_config_by_name/<string:kpi_name>')
def get_kpi_config_by_name(kpi_name):
    try:
        with app_config_engine.connect() as conn:
            # Case-insensitive search for KPI name starting with the provided name
            result = conn.execute(
                text("SELECT * FROM KPIConfigurations WHERE LOWER(name) = :name"),
                {"name": kpi_name.lower()}
            ).fetchone()
            if result:
                return jsonify({
                    'id': result.id,
                    'name': result.name,
                    'conditions': result.conditions,
                    'time_spans': result.time_spans
                })
            return jsonify({'error': 'KPI not found'}), 404
    except Exception as e:
        logger.error(f"Error getting KPI config by name: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Update the ChartConfigurations table definition
ChartConfigurations = Table('ChartConfigurations', app_config_metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('table_name', String, nullable=False),
    Column('x_axis', String, nullable=False),  # JSON containing x-axis configuration
    Column('y_axis', String, nullable=False),  # JSON containing y-axis configuration
    Column('chart_type', String, nullable=False),
    Column('time_spans', String),  # Add this column for time spans
    Column('grouping_type', String),  # For date/number grouping
    Column('grouping_value', String),  # Span value or date grouping type
    Column('is_enabled', Boolean, default=True),
    Column('created_at', DateTime, default=datetime.utcnow),
    Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
)

@app.route('/stats/charts', methods=['GET'])
def charts_config():
    try:
        # Get existing chart configurations
        with app_config_engine.connect() as conn:
            charts = conn.execute(text("""
                SELECT id, name, table_name, x_axis, y_axis, chart_type, 
                       time_spans, is_enabled, created_at, updated_at 
                FROM ChartConfigurations 
                ORDER BY created_at DESC
            """)).fetchall()
            
            # Convert Row objects to dictionaries and parse JSON strings
            formatted_charts = []
            for chart in charts:
                chart_dict = dict(chart._mapping)
                # Parse JSON strings
                chart_dict['x_axis'] = json.loads(chart_dict['x_axis'])
                chart_dict['y_axis'] = json.loads(chart_dict['y_axis'])
                if chart_dict['time_spans']:
                    chart_dict['time_spans'] = json.loads(chart_dict['time_spans'])
                formatted_charts.append(chart_dict)
            
            logger.info(f"Found {len(charts)} chart configurations")
            
        # Get available tables
        inspector = inspect(local_engine)
        available_tables = inspector.get_table_names()
        logger.info(f"Found {len(available_tables)} available tables")
        
        return render_template('charts_config.html', 
                             charts=formatted_charts,  # Send pre-parsed data
                             available_tables=available_tables,
                             chart_types=[
                                 {'id': 'column', 'name': 'Column Chart'},
                                 {'id': 'stacked_column', 'name': 'Stacked Column Chart'},
                                 {'id': 'bar', 'name': 'Bar Chart'},
                                 {'id': 'line', 'name': 'Line Chart'},
                                 {'id': 'scatter', 'name': 'Scatter Plot'},
                                 {'id': 'pie', 'name': 'Pie Chart'}
                             ])
    except Exception as e:
        logger.error(f"Error in charts_config: {str(e)}", exc_info=True)
        flash('Error loading chart configurations: ' + str(e), 'error')
        return redirect(url_for('stats'))

@app.route('/api/chart/get_column_info/<table_name>/<column_name>')
def get_column_info(table_name, column_name):
    try:
        inspector = inspect(local_engine)
        columns = inspector.get_columns(table_name)
        column_info = next((col for col in columns if col['name'] == column_name), None)
        
        if not column_info:
            return jsonify({'error': 'Column not found'}), 404
            
        # Get column type
        col_type = str(column_info['type'])
        
        # Get min and max values for numerical columns
        if 'INT' in col_type.upper() or 'FLOAT' in col_type.upper() or 'DECIMAL' in col_type.upper():
            with local_engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT MIN({column_name}), MAX({column_name}) FROM {table_name}"
                )).fetchone()
                min_val, max_val = result
                
                return jsonify({
                    'type': 'numeric',
                    'min': min_val,
                    'max': max_val,
                    'grouping_options': ['span']
                })
                
        # For date columns
        elif 'DATE' in col_type.upper() or 'TIMESTAMP' in col_type.upper():
            return jsonify({
                'type': 'date',
                'grouping_options': ['day', 'week', 'month', 'quarter', 'year']
            })
            
        # For other columns
        else:
            return jsonify({
                'type': 'other',
                'grouping_options': []
            })
            
    except Exception as e:
        logger.error(f"Error getting column info: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/save', methods=['POST'])
def save_chart():
    try:
        data = request.json
        logger.info(f"Saving chart with data: {data}")
        
        # Validate required fields
        required_fields = ['name', 'table_name', 'x_axis', 'y_axis', 'chart_type']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Process y-axis measures with steps and conditions
        y_axis_data = []
        for measure in data['y_axis']:
            measure_data = {
                'field': measure['field'],
                'method': measure['method'],
                'operator': measure.get('operator'),
                'conditions': measure.get('conditions', [])
            }
            y_axis_data.append(measure_data)
        
        chart_data = {
            'name': data['name'],
            'table_name': data['table_name'],
            'x_axis': json.dumps(data['x_axis']),
            'y_axis': json.dumps(y_axis_data),
            'chart_type': data['chart_type'],
            'time_spans': json.dumps(data.get('time_spans', [])),  # Optional time spans
            'is_enabled': True
        }
        
        with app_config_engine.begin() as conn:
            if data.get('id'):  # Update existing chart
                conn.execute(
                    text("""
                        UPDATE ChartConfigurations 
                        SET name=:name, table_name=:table_name, x_axis=:x_axis,
                            y_axis=:y_axis, chart_type=:chart_type,
                            time_spans=:time_spans,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=:id
                    """),
                    {**chart_data, 'id': data['id']}
                )
                logger.info(f"Updated chart {data['id']}")
            else:  # Create new chart
                conn.execute(
                    text("""
                        INSERT INTO ChartConfigurations 
                        (name, table_name, x_axis, y_axis, chart_type, 
                         time_spans, is_enabled)
                        VALUES 
                        (:name, :table_name, :x_axis, :y_axis, :chart_type,
                         :time_spans, :is_enabled)
                    """),
                    chart_data
                )
                logger.info("Created new chart")
                
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error saving chart: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/toggle/<int:chart_id>', methods=['POST'])
def toggle_chart(chart_id):
    try:
        with app_config_engine.begin() as conn:
            # Get current state
            result = conn.execute(
                text("SELECT is_enabled FROM ChartConfigurations WHERE id = :id"),
                {'id': chart_id}
            ).fetchone()
            
            if not result:
                return jsonify({'error': 'Chart not found'}), 404
                
            # Toggle state
            new_state = not result[0]
            conn.execute(
                text("""
                    UPDATE ChartConfigurations 
                    SET is_enabled = :state, updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """),
                {'id': chart_id, 'state': new_state}
            )
            
        return jsonify({'success': True, 'is_enabled': new_state})
    except Exception as e:
        logger.error(f"Error toggling chart: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/delete/<int:chart_id>', methods=['POST'])
def delete_chart(chart_id):
    try:
        with app_config_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM ChartConfigurations WHERE id = :id"),
                {'id': chart_id}
            )
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error deleting chart: {str(e)}")
        return jsonify({'error': str(e)}), 500

def recreate_charts_table():
    logger.info("Checking Charts Configuration table")
    try:
        with app_config_engine.begin() as conn:
            # Check if table exists and has the correct schema
            try:
                result = conn.execute(text("SELECT * FROM ChartConfigurations LIMIT 1"))
                columns = result.keys()
                required_columns = {'id', 'name', 'table_name', 'x_axis', 'y_axis', 'chart_type', 
                                  'time_spans', 'grouping_type', 'grouping_value', 'is_enabled', 
                                  'created_at', 'updated_at'}
                
                if required_columns.issubset(set(columns)):
                    logger.info("ChartConfigurations table exists with correct schema")
                    return
                
                logger.info("ChartConfigurations table needs to be updated")
            except Exception as e:
                logger.info("ChartConfigurations table does not exist or has incorrect schema")

            # Drop the existing table
            conn.execute(text("DROP TABLE IF EXISTS ChartConfigurations"))
            logger.info("Dropped existing ChartConfigurations table")

            # Create the table with the new schema
            conn.execute(text("""
                CREATE TABLE ChartConfigurations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    x_axis TEXT NOT NULL,
                    y_axis TEXT NOT NULL,
                    chart_type TEXT NOT NULL,
                    time_spans TEXT,
                    grouping_type TEXT,
                    grouping_value TEXT,
                    is_enabled BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            logger.info("Created new ChartConfigurations table with updated schema")

    except Exception as e:
        logger.error(f"Error recreating charts table: {str(e)}", exc_info=True)
        raise

# Add this line after your app_config_metadata.create_all(app_config_engine) call
recreate_charts_table()

def get_date_range(time_span):
    """Helper function to get date range for a given time span"""
    today = datetime.now().date()
    
    if time_span == "Today":
        return (today, today)
    elif time_span == "Yesterday":
        yesterday = today - timedelta(days=1)
        return (yesterday, yesterday)
    elif time_span == "Month to Date":
        return (today.replace(day=1), today)
    elif time_span == "Last Month to Date":
        last_month = today - relativedelta(months=1)
        last_month_start = last_month.replace(day=1)
        last_month_end = today.replace(day=1) - timedelta(days=1)
        return (last_month_start, last_month_end)
    elif time_span == "Year to Date":
        return (today.replace(month=1, day=1), today)
    elif time_span == "Last Year to Date":
        last_year = today - relativedelta(years=1)
        return (last_year.replace(month=1, day=1), last_year)
    else:
        return None

@app.route('/api/charts/data')
def get_charts_data():
    try:
        logger.info("Starting to fetch chart data")
        session = app_config_session()
        try:
            # Get enabled charts
            logger.info("Fetching enabled charts from database")
            charts = session.execute(
                text("SELECT * FROM ChartConfigurations WHERE is_enabled = 1")
            ).fetchall()
            logger.info(f"Found {len(charts)} enabled charts with raw data: {charts}")
            
            # If no charts found, return empty array (not an error)
            if not charts:
                logger.info("No charts found")
                return jsonify([])
            
            chart_data = []
            for chart in charts:
                try:
                    logger.info(f"Processing chart: {chart.name} (ID: {chart.id})")
                    # Parse configuration
                    x_axis = json.loads(chart.x_axis)
                    y_axis = json.loads(chart.y_axis)
                    time_spans = json.loads(chart.time_spans) if chart.time_spans else []
                    logger.info(f"Chart configuration - X-axis: {x_axis}, Y-axis: {y_axis}, Time spans: {time_spans}")
                    
                    # Build and execute query for each time span
                    datasets = []
                    all_labels = set()
                    
                    # If no time spans specified, use current data only
                    if not time_spans:
                        time_ranges = [(None, None)]
                    else:
                        # For each time span pair, get both ranges
                        time_ranges = []
                        for span_pair in time_spans:
                            current_range = get_date_range(span_pair[0])
                            comparison_range = get_date_range(span_pair[1])
                            if current_range and comparison_range:
                                time_ranges.extend([current_range, comparison_range])
                    
                    logger.info(f"Time ranges for chart: {time_ranges}")
                    
                    for time_range in time_ranges:
                        with local_engine.connect() as local_conn:
                            # Handle date grouping
                            if x_axis.get('grouping_type') in ['day', 'weekday', 'month', 'quarter', 'year']:
                                group_by = {
                                    'day': "strftime('%d', {})",  # Day number (01-31)
                                    'weekday': "case cast(strftime('%w', {}) as integer) "  # Day name (Sun-Sat)
                                             "when 0 then 'Sun' "
                                             "when 1 then 'Mon' "
                                             "when 2 then 'Tue' "
                                             "when 3 then 'Wed' "
                                             "when 4 then 'Thu' "
                                             "when 5 then 'Fri' "
                                             "when 6 then 'Sat' end",
                                    'month': "case cast(strftime('%m', {}) as integer) "  # Month name (Jan-Dec)
                                            "when 1 then 'Jan' "
                                            "when 2 then 'Feb' "
                                            "when 3 then 'Mar' "
                                            "when 4 then 'Apr' "
                                            "when 5 then 'May' "
                                            "when 6 then 'Jun' "
                                            "when 7 then 'Jul' "
                                            "when 8 then 'Aug' "
                                            "when 9 then 'Sep' "
                                            "when 10 then 'Oct' "
                                            "when 11 then 'Nov' "
                                            "when 12 then 'Dec' end",
                                    'year': "strftime('%Y', {})",  # Year (YYYY)
                                    'quarter': "('Q' || cast((cast(strftime('%m', {}) as integer) + 2) / 3 as text))"  # Quarter (Q1-Q4)
                                }[x_axis['grouping_type']]
                                group_by = group_by.format(x_axis['field'])
                            else:
                                group_by = x_axis['field']
                            
                            logger.info(f"Using group by: {group_by}")
                            
                            # Build measures with conditions
                            for measure in y_axis:
                                sql = get_calculation_sql(measure['method'], measure['field'])
                                conditions = []
                                params = {}
                                
                                # Add time range condition if specified
                                if time_range:
                                    start_date, end_date = time_range
                                    conditions.append(f"DATE({x_axis['field']}) BETWEEN :start_date AND :end_date")
                                    params.update({'start_date': start_date, 'end_date': end_date})
                                
                                # Add measure-specific conditions
                                for i, condition in enumerate(measure.get('conditions', [])):
                                    conditions.append(f"{condition['field']} {condition['operator']} :value_{i}")
                                    params[f'value_{i}'] = condition['value']
                                
                                where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
                                
                                query = f"""
                                    SELECT {group_by} as label, {sql} as value
                                    FROM {chart.table_name}
                                    {where_clause}
                                    GROUP BY label
                                    ORDER BY label
                                """
                                
                                logger.info(f"Executing query: {query} with params: {params}")
                                result = local_conn.execute(text(query), params).fetchall()
                                logger.info(f"Query returned {len(result)} rows")
                                
                                # Add labels and data to sets
                                labels = [row.label for row in result]
                                values = [row.value for row in result]
                                all_labels.update(labels)
                                
                                # Create dataset with time span information in name if applicable
                                color = generate_color()
                                dataset_name = f"{measure['field']} ({measure['method']})"
                                if time_range:
                                    start_date, end_date = time_range
                                    dataset_name += f" ({start_date.strftime('%Y-%m-%d')})"
                                
                                datasets.append({
                                    'label': dataset_name,
                                    'data': values,
                                    'labels': labels,
                                    'borderColor': color,
                                    'backgroundColor': color.replace('rgb', 'rgba').replace(')', ', 0.1)'),
                                    'fill': False
                                })
                    
                    # Sort labels and align datasets
                    sorted_labels = sorted(list(all_labels))
                    for dataset in datasets:
                        aligned_data = []
                        for label in sorted_labels:
                            try:
                                idx = dataset['labels'].index(label)
                                aligned_data.append(dataset['data'][idx])
                            except ValueError:
                                aligned_data.append(None)
                        dataset['data'] = aligned_data
                        del dataset['labels']
                    
                    chart_data.append({
                        'id': chart.id,
                        'name': chart.name,
                        'chart_type': chart.chart_type,
                        'labels': sorted_labels,
                        'datasets': datasets,
                        'is_enabled': chart.is_enabled,
                        'time_spans': time_spans,
                        'y_axis': y_axis,
                        'x_axis': x_axis
                    })
                    logger.info(f"Successfully processed chart {chart.id}")
                except Exception as e:
                    logger.error(f"Error processing chart {chart.id}: {str(e)}", exc_info=True)
                    continue
            
            logger.info(f"Returning {len(chart_data)} charts")
            return jsonify(chart_data)
        finally:
            app_config_session.remove()
            
    except Exception as e:
        logger.error(f"Error getting chart data: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def generate_color():
    """Generate a random color for chart datasets"""
    import random
    r = random.randint(0, 255)
    g = random.randint(0, 255)
    b = random.randint(0, 255)
    color = f'rgb({r}, {g}, {b})'
    logger.debug(f"Generated color: {color}")
    return color

@app.route('/api/chart/get/<int:chart_id>')
def get_chart(chart_id):
    try:
        with app_config_engine.connect() as conn:
            chart = conn.execute(
                text("SELECT * FROM ChartConfigurations WHERE id = :id"),
                {'id': chart_id}
            ).fetchone()
            
            if not chart:
                return jsonify({'error': 'Chart not found'}), 404
            
            # Parse x_axis to get grouping info
            x_axis = json.loads(chart.x_axis)
            
            return jsonify({
                'id': chart.id,
                'name': chart.name,
                'table_name': chart.table_name,
                'x_axis': chart.x_axis,
                'y_axis': chart.y_axis,
                'chart_type': chart.chart_type,
                'time_spans': chart.time_spans,
                'grouping_type': x_axis.get('grouping_type'),  # Get from x_axis JSON
                'grouping_value': x_axis.get('grouping_value'),  # Get from x_axis JSON
                'is_enabled': chart.is_enabled
            })
            
    except Exception as e:
        logger.error(f"Error getting chart: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Add these template filters
@app.template_filter('abbreviate_timespan')
def abbreviate_timespan(timespan):
    abbreviations = {
        'Today': 'TDY',
        'Yesterday': 'YDY',
        'Month to Date': 'MTD',
        'Last Month to Date': 'LMTD',
        'Year to Date': 'YTD',
        'Last Year to Date': 'LYTD'
    }
    return abbreviations.get(timespan, timespan)

@app.template_filter('operator_symbol')
def operator_symbol(operator):
    symbols = {
        '=': '=',
        '!=': '≠',
        '>': '>',
        '>=': '≥',
        '<': '<',
        '<=': '≤'
    }
    return symbols.get(operator, operator)

@app.template_filter('from_json')
def from_json(value):
    if not value:
        return []
    try:
        return json.loads(value)
    except:
        return []

@app.route('/api/save_layout', methods=['POST'])
def save_layout():
    try:
        layout = request.json
        with app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT OR REPLACE INTO UserLayouts (id, layout_data)
                    VALUES ('default', :layout)
                """),
                {'layout': json.dumps(layout)}
            )
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error saving layout: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_layout')
def get_layout():
    try:
        with app_config_engine.connect() as conn:
            result = conn.execute(
                text("SELECT layout_data FROM UserLayouts WHERE id = 'default'")
            ).scalar()
            if result:
                return jsonify(json.loads(result))
            return jsonify(None)
    except Exception as e:
        logger.error(f"Error getting layout: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Add this to your table definitions
UserLayouts = Table('UserLayouts', app_config_metadata,
    Column('id', String, primary_key=True),
    Column('layout_data', String, nullable=False),
    Column('created_at', DateTime, default=datetime.utcnow),
    Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
)

# Add this function to recreate the UserLayouts table if needed
def recreate_user_layouts_table():
    logger.info("Checking UserLayouts table")
    try:
        with app_config_engine.begin() as conn:
            # Check if table exists
            try:
                result = conn.execute(text("SELECT * FROM UserLayouts LIMIT 1"))
                columns = result.keys()
                required_columns = {'id', 'layout_data', 'created_at', 'updated_at'}
                
                if required_columns.issubset(set(columns)):
                    logger.info("UserLayouts table exists with correct schema")
                    return
                
                logger.info("UserLayouts table needs to be updated")
            except Exception as e:
                logger.info("UserLayouts table does not exist or has incorrect schema")

            # Drop the existing table
            conn.execute(text("DROP TABLE IF EXISTS UserLayouts"))
            logger.info("Dropped existing UserLayouts table")

            # Create the table with the new schema
            conn.execute(text("""
                CREATE TABLE UserLayouts (
                    id TEXT PRIMARY KEY,
                    layout_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            logger.info("Created new UserLayouts table")

    except Exception as e:
        logger.error(f"Error recreating UserLayouts table: {str(e)}", exc_info=True)
        raise

# Add this line after your other table creation calls
recreate_user_layouts_table()

# Modify the main execution block
if __name__ == '__main__':
    try:
        logger.info("Starting ATM Forecasting application")
        host = '127.0.0.1'
        port = int(os.environ.get('FLASK_PORT', 5000))
        
        # Check if the port is available
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((host, port))
        if result == 0:
            logger.error(f"Port {port} is already in use")
            # Try to find an available port
            sock.close()
            for test_port in range(port + 1, port + 10):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex((host, test_port))
                if result != 0:
                    port = test_port
                    break
                sock.close()
        sock.close()
        
        logger.info(f"Running on http://{host}:{port}")
        app.run(debug=True, host=host, port=port, use_reloader=True)
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}", exc_info=True)
        raise
