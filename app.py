import os
import logging
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from dotenv import load_dotenv, set_key
from lib.database import DatabaseManager
from lib.kpi_manager import KPIManager
from lib.chart_manager import ChartManager
from datetime import datetime
from sqlalchemy import text, inspect
import json
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__, 
           template_folder='templates',
           static_folder='static')

# Set secret key
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'fallback_secret_key')

# Initialize managers
db_manager = DatabaseManager()
kpi_manager = KPIManager(db_manager)
chart_manager = ChartManager(db_manager)

# Add these near the top of app.py after creating the Flask app
@app.template_filter('from_json')
def from_json_filter(value):
    return json.loads(value)

@app.template_filter('abbreviate_timespan')
def abbreviate_timespan_filter(value):
    abbreviations = {
        'Today': 'Today',
        'Yesterday': 'Yday',
        'Month to Date': 'MTD',
        'Last Month to Date': 'LMTD',
        'Year to Date': 'YTD',
        'Last Year to Date': 'LYTD'
    }
    return abbreviations.get(value, value)

@app.template_filter('operator_symbol')
def operator_symbol_filter(value):
    symbols = {
        '+': '+',
        '-': '−',
        '*': '×',
        '/': '÷'
    }
    return symbols.get(value, value)

# Error handlers
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

# Add this helper function at the top with other imports
def get_last_updated():
    try:
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT timestamp 
                    FROM RefreshHistory 
                    WHERE status = 'completed' 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """)
            ).fetchone()
            
            if result and result.timestamp:
                # Convert string to datetime if needed
                if isinstance(result.timestamp, str):
                    from datetime import datetime
                    try:
                        timestamp = datetime.strptime(result.timestamp, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        try:
                            timestamp = datetime.strptime(result.timestamp, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            return 'Invalid timestamp format'
                else:
                    timestamp = result.timestamp
                    
                return timestamp.strftime('%Y-%m-%d %H:%M:%S')
            return 'Never'
    except Exception as e:
        logger.error(f"Error getting last updated timestamp: {str(e)}")
        return 'Unknown'

# Routes
@app.route('/')
def index():
    try:
        logger.info("Accessing landing page")
        last_updated = get_last_updated()
        return render_template('index.html', last_updated=last_updated)
    except Exception as e:
        logger.error(f"Error accessing landing page: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/settings', methods=['GET'])
def settings():
    logger.info("Accessing settings page")
    config = db_manager.get_config()
    last_updated = get_last_updated()
    
    with db_manager.app_config_engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM ConfigTables"))
        added_tables = result.fetchall()
        
        result = connection.execute(text("SELECT * FROM CustomQueries"))
        custom_queries = result.fetchall()
    
    return render_template('settings.html', 
                         config=config, 
                         added_tables=added_tables, 
                         custom_queries=custom_queries,
                         last_updated=last_updated)

@app.route('/stats')
def stats():
    last_updated = get_last_updated()
    return render_template('stats.html', last_updated=last_updated)

# Add this function to convert SQLAlchemy Row objects to dictionaries
def row_to_dict(row):
    return {key: getattr(row, key) for key in row._fields}

# Update the stats_config route
@app.route('/stats/config')
def stats_config():
    try:
        with db_manager.local_engine.connect() as conn:
            inspector = inspect(db_manager.local_engine)
            available_tables = inspector.get_table_names()
        
        # Convert SQLAlchemy Row objects to dictionaries
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM KPIConfigurations"))
            kpis = [row_to_dict(row) for row in result]
        
        # Define time span pairs
        time_span_pairs = [
            ["Today", "Yesterday"],
            ["Month to Date", "Last Month to Date"],
            ["Year to Date", "Last Year to Date"]
        ]
        
        # Define calculation methods
        calculation_methods = [
            "Count",
            "Distinct Count",
            "Sum",
            "Average",
            "Maximum",
            "Minimum"
        ]
        
        last_updated = get_last_updated()
        
        return render_template('stats_config.html',
                             available_tables=available_tables,
                             time_span_pairs=time_span_pairs,
                             calculation_methods=calculation_methods,
                             kpis=kpis,
                             last_updated=last_updated)
    except Exception as e:
        logger.error(f"Error in stats_config: {str(e)}")
        flash(f'Error loading KPI configurations: {str(e)}', 'error')
        return redirect(url_for('stats'))

@app.route('/calculate_kpis')
def calculate_kpis():
    try:
        logger.info("Starting KPI calculations")
        with db_manager.app_config_engine.connect() as conn:
            kpis = conn.execute(text("SELECT * FROM KPIConfigurations")).fetchall()
        
        results = []
        for kpi in kpis:
            results.extend(kpi_manager.calculate_kpi_values(kpi))
        
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error calculating KPIs: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/charts/data')
def get_charts_data():
    try:
        chart_data = chart_manager.get_chart_data()
        return jsonify(chart_data)
    except Exception as e:
        logger.error(f"Error getting chart data: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/save', methods=['POST'])
def save_chart():
    try:
        chart_data = request.json
        success = chart_manager.save_chart(chart_data)
        if success:
            return jsonify({'success': True})
        return jsonify({'error': 'Failed to save chart'}), 500
    except Exception as e:
        logger.error(f"Error saving chart: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/toggle/<int:chart_id>', methods=['POST'])
def toggle_chart(chart_id):
    try:
        success, result = chart_manager.toggle_chart(chart_id)
        if success:
            return jsonify({'success': True, 'is_enabled': result})
        return jsonify({'error': result}), 500
    except Exception as e:
        logger.error(f"Error toggling chart: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/delete/<int:chart_id>', methods=['POST'])
def delete_chart(chart_id):
    try:
        success = chart_manager.delete_chart(chart_id)
        if success:
            return jsonify({'success': True})
        return jsonify({'error': 'Failed to delete chart'}), 500
    except Exception as e:
        logger.error(f"Error deleting chart: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# Add this to store refresh status
refresh_status = {
    'in_progress': False,
    'items': [],
    'start_time': None,
    'completed': False
}

@app.route('/refresh_data_route', methods=['POST'])
def refresh_data_route():
    global refresh_status
    try:
        if refresh_status['in_progress']:
            return jsonify({'error': 'Refresh already in progress'}), 400
            
        refresh_status.update({
            'in_progress': True,
            'items': [],
            'start_time': time.time(),
            'completed': False
        })
        
        # Start background task
        import threading
        thread = threading.Thread(target=refresh_data_task)
        thread.daemon = True
        thread.start()
        
        return jsonify({'message': 'Refresh started'})
        
    except Exception as e:
        logger.error(f"Error starting refresh: {str(e)}")
        return jsonify({'error': str(e)}), 500

def refresh_data_task():
    global refresh_status
    try:
        # Get tables and queries to refresh
        with db_manager.app_config_engine.connect() as conn:
            tables = conn.execute(text("SELECT * FROM ConfigTables")).fetchall()
            queries = conn.execute(text("SELECT * FROM CustomQueries")).fetchall()
        
        start_time = time.time()
        
        # Process tables
        for table in tables:
            # Get total row count first
            with db_manager.get_remote_engine().connect() as remote_conn:
                total_count = remote_conn.execute(text(f"SELECT COUNT(*) FROM {table.name}")).scalar()
            
            refresh_status['items'].append({
                'name': table.name,
                'type': 'table',
                'status': 'syncing',
                'start_time': time.time(),
                'rows_fetched': 0,
                'total_rows': total_count,
                'execution_time': 0
            })
            
            try:
                # Fetch and store data
                from data_fetcher import ATMDataFetcher
                fetcher = ATMDataFetcher(db_manager.get_remote_engine(), db_manager.local_engine)
                
                # Update the fetch_and_store_data method to accept a callback
                def progress_callback(fetched_rows):
                    item = next(item for item in refresh_status['items'] if item['name'] == table.name)
                    item.update({
                        'rows_fetched': fetched_rows,
                        'progress': (fetched_rows / total_count * 100) if total_count > 0 else 0
                    })
                
                data = fetcher.fetch_and_store_data(table.name, table.update_column, None, progress_callback)
                
                # Update status
                item = next(item for item in refresh_status['items'] if item['name'] == table.name)
                item.update({
                    'status': 'completed',
                    'rows_fetched': len(data) if data is not None else 0,
                    'total_rows': total_count,
                    'execution_time': round(time.time() - item['start_time'], 2)
                })
                
            except Exception as e:
                logger.error(f"Error refreshing table {table.name}: {str(e)}")
                item = next(item for item in refresh_status['items'] if item['name'] == table.name)
                item.update({
                    'status': 'error',
                    'error_message': str(e),
                    'execution_time': round(time.time() - item['start_time'], 2)
                })
        
        # Process custom queries
        for query in queries:
            # Get total row count first by executing a COUNT version of the query
            count_query = f"SELECT COUNT(*) FROM ({query.sql_query}) as subquery"
            with db_manager.get_remote_engine().connect() as remote_conn:
                total_count = remote_conn.execute(text(count_query)).scalar()
            
            refresh_status['items'].append({
                'name': query.name,
                'type': 'query',
                'status': 'syncing',
                'start_time': time.time(),
                'rows_fetched': 0,
                'total_rows': total_count,
                'execution_time': 0
            })
            
            try:
                # Execute query with progress tracking
                rows_fetched = 0
                with db_manager.get_remote_engine().connect() as remote_conn:
                    result = remote_conn.execution_options(stream_results=True).execute(text(query.sql_query))
                    
                    # Process results in chunks
                    chunk_size = 1000
                    chunks = []
                    while True:
                        chunk = result.fetchmany(chunk_size)
                        if not chunk:
                            break
                        chunks.append(chunk)
                        rows_fetched += len(chunk)
                        
                        # Update progress
                        item = next(item for item in refresh_status['items'] if item['name'] == query.name)
                        item.update({
                            'rows_fetched': rows_fetched,
                            'progress': (rows_fetched / total_count * 100) if total_count > 0 else 0
                        })
                    
                    # Combine all chunks and store results
                    if chunks:
                        import pandas as pd
                        df = pd.DataFrame([row._mapping for chunk in chunks for row in chunk])
                        df.columns = result.keys()
                        
                        with db_manager.local_engine.begin() as local_conn:
                            df.to_sql(f"custom_query_{query.id}", local_conn, if_exists='replace', index=False)
                    
                    # Update status
                    item = next(item for item in refresh_status['items'] if item['name'] == query.name)
                    item.update({
                        'status': 'completed',
                        'rows_fetched': rows_fetched,
                        'total_rows': total_count,
                        'execution_time': round(time.time() - item['start_time'], 2)
                    })
                    
            except Exception as e:
                logger.error(f"Error executing query {query.name}: {str(e)}")
                item = next(item for item in refresh_status['items'] if item['name'] == query.name)
                item.update({
                    'status': 'error',
                    'error_message': str(e),
                    'execution_time': round(time.time() - item['start_time'], 2)
                })
        
        # Update completion status
        refresh_status.update({
            'completed': True,
            'in_progress': False,
            'total_time': round(time.time() - start_time, 2)
        })
        
    except Exception as e:
        logger.error(f"Error in refresh task: {str(e)}")
        refresh_status.update({
            'completed': True,
            'in_progress': False,
            'error': str(e)
        })

@app.route('/refresh_status')
def get_refresh_status():
    """Get the current status of the refresh operation"""
    global refresh_status
    if refresh_status['in_progress']:
        return jsonify(refresh_status)
    else:
        # Return last refresh history
        try:
            with db_manager.app_config_engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT timestamp, total_time, total_rows, items, status
                        FROM RefreshHistory
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """)
                ).fetchone()
                
                if result:
                    # Parse the timestamp string into a datetime object if it's a string
                    timestamp = result.timestamp
                    if isinstance(timestamp, str):
                        from datetime import datetime
                        try:
                            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            try:
                                timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                timestamp = datetime.now()
                    
                    return jsonify({
                        'completed': True,
                        'in_progress': False,
                        'items': json.loads(result.items),
                        'total_time': result.total_time,
                        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # Format as string
                        'total_rows': result.total_rows,
                        'status': result.status
                    })
                return jsonify({
                    'completed': True,
                    'in_progress': False,
                    'items': [],
                    'error': 'No refresh history found'
                })
        except Exception as e:
            logger.error(f"Error getting refresh status: {str(e)}")
            return jsonify({'error': str(e)}), 500

# Add these routes to app.py

@app.route('/update_flask_settings', methods=['POST'])
def update_flask_settings():
    try:
        port = request.form.get('flask_port')
        if port:
            os.environ['FLASK_PORT'] = port
            set_key('.env', 'FLASK_PORT', port)
            flash('Flask settings updated successfully', 'success')
        return redirect(url_for('settings'))
    except Exception as e:
        logger.error(f"Error updating Flask settings: {str(e)}")
        flash(f'Error updating Flask settings: {str(e)}', 'error')
        return redirect(url_for('settings'))

@app.route('/update_db_settings', methods=['POST'])
def update_db_settings():
    try:
        # Create a mapping between form fields and environment variables
        field_to_env = {
            'host': 'DB_HOST',
            'port': 'DB_PORT',
            'db_name': 'DB_NAME',
            'db_username': 'DB_USERNAME',
            'db_password': 'DB_PASSWORD'
        }
        
        # Save each setting
        for form_field, env_var in field_to_env.items():
            value = request.form.get(form_field)
            if value:  # Only update if value is not empty
                os.environ[env_var] = value
                set_key('.env', env_var, value)
        
        flash('Database settings updated successfully', 'success')
    except Exception as e:
        logger.error(f"Error updating database settings: {str(e)}")
        flash(f'Error updating database settings: {str(e)}', 'error')
    
    return redirect(url_for('settings'))

@app.route('/stats/charts', methods=['GET'])
def charts_config():
    try:
        with db_manager.local_engine.connect() as conn:
            inspector = inspect(db_manager.local_engine)
            available_tables = inspector.get_table_names()
        
        with db_manager.app_config_engine.connect() as conn:
            charts = conn.execute(text("SELECT * FROM ChartConfigurations")).fetchall()
        
        return render_template('charts_config.html',
                             available_tables=available_tables,
                             charts=charts)
    except Exception as e:
        logger.error(f"Error accessing charts config: {str(e)}")
        flash(f'Error accessing charts config: {str(e)}', 'error')
        return redirect(url_for('stats'))

@app.route('/test_connection', methods=['GET'])
def test_db_connection_route():
    try:
        source_engine = db_manager.get_remote_engine()
        if db_manager.test_connection(source_engine):
            return jsonify({'message': 'Connection successful!'})
        return jsonify({'error': 'Connection failed'}), 500
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/save_layout', methods=['POST'])
def save_layout():
    try:
        layout_data = request.get_json()  # Use get_json() instead of .json
        if not layout_data:
            return jsonify({'error': 'No data provided'}), 400
            
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT OR REPLACE INTO UserLayouts (id, layout_data, updated_at)
                    VALUES ('default', :layout_data, CURRENT_TIMESTAMP)
                """),
                {'layout_data': json.dumps(layout_data)}
            )
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error saving layout: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_layout')
def get_layout():
    try:
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(
                text("SELECT layout_data FROM UserLayouts WHERE id = 'default'")
            ).fetchone()
            if result:
                return jsonify(json.loads(result[0]))
        return jsonify(None)
    except Exception as e:
        logger.error(f"Error getting layout: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/add_table', methods=['POST'])
def add_table():
    try:
        table_name = request.form.get('table_name')
        update_column = request.form.get('update_column')
        
        if not table_name or not update_column:
            flash('Table name and update column are required', 'error')
            return redirect(url_for('settings'))
            
        # Add table to configuration
        with db_manager.app_config_engine.begin() as conn:
            result = conn.execute(
                text("INSERT INTO ConfigTables (name, update_column) VALUES (:name, :update_column) RETURNING id"),
                {'name': table_name, 'update_column': update_column}
            )
            table_id = result.fetchone()[0]
            
            # Add initial status
            conn.execute(
                text("""
                    INSERT INTO TableSyncStatus 
                    (table_id, status, rows_fetched, last_sync) 
                    VALUES (:table_id, 'pending', 0, CURRENT_TIMESTAMP)
                """),
                {'table_id': table_id}
            )
        
        # Start background task to fetch data
        import threading
        thread = threading.Thread(target=fetch_table_data, args=(table_id, table_name, update_column))
        thread.daemon = True
        thread.start()
        
        flash('Table added successfully. Data sync started.', 'success')
        return redirect(url_for('settings'))
        
    except Exception as e:
        logger.error(f"Error adding table: {str(e)}")
        flash(f'Error adding table: {str(e)}', 'error')
        return redirect(url_for('settings'))

def fetch_table_data(table_id, table_name, update_column):
    """Background task to fetch table data"""
    try:
        # Update status to 'syncing'
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE TableSyncStatus 
                    SET status = 'syncing', last_sync = CURRENT_TIMESTAMP 
                    WHERE table_id = :table_id
                """),
                {'table_id': table_id}
            )
        
        # Fetch and store data
        from data_fetcher import ATMDataFetcher
        fetcher = ATMDataFetcher(db_manager.get_remote_engine(), db_manager.local_engine)
        data = fetcher.fetch_and_store_data(table_name, update_column, None)
        rows_fetched = len(data) if data is not None else 0
        
        # Update status to 'completed'
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE TableSyncStatus 
                    SET status = 'completed', 
                        rows_fetched = :rows_fetched, 
                        last_sync = CURRENT_TIMESTAMP 
                    WHERE table_id = :table_id
                """),
                {'table_id': table_id, 'rows_fetched': rows_fetched}
            )
            
    except Exception as e:
        logger.error(f"Error fetching data for table {table_name}: {str(e)}")
        # Update status to 'error'
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE TableSyncStatus 
                    SET status = 'error', 
                        error_message = :error,
                        last_sync = CURRENT_TIMESTAMP 
                    WHERE table_id = :table_id
                """),
                {'table_id': table_id, 'error': str(e)}
            )

@app.route('/get_table_status/<int:table_id>')
def get_table_status(table_id):
    """Get the current sync status of a table"""
    try:
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT status, rows_fetched, error_message, 
                           strftime('%Y-%m-%d %H:%M:%S', last_sync) as last_sync 
                    FROM TableSyncStatus 
                    WHERE table_id = :table_id
                """),
                {'table_id': table_id}
            ).fetchone()
            
            if result:
                return jsonify({
                    'status': result.status,
                    'rows_fetched': result.rows_fetched,
                    'error_message': result.error_message,
                    'last_sync': result.last_sync
                })
            return jsonify({'error': 'Table not found'}), 404
            
    except Exception as e:
        logger.error(f"Error getting table status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/add_custom_query', methods=['POST'])
def add_custom_query():
    try:
        query_name = request.form.get('query_name')
        sql_query = request.form.get('sql_query')
        update_column = request.form.get('update_column')
        query_id = request.form.get('query_id')
        
        if not all([query_name, sql_query, update_column]):
            flash('All fields are required', 'error')
            return redirect(url_for('settings'))
            
        # Add or update query
        with db_manager.app_config_engine.begin() as conn:
            if query_id:  # Update existing query
                conn.execute(
                    text("""
                        UPDATE CustomQueries 
                        SET name = :name, sql_query = :sql_query, update_column = :update_column 
                        WHERE id = :id
                    """),
                    {'id': query_id, 'name': query_name, 'sql_query': sql_query, 'update_column': update_column}
                )
            else:  # Add new query
                result = conn.execute(
                    text("""
                        INSERT INTO CustomQueries (name, sql_query, update_column) 
                        VALUES (:name, :sql_query, :update_column)
                        RETURNING id
                    """),
                    {'name': query_name, 'sql_query': sql_query, 'update_column': update_column}
                )
                query_id = result.fetchone()[0]
                
                # Add initial sync status
                conn.execute(
                    text("""
                        INSERT INTO CustomQuerySyncStatus 
                        (query_id, status, rows_fetched, last_sync) 
                        VALUES (:query_id, 'pending', 0, CURRENT_TIMESTAMP)
                    """),
                    {'query_id': query_id}
                )
        
        # Start background task to execute query
        import threading
        thread = threading.Thread(target=execute_custom_query, args=(query_id,))
        thread.daemon = True
        thread.start()
        
        flash('Custom query saved successfully. Data sync started.', 'success')
        return redirect(url_for('settings'))
        
    except Exception as e:
        logger.error(f"Error adding custom query: {str(e)}")
        flash(f'Error adding custom query: {str(e)}', 'error')
        return redirect(url_for('settings'))

def execute_custom_query(query_id):
    """Background task to execute custom query"""
    try:
        import time
        start_time = time.time()
        
        # Update status to 'syncing'
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE CustomQuerySyncStatus 
                    SET status = 'syncing', last_sync = CURRENT_TIMESTAMP 
                    WHERE query_id = :query_id
                """),
                {'query_id': query_id}
            )
            
            # Get query details
            result = conn.execute(
                text("SELECT name, sql_query FROM CustomQueries WHERE id = :id"),
                {'id': query_id}
            ).fetchone()
            
            if not result:
                raise Exception("Query not found")
                
            query_name = result.name
            sql_query = result.sql_query
        
        # Execute query on remote database
        with db_manager.get_remote_engine().connect() as remote_conn:
            result = remote_conn.execute(text(sql_query))
            rows = result.fetchall()
            
            # Store results in local database
            if rows:
                import pandas as pd
                df = pd.DataFrame(rows)
                df.columns = result.keys()
                
                with db_manager.local_engine.begin() as local_conn:
                    # Create or replace table
                    df.to_sql(f"custom_query_{query_id}", local_conn, if_exists='replace', index=False)
                    
                rows_fetched = len(rows)
            else:
                rows_fetched = 0
        
        execution_time = time.time() - start_time
        
        # Update status to 'completed'
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE CustomQuerySyncStatus 
                    SET status = 'completed', 
                        rows_fetched = :rows_fetched,
                        execution_time = :execution_time,
                        last_sync = CURRENT_TIMESTAMP 
                    WHERE query_id = :query_id
                """),
                {
                    'query_id': query_id, 
                    'rows_fetched': rows_fetched,
                    'execution_time': execution_time
                }
            )
            
    except Exception as e:
        logger.error(f"Error executing custom query {query_id}: {str(e)}")
        # Update status to 'error'
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE CustomQuerySyncStatus 
                    SET status = 'error', 
                        error_message = :error,
                        last_sync = CURRENT_TIMESTAMP 
                    WHERE query_id = :query_id
                """),
                {'query_id': query_id, 'error': str(e)}
            )

@app.route('/get_custom_query_status/<int:query_id>')
def get_custom_query_status(query_id):
    """Get the current sync status of a custom query"""
    try:
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT status, rows_fetched, error_message, execution_time,
                           strftime('%Y-%m-%d %H:%M:%S', last_sync) as last_sync 
                    FROM CustomQuerySyncStatus 
                    WHERE query_id = :query_id
                """),
                {'query_id': query_id}
            ).fetchone()
            
            if result:
                return jsonify({
                    'status': result.status,
                    'rows_fetched': result.rows_fetched,
                    'error_message': result.error_message,
                    'execution_time': result.execution_time,
                    'last_sync': result.last_sync
                })
            return jsonify({'error': 'Query not found'}), 404
            
    except Exception as e:
        logger.error(f"Error getting query status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/delete_custom_query/<int:query_id>', methods=['POST'])
def delete_custom_query(query_id):
    try:
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM CustomQueries WHERE id = :id"),
                {'id': query_id}
            )
        flash('Custom query deleted successfully', 'success')
    except Exception as e:
        logger.error(f"Error deleting custom query: {str(e)}")
        flash(f'Error deleting custom query: {str(e)}', 'error')
    return redirect(url_for('settings'))

@app.route('/get_custom_query/<int:query_id>')
def get_custom_query(query_id):
    try:
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM CustomQueries WHERE id = :id"),
                {"id": query_id}
            ).fetchone()
            
            if result:
                # Convert Row to dict to ensure all fields are included
                query_data = dict(result._mapping)
                logger.info(f"Retrieved custom query data: {query_data}")
                return jsonify({
                    'id': query_data['id'],
                    'name': query_data['name'],
                    'sql_query': query_data['sql_query'],
                    'update_column': query_data['update_column']
                })
            return jsonify({'error': 'Query not found'}), 404
    except Exception as e:
        logger.error(f"Error getting custom query: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_kpi_config_by_name/<string:kpi_name>')
def get_kpi_config_by_name(kpi_name):
    try:
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM KPIConfigurations WHERE LOWER(name) = LOWER(:name)"),
                {"name": kpi_name}
            ).fetchone()
            
            if result:
                kpi_data = row_to_dict(result)
                return jsonify(kpi_data)
            return jsonify({'error': 'KPI not found'}), 404
    except Exception as e:
        logger.error(f"Error getting KPI config: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_db_structure')
def get_db_structure():
    try:
        source_engine = db_manager.get_remote_engine()
        
        if not db_manager.test_connection(source_engine):
            return jsonify({'error': 'Could not connect to database'}), 500
        
        inspector = inspect(source_engine)
        tables = []  # Change from dict to list
        
        # Get all tables from the remote database
        for table_name in inspector.get_table_names(schema='dbo'):  # Add schema='dbo' for SQL Server
            try:
                columns = []
                for column in inspector.get_columns(table_name, schema='dbo'):  # Add schema here too
                    columns.append({
                        'name': column['name'],
                        'type': str(column['type']),
                        'nullable': column['nullable']
                    })
                tables.append({  # Add table object to list
                    'name': table_name,
                    'columns': columns
                })
                logger.info(f"Retrieved {len(columns)} columns for table {table_name}")
            except Exception as e:
                logger.error(f"Error getting columns for table {table_name}: {str(e)}")
                continue
        
        logger.info(f"Retrieved structure for {len(tables)} tables")
        return jsonify(tables)  # Return array directly
        
    except Exception as e:
        logger.error(f"Error getting database structure: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Add this route to app.py
@app.route('/delete_kpi/<int:kpi_id>', methods=['POST'])
def delete_kpi(kpi_id):
    try:
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM KPIConfigurations WHERE id = :id"),
                {'id': kpi_id}
            )
        flash('KPI deleted successfully', 'success')
    except Exception as e:
        logger.error(f"Error deleting KPI: {str(e)}")
        flash(f'Error deleting KPI: {str(e)}', 'error')
    return redirect(url_for('stats_config'))

# Update the save_kpi route in app.py
@app.route('/save_kpi', methods=['POST'])
def save_kpi():
    try:
        kpi_data = request.get_json()
        if not kpi_data:
            return jsonify({'error': 'No data provided'}), 400

        # Validate required fields
        required_fields = ['name', 'table_name', 'date_column', 'time_spans', 'calculation_steps']
        for field in required_fields:
            if field not in kpi_data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # Ensure JSON fields are properly serialized
        for field in ['time_spans', 'conditions', 'calculation_steps', 'dimensions']:
            if field in kpi_data and isinstance(kpi_data[field], (dict, list)):
                kpi_data[field] = json.dumps(kpi_data[field])

        # Save KPI using KPIManager
        success = kpi_manager.save_kpi(kpi_data)
        if success:
            return jsonify({'success': True})
        return jsonify({'error': 'Failed to save KPI'}), 500

    except Exception as e:
        logger.error(f"Error saving KPI: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_table_columns/<table_name>')
def get_table_columns(table_name):
    try:
        # Get column information from the remote database
        source_engine = db_manager.get_remote_engine()
        with source_engine.connect() as conn:
            inspector = inspect(source_engine)
            columns = []
            for column in inspector.get_columns(table_name):
                columns.append({
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column['nullable']
                })
            
            # Return column names as a simple list for dropdowns
            column_names = [col['name'] for col in columns]
            logger.info(f"Retrieved {len(column_names)} columns for table {table_name}")
            return jsonify(column_names)
            
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/get_column_info/<table_name>/<column_name>')
def get_column_info(table_name, column_name):
    try:
        with db_manager.local_engine.connect() as conn:
            inspector = inspect(db_manager.local_engine)
            for column in inspector.get_columns(table_name):
                if column['name'] == column_name:
                    # Determine column type
                    col_type = str(column['type']).lower()
                    if 'date' in col_type or 'time' in col_type:
                        type_info = 'date'
                    elif 'int' in col_type or 'float' in col_type or 'decimal' in col_type:
                        type_info = 'numeric'
                    else:
                        type_info = 'string'
                        
                    return jsonify({
                        'name': column['name'],
                        'type': type_info,
                        'nullable': column['nullable']
                    })
            
            return jsonify({'error': 'Column not found'}), 404
            
    except Exception as e:
        logger.error(f"Error getting column info for {table_name}.{column_name}: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Move this route up with other routes, before the main execution block
@app.route('/delete_table/<int:table_id>', methods=['POST'])
def delete_table(table_id):
    try:
        with db_manager.app_config_engine.begin() as conn:
            # Get the table name before deleting
            result = conn.execute(
                text("SELECT name FROM ConfigTables WHERE id = :id"),
                {'id': table_id}
            ).fetchone()
            
            if result:
                table_name = result[0]
                # Drop the table from local database if it exists
                with db_manager.local_engine.begin() as local_conn:
                    local_conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                
                # Delete the table configuration
                conn.execute(
                    text("DELETE FROM ConfigTables WHERE id = :id"),
                    {'id': table_id}
                )
                
            flash('Table configuration deleted successfully', 'success')
            
    except Exception as e:
        logger.error(f"Error deleting table: {str(e)}")
        flash(f'Error deleting table: {str(e)}', 'error')
        
    return redirect(url_for('settings'))

@app.route('/fetch_manually/<type>/<int:id>', methods=['POST'])
def fetch_manually(type, id):
    try:
        if type == 'table':
            # Get table info
            with db_manager.app_config_engine.connect() as conn:
                table = conn.execute(
                    text("SELECT name, update_column FROM ConfigTables WHERE id = :id"),
                    {'id': id}
                ).fetchone()
                
                if not table:
                    return jsonify({'error': 'Table not found'}), 404
                
                # Update status to pending
                conn.execute(
                    text("""
                        UPDATE TableSyncStatus 
                        SET status = 'pending', last_sync = CURRENT_TIMESTAMP 
                        WHERE table_id = :id
                    """),
                    {'id': id}
                )
                
                # Start background task to fetch data
                import threading
                thread = threading.Thread(
                    target=fetch_table_data,
                    args=(id, table.name, table.update_column)
                )
                thread.daemon = True
                thread.start()
                
        elif type == 'query':
            # Get query info
            with db_manager.app_config_engine.connect() as conn:
                query = conn.execute(
                    text("SELECT * FROM CustomQueries WHERE id = :id"),
                    {'id': id}
                ).fetchone()
                
                if not query:
                    return jsonify({'error': 'Query not found'}), 404
                
                # Update status to pending
                conn.execute(
                    text("""
                        UPDATE CustomQuerySyncStatus 
                        SET status = 'pending', last_sync = CURRENT_TIMESTAMP 
                        WHERE query_id = :id
                    """),
                    {'id': id}
                )
                
                # Start background task to execute query
                import threading
                thread = threading.Thread(
                    target=execute_custom_query,
                    args=(id,)
                )
                thread.daemon = True
                thread.start()
        
        return jsonify({'success': True})
        
    except Exception as e:
        logger.error(f"Error initiating manual fetch: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Main execution
if __name__ == '__main__':
    try:
        logger.info("Starting ATM Forecasting application")
        host = '127.0.0.1'
        port = int(os.environ.get('FLASK_PORT', 5000))
        
        def is_port_in_use(port):
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind((host, port))
                    return False
                except socket.error:
                    return True
        
        # Try to find an available port
        original_port = port
        while is_port_in_use(port):
            logger.warning(f"Port {port} is already in use")
            port += 1
            if port > original_port + 10:  # Limit the search to 10 ports
                raise Exception(f"Could not find an available port after trying {original_port} through {port-1}")
        
        if port != original_port:
            logger.info(f"Using alternative port {port}")
            os.environ['FLASK_PORT'] = str(port)
            
        logger.info(f"Running on http://{host}:{port}")
        app.run(
            debug=True, 
            host=host, 
            port=port, 
            use_reloader=True,
            use_debugger=True,
            threaded=True
        )
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}", exc_info=True)
        raise

