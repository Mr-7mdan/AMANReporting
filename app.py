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
from data_fetcher import ATMDataFetcher  # Add this at the top with other imports

# Near the top of app.py, update the logging configuration
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Reduce SQL Alchemy logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# Create Flask app
app = Flask(__name__, 
           template_folder='templates',
           static_folder='static')

# Set secret key
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'fallback_secret_key')

# Initialize managers
db_manager = DatabaseManager()

# Ensure tables exist in both databases
db_manager.create_tables()
db_manager.recreate_tables()

kpi_manager = KPIManager(db_manager)
chart_manager = ChartManager(db_manager)

# Add these near the top of app.py after creating the Flask app
@app.template_filter('from_json')
def from_json_filter(value):
    return json.loads(value)

@app.template_filter('abbreviate_timespan')
def abbreviate_timespan_filter(value):
    """Convert time span names to abbreviations"""
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
    """Convert operator to symbol"""
    symbols = {
        '=': '=',
        '!=': '≠',
        '>': '>',
        '>=': '≥',
        '<': '<',
        '<=': '≤'
    }
    return symbols.get(value, value)

@app.template_filter('format_condition')
def format_condition_filter(condition):
    """Format condition for display"""
    if isinstance(condition, str):
        condition = json.loads(condition)
    return f"{condition['field']} {operator_symbol_filter(condition['operator'])} {condition['value']}"

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
    try:
        logger.info("Accessing settings page")
        config = db_manager.get_config()
        last_updated = get_last_updated()
        
        with db_manager.app_config_engine.connect() as connection:
            # Get added tables with their sync status
            tables_query = text("""
                SELECT t.*, ts.status, ts.rows_fetched, ts.error_message, 
                       ts.last_sync, ts.progress
                FROM ConfigTables t
                LEFT JOIN TableSyncStatus ts ON t.id = ts.table_id
                ORDER BY t.name
            """)
            result = connection.execute(tables_query)
            added_tables = result.fetchall()
            logger.info(f"Retrieved {len(added_tables)} tables from ConfigTables: {[t.name for t in added_tables]}")
            
            # Get custom queries with their sync status
            queries_query = text("""
                SELECT q.*, qs.status, qs.rows_fetched, qs.error_message,
                       qs.last_sync, qs.progress, qs.execution_time
                FROM CustomQueries q
                LEFT JOIN CustomQuerySyncStatus qs ON q.id = qs.query_id
                ORDER BY q.name
            """)
            result = connection.execute(queries_query)
            custom_queries = result.fetchall()
            logger.info(f"Retrieved {len(custom_queries)} custom queries")
        
        return render_template('settings.html', 
                             config=config, 
                             added_tables=added_tables, 
                             custom_queries=custom_queries,
                             last_updated=last_updated)
                             
    except Exception as e:
        logger.error(f"Error loading settings page: {str(e)}", exc_info=True)
        flash(f'Error loading settings: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/stats')
def stats():
    try:
        # Get grid size preference with initialization
        with db_manager.app_config_engine.begin() as conn:
            # Check if UserPreferences table exists and has a default record
            result = conn.execute(text("""
                INSERT OR IGNORE INTO UserPreferences (id, grid_size, created_at, updated_at)
                VALUES (1, 48, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """))
            
            # Get grid size (will now always exist)
            result = conn.execute(text("SELECT grid_size FROM UserPreferences WHERE id = 1")).fetchone()
            grid_size = result.grid_size if result else 48  # Fallback to default if query fails
            
            logger.info(f"Using grid size: {grid_size}")
        
        last_updated = get_last_updated()
        
        # Get KPIs from database and calculate their values
        with db_manager.app_config_engine.connect() as conn:
            kpi_configs = conn.execute(text("SELECT * FROM KPIConfigurations")).fetchall()
            
        # Calculate KPI values
        kpis = []
        for kpi_config in kpi_configs:
            kpi_values = kpi_manager.calculate_kpi_values(kpi_config)
            kpis.extend(kpi_values)
        
        # Get enabled charts
        with db_manager.app_config_engine.connect() as conn:
            charts = conn.execute(
                text("SELECT * FROM ChartConfigurations WHERE is_enabled = 1")
            ).fetchall()
            
        # Convert charts to list of dictionaries with their data
        chart_data = []
        for chart in charts:
            # Convert SQLAlchemy Row to dictionary using _mapping
            chart_dict = dict(chart._mapping)
            
            # Parse JSON strings
            for field in ['x_axis', 'y_axis', 'time_spans']:
                if chart_dict.get(field):
                    try:
                        chart_dict[field] = json.loads(chart_dict[field])
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse {field} for chart {chart_dict.get('id')}")
                        chart_dict[field] = None
            
            chart_data.append(chart_dict)
        
        logger.info(f"Rendering stats page with {len(kpis)} KPIs and {len(chart_data)} charts")
        
        return render_template('stats.html',
                             last_updated=last_updated,
                             kpis=kpis,
                             charts=chart_data,
                             grid_size=grid_size)
                             
    except Exception as e:
        logger.error(f"Error rendering stats page: {str(e)}")
        flash(f'Error loading statistics: {str(e)}', 'error')
        return redirect(url_for('index'))

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
    """Background task to refresh all data"""
    try:
        start_time = time.time()
        total_rows = 0
        items = []
        
        # Get tables from ConfigTables
        with db_manager.app_config_engine.begin() as conn:
            tables = conn.execute(text("SELECT * FROM ConfigTables")).fetchall()
            queries = conn.execute(text("SELECT * FROM CustomQueries")).fetchall()
            
            # Update status for all tables to pending
            for table in tables:
                conn.execute(
                    text("""
                        UPDATE TableSyncStatus 
                        SET status = 'pending', 
                            last_sync = CURRENT_TIMESTAMP,
                            rows_fetched = 0,
                            progress = 0
                        WHERE table_id = :id
                    """),
                    {'id': table.id}
                )
            
            # Update status for all queries to pending
            for query in queries:
                conn.execute(
                    text("""
                        UPDATE CustomQuerySyncStatus 
                        SET status = 'pending', 
                            last_sync = CURRENT_TIMESTAMP,
                            rows_fetched = 0,
                            progress = 0
                        WHERE query_id = :id
                    """),
                    {'id': query.id}
                )
            
            # Record initial refresh history
            conn.execute(
                text("""
                    INSERT INTO RefreshHistory 
                    (timestamp, total_time, total_rows, items, status)
                    VALUES (CURRENT_TIMESTAMP, 0, 0, :items, 'in_progress')
                """),
                {
                    'items': json.dumps([{
                        'name': table.name,
                        'status': 'pending',
                        'rows_fetched': 0,
                        'progress': 0
                    } for table in tables] + [{
                        'name': f"Query: {query.name}",
                        'status': 'pending',
                        'rows_fetched': 0,
                        'progress': 0
                    } for query in queries])
                }
            )
        
        # Process tables first
        for table in tables:
            try:
                logger.info(f"Processing table: {table.name}")
                fetcher = ATMDataFetcher(db_manager.get_remote_engine(), db_manager.local_engine)
                
                def progress_callback(rows_fetched, total_count=None):
                    with db_manager.app_config_engine.begin() as conn:
                        progress = (rows_fetched / total_count * 100) if total_count else 0
                        # Update table status
                        conn.execute(
                            text("""
                                UPDATE TableSyncStatus 
                                SET status = 'syncing',
                                    rows_fetched = :rows_fetched,
                                    progress = :progress
                                WHERE table_id = :id
                            """),
                            {
                                'id': table.id,
                                'rows_fetched': rows_fetched,
                                'progress': progress
                            }
                        )
                        # Update refresh history
                        update_refresh_history(conn, table.name, 'syncing', rows_fetched, progress)
                
                start = time.time()
                data = fetcher.fetch_and_store_data(table.name, table.update_column, None, progress_callback)
                execution_time = time.time() - start
                
                rows = len(data) if hasattr(data, '__len__') else 0
                total_rows += rows
                
                items.append({
                    'name': table.name,
                    'status': 'completed',
                    'rows_fetched': rows,
                    'execution_time': round(execution_time, 2)
                })
                
                # Update final status
                with db_manager.app_config_engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE TableSyncStatus 
                            SET status = 'completed', 
                                rows_fetched = :rows,
                                last_sync = CURRENT_TIMESTAMP,
                                progress = 100
                            WHERE table_id = :id
                        """),
                        {'id': table.id, 'rows': rows}
                    )
                    update_refresh_history(conn, table.name, 'completed', rows, 100, execution_time)
                
            except Exception as e:
                logger.error(f"Error processing table {table.name}: {str(e)}")
                with db_manager.app_config_engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE TableSyncStatus 
                            SET status = 'error', 
                                error_message = :error,
                                last_sync = CURRENT_TIMESTAMP 
                            WHERE table_id = :id
                        """),
                        {'id': table.id, 'error': str(e)}
                    )
                    update_refresh_history(conn, table.name, 'error', 0, 0, error_message=str(e))
        
        # Then process custom queries
        for query in queries:
            try:
                logger.info(f"Processing custom query: {query.name}")
                start = time.time()
                execute_custom_query(query.id)  # This function handles its own status updates
                execution_time = time.time() - start
                
                # Get the results
                with db_manager.app_config_engine.connect() as conn:
                    result = conn.execute(
                        text("SELECT rows_fetched FROM CustomQuerySyncStatus WHERE query_id = :id"),
                        {'id': query.id}
                    ).fetchone()
                    
                    rows = result.rows_fetched if result else 0
                    total_rows += rows
                    
                    items.append({
                        'name': f"Query: {query.name}",
                        'status': 'completed',
                        'rows_fetched': rows,
                        'execution_time': round(execution_time, 2)
                    })
                    
                    update_refresh_history(conn, f"Query: {query.name}", 'completed', rows, 100, execution_time)
            
            except Exception as e:
                logger.error(f"Error processing query {query.name}: {str(e)}")
                with db_manager.app_config_engine.begin() as conn:
                    update_refresh_history(conn, f"Query: {query.name}", 'error', 0, 0, error_message=str(e))
        
        # Record final refresh history
        total_time = time.time() - start_time
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO RefreshHistory 
                    (timestamp, total_time, total_rows, items, status)
                    VALUES (CURRENT_TIMESTAMP, :total_time, :total_rows, :items, 'completed')
                """),
                {
                    'total_time': round(total_time, 2),
                    'total_rows': total_rows,
                    'items': json.dumps(items)
                }
            )
        
        logger.info(f"Refresh completed in {round(total_time, 2)}s, processed {total_rows} rows")
        return items
        
    except Exception as e:
        logger.error(f"Error in refresh task: {str(e)}")
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO RefreshHistory 
                    (timestamp, items, status)
                    VALUES (CURRENT_TIMESTAMP, :items, 'error')
                """),
                {
                    'items': json.dumps([{
                        'name': 'Refresh Task',
                        'status': 'error',
                        'error_message': str(e)
                    }])
                }
            )
        raise

def update_refresh_history(conn, name, status, rows_fetched, progress, execution_time=None, error_message=None):
    """Helper function to update refresh history during process"""
    # Get current history
    result = conn.execute(
        text("SELECT items FROM RefreshHistory WHERE status = 'in_progress' ORDER BY timestamp DESC LIMIT 1")
    ).fetchone()
    
    if result:
        items = json.loads(result.items)
        # Update the specific item
        for item in items:
            if item['name'] == name:
                item.update({
                    'status': status,
                    'rows_fetched': rows_fetched,
                    'progress': progress
                })
                if execution_time is not None:
                    item['execution_time'] = execution_time
                if error_message is not None:
                    item['error_message'] = error_message
                break
        
        # Update history
        conn.execute(
            text("UPDATE RefreshHistory SET items = :items WHERE status = 'in_progress'"),
            {'items': json.dumps(items)}
        )

@app.route('/refresh_status')
def get_refresh_status():
    """Get the current status of the data refresh process"""
    try:
        with db_manager.app_config_engine.connect() as conn:
            # Get the latest refresh history entry
            result = conn.execute(
                text("""
                    SELECT timestamp, total_time, total_rows, items, status
                    FROM RefreshHistory
                    ORDER BY timestamp DESC
                    LIMIT 1
                """)
            ).fetchone()
            
            if result:
                return jsonify({
                    'timestamp': result.timestamp,
                    'total_time': result.total_time,
                    'total_rows': result.total_rows,
                    'items': json.loads(result.items) if result.items else [],
                    'status': result.status,
                    'completed': result.status == 'completed',
                    'in_progress': False
                })
            
            return jsonify({
                'items': [],
                'completed': True,
                'in_progress': False
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
            # First check if table already exists
            existing = conn.execute(
                text("SELECT id FROM ConfigTables WHERE name = :name"),
                {'name': table_name}
            ).fetchone()
            
            if existing:
                flash('Table already exists', 'error')
                return redirect(url_for('settings'))
            
            # Add new table to ConfigTables
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
    """Background task to fetch and store table data"""
    try:
        logger.info(f"Starting fetch_table_data for table {table_name} (ID: {table_id})")
        
        # Update status to syncing
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE TableSyncStatus 
                    SET status = 'syncing', last_sync = CURRENT_TIMESTAMP 
                    WHERE table_id = :table_id
                """),
                {'table_id': table_id}
            )
        
        # Get total row count first
        with db_manager.get_remote_engine().connect() as remote_conn:
            total_count = remote_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            logger.info(f"Total rows to fetch for {table_name}: {total_count}")
        
        # Fetch and store data
        from data_fetcher import ATMDataFetcher
        fetcher = ATMDataFetcher(db_manager.get_remote_engine(), db_manager.local_engine)
        
        def progress_callback(fetched_rows):
            logger.info(f"Progress update for {table_name}: {fetched_rows}/{total_count} rows")
            with db_manager.app_config_engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE TableSyncStatus 
                        SET rows_fetched = :rows_fetched,
                            progress = :progress
                        WHERE table_id = :table_id
                    """),
                    {
                        'table_id': table_id,
                        'rows_fetched': fetched_rows,
                        'progress': (fetched_rows / total_count * 100) if total_count > 0 else 0
                    }
                )
        
        logger.info(f"Starting data fetch for {table_name}")
        data = fetcher.fetch_and_store_data(table_name, update_column, None, progress_callback)
        logger.info(f"Data fetch completed for {table_name}. Rows fetched: {len(data) if data is not None else 0}")
        
        # Verify data was stored
        with db_manager.local_engine.connect() as local_conn:
            stored_count = local_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            logger.info(f"Verified stored rows for {table_name}: {stored_count}")
            
            if stored_count == 0:
                raise Exception(f"No data was stored in the local database for {table_name}")
        
        # Update status to completed
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE TableSyncStatus 
                    SET status = 'completed', 
                        rows_fetched = :rows_fetched,
                        last_sync = CURRENT_TIMESTAMP 
                    WHERE table_id = :table_id
                """),
                {
                    'table_id': table_id,
                    'rows_fetched': len(data) if data is not None else 0
                }
            )
            
    except Exception as e:
        logger.error(f"Error fetching table data for {table_name}: {str(e)}", exc_info=True)
        # Update status to error
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
                           strftime('%Y-%m-%d %H:%M:%S', last_sync) as last_sync,
                           progress
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
                    'last_sync': result.last_sync,
                    'progress': result.progress
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
            
        # Add or update query within a transaction
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
                flash('Query updated successfully', 'success')
            else:  # Add new query
                # First check if query with same name exists
                existing = conn.execute(
                    text("SELECT id FROM CustomQueries WHERE name = :name"),
                    {'name': query_name}
                ).fetchone()
                
                if existing:
                    flash('A query with this name already exists', 'error')
                    return redirect(url_for('settings'))
                
                # Insert the query and get its ID
                result = conn.execute(
                    text("""
                        INSERT INTO CustomQueries (name, sql_query, update_column) 
                        VALUES (:name, :sql_query, :update_column)
                        RETURNING id
                    """),
                    {'name': query_name, 'sql_query': sql_query, 'update_column': update_column}
                )
                new_query_id = result.scalar()
                
                # Delete any existing sync status for this query (if it exists)
                conn.execute(
                    text("DELETE FROM CustomQuerySyncStatus WHERE query_id = :query_id"),
                    {'query_id': new_query_id}
                )
                
                # Insert new sync status
                conn.execute(
                    text("""
                        INSERT INTO CustomQuerySyncStatus 
                        (query_id, status, rows_fetched, last_sync) 
                        VALUES (:query_id, 'pending', 0, CURRENT_TIMESTAMP)
                    """),
                    {'query_id': new_query_id}
                )
                
                # Start background task to execute query
                import threading
                thread = threading.Thread(
                    target=execute_custom_query,
                    args=(new_query_id,)
                )
                thread.daemon = True
                thread.start()
                
                flash('Query added successfully', 'success')
                
        return redirect(url_for('settings'))
                
    except Exception as e:
        logger.error(f"Error saving custom query: {str(e)}", exc_info=True)
        flash(f'Error saving query: {str(e)}', 'error')
        return redirect(url_for('settings'))

def execute_custom_query(query_id):
    """Background task to execute and store custom query results"""
    try:
        logger.info(f"Starting execute_custom_query for query ID: {query_id}")
        start_time = time.time()
        
        # Update status to syncing
        with db_manager.app_config_engine.begin() as conn:
            # Get query details
            result = conn.execute(
                text("SELECT * FROM CustomQueries WHERE id = :id"),
                {'id': query_id}
            ).fetchone()
            
            if not result:
                raise Exception("Query not found")
            
            logger.info(f"Executing custom query: {result.name}")
            
            # Get total count first using a simpler query
            count_query = f"""
                WITH query_result AS (
                    {result.sql_query}
                )
                SELECT COUNT(*) FROM query_result
            """
            
            with db_manager.get_remote_engine().connect() as remote_conn:
                try:
                    total_count = remote_conn.execute(text(count_query)).scalar()
                    logger.info(f"Total rows to fetch: {total_count}")
                    
                    # Store total count in sync status
                    conn.execute(
                        text("""
                            UPDATE CustomQuerySyncStatus 
                            SET status = 'syncing',
                                last_sync = CURRENT_TIMESTAMP,
                                rows_fetched = 0,
                                progress = 0,
                                total_rows = :total_count
                            WHERE query_id = :query_id
                        """),
                        {'query_id': query_id, 'total_count': total_count}
                    )
                except Exception as e:
                    logger.warning(f"Failed to get total count, proceeding without progress tracking: {str(e)}")
                    total_count = None
                    
                    # Update status without total count
                    conn.execute(
                        text("""
                            UPDATE CustomQuerySyncStatus 
                            SET status = 'syncing',
                                last_sync = CURRENT_TIMESTAMP,
                                rows_fetched = 0,
                                progress = 0
                            WHERE query_id = :query_id
                        """),
                        {'query_id': query_id}
                    )
        
        # Execute query with chunked fetching
        with db_manager.get_remote_engine().connect() as remote_conn:
            logger.info("Executing query on remote database")
            result_proxy = remote_conn.execution_options(stream_results=True).execute(text(result.sql_query))
            
            # Process results in chunks
            chunk_size = 100  # Smaller chunks for more frequent updates
            chunks = []
            rows_fetched = 0
            
            while True:
                chunk = result_proxy.fetchmany(chunk_size)
                if not chunk:
                    break
                
                chunks.append(chunk)
                rows_fetched += len(chunk)
                
                # Calculate progress percentage
                if total_count:
                    progress = (rows_fetched / total_count) * 100
                    logger.info(f"Progress: {rows_fetched}/{total_count} rows ({progress:.1f}%)")
                else:
                    progress = 0
                    logger.info(f"Progress: {rows_fetched} rows fetched (total unknown)")
                
                # Update progress
                with db_manager.app_config_engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE CustomQuerySyncStatus 
                            SET rows_fetched = :rows_fetched,
                                progress = :progress
                            WHERE query_id = :query_id
                        """),
                        {
                            'query_id': query_id,
                            'rows_fetched': rows_fetched,
                            'progress': progress
                        }
                    )
            
            # Store results
            if chunks:
                import pandas as pd
                df = pd.DataFrame([dict(row._mapping) for chunk in chunks for row in chunk])
                
                # Use query name directly (sanitized for SQLite)
                table_name = ''.join(c.lower() if c.isalnum() else '_' for c in result.name)
                logger.info(f"Storing results in table: {table_name}")
                
                with db_manager.local_engine.begin() as local_conn:
                    # Drop table if exists
                    local_conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    
                    # Create table and insert data
                    df.to_sql(table_name, local_conn, if_exists='replace', index=False)
                    
                    # Verify data was stored
                    stored_count = local_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                    logger.info(f"Verified stored rows: {stored_count}")
                    
                    if stored_count == 0:
                        raise Exception(f"No data was stored in the local database for query {query_id}")
            
            execution_time = time.time() - start_time
            
            # Update status to completed
            with db_manager.app_config_engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE CustomQuerySyncStatus 
                        SET status = 'completed', 
                            rows_fetched = :rows_fetched,
                            execution_time = :execution_time,
                            last_sync = CURRENT_TIMESTAMP,
                            progress = 100
                        WHERE query_id = :query_id
                    """),
                    {
                        'query_id': query_id,
                        'rows_fetched': rows_fetched,
                        'execution_time': round(execution_time, 2)
                    }
                )
                
    except Exception as e:
        logger.error(f"Error executing custom query {query_id}: {str(e)}", exc_info=True)
        # Update status to error
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
                           strftime('%Y-%m-%d %H:%M:%S', last_sync) as last_sync,
                           progress,
                           (SELECT COUNT(*) FROM CustomQueries WHERE id = :query_id) as total_count
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
                    'last_sync': result.last_sync,
                    'progress': result.progress,
                    'total_count': result.total_count
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
        # Get column information from both local and remote databases
        local_engine = db_manager.local_engine
        remote_engine = db_manager.get_remote_engine()
        
        columns = []
        
        # Try local database first
        try:
            with local_engine.connect() as conn:
                inspector = inspect(local_engine)
                columns = [column['name'] for column in inspector.get_columns(table_name)]
                logger.info(f"Retrieved {len(columns)} columns from local database for table {table_name}")
        except Exception as local_error:
            logger.warning(f"Could not get columns from local database: {str(local_error)}")
            
            # If local fails, try remote database
            try:
                with remote_engine.connect() as conn:
                    inspector = inspect(remote_engine)
                    columns = [column['name'] for column in inspector.get_columns(table_name)]
                    logger.info(f"Retrieved {len(columns)} columns from remote database for table {table_name}")
            except Exception as remote_error:
                logger.error(f"Could not get columns from remote database: {str(remote_error)}")
                raise Exception(f"Failed to get columns from both databases for table {table_name}")
        
        if not columns:
            raise Exception(f"No columns found for table {table_name}")
            
        return jsonify(columns)
            
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
    """Handle manual fetch requests for tables and queries"""
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
                        SET status = 'pending', 
                            last_sync = CURRENT_TIMESTAMP,
                            rows_fetched = 0,
                            progress = 0
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
                        SET status = 'pending', 
                            last_sync = CURRENT_TIMESTAMP,
                            rows_fetched = 0,
                            progress = 0
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
        else:
            return jsonify({'error': 'Invalid type'}), 400
        
        return jsonify({'success': True})
        
    except Exception as e:
        logger.error(f"Error initiating manual fetch: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/save_grid_preference', methods=['POST'])
def save_grid_preference():
    try:
        data = request.json
        grid_size = data.get('grid_size')
        
        # Validate grid size
        if not isinstance(grid_size, (int, float)) or grid_size not in [48, 72, 96, 144]:
            logger.error(f"Invalid grid size received: {grid_size}")
            return jsonify({'error': f'Invalid grid size: {grid_size}. Must be one of: 48, 72, 96, 144'}), 400
        
        grid_size = int(grid_size)  # Ensure it's an integer
        logger.info(f"Saving grid size preference: {grid_size}")
        
        with db_manager.app_config_engine.begin() as conn:
            # Update or insert grid size preference
            conn.execute(
                text("""
                    INSERT OR REPLACE INTO UserPreferences 
                    (id, grid_size, created_at, updated_at)
                    VALUES (
                        1, 
                        :grid_size, 
                        COALESCE((SELECT created_at FROM UserPreferences WHERE id = 1), CURRENT_TIMESTAMP),
                        CURRENT_TIMESTAMP
                    )
                """),
                {'grid_size': grid_size}
            )
            logger.info(f"Successfully saved grid size: {grid_size}")
            
        return jsonify({'success': True, 'grid_size': grid_size})
    except Exception as e:
        logger.error(f"Error saving grid preference: {str(e)}")
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

