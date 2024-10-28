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

# Routes
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
    config = db_manager.get_config()
    
    with db_manager.app_config_engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM ConfigTables"))
        added_tables = result.fetchall()
        
        result = connection.execute(text("SELECT * FROM CustomQueries"))
        custom_queries = result.fetchall()
    
    return render_template('settings.html', 
                         config=config, 
                         added_tables=added_tables, 
                         custom_queries=custom_queries)

@app.route('/stats')
def stats():
    return render_template('stats.html')

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
        
        return render_template('stats_config.html',
                             available_tables=available_tables,
                             time_span_pairs=time_span_pairs,
                             calculation_methods=calculation_methods,
                             kpis=kpis)
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

@app.route('/refresh_data', methods=['POST'])
def refresh_data_route():
    try:
        logger.info("Starting data refresh")
        source_engine = db_manager.get_remote_engine()
        
        if not db_manager.test_connection(source_engine):
            flash('Failed to connect to source database', 'error')
            return redirect(url_for('settings'))
        
        # Refresh data using the data_fetcher module
        from data_fetcher import ATMDataFetcher
        fetcher = ATMDataFetcher(source_engine, db_manager.local_engine)
        
        with db_manager.app_config_engine.connect() as conn:
            result = conn.execute(text("SELECT name, update_column FROM ConfigTables"))
            added_tables = result.fetchall()
            
            for table_name, update_column in added_tables:
                try:
                    # Get the last record ID from the local database
                    last_record_id = fetcher.get_last_record_id(table_name, update_column)
                    logger.info(f"Last record ID for {table_name}: {last_record_id}")
                    
                    # Fetch and store new data
                    fetched_data = fetcher.fetch_and_store_data(table_name, update_column, last_record_id)
                    if not fetched_data.empty:
                        logger.info(f"Fetched {len(fetched_data)} new records for {table_name}")
                    else:
                        logger.info(f"No new data for {table_name}")
                except Exception as e:
                    logger.error(f"Error refreshing table {table_name}: {str(e)}")
                    flash(f'Error refreshing table {table_name}: {str(e)}', 'error')
        
        flash('Data refreshed successfully', 'success')
        return redirect(url_for('settings'))
        
    except Exception as e:
        logger.error(f"Error in refresh_data_route: {str(e)}", exc_info=True)
        flash(f'Error refreshing data: {str(e)}', 'error')
        return redirect(url_for('settings'))

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
        table_name = request.form['table_name']
        update_column = request.form['update_column']
        
        with db_manager.app_config_engine.begin() as conn:
            conn.execute(
                text("INSERT INTO ConfigTables (name, update_column) VALUES (:name, :update_column)"),
                {"name": table_name, "update_column": update_column}
            )
        
        flash('Table added successfully', 'success')
        return redirect(url_for('settings'))
    except Exception as e:
        logger.error(f"Error adding table: {str(e)}")
        flash(f'Error adding table: {str(e)}', 'error')
        return redirect(url_for('settings'))

@app.route('/add_custom_query', methods=['POST'])
def add_custom_query():
    try:
        query_data = {
            'name': request.form['name'],
            'sql_query': request.form['sql_query'],
            'update_column': request.form['update_column']
        }
        
        with db_manager.app_config_engine.begin() as conn:
            if request.form.get('query_id'):  # Update existing query
                query_data['id'] = request.form['query_id']
                conn.execute(
                    text("""
                        UPDATE CustomQueries 
                        SET name=:name, sql_query=:sql_query, update_column=:update_column
                        WHERE id=:id
                    """),
                    query_data
                )
                flash('Custom query updated successfully', 'success')
            else:  # Create new query
                conn.execute(
                    text("""
                        INSERT INTO CustomQueries (name, sql_query, update_column)
                        VALUES (:name, :sql_query, :update_column)
                    """),
                    query_data
                )
                flash('Custom query added successfully', 'success')
        
        return redirect(url_for('settings'))
    except Exception as e:
        logger.error(f"Error saving custom query: {str(e)}")
        flash(f'Error saving custom query: {str(e)}', 'error')
        return redirect(url_for('settings'))

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
        tables = {}
        
        for table_name in inspector.get_table_names():
            columns = []
            for column in inspector.get_columns(table_name):
                columns.append({
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column['nullable']
                })
            tables[table_name] = columns
        
        return jsonify({
            'tables': tables
        })
        
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
        # Get column information from the local database
        with db_manager.local_engine.connect() as conn:
            inspector = inspect(db_manager.local_engine)
            columns = []
            for column in inspector.get_columns(table_name):
                columns.append({
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column['nullable']
                })
            
            # Return column names as a simple list for dropdowns
            column_names = [col['name'] for col in columns]
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

