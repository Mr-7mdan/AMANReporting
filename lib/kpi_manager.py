import json
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import text

logger = logging.getLogger(__name__)

class KPIManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def get_date_range(self, period):
        """Helper function to get date range for a given time span"""
        today = datetime.now().date()
        
        if period == "Today":
            return (today, today)
        elif period == "Yesterday":
            yesterday = today - timedelta(days=1)
            return (yesterday, yesterday)
        elif period == "Month to Date":
            return (today.replace(day=1), today)
        elif period == "Last Month to Date":
            last_month = today - relativedelta(months=1)
            last_month_start = last_month.replace(day=1)
            last_month_end = today.replace(day=1) - timedelta(days=1)
            return (last_month_start, last_month_end)
        elif period == "Year to Date":
            return (today.replace(month=1, day=1), today)
        elif period == "Last Year to Date":
            last_year = today - relativedelta(years=1)
            return (last_year.replace(month=1, day=1), last_year)
        else:
            return None

    def get_calculation_sql(self, method, field):
        """Helper function to get SQL for calculation method"""
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

    def calculate_kpi_value(self, kpi, date_range, conditions, dimension_values=None):
        try:
            logger.info(f"Calculating KPI value for:")
            logger.info(f"KPI: {kpi}")
            logger.info(f"Date range: {date_range}")
            logger.info(f"Conditions: {conditions}")
            logger.info(f"Dimension values: {dimension_values}")
            
            calculation_steps = json.loads(kpi.calculation_steps)
            dimensions = json.loads(kpi.dimensions) if kpi.dimensions else []
            logger.info(f"Calculation steps: {calculation_steps}")
            logger.info(f"Dimensions: {dimensions}")
            
            # Initialize result with first step
            first_step = calculation_steps[0]
            if first_step['type'] == 'field':
                calculation_sql = self.get_calculation_sql(first_step['method'], f'"{first_step["field"]}"')
                
                # Start building the query
                if dimensions:
                    # Include dimension columns in SELECT and GROUP BY
                    dimension_cols = [f'"{dim}"' for dim in dimensions]
                    select_clause = f"SELECT {', '.join(dimension_cols)}, {calculation_sql} as step_result"
                    group_by_clause = f"GROUP BY {', '.join(dimension_cols)}"
                else:
                    select_clause = f"SELECT {calculation_sql} as step_result"
                    group_by_clause = ""
                
                query = f"{select_clause} FROM {kpi.table_name} "
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
            
            # Add dimension value filters if provided
            if dimension_values:
                for dim, value in dimension_values.items():
                    query += f' AND "{dim}" = :dim_{dim}'
                    params[f'dim_{dim}'] = value
            
            # Add GROUP BY clause if dimensions exist
            if dimensions and group_by_clause:
                query += f" {group_by_clause}"
            
            logger.info(f"First step query: {query}")
            logger.info(f"Parameters: {params}")
            
            # Execute the first step
            with self.db_manager.local_engine.connect() as conn:
                result_rows = conn.execute(text(query), params).fetchall()
                logger.info(f"Query returned {len(result_rows)} rows")
                
                # Process results
                if dimensions:
                    results = {}
                    for row in result_rows:
                        # Create dimension key
                        dim_key = tuple(str(row[i]) for i in range(len(dimensions)))
                        result_value = float(row[-1] or 0)  # Last column is the step_result
                        results[dim_key] = result_value
                else:
                    results = float(result_rows[0][0] if result_rows else 0)
                
                logger.info(f"Initial results: {results}")
                
                # Process subsequent steps
                for step in calculation_steps[1:]:
                    logger.info(f"Processing step: {step}")
                    
                    if step['type'] == 'field':
                        # Get the value from the database for each dimension combination
                        step_query = self.get_calculation_sql(step['method'], f'"{step["field"]}"')
                        
                        if dimensions:
                            dimension_cols = [f'"{dim}"' for dim in dimensions]
                            step_query = f"SELECT {', '.join(dimension_cols)}, {step_query} as step_result"
                            step_query += f" FROM {kpi.table_name}"
                            step_query += f" WHERE DATE({kpi.date_column}) BETWEEN :start_date AND :end_date"
                            if conditions:
                                for i, condition in enumerate(conditions):
                                    field = f'"{condition["field"]}"' if ' ' in condition["field"] else condition["field"]
                                    step_query += f" AND {field} {condition['operator']} :value_{i}"
                        else:
                            step_query = f"SELECT {step_query} FROM {kpi.table_name}"
                            step_query += f" WHERE DATE({kpi.date_column}) BETWEEN :start_date AND :end_date"
                            if conditions:
                                for i, condition in enumerate(conditions):
                                    field = f'"{condition["field"]}"' if ' ' in condition["field"] else condition["field"]
                                    step_query += f" AND {field} {condition['operator']} :value_{i}"
                        
                        logger.info(f"Step query: {step_query}")
                        step_rows = conn.execute(text(step_query), params).fetchall()
                        
                        if dimensions:
                            step_values = {}
                            for row in step_rows:
                                dim_key = tuple(str(row[i]) for i in range(len(dimensions)))
                                step_values[dim_key] = float(row[-1] or 0)
                        else:
                            step_values = float(step_rows[0][0] if step_rows else 0)
                    else:
                        # Use constant value for all dimension combinations
                        step_values = float(step['value'])
                    
                    logger.info(f"Step values: {step_values}")
                    
                    # Apply the operator
                    if dimensions:
                        for dim_key in results.keys():
                            value = step_values[dim_key] if isinstance(step_values, dict) else step_values
                            if step['operator'] == '+':
                                results[dim_key] += value
                            elif step['operator'] == '-':
                                results[dim_key] -= value
                            elif step['operator'] == '*':
                                results[dim_key] *= value
                            elif step['operator'] == '/':
                                if value != 0:
                                    results[dim_key] /= value
                                else:
                                    logger.warning(f"Division by zero attempted for dimension {dim_key}")
                    else:
                        value = step_values
                        if step['operator'] == '+':
                            results += value
                        elif step['operator'] == '-':
                            results -= value
                        elif step['operator'] == '*':
                            results *= value
                        elif step['operator'] == '/':
                            if value != 0:
                                results /= value
                            else:
                                logger.warning("Division by zero attempted")
                    
                    logger.info(f"Results after step: {results}")
                
                return results
                
        except Exception as e:
            logger.error(f"Error calculating KPI value: {str(e)}", exc_info=True)
            return {} if dimensions else 0

    def format_value(self, value):
        if isinstance(value, (int, float)):
            return '{:,.0f}'.format(value)
        return str(value)

    def save_kpi(self, kpi_data):
        try:
            logger.info(f"Saving KPI data: {kpi_data}")
            
            # Fix time_spans format - it's coming as a flat array, need to pair it
            if isinstance(kpi_data.get('time_spans'), list):
                time_spans = []
                spans = kpi_data['time_spans']
                # Group spans into pairs
                for i in range(0, len(spans), 2):
                    if i + 1 < len(spans):
                        time_spans.append([spans[i], spans[i + 1]])
                kpi_data['time_spans'] = json.dumps(time_spans)
            elif isinstance(kpi_data.get('time_spans'), str):
                try:
                    spans = json.loads(kpi_data['time_spans'])
                    if isinstance(spans, list):
                        time_spans = []
                        for i in range(0, len(spans), 2):
                            if i + 1 < len(spans):
                                time_spans.append([spans[i], spans[i + 1]])
                        kpi_data['time_spans'] = json.dumps(time_spans)
                except json.JSONDecodeError:
                    logger.error("Invalid time_spans format")
                    return False

            # Ensure other JSON fields are properly serialized
            for field in ['conditions', 'calculation_steps', 'dimensions']:
                if field in kpi_data and isinstance(kpi_data[field], (dict, list)):
                    kpi_data[field] = json.dumps(kpi_data[field])

            with self.db_manager.app_config_engine.begin() as conn:
                if kpi_data.get('id'):  # Update existing KPI
                    logger.info(f"Updating existing KPI with ID: {kpi_data['id']}")
                    update_query = """
                        UPDATE KPIConfigurations 
                        SET name=:name, 
                            table_name=:table_name, 
                            date_column=:date_column,
                            time_spans=:time_spans, 
                            conditions=:conditions,
                            calculation_steps=:calculation_steps,
                            dimensions=:dimensions
                        WHERE id=:id
                    """
                    conn.execute(text(update_query), kpi_data)
                    logger.info("KPI updated successfully")
                else:  # Create new KPI
                    logger.info("Creating new KPI")
                    insert_query = """
                        INSERT INTO KPIConfigurations 
                        (name, table_name, date_column, time_spans, conditions, calculation_steps, dimensions)
                        VALUES (:name, :table_name, :date_column, :time_spans, :conditions, :calculation_steps, :dimensions)
                    """
                    conn.execute(text(insert_query), kpi_data)
                    logger.info("New KPI created successfully")
                
                # Verify the save operation
                if kpi_data.get('id'):
                    result = conn.execute(
                        text("SELECT * FROM KPIConfigurations WHERE id = :id"),
                        {'id': kpi_data['id']}
                    ).fetchone()
                    logger.info(f"Verification - Saved KPI data: {result}")
                
                return True
        except Exception as e:
            logger.error(f"Error saving KPI: {str(e)}")
            return False

    def calculate_kpi_values(self, kpi):
        """Calculate KPI values for all time spans"""
        try:
            time_spans = json.loads(kpi.time_spans)
            conditions = json.loads(kpi.conditions) if kpi.conditions else []
            dimensions = json.loads(kpi.dimensions) if kpi.dimensions else []
            results = []
            
            if dimensions:
                # Create a single dimensional KPI card
                dimensional_data = {}
                for current_period, comparison_period in time_spans:
                    current_range = self.get_date_range(current_period)
                    comparison_range = self.get_date_range(comparison_period)
                    
                    current_values = self.calculate_kpi_value(kpi, current_range, conditions)
                    comparison_values = self.calculate_kpi_value(kpi, comparison_range, conditions)
                    
                    # Get all unique dimension combinations
                    all_dimensions = set(current_values.keys()) | set(comparison_values.keys())
                    
                    for dim_key in all_dimensions:
                        current_value = current_values.get(dim_key, 0)
                        comparison_value = comparison_values.get(dim_key, 0)
                        
                        # Calculate change percentage
                        if comparison_value and comparison_value != 0:
                            change = ((current_value - comparison_value) / comparison_value) * 100
                        else:
                            change = 0
                        
                        # Create dimension label and use it as key
                        dimension_values = dict(zip(dimensions, dim_key))
                        dimension_key = '_'.join(str(v) for v in dim_key)  # Convert tuple to string key
                        
                        if dimension_key not in dimensional_data:
                            dimensional_data[dimension_key] = {
                                'dimension_values': dimension_values,
                                'time_spans': []
                            }
                        
                        dimensional_data[dimension_key]['time_spans'].append({
                            'current_period': current_period,
                            'comparison_period': comparison_period,
                            'current_value': self.format_value(current_value),
                            'comparison_value': self.format_value(comparison_value),
                            'change': round(change, 1)
                        })
                
                # Add single dimensional KPI card
                results.append({
                    'name': kpi.name,
                    'type': 'dimensional',
                    'dimensions': dimensions,
                    'dimensional_data': dimensional_data
                })
                
            else:
                # Create a single multi-timespan KPI card
                timespan_data = []
                for current_period, comparison_period in time_spans:
                    current_range = self.get_date_range(current_period)
                    comparison_range = self.get_date_range(comparison_period)
                    
                    current_value = self.calculate_kpi_value(kpi, current_range, conditions)
                    comparison_value = self.calculate_kpi_value(kpi, comparison_range, conditions)
                    
                    # Calculate change percentage
                    if comparison_value and comparison_value != 0:
                        change = ((current_value - comparison_value) / comparison_value) * 100
                    else:
                        change = 0
                    
                    timespan_data.append({
                        'current_period': current_period,
                        'comparison_period': comparison_period,
                        'current_value': self.format_value(current_value),
                        'comparison_value': self.format_value(comparison_value),
                        'change': round(change, 1)
                    })
                
                results.append({
                    'name': kpi.name,
                    'type': 'multi_timespan',
                    'timespan_data': timespan_data
                })
            
            return results
        except Exception as e:
            logger.error(f"Error calculating KPI values: {str(e)}")
            return []

    def fix_time_spans(self):
        """Fix incorrectly formatted time spans in existing KPIs"""
        try:
            with self.db_manager.app_config_engine.begin() as conn:
                # Get all KPIs
                kpis = conn.execute(text("SELECT * FROM KPIConfigurations")).fetchall()
                
                for kpi in kpis:
                    try:
                        # Parse existing time spans
                        time_spans = json.loads(kpi.time_spans) if kpi.time_spans else []
                        
                        # Fix time spans format if needed
                        if not isinstance(time_spans, list) or not all(isinstance(span, list) and len(span) == 2 for span in time_spans):
                            # Set default time spans
                            time_spans = [
                                ["Today", "Yesterday"],
                                ["Month to Date", "Last Month to Date"],
                                ["Year to Date", "Last Year to Date"]
                            ]
                            
                            # Update KPI with fixed time spans
                            conn.execute(
                                text("""
                                    UPDATE KPIConfigurations 
                                    SET time_spans = :time_spans
                                    WHERE id = :id
                                """),
                                {
                                    'id': kpi.id,
                                    'time_spans': json.dumps(time_spans)
                                }
                            )
                            logger.info(f"Fixed time spans for KPI {kpi.id}")
                    except Exception as e:
                        logger.error(f"Error fixing time spans for KPI {kpi.id}: {str(e)}")
                        continue
                        
            return True
        except Exception as e:
            logger.error(f"Error fixing time spans: {str(e)}")
            return False

