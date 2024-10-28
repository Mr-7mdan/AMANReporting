import json
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # Fix the import
from sqlalchemy import text

logger = logging.getLogger(__name__)

class ChartManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def get_chart_data(self):
        """Get all enabled charts with their data"""
        try:
            logger.info("Starting to fetch chart data")
            session = self.db_manager.app_config_session()
            try:
                # Get enabled charts
                logger.info("Fetching enabled charts from database")
                charts = session.execute(
                    text("SELECT * FROM ChartConfigurations WHERE is_enabled = 1")
                ).fetchall()
                logger.info(f"Found {len(charts)} enabled charts")
                
                chart_data = []
                for chart in charts:
                    try:
                        logger.info(f"Processing chart: {chart.name} (ID: {chart.id})")
                        # Parse configuration
                        x_axis = json.loads(chart.x_axis)
                        y_axis = json.loads(chart.y_axis)
                        time_spans = json.loads(chart.time_spans) if chart.time_spans else []
                        logger.info(f"Chart configuration - X-axis: {x_axis}, Y-axis: {y_axis}, Time spans: {time_spans}")
                        
                        datasets = self.get_chart_datasets(chart, x_axis, y_axis, time_spans)
                        
                        chart_data.append({
                            'id': chart.id,
                            'name': chart.name,
                            'chart_type': chart.chart_type,
                            'labels': sorted(list(set(label for dataset in datasets for label in dataset['labels']))),
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
                return chart_data
            finally:
                self.db_manager.app_config_session.remove()
                
        except Exception as e:
            logger.error(f"Error getting chart data: {str(e)}", exc_info=True)
            return []

    def get_chart_datasets(self, chart, x_axis, y_axis, time_spans):
        """Get datasets for a specific chart"""
        datasets = []
        all_labels = set()
        
        # If no time spans specified, use current data only
        if not time_spans:
            time_ranges = [(None, None)]
        else:
            # For each time span pair, get both ranges
            time_ranges = []
            for span_pair in time_spans:
                current_range = self.get_date_range(span_pair[0])
                comparison_range = self.get_date_range(span_pair[1])
                if current_range and comparison_range:
                    time_ranges.extend([current_range, comparison_range])
        
        logger.info(f"Time ranges for chart: {time_ranges}")
        
        for time_range in time_ranges:
            with self.db_manager.local_engine.connect() as local_conn:
                # Handle date grouping
                if x_axis.get('grouping_type') in ['day', 'weekday', 'month', 'quarter', 'year']:
                    group_by = self.get_date_grouping_sql(x_axis.get('grouping_type'), x_axis['field'])
                else:
                    group_by = x_axis['field']
                
                # Build measures with conditions
                for measure in y_axis:
                    sql = self.get_calculation_sql(measure['method'], measure['field'])
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
                    color = self.generate_color()
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
        
        return datasets

    def get_date_grouping_sql(self, grouping_type, field):
        """Get SQL for date grouping"""
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
        }[grouping_type]
        
        return group_by.format(field)

    def get_calculation_sql(self, method, field):
        """Get SQL for calculation method"""
        if method == 'Count':
            return 'COUNT(*)'
        elif method == 'Distinct Count':
            return f'COUNT(DISTINCT {field})'
        elif method == 'Sum':
            return f'SUM({field})'
        elif method == 'Average':
            return f'AVG({field})'
        elif method == 'Maximum':
            return f'MAX({field})'
        elif method == 'Minimum':
            return f'MIN({field})'
        return 'COUNT(*)'

    def generate_color(self):
        """Generate a random color for chart datasets"""
        import random
        r = random.randint(0, 255)
        g = random.randint(0, 255)
        b = random.randint(0, 255)
        return f'rgb({r}, {g}, {b})'

    def save_chart(self, chart_data):
        """Save or update a chart configuration"""
        try:
            with self.db_manager.app_config_engine.begin() as conn:
                if chart_data.get('id'):  # Update existing chart
                    conn.execute(
                        text("""
                            UPDATE ChartConfigurations 
                            SET name=:name, table_name=:table_name, x_axis=:x_axis,
                                y_axis=:y_axis, chart_type=:chart_type,
                                time_spans=:time_spans,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE id=:id
                        """),
                        chart_data
                    )
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
            return True
        except Exception as e:
            logger.error(f"Error saving chart: {str(e)}")
            return False

    def toggle_chart(self, chart_id):
        """Toggle chart enabled/disabled state"""
        try:
            with self.db_manager.app_config_engine.begin() as conn:
                # Get current state
                result = conn.execute(
                    text("SELECT is_enabled FROM ChartConfigurations WHERE id = :id"),
                    {'id': chart_id}
                ).fetchone()
                
                if not result:
                    return False, "Chart not found"
                    
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
                
                return True, new_state
        except Exception as e:
            logger.error(f"Error toggling chart: {str(e)}")
            return False, str(e)

    def delete_chart(self, chart_id):
        """Delete a chart configuration"""
        try:
            with self.db_manager.app_config_engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM ChartConfigurations WHERE id = :id"),
                    {'id': chart_id}
                )
            return True
        except Exception as e:
            logger.error(f"Error deleting chart: {str(e)}")
            return False

    def get_date_range(self, period):
        """Get date range for a given time span"""
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
