from db_utils import setup_database_connections, get_last_record_id, refresh_data
import logging

def setup_logging():
    # Create a logger
    logger = logging.getLogger('ATMForecast')
    logger.setLevel(logging.DEBUG)

    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)

    return logger

def main():
    logger = setup_logging()
    logger.info("Starting ATM Forecast data fetching process")

    # Enable SQLAlchemy logging
    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

    # Read configuration from environment variables
    import os

    config = {
        'db_username': os.environ.get('DB_USERNAME'),
        'db_password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
        'db_name': os.environ.get('DB_NAME')
    }

    # Validate that all required environment variables are set
    missing_vars = [key for key, value in config.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    source_engine, local_engine = setup_database_connections(config)

    # Get the last record date
    last_record_id = get_last_record_id(local_engine)

    # Refresh the data
    refresh_data(source_engine, local_engine)

    logger.info("ATM Forecast data fetching process completed")

if __name__ == "__main__":
    main()
