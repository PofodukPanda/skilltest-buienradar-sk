from scripts import get_buienradar_data, insert_data, setup_database
from helper import get_connection
import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Main execution
if __name__ == "__main__":
    data = get_buienradar_data.get_buienradar_data()
    if data:
        conn = get_connection()
        setup_database.setup_database(conn)
        insert_data.insert_stations(conn, data)
        insert_data.insert_measurements(conn, data)
        conn.close()
        logger.info("All data processing completed successfully.")
    else:
        logger.warning("No data fetched. Exiting script.")
