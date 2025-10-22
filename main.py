from scripts import get_buienradar_data, insert_data, setup_database
from helper import get_connection
import logging
import sqlite3

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main():
    # Initialize DB Connection
    conn = get_connection()

    # Setup Database if not yet done
    setup_database.setup_database(conn)

    # Get Buienradar Data
    data = get_buienradar_data.get_buienradar_data()
    if not data:
        logging.error("No data fetched, End of ETL run.")
        return
    
    # Insert Data
    try:
        insert_data.insert_stations(conn, data)
        insert_data.insert_measurements(conn, data)
    except sqlite3.Error as e:
        logging.error(f"ETL failed due to database error: {e}")
    finally: # To ensure the closure of the connection
        conn.close()


# Run main
if __name__ == "__main__":
    main()

