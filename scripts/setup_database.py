import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def setup_database(conn):
    cursor = conn.cursor()

    # Stations table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_stations (
            stationid INTEGER PRIMARY KEY,
            stationname TEXT,
            lat REAL,
            lon REAL,
            regio TEXT
        )
    """)

    # Measurements table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_station_measurements (
            measurementid INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            temperature REAL,
            groundtemperature REAL,
            feeltemperature REAL,
            windgusts REAL,
            windspeedBft REAL,
            humidity REAL,
            precipitation REAL,
            sunpower REAL,
            stationid INTEGER,
            FOREIGN KEY (stationid) REFERENCES weather_stations(stationid)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        )
    """)
    conn.commit()
    logger.info("Database tables created.")