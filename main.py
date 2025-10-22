import sqlite3
import requests
import logging


# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Helper function to create a connection with always foreign keys enabled
def get_connection(db_name="buienradar_weather.db"):
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def get_buienradar_data():
    url = "https://json.buienradar.nl/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # raises exception for HTTP errors

        data = response.json()  # convert JSON to Python dict
        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching Buienradar data: {e}")
        return None


# Step 1: Create database schema
def setup_database(conn):
    cursor = conn.cursor()

    # Station table — use stationid from Buienradar as PRIMARY KEY (no AUTOINCREMENT)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_stations (
            stationid INTEGER PRIMARY KEY,
            stationname TEXT,
            lat REAL,
            lon REAL,
            regio TEXT
        )
    """)

    # Measurements table — link stationid as foreign key
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

# Step 2: Insert or update stations
def insert_stations(conn, data):
    cursor = conn.cursor()
    stations = data.get("actual", {}).get("stationmeasurements", [])

    for s in stations:
        cursor.execute("""
            INSERT INTO weather_stations (stationid, stationname, lat, lon, regio)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(stationid) DO UPDATE SET
                stationname=excluded.stationname,
                lat=excluded.lat,
                lon=excluded.lon,
                regio=excluded.regio;
        """, (
            s.get("stationid"),
            s.get("stationname"),
            s.get("lat"),
            s.get("lon"),
            s.get("regio")
        ))

    conn.commit()
    print(f"✅ Inserted/updated {len(stations)} stations.")

# Step 3: Insert measurements linked by stationid
def insert_measurements(conn, data):
    cursor = conn.cursor()
    stations = data.get("actual", {}).get("stationmeasurements", [])

    for s in stations:
        cursor.execute("""
            INSERT INTO weather_station_measurements
            (timestamp, temperature, groundtemperature, feeltemperature, windgusts, windspeedBft,
             humidity, precipitation, sunpower, stationid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            s.get("timestamp"),
            s.get("temperature"),  
            s.get("groundtemperature"),
            s.get("feeltemperature"),
            s.get("windgusts"),
            s.get("windspeedBft"),
            s.get("humidity"),
            s.get("precipitation"),
            s.get("sunpower"),
            s.get("stationid")
        ))

    conn.commit()
    print(f"✅ Inserted {len(stations)} station measurements.")

# Main execution
if __name__ == "__main__":
    data = get_buienradar_data()
    conn = get_connection()
    setup_database(conn)
    insert_stations(conn, data)
    insert_measurements(conn, data)
    conn.close()
    print("🏁 Done.")
