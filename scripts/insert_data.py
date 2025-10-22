import logging
import sqlite3

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def insert_stations(conn, data):
    cursor = conn.cursor()
    stations = data.get("actual", {}).get("stationmeasurements", [])
    try:
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
        logger.info(f"Inserted/updated {len(stations)} stations.")
    except sqlite3.Error as e: 
        conn.rollback()  # undo partial changes
        logger.error(f"Error inserting Weather Stations data: {e}")
        raise  # re-raise if you want the calling code to handle it


def insert_measurements(conn, data):
    cursor = conn.cursor()
    stations = data.get("actual", {}).get("stationmeasurements", [])
    try:
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
        logger.info(f"Inserted {len(stations)} station measurements.")

    except sqlite3.Error as e:  
        conn.rollback()  # undo partial changes
        logger.error(f"Error inserting Weather Measurements data: {e}")
        raise  # re-raise if you want the calling code to handle it