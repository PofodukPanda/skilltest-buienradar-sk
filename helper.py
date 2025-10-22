import sqlite3
from pathlib import Path

# Helper function to create a connection with always foreign keys enabled
def get_connection(db_name="buienradar_weather.db"):
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def run_sql_script(conn, sql_file_path):
    cursor = conn.cursor()
    sql = Path(sql_file_path).read_text()
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()
    return results