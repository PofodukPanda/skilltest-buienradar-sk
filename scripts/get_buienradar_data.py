import requests
import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def get_buienradar_data():
    url = "https://json.buienradar.nl/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # raises exception for HTTP errors

        data = response.json()  # convert JSON to Python dict
        logger.info("Successfully fetched Buienradar data.")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Buienradar data: {e}")
        return None