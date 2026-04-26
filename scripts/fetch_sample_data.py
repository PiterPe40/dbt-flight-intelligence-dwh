"""
Fetch sample flight data from OpenSky Network and load into DuckDB.

Use this script to populate the database with real data for testing.
Run from the project root: python scripts/fetch_sample_data.py
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dags.helpers.opensky_client import OpenSkyClient
from dags.helpers.data_loader import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-20s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Fetch yesterday's flights and load into local DuckDB."""
    # Force local mode
    os.environ["USE_LOCAL_DB"] = "true"
    os.environ["LOCAL_DB_PATH"] = "./data/flight_intelligence.duckdb"

    # Initialize clients
    client = OpenSkyClient(
        username=os.getenv("OPENSKY_USERNAME"),
        password=os.getenv("OPENSKY_PASSWORD"),
    )
    loader = DataLoader()

    # Fetch yesterday's data
    yesterday = datetime.utcnow() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    logger.info(f"Fetching flights for {date_str}...")
    logger.info("(This may take 5-10 minutes due to API rate limits)")

    flights = client.fetch_daily_flights(yesterday)

    if not flights:
        logger.warning("No flights returned. Check your OpenSky credentials.")
        logger.info(
            "Without credentials, OpenSky only provides real-time state vectors, "
            "not historical flight data. Register at https://opensky-network.org"
        )
        return

    # Load into DuckDB
    rows = loader.load_flights(flights, date_str)
    logger.info(f"Successfully loaded {rows} flights into DuckDB")

    # Print summary
    logger.info("\n--- Summary ---")
    logger.info(f"Date: {date_str}")
    logger.info(f"Total flights: {len(flights)}")
    logger.info(f"Rows loaded: {rows}")

    # Quick stats
    import duckdb

    conn = duckdb.connect("./data/flight_intelligence.duckdb")
    total = conn.execute("SELECT COUNT(*) FROM raw_flights").fetchone()[0]
    unique_aircraft = conn.execute(
        "SELECT COUNT(DISTINCT icao24) FROM raw_flights"
    ).fetchone()[0]

    logger.info(f"Total rows in raw_flights: {total}")
    logger.info(f"Unique aircraft: {unique_aircraft}")
    conn.close()

    loader.close()
    logger.info("Done!")


if __name__ == "__main__":
    main()
