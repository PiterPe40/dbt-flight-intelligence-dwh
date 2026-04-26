"""
Data Loader — loads raw flight data into BigQuery or DuckDB.

Supports two backends:
  - BigQuery (production/cloud)
  - DuckDB (local development/testing)

The loader automatically selects the backend based on the USE_LOCAL_DB
environment variable.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataLoader:
    """Unified data loader with BigQuery and DuckDB backends.

    Usage:
        loader = DataLoader()       # auto-detects backend from env vars
        loader.load_flights(df)      # loads flight DataFrame into raw table
    """

    def __init__(self):
        """Initialize the appropriate database backend."""
        self.use_local = os.getenv("USE_LOCAL_DB", "true").lower() == "true"

        if self.use_local:
            self._init_duckdb()
        else:
            self._init_bigquery()

    # ── DuckDB Backend ───────────────────────
    def _init_duckdb(self):
        """Initialize DuckDB connection and create tables if needed."""
        import duckdb

        db_path = os.getenv("LOCAL_DB_PATH", "./data/flight_intelligence.duckdb")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.conn = duckdb.connect(db_path)
        self.backend = "duckdb"
        logger.info(f"Using DuckDB backend: {db_path}")

        self._create_duckdb_tables()

    def _create_duckdb_tables(self):
        """Create raw tables in DuckDB if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_flights (
                icao24          VARCHAR,
                firstSeen       BIGINT,
                estDepartureAirport VARCHAR,
                lastSeen        BIGINT,
                estArrivalAirport   VARCHAR,
                callsign        VARCHAR,
                estDepartureAirportHorizDistance INTEGER,
                estDepartureAirportVertDistance  INTEGER,
                estArrivalAirportHorizDistance   INTEGER,
                estArrivalAirportVertDistance    INTEGER,
                departureAirportCandidatesCount  INTEGER,
                arrivalAirportCandidatesCount    INTEGER,
                _source_airport  VARCHAR,
                _source_direction VARCHAR,
                loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_date     DATE
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_aircraft_states (
                icao24          VARCHAR,
                callsign        VARCHAR,
                origin_country  VARCHAR,
                time_position   BIGINT,
                last_contact    BIGINT,
                longitude       DOUBLE,
                latitude        DOUBLE,
                baro_altitude   DOUBLE,
                on_ground       BOOLEAN,
                velocity        DOUBLE,
                true_track      DOUBLE,
                vertical_rate   DOUBLE,
                geo_altitude    DOUBLE,
                squawk          VARCHAR,
                spi             BOOLEAN,
                position_source INTEGER,
                loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Migration: add new columns if table was created before they existed
        try:
            self.conn.execute("ALTER TABLE raw_flights ADD COLUMN IF NOT EXISTS _source_airport VARCHAR")
            self.conn.execute("ALTER TABLE raw_flights ADD COLUMN IF NOT EXISTS _source_direction VARCHAR")
        except Exception:
            pass  # Column already exists or DuckDB version doesn't support IF NOT EXISTS

        logger.info("DuckDB tables verified/created")

    # ── BigQuery Backend ─────────────────────
    def _init_bigquery(self):
        """Initialize BigQuery client."""
        from google.cloud import bigquery

        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset = os.getenv("BQ_DATASET", "flight_intelligence")
        self.client = bigquery.Client(project=self.project_id)
        self.backend = "bigquery"
        logger.info(f"Using BigQuery backend: {self.project_id}.{self.dataset}")

        self._create_bq_dataset()

    def _create_bq_dataset(self):
        """Create BigQuery dataset if it doesn't exist."""
        from google.cloud import bigquery

        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"

        try:
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"BigQuery dataset '{self.dataset}' ready")
        except Exception as e:
            logger.error(f"Failed to create dataset: {e}")
            raise

    # ── Load Methods ─────────────────────────
    def load_flights(self, flights: list[dict], source_date: str) -> int:
        """Load raw flight records into the database.

        Args:
            flights: List of flight dicts from OpenSky API
            source_date: Date string (YYYY-MM-DD) the flights are from

        Returns:
            Number of records loaded
        """
        if not flights:
            logger.warning("No flights to load")
            return 0

        df = pd.DataFrame(flights)
        df["loaded_at"] = datetime.utcnow()
        df["source_date"] = pd.to_datetime(source_date).date()

        # Ensure expected columns exist (with defaults for optional metadata)
        for col in ["_source_airport", "_source_direction"]:
            if col not in df.columns:
                df[col] = None

        # Reorder columns to match raw_flights table schema
        expected_cols = [
            "icao24", "firstSeen", "estDepartureAirport", "lastSeen",
            "estArrivalAirport", "callsign",
            "estDepartureAirportHorizDistance", "estDepartureAirportVertDistance",
            "estArrivalAirportHorizDistance", "estArrivalAirportVertDistance",
            "departureAirportCandidatesCount", "arrivalAirportCandidatesCount",
            "_source_airport", "_source_direction", "loaded_at", "source_date",
        ]
        # Keep only columns that exist in both DataFrame and expected list
        cols_to_use = [c for c in expected_cols if c in df.columns]
        df = df[cols_to_use]

        if self.use_local:
            return self._load_duckdb(df, "raw_flights")
        else:
            return self._load_bigquery(df, "raw_flights")

    def load_aircraft_states(self, states_data: dict) -> int:
        """Load aircraft state vectors into the database.

        Args:
            states_data: Response from OpenSky /states/all endpoint

        Returns:
            Number of records loaded
        """
        if not states_data or not states_data.get("states"):
            logger.warning("No aircraft states to load")
            return 0

        columns = [
            "icao24", "callsign", "origin_country", "time_position",
            "last_contact", "longitude", "latitude", "baro_altitude",
            "on_ground", "velocity", "true_track", "vertical_rate",
            "sensors", "geo_altitude", "squawk", "spi", "position_source",
        ]

        df = pd.DataFrame(states_data["states"], columns=columns)
        df = df.drop(columns=["sensors"], errors="ignore")
        df["loaded_at"] = datetime.utcnow()

        if self.use_local:
            return self._load_duckdb(df, "raw_aircraft_states")
        else:
            return self._load_bigquery(df, "raw_aircraft_states")

    def _load_duckdb(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame into DuckDB table.

        Uses INSERT to append new records (idempotency handled by dbt downstream).
        """
        row_count = len(df)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        logger.info(f"Loaded {row_count} rows into DuckDB table '{table_name}'")
        return row_count

    def _load_bigquery(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame into BigQuery table.

        Uses WRITE_APPEND disposition — deduplication handled by dbt.
        """
        from google.cloud.bigquery import LoadJobConfig, WriteDisposition

        table_ref = f"{self.project_id}.{self.dataset}.{table_name}"

        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion

        row_count = len(df)
        logger.info(f"Loaded {row_count} rows into BigQuery table '{table_ref}'")
        return row_count

    def close(self):
        """Close database connections."""
        if self.use_local and hasattr(self, "conn"):
            self.conn.close()
            logger.info("DuckDB connection closed")


# ── Quick test ───────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    loader = DataLoader()
    print(f"Backend: {loader.backend}")

    # Test with dummy data
    test_flights = [
        {
            "icao24": "3c6752",
            "firstSeen": 1700000000,
            "estDepartureAirport": "EDDF",
            "lastSeen": 1700003600,
            "estArrivalAirport": "EPWA",
            "callsign": "DLH123",
            "estDepartureAirportHorizDistance": 1000,
            "estDepartureAirportVertDistance": 50,
            "estArrivalAirportHorizDistance": 800,
            "estArrivalAirportVertDistance": 40,
            "departureAirportCandidatesCount": 1,
            "arrivalAirportCandidatesCount": 1,
        }
    ]

    count = loader.load_flights(test_flights, "2026-04-24")
    print(f"Loaded {count} test records")
    loader.close()
