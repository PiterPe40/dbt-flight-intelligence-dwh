"""
Demo Data Generator — creates realistic synthetic flight data for portfolio demo.

Generates 30 days of flight data with:
- Realistic civilian traffic patterns (hub airports get more flights)
- Military flights (~3-5% of total, from ICAO24 military ranges)
- Anomaly spikes on 2-3 random days (for anomaly detection to find)
- Proper time distributions (more flights during day, fewer at night)
- Realistic callsigns (airline codes + flight numbers)

Usage:
    python scripts/generate_demo_data.py
    python scripts/generate_demo_data.py --days 14
    python scripts/generate_demo_data.py --with-anomalies

This is meant for portfolio/demo purposes when OpenSky API is unavailable.
"""

import os
import sys
import random
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dags.helpers.data_loader import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Airport definitions with traffic weights ──────────────
# Weight = relative traffic volume (10 = busiest hubs)

AIRPORTS = {
    # Major European hubs (heavy traffic)
    "EDDF": {"weight": 10, "country": "DE", "name": "Frankfurt"},
    "EGLL": {"weight": 10, "country": "GB", "name": "London Heathrow"},
    "EHAM": {"weight": 9, "country": "NL", "name": "Amsterdam"},
    "LFPG": {"weight": 9, "country": "FR", "name": "Paris CDG"},
    "EDDM": {"weight": 7, "country": "DE", "name": "Munich"},
    "LEMD": {"weight": 7, "country": "ES", "name": "Madrid"},
    "LIRF": {"weight": 7, "country": "IT", "name": "Rome Fiumicino"},
    "LTFM": {"weight": 8, "country": "TR", "name": "Istanbul"},
    "LSZH": {"weight": 6, "country": "CH", "name": "Zurich"},
    "LOWW": {"weight": 5, "country": "AT", "name": "Vienna"},
    "EKCH": {"weight": 5, "country": "DK", "name": "Copenhagen"},
    "ESSA": {"weight": 4, "country": "SE", "name": "Stockholm"},
    "ENGM": {"weight": 4, "country": "NO", "name": "Oslo"},
    "EFHK": {"weight": 4, "country": "FI", "name": "Helsinki"},
    "EBBR": {"weight": 5, "country": "BE", "name": "Brussels"},
    "EDDB": {"weight": 5, "country": "DE", "name": "Berlin"},
    "EDDH": {"weight": 3, "country": "DE", "name": "Hamburg"},
    "EGKK": {"weight": 6, "country": "GB", "name": "London Gatwick"},
    "LKPR": {"weight": 4, "country": "CZ", "name": "Prague"},
    "LHBP": {"weight": 3, "country": "HU", "name": "Budapest"},
    # Poland
    "EPWA": {"weight": 5, "country": "PL", "name": "Warsaw Chopin"},
    "EPKK": {"weight": 3, "country": "PL", "name": "Krakow"},
    "EPGD": {"weight": 2, "country": "PL", "name": "Gdansk"},
    # US hubs
    "KJFK": {"weight": 9, "country": "US", "name": "New York JFK"},
    "KATL": {"weight": 10, "country": "US", "name": "Atlanta"},
    "KORD": {"weight": 8, "country": "US", "name": "Chicago O'Hare"},
    "KLAX": {"weight": 8, "country": "US", "name": "Los Angeles"},
    "KDFW": {"weight": 7, "country": "US", "name": "Dallas"},
    # Military-adjacent
    "ETAR": {"weight": 1, "country": "DE", "name": "Ramstein AB"},
    "ETNG": {"weight": 1, "country": "DE", "name": "Geilenkirchen NATO"},
    "EPMI": {"weight": 1, "country": "PL", "name": "Warsaw Modlin"},
}

# Airline callsign prefixes by region
AIRLINES = {
    "EU": ["DLH", "BAW", "AFR", "KLM", "SAS", "AUA", "LOT", "WZZ", "RYR",
           "EZY", "VLG", "TAP", "AZA", "THY", "SWR", "BEL", "FIN", "NAX"],
    "US": ["AAL", "DAL", "UAL", "SWA", "JBU", "ASA", "FFT", "SKW"],
    "INTL": ["UAE", "QTR", "SIA", "CPA", "ANA", "JAL", "CSN", "CCA"],
}

# Military ICAO24 ranges (from seeds)
MILITARY_RANGES = [
    ("3b0000", "3bffff", "DE"),  # Bundeswehr
    ("448000", "44ffff", "PL"),  # Polish AF
    ("500000", "503fff", "GB"),  # RAF
    ("ae0000", "aeffff", "US"),  # USAF
    ("710000", "713fff", "FR"),  # French AF
    ("480000", "480fff", "NL"),  # RNLAF
    ("a00000", "a0ffff", "US"),  # US Army
]

MILITARY_CALLSIGNS = {
    "US": ["RCH", "REACH", "DUKE", "EVAC", "HERO", "IRON", "BOLT"],
    "DE": ["GAF", "GAFM", "GERM"],
    "GB": ["RRR", "ASCOT", "TARTN"],
    "PL": ["PLF", "PLAF"],
    "FR": ["FAF", "CTM", "FRAF"],
    "NL": ["NAF", "RNLAF"],
}


def random_icao24(military: bool = False, country: str = None) -> str:
    """Generate a random ICAO24 hex address."""
    if military:
        # Pick from military ranges
        ranges = MILITARY_RANGES
        if country:
            ranges = [r for r in ranges if r[2] == country] or MILITARY_RANGES
        rng = random.choice(ranges)
        start = int(rng[0], 16)
        end = int(rng[1], 16)
        return format(random.randint(start, end), "06x")
    else:
        # Civilian: random in common ranges
        return format(random.randint(0x300000, 0xCFFFFF), "06x")


def random_callsign(military: bool = False, country: str = None) -> str:
    """Generate a realistic callsign."""
    if military:
        prefixes = MILITARY_CALLSIGNS.get(country, ["MIL"])
        prefix = random.choice(prefixes)
        return f"{prefix}{random.randint(1, 999):03d}"
    else:
        region = random.choices(["EU", "US", "INTL"], weights=[60, 25, 15])[0]
        airline = random.choice(AIRLINES[region])
        return f"{airline}{random.randint(1, 9999):04d}"


def generate_flight_time(date: datetime, is_military: bool = False) -> tuple:
    """Generate realistic departure and arrival times.

    Returns (firstSeen_unix, lastSeen_unix, duration_seconds)
    """
    # Hour distribution: more flights 06:00-22:00, fewer at night
    if is_military:
        # Military: more uniform, some night flights
        hour_weights = [1, 1, 1, 1, 2, 3, 5, 6, 7, 7, 6, 6,
                        6, 6, 7, 7, 6, 5, 4, 3, 2, 2, 1, 1]
    else:
        # Civilian: peak morning/evening, quiet at night
        hour_weights = [1, 1, 1, 1, 2, 4, 8, 10, 9, 8, 7, 7,
                        7, 7, 8, 9, 10, 9, 8, 6, 4, 3, 2, 1]

    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)

    dep_time = date.replace(hour=hour, minute=minute, second=random.randint(0, 59))
    first_seen = int(dep_time.timestamp())

    # Flight duration: 30 min to 12 hours
    if is_military:
        duration = random.choice([
            random.randint(1800, 5400),    # 30min-1.5h (short patrol/training)
            random.randint(5400, 14400),   # 1.5h-4h (medium range)
            random.randint(14400, 36000),  # 4h-10h (long range transport)
        ])
    else:
        # Weighted toward short-medium haul in Europe
        duration = random.choices(
            [random.randint(2700, 5400),     # 45min-1.5h (short haul)
             random.randint(5400, 10800),    # 1.5h-3h (medium haul)
             random.randint(10800, 25200),   # 3h-7h (long haul)
             random.randint(25200, 43200)],  # 7h-12h (ultra long)
            weights=[40, 35, 20, 5]
        )[0]

    last_seen = first_seen + duration
    return first_seen, last_seen, duration


def pick_route(airports: dict) -> tuple:
    """Pick a random origin-destination pair weighted by airport traffic."""
    codes = list(airports.keys())
    weights = [airports[c]["weight"] for c in codes]

    origin = random.choices(codes, weights=weights)[0]
    # Destination: different from origin, also weighted
    dest_codes = [c for c in codes if c != origin]
    dest_weights = [airports[c]["weight"] for c in dest_codes]
    destination = random.choices(dest_codes, weights=dest_weights)[0]

    return origin, destination


def generate_day_flights(
    date: datetime,
    base_civilian: int = 800,
    base_military: int = 30,
    anomaly_multiplier: float = 1.0,
) -> list[dict]:
    """Generate all flights for a single day.

    Args:
        date: The date to generate for
        base_civilian: Base number of civilian flights
        base_military: Base number of military flights
        anomaly_multiplier: >1.0 creates more military flights (anomaly day)
    """
    flights = []

    # Day-of-week variation (fewer on weekends)
    dow = date.weekday()  # 0=Mon
    if dow >= 5:  # Weekend
        civilian_count = int(base_civilian * random.uniform(0.65, 0.80))
        military_count = int(base_military * random.uniform(0.4, 0.7))
    elif dow == 4:  # Friday
        civilian_count = int(base_civilian * random.uniform(0.95, 1.10))
        military_count = int(base_military * random.uniform(0.8, 1.0))
    else:  # Mon-Thu
        civilian_count = int(base_civilian * random.uniform(0.90, 1.10))
        military_count = int(base_military * random.uniform(0.85, 1.15))

    # Apply anomaly multiplier to military
    military_count = int(military_count * anomaly_multiplier)

    # Add some random noise
    civilian_count += random.randint(-20, 20)
    military_count += random.randint(-3, 3)

    # ── Generate civilian flights ──────
    for _ in range(max(civilian_count, 0)):
        origin, dest = pick_route(AIRPORTS)
        first_seen, last_seen, _ = generate_flight_time(date, is_military=False)
        callsign = random_callsign(military=False)
        icao24 = random_icao24(military=False)

        flights.append({
            "icao24": icao24,
            "firstSeen": first_seen,
            "estDepartureAirport": origin,
            "lastSeen": last_seen,
            "estArrivalAirport": dest,
            "callsign": callsign,
            "estDepartureAirportHorizDistance": random.randint(200, 5000),
            "estDepartureAirportVertDistance": random.randint(10, 200),
            "estArrivalAirportHorizDistance": random.randint(200, 5000),
            "estArrivalAirportVertDistance": random.randint(10, 200),
            "departureAirportCandidatesCount": random.randint(1, 4),
            "arrivalAirportCandidatesCount": random.randint(1, 4),
            "_source_airport": origin,
            "_source_direction": "departure",
        })

    # ── Generate military flights ──────
    military_airports = ["ETAR", "ETNG", "EPMI", "EDDF", "EGLL", "EPWA",
                         "EDDM", "EHAM", "LFPG", "KJFK", "KATL"]

    for _ in range(max(military_count, 0)):
        # Military flights often involve military or large civilian airports
        origin = random.choice(military_airports)
        dest = random.choice([a for a in military_airports if a != origin])

        country = AIRPORTS.get(origin, {}).get("country", "US")
        first_seen, last_seen, _ = generate_flight_time(date, is_military=True)
        callsign = random_callsign(military=True, country=country)
        icao24 = random_icao24(military=True, country=country)

        flights.append({
            "icao24": icao24,
            "firstSeen": first_seen,
            "estDepartureAirport": origin,
            "lastSeen": last_seen,
            "estArrivalAirport": dest,
            "callsign": callsign,
            "estDepartureAirportHorizDistance": random.randint(200, 3000),
            "estDepartureAirportVertDistance": random.randint(10, 150),
            "estArrivalAirportHorizDistance": random.randint(200, 3000),
            "estArrivalAirportVertDistance": random.randint(10, 150),
            "departureAirportCandidatesCount": random.randint(1, 2),
            "arrivalAirportCandidatesCount": random.randint(1, 2),
            "_source_airport": origin,
            "_source_direction": "departure",
        })

    return flights


def run_generate(days: int = 30, with_anomalies: bool = True):
    """Generate demo data and load it into DuckDB."""

    # ── Setup ──────────────────────────
    os.environ["USE_LOCAL_DB"] = "true"
    db_path = os.getenv("LOCAL_DB_PATH", str(PROJECT_ROOT / "data" / "flight_intelligence.duckdb"))
    os.environ["LOCAL_DB_PATH"] = db_path

    # Delete old DB to start fresh (if possible)
    if Path(db_path).exists():
        try:
            Path(db_path).unlink()
            logger.info("Removed old DuckDB file")
        except (PermissionError, OSError):
            logger.warning("Cannot remove old DB — will drop and recreate tables")

    loader = DataLoader()

    # Clear existing data
    if hasattr(loader, 'conn'):
        try:
            loader.conn.execute("DELETE FROM raw_flights")
            logger.info("Cleared existing raw_flights data")
        except Exception:
            pass  # Table might not exist yet

    # ── Date range ────────────────────
    end_date = datetime(2026, 4, 25)  # Fixed for reproducibility
    start_date = end_date - timedelta(days=days - 1)

    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current)
        current += timedelta(days=1)

    # ── Pick anomaly days ──────────────
    anomaly_days = set()
    if with_anomalies and days >= 7:
        # 2-3 anomaly days scattered through the period
        num_anomalies = min(3, max(2, days // 10))
        # Avoid first 7 days (need baseline), pick from later dates
        candidate_days = all_dates[7:]
        if candidate_days:
            anomaly_days = set(random.sample(
                candidate_days,
                min(num_anomalies, len(candidate_days))
            ))
            for ad in sorted(anomaly_days):
                logger.info(f"  ANOMALY DAY: {ad.date()} (military flights 2.5-4x normal)")

    # ── Generate ──────────────────────
    logger.info("=" * 60)
    logger.info("FLIGHT INTELLIGENCE — DEMO DATA GENERATOR")
    logger.info("=" * 60)
    logger.info(f"Date range    : {start_date.date()} -> {end_date.date()}")
    logger.info(f"Days          : {days}")
    logger.info(f"Anomaly days  : {len(anomaly_days)}")
    logger.info(f"Database      : {db_path}")
    logger.info("=" * 60)

    total_flights = 0
    total_military = 0

    random.seed(42)  # Reproducible results

    for date in all_dates:
        is_anomaly = date in anomaly_days
        multiplier = random.uniform(2.5, 4.0) if is_anomaly else 1.0

        flights = generate_day_flights(
            date=date,
            base_civilian=800,
            base_military=30,
            anomaly_multiplier=multiplier,
        )

        date_str = date.strftime("%Y-%m-%d")
        count = loader.load_flights(flights, date_str)

        mil_count = sum(1 for f in flights if any(
            int(r[0], 16) <= int(f["icao24"], 16) <= int(r[1], 16)
            for r in MILITARY_RANGES
        ))

        marker = " ** ANOMALY **" if is_anomaly else ""
        logger.info(
            f"  {date_str}: {count} flights "
            f"({count - mil_count} civilian + {mil_count} military){marker}"
        )

        total_flights += count
        total_military += mil_count

    loader.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("DEMO DATA GENERATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total flights : {total_flights:,}")
    logger.info(f"Military      : {total_military:,} ({total_military/total_flights*100:.1f}%)")
    logger.info(f"Civilian      : {total_flights - total_military:,}")
    logger.info(f"Anomaly days  : {len(anomaly_days)}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  cd dbt_project")
    logger.info("  dbt seed --target local")
    logger.info("  dbt run --target local")
    logger.info("  dbt test --target local")
    logger.info("")
    logger.info("Then launch dashboard:")
    logger.info("  cd ..")
    logger.info("  streamlit run streamlit_app/app.py")


def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic demo flight data for portfolio",
    )
    parser.add_argument("--days", type=int, default=30, help="Days of data (default: 30)")
    parser.add_argument("--no-anomalies", action="store_true", help="Skip anomaly days")
    args = parser.parse_args()

    # Load .env (for DB path if customized)
    try:
        from dotenv import load_dotenv
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass

    run_generate(days=args.days, with_anomalies=not args.no_anomalies)


if __name__ == "__main__":
    main()
