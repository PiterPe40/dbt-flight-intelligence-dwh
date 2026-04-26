"""
Backfill Script — fetch 30 days of historical flight data from OpenSky.

Uses /flights/departure and /flights/arrival endpoints (available on free accounts).
The /flights/all endpoint requires premium access, so we collect data
from a curated list of major airports instead.

Usage:
    python scripts/backfill_30days.py

    Options:
        --days 14       Fetch last 14 days instead of 30
        --start 2026-03-01 --end 2026-03-31   Specific date range

Notes:
    - Requires OpenSky account (free) for historical data access
    - Takes 1-4 hours depending on --days
    - Rate limits: script auto-pauses between requests
    - Safe to re-run: progress is saved after each airport+day
    - You can stop (Ctrl+C) and resume anytime
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dags.helpers.opensky_client import OpenSkyClient
from dags.helpers.data_loader import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Airports to fetch ────────────────────────────────────
# Major European + US hubs — gives good coverage for both
# civilian and military traffic analysis.

AIRPORTS = [
    # Europe — major hubs
    "EDDF",  # Frankfurt
    "EGLL",  # London Heathrow
    "EHAM",  # Amsterdam Schiphol
    "LFPG",  # Paris CDG
    "EDDM",  # Munich
    "LEMD",  # Madrid
    "LIRF",  # Rome Fiumicino
    "LSZH",  # Zurich
    "LOWW",  # Vienna
    "EKCH",  # Copenhagen
    "ESSA",  # Stockholm Arlanda
    "ENGM",  # Oslo
    "EFHK",  # Helsinki
    "EBBR",  # Brussels
    # Poland
    "EPWA",  # Warsaw Chopin
    "EPKK",  # Krakow
    "EPGD",  # Gdansk
    # Germany — additional
    "EDDB",  # Berlin
    "EDDH",  # Hamburg
    # UK — additional
    "EGKK",  # London Gatwick
    # Turkey
    "LTFM",  # Istanbul
    # US — major hubs
    "KJFK",  # New York JFK
    "KATL",  # Atlanta
    "KORD",  # Chicago O'Hare
    "KLAX",  # Los Angeles
    "KDFW",  # Dallas
    # Military-adjacent airports (catch military traffic)
    "ETAR",  # Ramstein Air Base (DE)
    "ETNG",  # Geilenkirchen NATO (DE)
]

# ── Progress tracking ────────────────────────────────────

PROGRESS_FILE = PROJECT_ROOT / "data" / ".backfill_progress.json"


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"completed_keys": [], "total_flights": 0, "total_requests": 0}


def save_progress(progress: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ── Core backfill ────────────────────────────────────────

def run_backfill(days: int = 30, start_date: str = None, end_date: str = None):
    # ── Validate credentials ─────────────
    # Prefer OAuth2, fall back to legacy basic auth
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")

    if not (client_id and client_secret) and not (username and password):
        logger.error(
            "OpenSky credentials required for historical data.\n"
            "  1. Register at https://opensky-network.org\n"
            "  2. Go to Account -> API Clients -> Create new client\n"
            "  3. Add to .env file:\n"
            "     OPENSKY_CLIENT_ID=your_client_id\n"
            "     OPENSKY_CLIENT_SECRET=your_client_secret\n"
            "\n"
            "  (Legacy basic auth with USERNAME/PASSWORD is deprecated since March 2026)"
        )
        sys.exit(1)

    # ── Determine date range ─────────────
    if start_date and end_date:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d")
        dt_end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        dt_end = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)
        dt_start = dt_end - timedelta(days=days - 1)

    all_dates = []
    current = dt_start
    while current <= dt_end:
        all_dates.append(current)
        current += timedelta(days=1)

    # ── Load progress ────────────────────
    progress = load_progress()
    completed = set(progress["completed_keys"])

    # Build work items: (date, airport, direction)
    work_items = []
    for date in all_dates:
        for airport in AIRPORTS:
            for direction in ["departure", "arrival"]:
                key = f"{date.strftime('%Y-%m-%d')}|{airport}|{direction}"
                if key not in completed:
                    work_items.append((date, airport, direction, key))

    total_work = len(all_dates) * len(AIRPORTS) * 2  # 2 = departure + arrival
    already_done = total_work - len(work_items)

    logger.info("=" * 60)
    logger.info("FLIGHT INTELLIGENCE — BACKFILL")
    logger.info("=" * 60)
    logger.info(f"Date range   : {dt_start.date()} -> {dt_end.date()}")
    logger.info(f"Days         : {len(all_dates)}")
    logger.info(f"Airports     : {len(AIRPORTS)}")
    logger.info(f"Total tasks  : {total_work}")
    logger.info(f"Already done : {already_done}")
    logger.info(f"Remaining    : {len(work_items)}")
    logger.info("=" * 60)

    if not work_items:
        logger.info("All data already fetched! Run dbt to process.")
        return

    # Estimate: ~10s per request (fetch + sleep)
    est_minutes = len(work_items) * 10 / 60
    logger.info(f"Estimated time: ~{est_minutes:.0f} minutes ({est_minutes/60:.1f} hours)")
    logger.info("(You can stop anytime with Ctrl+C — progress is saved)")
    logger.info("")

    # ── Initialize clients ───────────────
    os.environ["USE_LOCAL_DB"] = "true"
    os.environ["LOCAL_DB_PATH"] = str(PROJECT_ROOT / "data" / "flight_intelligence.duckdb")

    client = OpenSkyClient(
        username=username,
        password=password,
        client_id=client_id,
        client_secret=client_secret,
    )
    loader = DataLoader()

    # ── Fetch loop ───────────────────────
    flights_this_session = 0
    requests_this_session = 0
    consecutive_errors = 0

    for idx, (date, airport, direction, key) in enumerate(work_items):
        date_str = date.strftime("%Y-%m-%d")
        begin_ts = int(date.replace(hour=0, minute=0, second=0).timestamp())
        end_ts = begin_ts + 86400  # +24 hours

        # Show progress
        pct = (already_done + idx) / total_work * 100
        logger.info(
            f"[{already_done + idx + 1}/{total_work}] ({pct:.0f}%) "
            f"{date_str} {airport} {direction}"
        )

        try:
            if direction == "departure":
                flights = client.fetch_departures(airport, begin_ts, end_ts)
            else:
                flights = client.fetch_arrivals(airport, begin_ts, end_ts)

            requests_this_session += 1
            consecutive_errors = 0

            if flights:
                # Add source metadata
                for f in flights:
                    f["_source_airport"] = airport
                    f["_source_direction"] = direction

                rows = loader.load_flights(flights, date_str)
                flights_this_session += rows
                logger.info(f"  -> {rows} flights loaded")
            else:
                logger.info(f"  -> no data")

        except Exception as e:
            error_msg = str(e)
            logger.warning(f"  -> FAILED: {error_msg}")
            consecutive_errors += 1

            # If we get many consecutive errors, pause longer
            if consecutive_errors >= 5:
                logger.warning(
                    f"  {consecutive_errors} consecutive errors. "
                    f"Pausing 120s to let rate limit reset..."
                )
                time.sleep(120)
                consecutive_errors = 0
                continue

            # 403 specifically — might need longer pause
            if "403" in error_msg:
                logger.warning("  403 Forbidden — pausing 60s...")
                time.sleep(60)
                continue

        # ── Save progress ────────────────
        progress["completed_keys"].append(key)
        progress["total_flights"] += len(flights) if flights else 0
        progress["total_requests"] = progress.get("total_requests", 0) + 1

        # Save every 10 requests
        if (idx + 1) % 10 == 0:
            save_progress(progress)

        # ── Rate limit pause ─────────────
        # OpenSky free: ~1 request per 5 seconds for authenticated users
        time.sleep(7)

    # Final save
    save_progress(progress)
    loader.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Requests made : {requests_this_session}")
    logger.info(f"Flights loaded: {flights_this_session:,}")
    logger.info(f"Total in DB   : {progress['total_flights']:,}")
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


# ── CLI ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical flight data from OpenSky Network",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick 7-day backfill (~30-60 min)
  python scripts/backfill_30days.py --days 7

  # Full 30-day backfill (~2-4 hours)
  python scripts/backfill_30days.py

  # Specific date range
  python scripts/backfill_30days.py --start 2026-04-01 --end 2026-04-25

  # Resume interrupted backfill (auto-detects progress)
  python scripts/backfill_30days.py

  # Reset and start from scratch
  python scripts/backfill_30days.py --reset-progress
        """,
    )

    parser.add_argument("--days", type=int, default=30, help="Days to backfill (default: 30)")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--reset-progress", action="store_true", help="Reset and start over")

    args = parser.parse_args()

    # Load .env
    try:
        from dotenv import load_dotenv
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded .env from {env_path}")
    except ImportError:
        pass

    if args.reset_progress and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        logger.info("Progress reset")

    run_backfill(days=args.days, start_date=args.start, end_date=args.end)


if __name__ == "__main__":
    main()
