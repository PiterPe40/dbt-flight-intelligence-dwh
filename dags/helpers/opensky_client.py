"""
OpenSky Network API Client.

Handles communication with the OpenSky Network REST API to fetch
flight data (departures, arrivals) and real-time aircraft states.

Supports two authentication methods:
  - OAuth2 client credentials (recommended, required since March 2026)
  - Legacy basic auth (username/password, deprecated)

API Documentation: https://openskynetwork.github.io/opensky-api/rest.html
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class OpenSkyTokenManager:
    """Manages OAuth2 tokens for the OpenSky Network API.

    Since March 2026, OpenSky requires OAuth2 client credentials flow.
    To get your client_id and client_secret:
      1. Log in at https://opensky-network.org
      2. Go to Account -> API Clients
      3. Create a new API client
      4. Copy client_id and client_secret to your .env file
    """

    TOKEN_URL = (
        "https://auth.opensky-network.org/auth/realms/opensky-network"
        "/protocol/openid-connect/token"
    )

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._token_expires_at: float = 0

    def get_token(self) -> str:
        """Get a valid access token, refreshing if expired.

        Tokens are valid for ~30 minutes. We refresh 60 seconds early
        to avoid edge-case expiration during a request.
        """
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        logger.info("Requesting new OAuth2 token from OpenSky...")
        try:
            response = requests.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            self._token = data["access_token"]
            # expires_in is typically 1800 (30 minutes)
            self._token_expires_at = time.time() + data.get("expires_in", 1800)

            logger.info(
                f"OAuth2 token acquired (expires in {data.get('expires_in', '?')}s)"
            )
            return self._token

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"Failed to get OAuth2 token: {e}. "
                f"Check your OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET in .env"
            )
            raise
        except Exception as e:
            logger.error(f"Token request failed: {e}")
            raise


class OpenSkyClient:
    """Client for the OpenSky Network REST API.

    The OpenSky Network provides free access to ADS-B flight data.
    - Without credentials: last 1 hour of state vectors only
    - With free account: last 30 days of flight data
    - Rate limits: ~5 requests/10 seconds (anonymous), ~1 req/5s (authenticated)

    Authentication (choose one):
      - OAuth2 (recommended): pass client_id + client_secret
      - Legacy basic auth (deprecated): pass username + password
    """

    BASE_URL = "https://opensky-network.org/api"

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        """Initialize the OpenSky client.

        Args:
            username: (deprecated) OpenSky account username for basic auth
            password: (deprecated) OpenSky account password for basic auth
            client_id: OAuth2 client ID (from OpenSky Account -> API Clients)
            client_secret: OAuth2 client secret
        """
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "FlightIntelligenceDWH/1.0",
        })

        # ── Authentication setup ──────────────────
        self.token_manager: Optional[OpenSkyTokenManager] = None
        self.auth = None

        if client_id and client_secret:
            # Preferred: OAuth2 client credentials
            self.token_manager = OpenSkyTokenManager(client_id, client_secret)
            logger.info("OpenSky client initialized with OAuth2 authentication")
        elif username and password:
            # Legacy: basic auth (deprecated since March 2026)
            self.auth = (username, password)
            logger.warning(
                "OpenSky client initialized with LEGACY basic auth. "
                "Basic auth is deprecated since March 2026. "
                "Please switch to OAuth2: set OPENSKY_CLIENT_ID and "
                "OPENSKY_CLIENT_SECRET in your .env file. "
                "See: https://opensky-network.org -> Account -> API Clients"
            )
        else:
            logger.warning(
                "OpenSky client initialized WITHOUT authentication. "
                "Data access limited to last 1 hour. "
                "Register at https://opensky-network.org for extended access."
            )

    def _get_auth_headers(self) -> dict:
        """Get authentication headers for the current request."""
        if self.token_manager:
            token = self.token_manager.get_token()
            return {"Authorization": f"Bearer {token}"}
        return {}

    def _make_request(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """Make an authenticated request to the OpenSky API with retry logic.

        Args:
            endpoint: API endpoint path (e.g., '/flights/departure')
            params: Query parameters dictionary

        Returns:
            Parsed JSON response as dict or list

        Raises:
            requests.exceptions.HTTPError: On non-retryable HTTP errors
        """
        url = f"{self.BASE_URL}{endpoint}"
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # Apply auth: OAuth2 Bearer token or legacy basic auth
                headers = self._get_auth_headers()

                response = self.session.get(
                    url,
                    params=params,
                    auth=self.auth,  # None if using OAuth2
                    headers=headers,
                    timeout=60,
                )
                response.raise_for_status()
                logger.info(f"Successfully fetched: {endpoint} (attempt {attempt + 1})")
                return response.json()

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    # Rate limited — back off with increasing delay
                    wait_time = 30 * (attempt + 1)
                    logger.warning(f"Rate limited (429). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code == 401:
                    # Unauthorized — token might be expired
                    if self.token_manager:
                        logger.warning(
                            "Got 401 Unauthorized — token may have expired. "
                            "Refreshing token and retrying..."
                        )
                        self.token_manager._token = None  # Force refresh
                        time.sleep(2)
                    else:
                        logger.error(
                            "Got 401 Unauthorized. If using basic auth, "
                            "note it was deprecated in March 2026. "
                            "Switch to OAuth2 (OPENSKY_CLIENT_ID/SECRET)."
                        )
                        return []
                elif response.status_code == 404:
                    logger.warning(f"No data found for: {endpoint}")
                    return []
                elif response.status_code == 403:
                    # 403 = endpoint not available or auth method rejected
                    premium_endpoints = ["/flights/all"]
                    if endpoint in premium_endpoints:
                        logger.error(
                            f"Access denied (403) for {endpoint}. "
                            f"This endpoint requires a premium OpenSky account. "
                            f"Use /flights/departure and /flights/arrival instead "
                            f"(available on free accounts)."
                        )
                    elif self.auth and not self.token_manager:
                        logger.error(
                            f"Access denied (403) for {endpoint}. "
                            f"Basic auth was deprecated in March 2026. "
                            f"Switch to OAuth2: set OPENSKY_CLIENT_ID and "
                            f"OPENSKY_CLIENT_SECRET in your .env file."
                        )
                    else:
                        logger.error(
                            f"Access denied (403) for {endpoint}. "
                            f"Check your OpenSky credentials or account permissions. "
                            f"If this persists, your account may be temporarily blocked "
                            f"due to rate limiting."
                        )
                    return []
                elif response.status_code == 500:
                    wait_time = 15 * (attempt + 1)
                    logger.warning(
                        f"Server error (500) for {endpoint}. "
                        f"Retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"HTTP error {response.status_code}: {e}")
                    raise

            except requests.exceptions.ConnectionError as e:
                wait_time = 10 * (attempt + 1)
                logger.warning(f"Connection error. Retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout on attempt {attempt + 1}/{max_retries}")
                time.sleep(5)

        raise requests.exceptions.ConnectionError(
            f"Failed to fetch {endpoint} after {max_retries} attempts"
        )

    def fetch_flights(
        self,
        begin: int,
        end: int,
    ) -> list[dict]:
        """Fetch all flights within a time window.

        NOTE: /flights/all requires a PREMIUM OpenSky account.
        For free accounts, use fetch_departures() and fetch_arrivals() instead.

        Args:
            begin: Start of time interval as Unix timestamp
            end: End of time interval as Unix timestamp (max 2h window)

        Returns:
            List of flight records
        """
        logger.warning(
            "fetch_flights() uses /flights/all which requires premium access. "
            "Consider using fetch_departures()/fetch_arrivals() for free accounts."
        )
        params = {"begin": begin, "end": end}
        data = self._make_request("/flights/all", params=params)
        return data if data else []

    def fetch_departures(self, airport: str, begin: int, end: int) -> list[dict]:
        """Fetch departures from a specific airport.

        Args:
            airport: ICAO airport code (e.g., 'EPWA' for Warsaw)
            begin: Start Unix timestamp
            end: End Unix timestamp (max 7 days window)

        Returns:
            List of departure records
        """
        params = {"airport": airport, "begin": begin, "end": end}
        data = self._make_request("/flights/departure", params=params)
        return data if data else []

    def fetch_arrivals(self, airport: str, begin: int, end: int) -> list[dict]:
        """Fetch arrivals at a specific airport.

        Args:
            airport: ICAO airport code
            begin: Start Unix timestamp
            end: End Unix timestamp (max 7 days window)

        Returns:
            List of arrival records
        """
        params = {"airport": airport, "begin": begin, "end": end}
        data = self._make_request("/flights/arrival", params=params)
        return data if data else []

    def fetch_states(self, icao24: Optional[str] = None) -> dict:
        """Fetch current state vectors (real-time aircraft positions).

        Args:
            icao24: Optional specific aircraft ICAO24 address to filter

        Returns:
            Dict with 'time' and 'states' keys
        """
        params = {}
        if icao24:
            params["icao24"] = icao24

        return self._make_request("/states/all", params=params)

    def fetch_airport_daily(
        self, airport: str, date: datetime
    ) -> list[dict]:
        """Fetch departures and arrivals for an airport on a specific date.

        This method works with FREE OpenSky accounts (unlike fetch_daily_flights).
        The /flights/departure and /flights/arrival endpoints accept up to 7-day windows.

        Args:
            airport: ICAO airport code (e.g., 'EPWA')
            date: The date to fetch flights for

        Returns:
            Deduplicated list of flight records (departures + arrivals)
        """
        day_start = datetime(date.year, date.month, date.day, 0, 0, 0)
        day_end = day_start + timedelta(days=1)
        begin_ts = int(day_start.timestamp())
        end_ts = int(day_end.timestamp())

        departures = self.fetch_departures(airport, begin_ts, end_ts)
        time.sleep(5)  # rate limit
        arrivals = self.fetch_arrivals(airport, begin_ts, end_ts)

        # Deduplicate by icao24 + firstSeen
        seen = set()
        combined = []
        for flight in departures + arrivals:
            key = (flight.get("icao24", ""), flight.get("firstSeen", 0))
            if key not in seen:
                seen.add(key)
                combined.append(flight)

        logger.info(
            f"{airport} on {date.date()}: "
            f"{len(departures)} departures + {len(arrivals)} arrivals = "
            f"{len(combined)} unique flights"
        )
        return combined

    def fetch_daily_flights(self, date: datetime) -> list[dict]:
        """Fetch all flights for a specific date, handling the 2-hour window limit.

        NOTE: Uses /flights/all which requires PREMIUM access.
        For free accounts, use fetch_airport_daily() instead.

        Args:
            date: The date to fetch flights for

        Returns:
            List of all flight records for the given date
        """
        all_flights = []
        chunk_hours = 2
        chunks_per_day = 24 // chunk_hours

        day_start = datetime(date.year, date.month, date.day, 0, 0, 0)

        for chunk_idx in range(chunks_per_day):
            chunk_start = day_start + timedelta(hours=chunk_idx * chunk_hours)
            chunk_end = chunk_start + timedelta(hours=chunk_hours)

            begin_ts = int(chunk_start.timestamp())
            end_ts = int(chunk_end.timestamp())

            logger.info(
                f"Fetching chunk {chunk_idx + 1}/{chunks_per_day}: "
                f"{chunk_start.strftime('%H:%M')} - {chunk_end.strftime('%H:%M')}"
            )

            try:
                flights = self.fetch_flights(begin=begin_ts, end=end_ts)
                all_flights.extend(flights)
                logger.info(f"  Got {len(flights)} flights in this chunk")
            except Exception as e:
                logger.warning(f"  Failed to fetch chunk {chunk_idx + 1}: {e}")
                continue

            # Respect rate limits between chunks
            if chunk_idx < chunks_per_day - 1:
                time.sleep(5)

        logger.info(f"Total flights for {date.date()}: {len(all_flights)}")
        return all_flights

    def fetch_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> dict[str, list[dict]]:
        """Fetch flights for a range of dates.

        Args:
            start_date: First date to fetch
            end_date: Last date to fetch (inclusive)

        Returns:
            Dict mapping date strings to lists of flight records
        """
        results = {}
        current = start_date

        while current <= end_date:
            date_key = current.strftime("%Y-%m-%d")
            logger.info(f"Fetching flights for {date_key}...")

            try:
                flights = self.fetch_daily_flights(current)
                results[date_key] = flights
            except Exception as e:
                logger.error(f"Failed to fetch {date_key}: {e}")
                results[date_key] = []

            current += timedelta(days=1)

            # Longer pause between days
            if current <= end_date:
                time.sleep(10)

        return results


# ── Quick test ───────────────────────────────
if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO)

    # Try OAuth2 first, fall back to legacy basic auth
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")

    client = OpenSkyClient(
        username=username,
        password=password,
        client_id=client_id,
        client_secret=client_secret,
    )

    # Test with a departure query (works on free accounts)
    from datetime import timezone
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    flights = client.fetch_airport_daily("EPWA", yesterday)
    print(f"\nFetched {len(flights)} flights for EPWA on {yesterday.date()}")

    if flights:
        sample = flights[0]
        print(f"Sample flight: {sample}")
