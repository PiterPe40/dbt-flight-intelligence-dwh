"""
Flight Intelligence Dashboard — Streamlit Application.

Interactive dashboard with:
  1. Airport Network Map — global map with PageRank-sized markers
  2. Military Activity Anomaly Monitor — time series + anomaly flags
  3. Airport Hub Rankings — sortable table with graph metrics
  4. Route Performance — top routes by frequency and efficiency
"""

import os
import sys

import streamlit as st
import pandas as pd
import numpy as np

# ── Page config (must be first Streamlit command) ────────
st.set_page_config(
    page_title="Flight Intelligence Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database connection ──────────────────────────────────

DB_PATH = os.getenv(
    "LOCAL_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "flight_intelligence.duckdb"),
)


@st.cache_resource
def get_connection():
    """Create a DuckDB connection (cached across reruns)."""
    import duckdb
    return duckdb.connect(DB_PATH, read_only=True)


def query(sql: str) -> pd.DataFrame:
    """Execute SQL query and return DataFrame."""
    try:
        conn = get_connection()
        return conn.execute(sql).fetchdf()
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()


def table_exists(table_name: str, schema: str = "main") -> bool:
    """Check if a table/view exists in DuckDB."""
    try:
        conn = get_connection()
        result = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()
        return result[0] > 0
    except Exception:
        return False


# ── Check for data ───────────────────────────────────────

def check_data_available():
    """Check if DuckDB file exists and has data."""
    if not os.path.exists(DB_PATH):
        return False, "no_db"
    try:
        conn = get_connection()
        tables = conn.execute("SHOW TABLES").fetchdf()
        if tables.empty:
            return False, "no_tables"
        return True, "ok"
    except Exception as e:
        return False, str(e)


# ── Sidebar ──────────────────────────────────────────────

with st.sidebar:
    st.title("✈️ Flight Intelligence")
    st.caption("Military Activity Anomaly Detector")
    st.divider()

    page = st.radio(
        "Dashboard",
        [
            "🗺️ Airport Network Map",
            "⚠️ Military Anomalies",
            "🏆 Hub Rankings",
            "📊 Route Performance",
            "ℹ️ About",
        ],
    )

    st.divider()
    st.caption(f"Database: `{os.path.basename(DB_PATH)}`")

    data_ok, status = check_data_available()
    if data_ok:
        st.success("Database connected")
    else:
        st.error(f"No data: {status}")


# ── Helper: generate demo data ───────────────────────────

def generate_demo_airports() -> pd.DataFrame:
    """Generate demo airport data if real data not available."""
    return pd.DataFrame({
        "airport_id": ["EDDF", "EGLL", "EHAM", "LFPG", "EPWA", "LEMD", "LIRF", "LSZH", "KJFK", "LTFM",
                       "LOWW", "EKCH", "ESSA", "EBBR", "ENGM", "EFHK", "EPKK", "EDDM", "EDDB", "KATL"],
        "airport_name": ["Frankfurt", "London Heathrow", "Amsterdam Schiphol", "Paris CDG", "Warsaw Chopin",
                         "Madrid Barajas", "Rome Fiumicino", "Zurich", "New York JFK", "Istanbul",
                         "Vienna", "Copenhagen", "Stockholm", "Brussels", "Oslo", "Helsinki",
                         "Krakow", "Munich", "Berlin", "Atlanta"],
        "latitude": [50.04, 51.47, 52.31, 49.01, 52.17, 40.49, 41.80, 47.46, 40.64, 41.28,
                     48.11, 55.62, 59.65, 50.90, 60.20, 60.32, 50.08, 48.35, 52.35, 33.64],
        "longitude": [8.56, -0.45, 4.76, 2.55, 20.97, -3.57, 12.24, 8.55, -73.78, 28.75,
                      16.57, 12.66, 17.92, 4.48, 11.10, 24.96, 19.78, 11.79, 13.49, -84.43],
        "pagerank_score": [0.082, 0.075, 0.068, 0.065, 0.038, 0.042, 0.040, 0.035, 0.055, 0.048,
                           0.032, 0.030, 0.028, 0.027, 0.025, 0.022, 0.018, 0.045, 0.037, 0.052],
        "total_degree": [180, 165, 150, 148, 85, 95, 92, 78, 120, 110,
                         72, 68, 62, 60, 55, 48, 35, 105, 82, 115],
        "hub_rank": list(range(1, 21)),
        "betweenness_centrality": [0.12, 0.11, 0.09, 0.085, 0.04, 0.05, 0.048, 0.035, 0.08, 0.065,
                                    0.032, 0.028, 0.025, 0.024, 0.020, 0.018, 0.012, 0.06, 0.038, 0.072],
        "country_code": ["DE", "GB", "NL", "FR", "PL", "ES", "IT", "CH", "US", "TR",
                         "AT", "DK", "SE", "BE", "NO", "FI", "PL", "DE", "DE", "US"],
    })


def generate_demo_anomalies() -> pd.DataFrame:
    """Generate demo military anomaly data."""
    np.random.seed(42)
    dates = pd.date_range("2026-03-01", "2026-04-24", freq="D")
    countries = ["DE", "PL", "US", "GB", "FR"]
    rows = []
    for country in countries:
        base = {"DE": 18, "PL": 8, "US": 25, "GB": 15, "FR": 12}[country]
        std = {"DE": 4, "PL": 3, "US": 6, "GB": 4, "FR": 3}[country]
        for date in dates:
            count = max(0, int(np.random.normal(base, std)))
            # Add some spikes
            if np.random.random() < 0.05:
                count = int(count * 2.5)
            z = (count - base) / max(std, 1)
            level = "NORMAL"
            if z >= 3.5: level = "CRITICAL"
            elif z >= 2.5: level = "HIGH"
            elif z >= 1.5: level = "ELEVATED"
            rows.append({
                "date_id": date,
                "country_code": country,
                "military_flight_count": count,
                "baseline_mean": base,
                "z_score": round(z, 2),
                "anomaly_level": level,
            })
    return pd.DataFrame(rows)


def generate_demo_routes() -> pd.DataFrame:
    """Generate demo route performance data."""
    routes = [
        ("EDDF", "EGLL", "Frankfurt", "London Heathrow", 635, 85, 450),
        ("EHAM", "LFPG", "Amsterdam", "Paris CDG", 430, 70, 380),
        ("EPWA", "EDDF", "Warsaw", "Frankfurt", 892, 105, 320),
        ("EGLL", "KJFK", "London", "New York JFK", 5540, 445, 280),
        ("EDDM", "LIRF", "Munich", "Rome", 815, 100, 260),
        ("EDDF", "LTFM", "Frankfurt", "Istanbul", 1860, 180, 240),
        ("LFPG", "LEMD", "Paris", "Madrid", 1050, 125, 220),
        ("EHAM", "EGLL", "Amsterdam", "London", 370, 60, 200),
        ("EPWA", "EHAM", "Warsaw", "Amsterdam", 1095, 130, 190),
        ("LSZH", "EDDF", "Zurich", "Frankfurt", 305, 55, 180),
    ]
    return pd.DataFrame(routes, columns=[
        "origin_airport_id", "destination_airport_id",
        "origin_airport_name", "destination_airport_name",
        "avg_distance_km", "avg_duration_min", "total_flights",
    ])


# ── Load data (real or demo) ────────────────────────────

@st.cache_data(ttl=300)
def load_airports():
    if data_ok:
        # Try marts first, fall back to seeds
        df = query("SELECT * FROM marts.mart_pagerank_airports") if table_exists("mart_pagerank_airports") else pd.DataFrame()
        if df.empty:
            df = query("SELECT * FROM marts.mart_airport_network") if table_exists("mart_airport_network") else pd.DataFrame()
        if df.empty:
            df = query("SELECT * FROM airport_metadata") if table_exists("airport_metadata") else pd.DataFrame()
        if not df.empty:
            return df
    return generate_demo_airports()


@st.cache_data(ttl=300)
def load_anomalies():
    if data_ok:
        df = query("SELECT * FROM marts.mart_military_anomaly_scores") if table_exists("mart_military_anomaly_scores") else pd.DataFrame()
        if df.empty:
            df = query("SELECT * FROM marts.mart_military_activity") if table_exists("mart_military_activity") else pd.DataFrame()
        if not df.empty:
            return df
    return generate_demo_anomalies()


@st.cache_data(ttl=300)
def load_routes():
    if data_ok:
        df = query("SELECT * FROM marts.mart_flight_performance") if table_exists("mart_flight_performance") else pd.DataFrame()
        if not df.empty:
            return df
    return generate_demo_routes()


# ══════════════════════════════════════════════════════════
# PAGE: Airport Network Map
# ══════════════════════════════════════════════════════════

if page == "🗺️ Airport Network Map":
    st.title("🗺️ Airport Network Map")
    st.caption("Airport importance based on PageRank — larger markers = higher score")

    airports = load_airports()

    if airports.empty:
        st.warning("No airport data available")
    else:
        # Ensure required columns
        lat_col = "latitude" if "latitude" in airports.columns else "lat"
        lon_col = "longitude" if "longitude" in airports.columns else "lon"

        if lat_col in airports.columns and lon_col in airports.columns:
            map_df = airports.copy()
            map_df = map_df.dropna(subset=[lat_col, lon_col])

            # Determine size column
            size_col = "pagerank_score" if "pagerank_score" in map_df.columns else None
            if size_col is None and "total_degree" in map_df.columns:
                size_col = "total_degree"

            if size_col:
                # Scale for visibility
                min_size = map_df[size_col].min()
                max_size = map_df[size_col].max()
                if max_size > min_size:
                    map_df["_size"] = ((map_df[size_col] - min_size) / (max_size - min_size) * 800 + 50)
                else:
                    map_df["_size"] = 200
            else:
                map_df["_size"] = 200

            # Metrics row
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Airports", len(map_df))
            if "country_code" in map_df.columns:
                col2.metric("Countries", map_df["country_code"].nunique())
            if size_col and "airport_id" in map_df.columns:
                top_airport = map_df.loc[map_df[size_col].idxmax()]
                name = top_airport.get("airport_name", top_airport.get("airport_id", "N/A"))
                col3.metric("Top Hub", name)

            # Filters
            if "country_code" in map_df.columns:
                countries = sorted(map_df["country_code"].dropna().unique())
                selected_countries = st.multiselect(
                    "Filter by country",
                    countries,
                    default=None,
                    placeholder="All countries",
                )
                if selected_countries:
                    map_df = map_df[map_df["country_code"].isin(selected_countries)]

            # Map
            st.map(
                map_df,
                latitude=lat_col,
                longitude=lon_col,
                size="_size",
            )

            # Top airports table
            st.subheader("Top 15 Airports by PageRank")
            display_cols = [c for c in ["hub_rank", "airport_id", "airport_name", "country_code",
                                         "pagerank_score", "total_degree", "betweenness_centrality"]
                           if c in map_df.columns]
            if display_cols:
                top_df = map_df.sort_values(
                    size_col if size_col else display_cols[0], ascending=False
                ).head(15)
                st.dataframe(top_df[display_cols], use_container_width=True, hide_index=True)

        else:
            st.error(f"Missing coordinate columns. Available: {list(airports.columns)}")


# ══════════════════════════════════════════════════════════
# PAGE: Military Anomalies
# ══════════════════════════════════════════════════════════

elif page == "⚠️ Military Anomalies":
    st.title("⚠️ Military Activity Anomaly Monitor")
    st.caption("Z-score based anomaly detection — days with unusual military flight activity")

    anomalies = load_anomalies()

    if anomalies.empty:
        st.warning("No anomaly data available")
    else:
        # Ensure date column is datetime
        date_col = "date_id" if "date_id" in anomalies.columns else anomalies.columns[0]
        anomalies[date_col] = pd.to_datetime(anomalies[date_col])

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        if "anomaly_level" in anomalies.columns:
            critical = len(anomalies[anomalies["anomaly_level"] == "CRITICAL"])
            high = len(anomalies[anomalies["anomaly_level"] == "HIGH"])
            elevated = len(anomalies[anomalies["anomaly_level"] == "ELEVATED"])
            col1.metric("🔴 CRITICAL", critical)
            col2.metric("🟠 HIGH", high)
            col3.metric("🟡 ELEVATED", elevated)
        if "country_code" in anomalies.columns:
            col4.metric("Countries Monitored", anomalies["country_code"].nunique())

        st.divider()

        # Country selector
        if "country_code" in anomalies.columns:
            countries = sorted(anomalies["country_code"].dropna().unique())
            selected_country = st.selectbox("Select country", countries, index=0)
            country_data = anomalies[anomalies["country_code"] == selected_country].sort_values(date_col)
        else:
            country_data = anomalies.sort_values(date_col)

        # Time series chart: military flights
        if "military_flight_count" in country_data.columns:
            st.subheader(f"Daily Military Flights — {selected_country if 'country_code' in anomalies.columns else 'All'}")

            chart_data = country_data.set_index(date_col)[["military_flight_count"]]
            if "baseline_mean" in country_data.columns:
                chart_data["baseline_mean"] = country_data.set_index(date_col)["baseline_mean"]

            st.line_chart(chart_data)

        # Z-score chart
        if "z_score" in country_data.columns:
            st.subheader("Z-score Over Time")

            zscore_chart = country_data.set_index(date_col)[["z_score"]]
            st.area_chart(zscore_chart)

            # Threshold lines description
            st.caption("Threshold levels: ELEVATED (1.5) | HIGH (2.5) | CRITICAL (3.5)")

        # Anomaly events table
        if "anomaly_level" in country_data.columns:
            anomaly_events = country_data[country_data["anomaly_level"].isin(["ELEVATED", "HIGH", "CRITICAL"])]
            if not anomaly_events.empty:
                st.subheader(f"Anomaly Events ({len(anomaly_events)})")
                display_cols = [c for c in [date_col, "country_code", "military_flight_count",
                                            "baseline_mean", "z_score", "anomaly_level"]
                               if c in anomaly_events.columns]
                st.dataframe(
                    anomaly_events[display_cols].sort_values("z_score", ascending=False),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No anomalies detected for this country in the selected period.")


# ══════════════════════════════════════════════════════════
# PAGE: Hub Rankings
# ══════════════════════════════════════════════════════════

elif page == "🏆 Hub Rankings":
    st.title("🏆 Airport Hub Rankings")
    st.caption("Network centrality metrics from graph analysis")

    airports = load_airports()

    if airports.empty:
        st.warning("No hub data available")
    else:
        # Metric selector
        metric_options = {}
        if "pagerank_score" in airports.columns:
            metric_options["PageRank Score"] = "pagerank_score"
        if "total_degree" in airports.columns:
            metric_options["Total Connections (Degree)"] = "total_degree"
        if "betweenness_centrality" in airports.columns:
            metric_options["Betweenness Centrality"] = "betweenness_centrality"

        if metric_options:
            selected_metric_name = st.selectbox("Rank by", list(metric_options.keys()))
            selected_metric = metric_options[selected_metric_name]

            sorted_df = airports.sort_values(selected_metric, ascending=False).head(20)

            # Bar chart
            st.subheader(f"Top 20 Airports by {selected_metric_name}")

            chart_col = "airport_name" if "airport_name" in sorted_df.columns else "airport_id"
            chart_data = sorted_df.set_index(chart_col)[[selected_metric]]
            st.bar_chart(chart_data)

            # Full table
            st.subheader("Detailed Metrics")
            display_cols = [c for c in ["hub_rank", "airport_id", "airport_name", "country_code",
                                         "pagerank_score", "total_degree", "betweenness_centrality",
                                         "clustering_coefficient"]
                           if c in sorted_df.columns]
            st.dataframe(sorted_df[display_cols], use_container_width=True, hide_index=True)
        else:
            st.dataframe(airports, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════
# PAGE: Route Performance
# ══════════════════════════════════════════════════════════

elif page == "📊 Route Performance":
    st.title("📊 Route Performance")
    st.caption("Top air routes by frequency, distance, and efficiency")

    routes = load_routes()

    if routes.empty:
        st.warning("No route data available")
    else:
        # Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Routes", len(routes))
        if "total_flights" in routes.columns:
            col2.metric("Total Flights", f"{routes['total_flights'].sum():,.0f}")
        if "avg_distance_km" in routes.columns:
            col3.metric("Avg Distance", f"{routes['avg_distance_km'].mean():,.0f} km")

        st.divider()

        # Sort selector
        sort_options = {}
        if "total_flights" in routes.columns:
            sort_options["Most Flights"] = "total_flights"
        if "avg_distance_km" in routes.columns:
            sort_options["Longest Routes"] = "avg_distance_km"
        if "avg_duration_min" in routes.columns:
            sort_options["Longest Duration"] = "avg_duration_min"

        if sort_options:
            sort_by = st.selectbox("Sort by", list(sort_options.keys()))
            sorted_routes = routes.sort_values(sort_options[sort_by], ascending=False).head(20)
        else:
            sorted_routes = routes.head(20)

        # Chart: top routes by flights
        if "total_flights" in sorted_routes.columns:
            st.subheader("Top Routes by Flight Count")

            # Create route label
            if "origin_airport_name" in sorted_routes.columns and "destination_airport_name" in sorted_routes.columns:
                sorted_routes = sorted_routes.copy()
                sorted_routes["route"] = sorted_routes["origin_airport_name"] + " → " + sorted_routes["destination_airport_name"]
            elif "origin_airport_id" in sorted_routes.columns:
                sorted_routes = sorted_routes.copy()
                sorted_routes["route"] = sorted_routes["origin_airport_id"] + " → " + sorted_routes["destination_airport_id"]

            if "route" in sorted_routes.columns:
                chart_data = sorted_routes.set_index("route")[["total_flights"]].head(15)
                st.bar_chart(chart_data)

        # Table
        st.subheader("Route Details")
        display_cols = [c for c in ["origin_airport_id", "destination_airport_id",
                                     "origin_airport_name", "destination_airport_name",
                                     "total_flights", "avg_duration_min", "avg_distance_km"]
                       if c in sorted_routes.columns]
        st.dataframe(sorted_routes[display_cols], use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════
# PAGE: About
# ══════════════════════════════════════════════════════════

elif page == "ℹ️ About":
    st.title("ℹ️ About Flight Intelligence")

    st.markdown("""
    ### What is this?

    A data warehouse built on public aviation data from the **OpenSky Network API**
    that implements two advanced analytical algorithms:

    **1. Military Activity Anomaly Detector** — Detects days when military flight activity
    significantly exceeds the statistical baseline (Z-score analysis stratified by
    country and day-of-week).

    **2. Airport Network Graph Analysis** — Models the global flight network as a directed
    weighted graph and computes PageRank, betweenness centrality, and clustering
    coefficients to identify key aviation hubs.

    ### Tech Stack

    | Component | Technology |
    |---|---|
    | Transformation | dbt (staging → intermediate → marts) |
    | Cloud DWH | Google BigQuery |
    | Local DWH | DuckDB |
    | Orchestration | Apache Airflow |
    | Graph Analysis | NetworkX (PageRank) |
    | Anomaly Detection | Z-score (numpy/scipy) |
    | Dashboard | Streamlit |
    | Data Source | OpenSky Network (ADS-B) |

    ### Data Flow

    ```
    OpenSky API → Airflow DAG → BigQuery/DuckDB → dbt transforms → Streamlit dashboard
    ```

    ### Author

    Built as a data engineering portfolio project.
    """)

    # Database info
    st.divider()
    st.subheader("Database Status")

    ok, status = check_data_available()
    if ok:
        st.success(f"Connected to: `{DB_PATH}`")
        try:
            tables = get_connection().execute("SHOW TABLES").fetchdf()
            st.dataframe(tables, use_container_width=True, hide_index=True)
        except Exception:
            pass
    else:
        st.warning(f"Database not available: {status}")
        st.info(
            "To populate the database, run:\n"
            "```\n"
            "python scripts/fetch_sample_data.py\n"
            "cd dbt_project && dbt seed --target local && dbt run --target local\n"
            "```"
        )
