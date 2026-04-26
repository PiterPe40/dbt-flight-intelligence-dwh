# Flight Intelligence: Military Activity Anomaly Detector

A data warehouse built on public aviation data (OpenSky Network API) that implements **military flight anomaly detection** and **airport network graph analysis** using dbt, BigQuery/DuckDB, and Apache Airflow.

```
OpenSky Network API  ──→  Airflow DAG  ──→  BigQuery / DuckDB
(or demo data generator)                        │
                                           dbt transforms
                                           (staging → intermediate → marts)
                                                │
                                   ┌────────────┴────────────┐
                                   ▼                         ▼
                         Military Anomaly           Airport Network
                         Detector (Z-score)         Graph (PageRank)
                                   │                         │
                                   └────────┬────────────────┘
                                            ▼
                                    Streamlit Dashboard
                                    (map, charts, rankings)
```

---

## What This Project Does

**1. Military Activity Anomaly Detector** — Detects days when military flight activity significantly exceeds the statistical baseline for a given country and day of week. Uses Z-score analysis to flag potential exercises or unusual military movements as NORMAL / ELEVATED / HIGH / CRITICAL.

**2. Airport Network Graph Analysis** — Models the global flight network as a directed weighted graph. Computes PageRank, betweenness centrality, and clustering coefficients to identify key aviation hubs.

**3. Interactive Dashboard** — Streamlit app with airport network map, anomaly timeline, hub rankings, and route performance metrics.

---

## Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│  OpenSky Network │     │   Airflow DAG    │     │   BigQuery / DuckDB      │
│  REST API        │────▶│   (daily @ 06:00)│────▶│                          │
│  (OAuth2 auth)   │     │                  │     │  raw_flights             │
└──────────────────┘     └──────────────────┘     │         │                │
                                                  │    dbt staging           │
┌──────────────────┐                              │         │                │
│  Demo Data       │──────────────────────────▶   │    dbt intermediate      │
│  Generator       │  (alternative for demos)     │         │                │
└──────────────────┘                              │    dbt marts             │
                                                  │    ├── flight_performance│
                                                  │    ├── airport_network   │
                                                  │    ├── military_activity │
                                                  │    └── anomaly_flags     │
                                                  │                          │
                                                  │    dbt Python models     │
                                                  │    ├── anomaly_scores    │
                                                  │    └── pagerank_airports │
                                                  └──────────────┬───────────┘
                                                                 │
                                                  ┌──────────────▼───────────┐
                                                  │    Streamlit Dashboard   │
                                                  │    (port 8501)           │
                                                  └──────────────────────────┘
```

### dbt Layer Architecture

| Layer | Purpose | Materialization |
|---|---|---|
| **Staging** | Rename columns, cast types, filter nulls | View |
| **Intermediate** | Joins, enrichment, haversine distance, military classification | View |
| **Marts (SQL)** | Aggregated analytics tables for dashboarding | Table |
| **Marts (Python)** | Advanced algorithms: Z-score anomaly, NetworkX PageRank | Table |

---

## Tech Stack

| Component | Technology |
|---|---|
| **Language** | Python 3.11 |
| **Transformation** | dbt-core 1.7+ |
| **Cloud DWH** | Google BigQuery |
| **Local DWH** | DuckDB (development fallback) |
| **Orchestration** | Apache Airflow 2.8 |
| **Data Source** | OpenSky Network API (ADS-B, OAuth2) |
| **Graph Analysis** | NetworkX |
| **Anomaly Detection** | scipy / numpy (Z-score) |
| **Dashboard** | Streamlit (interactive map, charts) |
| **Containerization** | Docker + Docker Compose |
| **Testing** | dbt tests (36 tests) |

---

## Quick Start

### Prerequisites

- Python 3.10+ with pip
- (Optional) Docker & Docker Compose
- (Optional) Google Cloud account with BigQuery API enabled
- (Optional) OpenSky Network account with OAuth2 API client

### Option A: Demo Data (quickest — no API needed)

```bash
git clone https://github.com/YOUR_USERNAME/dbt-flight-intelligence-dwh.git
cd dbt-flight-intelligence-dwh

# Create virtual environment
python -m venv venv
source venv/bin/activate        # Linux/Mac
# .\venv\Scripts\Activate.ps1   # Windows PowerShell

pip install -r requirements.txt

# Generate 30 days of realistic demo data (~2 seconds)
python scripts/generate_demo_data.py

# Run dbt pipeline
cd dbt_project
dbt deps
dbt seed --target local
dbt run --target local
dbt test --target local

# Launch dashboard
cd ..
streamlit run streamlit_app/app.py
```

### Option B: Real OpenSky Data (OAuth2)

```bash
# 1. Copy and configure .env
cp .env.example .env

# 2. Log in at https://opensky-network.org → Account → API Clients
#    Create a new client, then add to .env:
#    OPENSKY_CLIENT_ID=your_client_id
#    OPENSKY_CLIENT_SECRET=your_client_secret

# 3. Run backfill (7 days = ~1 hour, 30 days = ~4 hours)
python scripts/backfill_30days.py --days 7

# 4. Run dbt pipeline
cd dbt_project
dbt seed --target local
dbt run --target local
dbt test --target local
```

### Option C: Docker (Airflow + Streamlit)

```bash
cp .env.example .env
# Edit .env with your settings

docker-compose up -d

# Access:
# Airflow UI:  http://localhost:8080  (admin / admin)
# Streamlit:   http://localhost:8501
```

---

## Demo Data Generator

The project includes a synthetic data generator (`scripts/generate_demo_data.py`) that creates realistic flight data for portfolio demos:

- **30 days** of data with ~800 flights/day
- **Realistic traffic patterns**: hub airports get more flights, weekends have fewer
- **Military flights** (~5% of total) from real ICAO24 hex ranges
- **Anomaly spikes**: 2-3 days with 2.5-4x military activity (for detection to find)
- **Proper callsigns**: airline codes (DLH, BAW, LOT) + military (RCH, GAF, PLF)
- **Reproducible**: seeded random generator (`random.seed(42)`)

---

## Project Structure

```
dbt-flight-intelligence-dwh/
├── dags/
│   ├── daily_flight_pipeline.py        # Airflow DAG definition
│   └── helpers/
│       ├── opensky_client.py           # OpenSky API client (OAuth2 + legacy)
│       └── data_loader.py             # BigQuery/DuckDB strategy pattern
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml.example            # BigQuery + DuckDB profiles
│   ├── packages.yml                    # dbt-utils, dbt-expectations
│   ├── models/
│   │   ├── staging/                    # Layer 1: Clean + rename
│   │   │   ├── stg_opensky__flights.sql
│   │   │   ├── stg_opensky__airports.sql
│   │   │   └── stg_opensky__military_ranges.sql
│   │   ├── intermediate/              # Layer 2: Enrich + join
│   │   │   ├── int_flights_enriched.sql
│   │   │   ├── int_military_flights.sql
│   │   │   ├── int_airport_connections.sql
│   │   │   └── int_daily_military_counts.sql
│   │   ├── marts/                     # Layer 3: Analytics tables
│   │   │   ├── mart_flight_performance.sql
│   │   │   ├── mart_airport_network.sql
│   │   │   ├── mart_military_activity.sql
│   │   │   └── mart_anomaly_flags.sql
│   │   └── python/                    # Layer 4: Python models
│   │       ├── mart_military_anomaly_scores.py  # Z-score anomaly detection
│   │       └── mart_pagerank_airports.py        # NetworkX PageRank
│   ├── seeds/
│   │   ├── military_icao24_ranges.csv  # 23 countries' military hex ranges
│   │   └── airport_metadata.csv        # 55 airports with coordinates
│   └── tests/
│       ├── assert_no_negative_duration.sql
│       ├── assert_military_ratio_reasonable.sql
│       └── assert_flight_duration_max_24h.sql
│
├── streamlit_app/
│   └── app.py                          # Interactive dashboard
│
├── scripts/
│   ├── generate_demo_data.py           # Synthetic data generator (no API needed)
│   ├── backfill_30days.py              # Historical data from OpenSky API
│   ├── fetch_sample_data.py            # Quick data sample
│   └── run_local.sh                    # One-command local setup
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── PRZEWODNIK_EDUKACYJNY.md            # Educational guide (PL)
├── INSTRUKCJA_URUCHOMIENIA.md          # Setup instructions (PL)
└── LINKEDIN_POST.md                    # LinkedIn post concept
```

---

## Algorithms

### Military Activity Anomaly Detector

Detects statistically abnormal spikes in military aviation activity.

**Method:** Z-score per country per day-of-week:
```
z = (observed_count - baseline_mean) / baseline_std
```

| Z-score | Level | Interpretation |
|---|---|---|
| < 1.5 | NORMAL | Within expected range |
| 1.5 - 2.5 | ELEVATED | Above average activity |
| 2.5 - 3.5 | HIGH | Significantly above normal |
| > 3.5 | CRITICAL | Extreme anomaly — potential exercise or operation |

### Airport Network Graph Analysis (PageRank)

Models airports as nodes and flight routes as weighted directed edges.

**Metrics computed:**
- **PageRank** — Airport importance (Google's algorithm adapted for aviation)
- **Betweenness Centrality** — How critical an airport is as a transfer point
- **Clustering Coefficient** — How interconnected an airport's neighbors are
- **Degree Centrality** — Number and weight of connections

---

## Data Quality Tests

| Test | What it checks |
|---|---|
| `unique` / `not_null` on PKs | Basic integrity |
| `accepted_range` on duration | No negative or >24h flights |
| `accepted_values` on anomaly_level | Only valid categories |
| `assert_military_ratio_reasonable` | Military % < 20% of all traffic |
| `assert_no_negative_duration` | All durations positive |

**36 tests total — all passing.**

---

## OpenSky API Authentication

Since March 2026, OpenSky Network requires **OAuth2 client credentials** (basic auth is deprecated).

To set up:
1. Log in at [opensky-network.org](https://opensky-network.org)
2. Go to **Account → API Clients**
3. Create a new API client
4. Copy `client_id` and `client_secret` to your `.env` file

The project also works fully offline using the demo data generator.

---

## Skills Demonstrated

- **Data Modeling** — Star schema, dbt layer architecture (staging → marts)
- **SQL at Scale** — BigQuery, window functions, CTEs, aggregations
- **dbt** — Models, tests, documentation, Python models, macros, seeds
- **Algorithms** — Anomaly detection (Z-score), Graph analysis (PageRank, NetworkX)
- **Orchestration** — Airflow DAG with task dependencies and parallelism
- **Data Quality** — 36 dbt tests, custom SQL tests, data contracts
- **OSINT** — Creative use of public ADS-B data for intelligence analysis
- **Containerization** — Docker + Docker Compose for reproducible environments
- **Dashboard** — Interactive Streamlit app with maps and charts

---

## License

MIT

---

*Built as project #2 in a data engineering portfolio series.*
