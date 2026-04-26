# LinkedIn Post — Flight Intelligence DWH

## Post (English)

---

I built a data warehouse that detects anomalies in military aviation activity.

The idea: every aircraft broadcasts its position via ADS-B signals. OpenSky Network collects this data publicly. Military aircraft have known ICAO24 hex code ranges assigned by country.

So I built a pipeline that:

1. Extracts flight data from OpenSky Network API (with OAuth2 auth) — or generates realistic synthetic data for offline demo (30 days, ~23K flights, including deliberate anomaly spikes)

2. Loads raw data into BigQuery (or DuckDB for local dev) — following the ELT pattern

3. Transforms through dbt layer architecture:
   - Staging: clean & rename
   - Intermediate: enrich with airport coordinates, classify military aircraft by ICAO24 ranges, compute haversine distances
   - Marts: aggregate into analytics-ready tables

4. Runs two algorithms:
   - Military Activity Anomaly Detector — Z-score analysis stratified by country and day-of-week. Flags days as NORMAL / ELEVATED / HIGH / CRITICAL based on statistical deviation from baseline
   - Airport Network Graph Analysis — PageRank and betweenness centrality using NetworkX to identify the most critical aviation hubs

5. Presents results in an interactive Streamlit dashboard with airport map, anomaly timeline, and hub rankings

Important note: the demo version uses synthetic data generated to mimic realistic traffic patterns — including intentional anomaly days where military activity spikes 2.5-4x above normal. This lets the full pipeline demonstrate its detection capabilities without requiring live API access. The architecture is production-ready and can switch to real OpenSky data by adding OAuth2 credentials.

Full dbt lineage: 3 staging models → 4 intermediate → 4 SQL marts + 2 Python models → 36 data quality tests (all passing).

Tech stack: Python, dbt, BigQuery, DuckDB, Apache Airflow, Docker, NetworkX, Streamlit

What I learned building this:
- ELT > ETL when you have a proper data warehouse — let dbt do the heavy lifting
- dbt's layer architecture (staging → intermediate → marts) makes debugging and reuse so much easier than monolithic SQL
- Python models in dbt are powerful — I run Z-score anomaly detection and NetworkX PageRank directly in the transformation layer
- The Strategy pattern for dual backends (BigQuery/DuckDB) saves hours of environment switching
- Having a synthetic data generator is essential for portfolio projects — you can demo the full pipeline anytime, anywhere, without API dependencies
- Military callsign patterns (RCH = US transport, GAF = German Air Force) are a fascinating OSINT rabbit hole

This is project #2 in my data engineering portfolio series. Project #1 was a Crypto ETL Pipeline with anomaly detection and technical indicators.

GitHub: [link]

#DataEngineering #dbt #BigQuery #Airflow #OSINT #Python #Portfolio

---

## Suggested visuals for the post

Best format: **carousel (PDF/images)** — gets highest engagement on LinkedIn.

Slide ideas:
1. Architecture diagram (the ASCII one from README, but as a clean graphic)
2. Screenshot: Streamlit dashboard — airport map with connections
3. Screenshot: Anomaly detection chart showing the spikes on anomaly days
4. Screenshot: dbt lineage graph (`dbt docs generate && dbt docs serve`)
5. Table: PageRank top 10 airports (KATL, EDDF, EGLL at the top)
6. Code snippet: the Z-score calculation (clean, readable, impressive)

## Posting tips

- Post on Tuesday-Thursday morning (highest engagement)
- Tag relevant people/communities: dbt Community, Apache Airflow
- First comment: link to GitHub repo
- Reply to every comment within 24h
- Mention it's a portfolio project — people love supporting career builders

## Hashtag alternatives

Primary: #DataEngineering #dbt #BigQuery #Airflow #Python
Secondary: #OSINT #Aviation #Portfolio #OpenSource #DataWarehouse
Niche: #AnomalyDetection #GraphAnalysis #PageRank #NetworkX #Streamlit
