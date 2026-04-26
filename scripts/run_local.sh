#!/bin/bash
# ──────────────────────────────────────────────
# Run the Flight Intelligence pipeline locally
# (without Docker, without BigQuery — uses DuckDB)
# ──────────────────────────────────────────────

set -e

echo "=== Flight Intelligence — Local Run ==="
echo ""

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "ERROR: Run this script from the project root directory"
    exit 1
fi

# Create data directory
mkdir -p data

# Install dependencies (if not in venv)
echo "1/5  Installing dependencies..."
pip install -r requirements.txt --quiet

# Navigate to dbt project
cd dbt_project

# Copy profiles if not exists
if [ ! -f "profiles.yml" ]; then
    echo "     Creating profiles.yml from example..."
    cp profiles.yml.example profiles.yml
fi

# Install dbt packages
echo "2/5  Installing dbt packages..."
dbt deps --quiet

# Load seed data
echo "3/5  Loading seed data..."
dbt seed --target local

# Run all models
echo "4/5  Running dbt models..."
dbt run --target local

# Run tests
echo "5/5  Running dbt tests..."
dbt test --target local

echo ""
echo "=== Pipeline complete! ==="
echo "Database: data/flight_intelligence.duckdb"
echo ""
echo "To explore results:"
echo "  python -c \"import duckdb; conn = duckdb.connect('data/flight_intelligence.duckdb'); print(conn.sql('SHOW TABLES'))\""
echo ""
echo "To generate dbt docs:"
echo "  cd dbt_project && dbt docs generate --target local && dbt docs serve"
