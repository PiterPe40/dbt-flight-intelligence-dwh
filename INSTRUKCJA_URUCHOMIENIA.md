# Instrukcja uruchomienia — krok po kroku

Od zera do dzialajacego projektu z danymi, dbt i dashboardem Streamlit.

---

## Etap 0: Co musisz miec zainstalowane

Zanim zaczniesz, upewnij sie ze masz:

1. **Python 3.11+** — sprawdz: `python --version`
2. **Git** — sprawdz: `git --version`
3. **Docker Desktop** — sprawdz: `docker --version` i `docker-compose --version`
4. (Opcjonalnie) **VS Code** — edytor kodu

Jesli czegos brakuje:
- Python: https://www.python.org/downloads/
- Git: https://git-scm.com/downloads
- Docker Desktop: https://www.docker.com/products/docker-desktop/

---

## Etap 1: Przygotowanie srodowiska

### 1.1 Otworz terminal w folderze projektu

```bash
cd "C:\Users\annad\Desktop\Projekt 2 śledzenie lotów"
```

### 1.2 Stworz wirtualne srodowisko Pythona

```bash
python -m venv venv
```

Aktywuj je:

```bash
# Windows (PowerShell)
.\venv\Scripts\Activate.ps1

# Windows (cmd)
.\venv\Scripts\activate.bat

# Linux / Mac
source venv/bin/activate
```

Powinienes zobaczyc `(venv)` na poczatku linii w terminalu.

### 1.3 Zainstaluj zaleznosci

```bash
pip install -r requirements.txt
```

To zainstaluje: dbt, pandas, networkx, requests, streamlit i reszta.
Moze to potrwac 2-3 minuty.

### 1.4 Skopiuj plik konfiguracyjny

```bash
cp .env.example .env
```

Na razie nie musisz niczego zmieniac — domyslnie projekt dziala na DuckDB (lokalnie).

---

## Etap 2: Zaloz konto OpenSky Network (opcjonalne ale zalecane)

Bez konta masz dostep tylko do danych real-time (stan przestrzeni powietrznej).
Z kontem (darmowym) masz dostep do HISTORII LOTOW z ostatnich 30 dni.

### 2.1 Rejestracja

1. Wejdz na: https://opensky-network.org
2. Kliknij "Register" (prawy gorny rog)
3. Wypelnij formularz (email, login, haslo)
4. Potwierdz email

### 2.2 Dodaj dane logowania do .env

Otworz plik `.env` w edytorze i uzupelnij:

```
OPENSKY_USERNAME=twoj_login
OPENSKY_PASSWORD=twoje_haslo
```

---

## Etap 3: Uruchom dbt lokalnie (DuckDB — bez chmury)

To jest NAJSZYBSZA sciezka do zobaczenia dzialajacego projektu.

### 3.1 Przygotuj profil dbt

```bash
cd dbt_project
cp profiles.yml.example profiles.yml
```

### 3.2 Zainstaluj pakiety dbt

```bash
dbt deps
```

Pobierze pakiety: dbt-utils, dbt-expectations, codegen.

### 3.3 Zaladuj dane referencyjne (seeds)

```bash
dbt seed --target local
```

Powinien pojawic sie output:
```
Completed successfully
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

To zaladowalo `military_icao24_ranges.csv` i `airport_metadata.csv` do DuckDB.

### 3.4 Pobierz dane lotnicze — BACKFILL 30 dni

Algorytm anomalii potrzebuje ~30 dni danych do budowania baseline.
Mamy skrypt, ktory pobiera te dane za jednym zamachem.

**WAZNE:** Wymaga konta OpenSky (darmowe) — patrz Etap 2.

Wroc do glownego folderu projektu:

```bash
cd ..
```

**Opcja A — Szybki start (~20-30 min):** 7 dni, 6 godzin dziennie.
Wystarczy do demo i screenshotow na LinkedIn:

```bash
python scripts/backfill_30days.py --days 7 --hours-per-day 6
```

**Opcja B — Pelny backfill (~2-4h):** 30 dni, pelne dane.
Najlepsza jakosc anomaly detection:

```bash
python scripts/backfill_30days.py --days 30 --hours-per-day 12
```

**Opcja C — Maksimum danych (~4-6h):** 30 dni, 24h/dzien:

```bash
python scripts/backfill_30days.py
```

Skrypt:
- Pokazuje postep i szacowany czas
- Zapisuje progres po kazdym dniu — mozesz przerwac (Ctrl+C) i wznowic pozniej
- Automatycznie pomija juz pobrane dni
- Sam zarzadza pauzami miedzy zapytaniami (rate limit)

Jesli NIE masz konta OpenSky — mozesz przejsc do kroku 3.5.
Dashboard uruchomi sie z danymi demo (wbudowanymi w aplikacje).

### 3.5 Uruchom modele dbt

```bash
cd dbt_project
dbt run --target local
```

Powinien pojawic sie output z lista uruchomionych modeli:
```
1 of 11 OK created view stg_opensky__flights
2 of 11 OK created view stg_opensky__airports
...
11 of 11 OK created table mart_pagerank_airports
```

### 3.6 Uruchom testy

```bash
dbt test --target local
```

Oczekiwany output:
```
Completed successfully
Done. PASS=X WARN=0 ERROR=0 SKIP=0
```

### 3.7 Wygeneruj dokumentacje dbt

```bash
dbt docs generate --target local
dbt docs serve
```

Otworzy sie przegladarka z interaktywna dokumentacja dbt — w tym
**DAG lineage graph** (diagram zaleznosci modeli). To jest SWIETNY
screenshot do LinkedIn i portfolio!

**Ctrl+C** w terminalu zeby zatrzymac serwer dokumentacji.

---

## Etap 4: Uruchom Airflow (Docker)

### 4.1 Upewnij sie ze Docker Desktop jest uruchomiony

Ikona Docker powinna byc w zasobniku systemowym (system tray).

### 4.2 Uruchom caly stack

```bash
cd "C:\Users\annad\Desktop\Projekt 2 śledzenie lotów"
docker-compose up -d
```

Pierwsze uruchomienie moze potrwac 5-10 minut (budowanie obrazu Docker).

### 4.3 Sprawdz status

```bash
docker-compose ps
```

Powinny byc 4 kontenery z statusem "running" lub "healthy":
- `flight_airflow_db` — baza metadata Airflow
- `flight_airflow_init` — inicjalizacja (exited 0 = OK)
- `flight_airflow_webserver` — UI Airflow
- `flight_airflow_scheduler` — scheduler

### 4.4 Otworz Airflow UI

W przegladarce wejdz na: **http://localhost:8080**

Login: `admin`
Haslo: `admin`

Zobaczysz DAG `daily_flight_pipeline`. Mozesz:
1. Wlaczyc go przyciskiem (toggle ON)
2. Uruchomic recznie (przycisk "Play" → "Trigger DAG")
3. Obserwowac przebieg w zakladce "Graph"

### 4.5 Zatrzymanie Airflow

```bash
docker-compose down
```

Dodaj `-v` zeby usunac tez dane: `docker-compose down -v`

---

## Etap 5: Uruchom dashboard Streamlit

### 5.1 Upewnij sie ze masz dane w DuckDB

(Etap 3 musi byc ukonczony — potrzebujesz pliku `data/flight_intelligence.duckdb`)

### 5.2 Uruchom dashboard

```bash
cd "C:\Users\annad\Desktop\Projekt 2 śledzenie lotów"
streamlit run streamlit_app/app.py
```

Otworzy sie przegladarka z interaktywnym dashboardem:
- Mapa lotnisk z ranking PageRank
- Wykresy anomalii wojskowych
- Tabele z metrykami sieci

### 5.3 Zrob screenshoty!

Dashboard to swietny material na:
- README.md na GitHubie
- Post LinkedIn
- Portfolio

---

## Etap 6: Wrzuc na GitHub

### 6.1 Inicjalizacja repozytorium

```bash
cd "C:\Users\annad\Desktop\Projekt 2 śledzenie lotów"
git init
git add .
git commit -m "Initial commit: Flight Intelligence DWH with dbt, Airflow, and Streamlit dashboard"
```

### 6.2 Stworz repo na GitHubie

1. Wejdz na https://github.com/new
2. Nazwa repozytorium: `dbt-flight-intelligence-dwh`
3. Opis: `Data warehouse with military flight anomaly detection and airport network graph analysis. Built with dbt, BigQuery/DuckDB, Airflow, and Streamlit.`
4. Publiczne (Public)
5. **NIE** zaznaczaj "Add a README" (juz mamy)
6. Kliknij "Create repository"

### 6.3 Wypchnij kod

GitHub pokaze Ci instrukcje. Uzyj wariantu "push an existing repository":

```bash
git remote add origin https://github.com/TWOJ_USERNAME/dbt-flight-intelligence-dwh.git
git branch -M main
git push -u origin main
```

### 6.4 Sprawdz na GitHubie

Odswiez strone repozytorium — powinienes zobaczyc README z opisem projektu.

---

## Etap 7: (Opcjonalnie) BigQuery — produkcja chmurowa

Jesli chcesz uruchomic projekt na BigQuery zamiast DuckDB:

### 7.1 Stworz projekt GCP

1. Wejdz na https://console.cloud.google.com
2. Kliknij "Select a project" → "New Project"
3. Nazwa: `flight-intelligence`
4. Kliknij "Create"

### 7.2 Wlacz BigQuery API

1. W menu bocznym: APIs & Services → Library
2. Wyszukaj "BigQuery API"
3. Kliknij "Enable"

### 7.3 Stworz Service Account

1. IAM & Admin → Service Accounts → Create Service Account
2. Nazwa: `dbt-runner`
3. Rola: BigQuery Admin (lub BigQuery Data Editor + BigQuery Job User)
4. Kliknij na konto → Keys → Add Key → Create new key → JSON
5. Pobierz plik JSON

### 7.4 Skonfiguruj projekt

```bash
mkdir credentials
cp sciezka/do/pobranego-klucza.json credentials/gcp-credentials.json
```

Edytuj `.env`:
```
USE_LOCAL_DB=false
GCP_PROJECT_ID=flight-intelligence
BQ_DATASET=flight_intelligence
```

### 7.5 Uruchom dbt na BigQuery

```bash
cd dbt_project
dbt seed --target dev
dbt run --target dev
dbt test --target dev
```

---

## Rozwiazywanie problemow

### "dbt command not found"
Upewnij sie ze venv jest aktywne: `.\venv\Scripts\Activate.ps1`

### "No module named 'duckdb'"
Zainstaluj ponownie: `pip install duckdb dbt-duckdb`

### "Connection refused" na Airflow
Docker moze potrzebowac kilka minut po uruchomieniu. Poczekaj i odswiez strone.

### "Rate limited" przy pobieraniu danych
OpenSky ma limity. Poczekaj 5 minut i sprobuj ponownie.
Jesli czesto dostajesz 429, zmniejsz czestotliwosc w `opensky_client.py` (zwieksz `time.sleep`).

### "Permission denied" na Docker
Uruchom Docker Desktop jako administrator. Na Windows moze tez wymagac WSL2.

### dbt test FAIL
To normalnie oznacza problem z jakoscia danych — sprawdz co konkretnie failuje.
Jesli to `assert_military_ratio_reasonable` — zakresy ICAO24 moga byc zbyt szerokie.

---

## Kolejnosc uruchamiania — sciagawka

### Sciezka szybka (~30 min do dzialajacego dashboardu)

```
1. pip install -r requirements.txt                                ← zaleznosci
2. cp .env.example .env && (uzupelnij OPENSKY login/haslo)        ← konfiguracja
3. cd dbt_project && cp profiles.yml.example profiles.yml         ← profil dbt
4. dbt deps                                                       ← pakiety dbt
5. dbt seed --target local                                        ← dane referencyjne
6. cd .. && python scripts/backfill_30days.py --days 7 --hours-per-day 6  ← dane z API
7. cd dbt_project && dbt run --target local                       ← transformacje
8. dbt test --target local                                        ← testy
9. cd .. && streamlit run streamlit_app/app.py                    ← dashboard!
```

### Sciezka pelna (~3-4h, pelne dane na 30 dni)

```
1-5. (tak samo jak wyzej)
6. cd .. && python scripts/backfill_30days.py --days 30 --hours-per-day 12  ← backfill
7-9. (tak samo jak wyzej)
```

### Po zrobieniu screenshotow — push na GitHub

```
git init && git add . && git commit -m "Initial commit: Flight Intelligence DWH"
```

---

*Instrukcja uruchomienia — Flight Intelligence DWH*
