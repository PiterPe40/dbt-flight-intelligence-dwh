# Flight Intelligence DWH — Przewodnik Edukacyjny

Kompletny opis projektu: każdy plik, każda funkcja, linia po linii.
Dowiesz sie CO robi kod, DLACZEGO tak to zbudowalismy i JAKIE technologie za tym stoja.

---

## Spis tresci

1. [Architektura projektu — duzy obraz](#1-architektura-projektu)
2. [Czym jest Data Warehouse (hurtownia danych)?](#2-czym-jest-data-warehouse)
3. [Czym jest dbt?](#3-czym-jest-dbt)
4. [dags/helpers/opensky_client.py — Klient API](#4-opensky-clientpy)
5. [dags/helpers/data_loader.py — Ladowanie danych](#5-data-loaderpy)
6. [dags/daily_flight_pipeline.py — Airflow DAG](#6-airflow-dag)
7. [Modele dbt: Staging](#7-modele-staging)
8. [Modele dbt: Intermediate](#8-modele-intermediate)
9. [Modele dbt: Marts (SQL)](#9-modele-marts-sql)
10. [Modele dbt: Python Models](#10-modele-python)
11. [Algorytm 1: Military Anomaly Detector (Z-score)](#11-algorytm-z-score)
12. [Algorytm 2: Airport Network Graph (PageRank)](#12-algorytm-pagerank)
13. [Seeds — dane statyczne](#13-seeds)
14. [Testy jakosci danych](#14-testy)
15. [Docker i infrastruktura](#15-docker)
16. [BigQuery vs DuckDB](#16-bigquery-vs-duckdb)
17. [Star Schema — schemat gwiazdy](#17-star-schema)
18. [Slownik technologii](#18-slownik)

---

## 1. Architektura projektu

### Czym rozni sie ten projekt od Crypto ETL?

W projekcie Crypto ETL mielismy prosty pipeline: Extract → Transform → Load → Analyze.
Jeden skrypt Pythona robil wszystko.

Tutaj idziemy POZIOM WYZEJ:

- **dbt** zastepuje recznie pisane skrypty transformacji — zamiast `transform.py` mamy modele SQL
- **BigQuery** (lub DuckDB) zastepuje PostgreSQL — hurtownia danych zamiast bazy transakcyjnej
- **Star Schema** — profesjonalny model danych z tabelami faktow i wymiarow
- **Warstwy dbt** — staging → intermediate → marts — zamiast jednej wielkiej transformacji

### Jak przeplywaja dane?

```
OpenSky Network API (dane lotnicze z calego swiata)
       |
       v
  EXTRACT (dags/helpers/opensky_client.py)
  Pobiera dane o lotach za poprzedni dzien
       |
       v
  LOAD RAW (dags/helpers/data_loader.py)
  Laduje surowe dane do BigQuery/DuckDB (tabela raw_flights)
       |
       v
  dbt STAGING (models/staging/)
  Czysci dane: zmienia nazwy kolumn, typy, filtruje NULL-e
       |
       v
  dbt INTERMEDIATE (models/intermediate/)
  Wzbogaca: laczy z lotniskami, klasyfikuje wojskowe, liczy dystans
       |
       v
  dbt MARTS (models/marts/)
  Tabele gotowe do analizy: wydajnosc tras, siec lotnisk, anomalie
       |
       v
  dbt PYTHON MODELS (models/python/)
  Zaawansowane algorytmy: Z-score anomaly detection, PageRank
       |
       v
  AIRFLOW automatycznie uruchamia caly flow codziennie o 06:00 UTC
```

### Roznica ELT vs ETL

W projekcie Crypto ETL stosowalismy wzorzec **ETL** — Extract, Transform, Load.
Najpierw transformowalismy dane w Pythonie, potem ladowalismy do bazy.

Tutaj stosujemy **ELT** — Extract, Load, Transform:
1. **Extract** — pobierz surowe dane z API
2. **Load** — zaladuj SUROWE dane do hurtowni (BigQuery/DuckDB)
3. **Transform** — przeksztalc dane WEWNATRZ hurtowni za pomoca dbt (SQL)

**Dlaczego ELT?** Bo hurtownie danych (BigQuery, Snowflake, Redshift) sa MEGA
szybkie w przetwarzaniu SQL. Zamiast transformowac w Pythonie na swoim laptopie,
ladujemy surowe dane i transformujemy je silnikiem hurtowni ktory ma tysiace CPU.

To jest STANDARD w nowoczesnej inzynierii danych. Niemal kazda firma, ktora uzywa
dbt, stosuje podejscie ELT.

---

## 2. Czym jest Data Warehouse?

### Baza transakcyjna vs Hurtownia danych

**Baza transakcyjna (PostgreSQL, MySQL):**
- Zoptymalizowana pod ZAPIS — szybko wstawiasz/aktualizujesz pojedyncze wiersze
- Uzywa sie w aplikacjach: rejestracja usera, zapis zamowienia
- Normalizacja — dane rozbite na wiele tabel (zeby nie powielac)

**Hurtownia danych (BigQuery, Snowflake, Redshift):**
- Zoptymalizowana pod ODCZYT — szybko analizujesz miliony/miliardy wierszy
- Uzywa sie do analityki: raporty, dashboardy, ML
- Denormalizacja — dane czesto powielone (zeby szybciej czytac)
- Kolumnowe przechowywanie — czyta tylko potrzebne kolumny

**Analogia:** Baza transakcyjna = kasa w sklepie (szybko obsluguje kolejke).
Hurtownia = magazyn centralny (szybko wyszukuje towary do raportu).

### BigQuery — co to jest?

BigQuery to chmurowa hurtownia danych od Google:
- **Serverless** — nie musisz zarzadzac serwerami, infra, pamiecia
- **Kolumnowa** — czyta tylko kolumny, ktore potrzebujesz (szybkie!)
- **Skalowalna** — od megabajtow do petabajtow
- **Darmowy tier** — 1 TB zapytan/miesiac za darmo (nam w zuplenosci wystarczy)

### DuckDB — lokalna alternatywa

DuckDB to "SQLite dla analityki":
- Dziala lokalnie, bez serwera, bez chmury
- Kolumnowa (jak BigQuery), wiec jest szybka do analityki
- Jeden plik na dysku (`.duckdb`)
- Swietna do developmentu i testowania

W naszym projekcie uzywamy DuckDB do rozwoju lokalnego, a BigQuery do produkcji.

---

## 3. Czym jest dbt?

### dbt = data build tool

dbt to narzedzie, ktore transformuje dane WEWNATRZ hurtowni za pomoca SQL.
Wyobraz sobie to tak:

**Bez dbt:** Piszesz setki plikow SQL, uruchamiasz je rece w odpowiedniej kolejnosci,
nie masz testow, nie masz dokumentacji, nie wiesz co od czego zalezy.

**Z dbt:** Piszesz modele SQL (pliki `.sql`), dbt automatycznie:
- Uruchamia je w odpowiedniej kolejnosci (na podstawie `ref()`)
- Tworzy tabele/widoki w hurtowni
- Uruchamia testy jakosci danych
- Generuje dokumentacje

### Kluczowe koncepty dbt

**Model** = jeden plik SQL, ktory tworzy jedna tabele/widok.
```sql
-- models/marts/mart_airport_network.sql
select * from {{ ref('int_airport_connections') }}
```

**ref()** = referencja do innego modelu. dbt uzywa tego do:
1. Ustalenia kolejnosci uruchamiania (DAG zaleznosciowy)
2. Automatycznego wstawienia poprawnej nazwy tabeli/schematu

**source()** = referencja do tabeli zrodlowej (nie zarzadzanej przez dbt):
```sql
select * from {{ source('opensky', 'raw_flights') }}
```

**Materialization** = jak dbt tworzy wynik modelu:
- `view` — widok SQL (nie zajmuje miejsca, przelicza sie przy kazdym zapytaniu)
- `table` — tabela fizyczna (zajmuje miejsce, szybsza do zapytan)
- `incremental` — aktualizuje tylko nowe dane (efektywne dla duzych tabel)

**Seeds** = male pliki CSV wczytywane do hurtowni jako tabele:
```bash
dbt seed  # wczytuje pliki z katalogu seeds/
```

### Warstwy dbt (Layer Architecture)

Organizacja modeli w warstwy to STANDARD w projektach dbt:

```
Layer 1: STAGING (stg_*)
├── Minimalne czyszczenie
├── Zmiana nazw kolumn (camelCase → snake_case)
├── Konwersja typow
├── Filtracja NULL-i
└── ZERO logiki biznesowej

Layer 2: INTERMEDIATE (int_*)
├── Joiny (laczenie tabel)
├── Wzbogacanie danymi referencyjnymi
├── Obliczenia pochodne
└── Filtracja biznesowa

Layer 3: MARTS (mart_*)
├── Gotowe tabele do analizy
├── Agregacje
├── Metryki biznesowe
└── Dane gotowe do dashboardu
```

**Dlaczego warstwy?**
1. **Debugowanie** — jesli cos nie dziala, wiesz w ktorej warstwie szukac
2. **Reuzywanie** — intermediate mozna uzyc w wielu martach
3. **Czytelnosc** — kazdy model robi JEDNA rzecz
4. **Standard branzy** — tak buduja hurtownie w duzych firmach

---

## 4. opensky_client.py

**Cel:** Pobierac dane z OpenSky Network API — publicznego API z danymi lotniczymi.

### Co to jest OpenSky Network?

OpenSky to projekt naukowy, ktory zbiera dane **ADS-B** (Automatic Dependent
Surveillance-Broadcast) — sygnal, ktory kazdy samolot nadaje ze swoja pozycja,
predkoscia, wysokoscia i identyfikatorem (icao24).

Kazdy samolot ma unikalny kod **ICAO24** — 6-znakowy hex (np. `3c6752`).
Te kody sa przydzielane przez miedzzynarodowa organizacje lotnicza ICAO
i zakresy sa publiczne — co pozwala nam identyfikowac samoloty WOJSKOWE.

### Klasa OpenSkyClient

```python
class OpenSkyClient:
    BASE_URL = "https://opensky-network.org/api"
```

Klasa opakowuje komunikacje z API. Dlaczego klasa a nie zwykle funkcje?
Bo dzielimy wspolny stan: sesje HTTP, dane uwierzytelniajace, URL bazowy.

### Konstruktor — uwierzytelnianie

```python
def __init__(self, username=None, password=None):
    self.session = requests.Session()
    self.auth = (username, password) if username and password else None
```

**Czemu uwierzytelnianie jest wazne?**
- Bez konta: dostep tylko do ostatniej 1 godziny danych
- Z kontem (darmowe): dostep do ostatnich 30 dni
- Rate limit: ~5 req/10s (anonimowo), ~1 req/5s (z kontem)

### _make_request — retry pattern

```python
def _make_request(self, endpoint, params=None):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = self.session.get(url, params=params, auth=self.auth, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Rate limited
                wait_time = 30 * (attempt + 1)
                time.sleep(wait_time)
```

Ten sam wzorzec co w Crypto ETL, ale z dluzszymi opoznieniami bo OpenSky
jest bardziej restrykcyjne niz CoinGecko. Kluczowe roznice:

- `timeout=60` zamiast 30 — API lotnicze jest wolniejsze (duzo danych)
- `auth=self.auth` — Basic HTTP Authentication (login:haslo)
- `30 * (attempt + 1)` — bardziej konserwatywny backoff (30s, 60s, 90s)

### fetch_daily_flights — obsluga limitu 2h

```python
def fetch_daily_flights(self, date):
    chunk_hours = 2
    chunks_per_day = 24 // chunk_hours  # = 12 chunkow

    for chunk_idx in range(chunks_per_day):
        chunk_start = day_start + timedelta(hours=chunk_idx * chunk_hours)
        chunk_end = chunk_start + timedelta(hours=chunk_hours)
        flights = self.fetch_flights(begin=begin_ts, end=end_ts)
        all_flights.extend(flights)
        time.sleep(5)  # szanuj rate limit
```

**WAZNE:** API `/flights/all` akceptuje maksymalnie 2-godzinne okna czasowe.
Zeby pobrac caly dzien, dzielimy go na 12 chunkow po 2 godziny.

To czesty problem przy pracy z API — musisz dostosowac sie do ograniczen
providera. Rozwiazanie: **chunking** (podzial na kawalki).

---

## 5. data_loader.py

**Cel:** Zaladowac surowe dane do hurtowni (BigQuery lub DuckDB).

### Wzorzec Strategy — dwa backendy

```python
class DataLoader:
    def __init__(self):
        self.use_local = os.getenv("USE_LOCAL_DB", "true").lower() == "true"
        if self.use_local:
            self._init_duckdb()
        else:
            self._init_bigquery()
```

To jest wzorzec **Strategy** — jeden interfejs (`load_flights`), dwie implementacje.
Reszta kodu nie wie i nie musi wiedziec, czy dane leca do BigQuery czy DuckDB.

**Dlaczego dwa backendy?**
- DuckDB: szybki start, zero konfiguracji, darmowe, lokalne
- BigQuery: produkcja, skalowalne, standardowe w branzy

### DuckDB — tworzenie tabel

```python
self.conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_flights (
        icao24 VARCHAR,
        firstSeen BIGINT,
        ...
    )
""")
```

Schemat tabeli raw jest 1:1 z formatem JSON z API OpenSky.
Celowo NIE transformujemy danych na tym etapie — to zadanie dbt.

**IF NOT EXISTS** — bezpieczne do wielokrotnego uruchomienia (idempotentnosc).

### BigQuery — WRITE_APPEND

```python
job_config = LoadJobConfig(
    write_disposition=WriteDisposition.WRITE_APPEND,
    autodetect=True,
)
```

**WRITE_APPEND** — dodaje nowe wiersze do istniejacych.
Deduplikacja jest robiona pozniej w warstwie staging dbt.
To STANDARD w podejsciu ELT — laduj surowe, czysc pozniej.

**autodetect=True** — BigQuery automatycznie wykrywa typy kolumn z DataFrame.

---

## 6. Airflow DAG

**Cel:** Automatyczne uruchamianie calego pipeline'u codziennie o 06:00 UTC.

### Roznice vs Crypto ETL DAG

W Crypto ETL mielismy 3 proste taski z PythonOperator.
Tutaj mamy 7 taskow, w tym BashOperator do uruchamiania dbt:

```
fetch_opensky_flights → load_to_raw → dbt_staging → dbt_intermediate
                                                         |
                                                    ┌────┴────┐
                                                    v         v
                                              dbt_marts  dbt_python
                                                    |         |
                                                    └────┬────┘
                                                         v
                                                     dbt_test
                                                         v
                                              check_anomalies_and_alert
```

### BashOperator — uruchamianie dbt

```python
dbt_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_PROFILES_DIR}",
)
```

**Dlaczego BashOperator a nie PythonOperator?**
Bo dbt to narzedzie CLI (command-line interface). Uruchamiasz go komenda `dbt run`.
`--select staging` mowi dbt: "uruchom TYLKO modele z folderu staging".

To pozwala na kontrole granularnosci — mozesz uruchomic kazda warstwe osobno.

### Rownolegle taski

```python
dbt_intermediate >> [dbt_marts, dbt_python]
```

`[dbt_marts, dbt_python]` — Airflow uruchomi te dwa taski ROWNOLEGLE.
SQL marts i Python models sa od siebie niezalezne (oba zaleza od intermediate),
wiec nie musza czekac na siebie nawzajem. To OPTYMALIZACJA — pipeline konczy sie szybciej.

### Task 7: Alert na anomalie

```python
def check_anomalies_and_alert(**context):
    anomalies = conn.execute("""
        SELECT ... FROM marts.mart_military_anomaly_scores
        WHERE anomaly_level IN ('CRITICAL', 'HIGH')
    """).fetchdf()
```

Po uruchomieniu testow sprawdzamy wyniki anomaly detectora.
Jesli sa alerty CRITICAL lub HIGH, logujemy ostrzezenie.
W wersji produkcyjnej wyslalibysmy powiadomienie na Slack lub email.

---

## 7. Modele Staging

### stg_opensky__flights.sql

```sql
{{ dbt_utils.generate_surrogate_key(['icao24', 'firstSeen', 'estDepartureAirport']) }}
    as flight_id,
```

**Surrogate key** — sztuczny klucz glowny stworzony z hasha kilku kolumn.
OpenSky API nie ma naturalnego ID lotu, wiec tworzymy go sami z:
- `icao24` — identyfikator samolotu
- `firstSeen` — czas odlotu
- `estDepartureAirport` — lotnisko startowe

`dbt_utils.generate_surrogate_key()` — makro z pakietu dbt-utils,
generuje hash MD5 z podanych kolumn. Wynik: np. `a1b2c3d4e5f6...`

### Konwencja nazewnicza

```
stg_opensky__flights.sql
│   │         │
│   │         └── nazwa encji (tabela/obiekt)
│   └────────── nazwa zrodla (system zrodlowy)
└────────────── prefiks warstwy (staging)
```

Dwa podkreslniki (`__`) oddzielaja zrodlo od encji. To KONWENCJA dbt —
ulatwia nawigacje w duzych projektach z wieloma zrodlami.

### Czyszczenie danych

```sql
lower(trim(icao24))                     as icao24,
upper(trim(estDepartureAirport))        as origin_airport_id,
```

- `trim()` — usuwa biale znaki z poczatku/konca
- `lower()` / `upper()` — standaryzacja wielkosci liter
- ICAO24 → lowercase (standard hex), Lotniska → uppercase (standard ICAO)

```sql
where
    icao24 is not null
    and lastSeen > firstSeen  -- przylot musi byc PO odlocie
```

Filtrujemy oczywiscie bledne dane: brak identyfikatora lub ujemny czas lotu.
Te filtry to **data quality gates** — bramy jakosci na wejsciu do pipeline'u.

---

## 8. Modele Intermediate

### int_flights_enriched.sql — serce projektu

Ten model LACZY trzy zrodla:
1. `stg_opensky__flights` — dane o lotach
2. `stg_opensky__airports` — dane o lotniskach (seed)
3. `stg_opensky__military_ranges` — zakresy ICAO24 wojska (seed)

### Haversine — obliczanie dystansu

```sql
6371.0 * 2 * asin(sqrt(
    power(sin(radians(dest.latitude - orig.latitude) / 2), 2)
    + cos(radians(orig.latitude))
      * cos(radians(dest.latitude))
      * power(sin(radians(dest.longitude - orig.longitude) / 2), 2)
))
```

**Haversine** to wzor na odleglosc miedzy dwoma punktami na kuli ziemskiej.
Uwzglednia krzywizne Ziemi (w odroznieniu od prostej odleglosci euklidesowej).

- `6371.0` — promien Ziemi w km
- `radians()` — zamienia stopnie na radiany (wymagane przez funkcje trygonometryczne)
- Wynik: odleglosc w kilometrach "po luku wielkiego kola"

**Przyklad:** Warszawa (52.17N, 20.97E) → Frankfurt (50.04N, 8.56E) = ~892 km

### Klasyfikacja wojskowa

```sql
left join military_ranges mr
    on f.icao24 >= mr.range_start
    and f.icao24 <= mr.range_end
```

**Range join** — sprawdzamy czy ICAO24 samolotu MIESCI SIE w zakresie
przydzielonym wojsku danego kraju. Na przyklad:

- ICAO24 = `ae1234` → miesci sie w `ae0000-aeffff` → US Air Force
- ICAO24 = `448123` → miesci sie w `448000-44ffff` → Polskie Sily Powietrzne

Porownanie hex stringow dziala poprawnie bo ICAO24 to kody szesnastkowe
o stalej dlugosci (6 znakow).

### int_military_flights.sql — klasyfikacja po callsign

```sql
case
    when callsign like 'RCH%'   then 'TRANSPORT'      -- US Air Mobility Command
    when callsign like 'GAF%'   then 'TRANSPORT'      -- German Air Force
    when callsign like 'FORTE%' then 'SURVEILLANCE'    -- Global Hawk drone
    else 'UNKNOWN'
end as military_category
```

**Callsign** (znak wywolawczy) to identyfikator lotu widoczny dla kontroli ruchu.
Samoloty wojskowe uzywaja charakterystycznych prefiksow:

- `RCH` = Reach — US Air Mobility Command (transportowce C-17, C-5)
- `FORTE` = Global Hawk — dron rozpoznawczy US Air Force
- `GAF` = German Air Force

To jest technika **OSINT** (Open Source Intelligence) — wyciaganie informacji
wywiadowczych z publiczinie dostepnych danych. Popularna w srodowisku analitykow.

### int_airport_connections.sql — lista krawedzi grafu

```sql
select
    origin_airport_id,
    destination_airport_id,
    count(*) as flight_count,  -- waga krawedzi
    ...
from int_flights_enriched
group by 1, 2
```

Ten model tworzy **liste krawedzi** (edge list) grafu polaczen lotniczych:
- Kazda para lotnisk (origin → destination) = jedna krawedz
- `flight_count` = waga krawedzi (ile lotow na tej trasie)

To jest INPUT dla algorytmu PageRank w Python modelu.

---

## 9. Modele Marts (SQL)

### mart_flight_performance.sql

Agreguje loty na poziomie TRAS (origin → destination):

```sql
avg(flight_duration_min) as avg_duration_min,
round(total_flights * 1.0 / nullif(active_days, 0), 2) as flights_per_day,
round(avg_duration_min / nullif(avg_distance_km, 0), 4) as min_per_km,
```

- `flights_per_day` — czestotliwosc polaczenia
- `min_per_km` — wydajnosc trasy (czas na kilometr)

**nullif(x, 0)** — zamienia 0 na NULL zeby uniknac dzielenia przez zero.
To WAZNA technika obronna w SQL!

### mart_military_activity.sql — window functions

```sql
avg(military_flight_count) over (
    partition by country_code
    order by date_id
    rows between 6 preceding and current row
) as rolling_7d_avg,
```

**Window function** (funkcja okna) — oblicza agregat BEZ redukowania wierszy.
W odroznieniu od `GROUP BY`, zachowuje kazdy wiersz i dodaje obliczone pole.

- `partition by country_code` — osobna srednia dla kazdego kraju
- `order by date_id` — chronologicznie
- `rows between 6 preceding and current row` — 7-dniowe okno (dzis + 6 poprzednich)

**Rolling average** to wazna technika wygładzania szumu w danych.
Zamiast patrzec na pojedyncze dni, widzisz TREND.

```sql
lag(military_flight_count, 7) over (...) as prev_week_count
```

**lag()** — daje wartosc z N wierszy WSTECZ.
`lag(x, 7)` = wartosc sprzed 7 dni (ten sam dzien tygodnia, tydzien wczesniej).
Przydatne do porownania tydzien-do-tygodnia.

---

## 10. Modele Python (dbt Python Models)

### Czym sa Python models w dbt?

Od dbt 1.3 mozesz pisac modele nie tylko w SQL, ale takze w Pythonie.
Uzywasz ich gdy SQL nie wystarczy — np. do algorytmow grafowych (NetworkX)
lub statystycznych (scipy).

```python
def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("int_daily_military_counts").to_pandas()
    # ... przetwarzanie w pandas/numpy ...
    return result_df
```

**Kluczowe roznice vs zwykly Python:**
- Funkcja MUSI nazywac sie `model(dbt, session)`
- Uzywasz `dbt.ref()` zamiast SQL SELECT
- `.to_pandas()` zamienia tabelę SQL na DataFrame
- Zwracasz DataFrame — dbt automatycznie tworzy z niego tabele

**Kiedy uzyc Python model?**
- Algorytmy grafowe (NetworkX)
- Machine Learning (scikit-learn)
- Skomplikowana statystyka (scipy)
- Przetwarzanie, ktore jest uciazliwe lub niemozliwe w SQL

---

## 11. Algorytm 1: Military Anomaly Detector (Z-score)

### Co to jest Z-score?

Z-score mowi "ile odchylen standardowych dana wartosc jest od sredniej".
To TEN SAM algorytm co w projekcie Crypto ETL, ale zastosowany do danych lotniczych.

**Wzor:** `z = (x - mu) / sigma`
- x = dzisiejsza liczba lotow wojskowych
- mu = srednia historyczna (baseline)
- sigma = odchylenie standardowe

### Dlaczego stratyfikujemy po dniu tygodnia?

```python
baseline = df.groupby(["country_code", "day_of_week"]).agg(
    baseline_mean=("military_flight_count", "mean"),
    baseline_std=("military_flight_count", "std"),
)
```

Nie porownujemy poniedzialku ze srednia z calego tygodnia!
Ruch wojskowy ma wzorzec tygodniowy — mniej lotow w weekendy.
Gdybysmy nie uwzglednili dnia tygodnia, kazda sobota bylaby "anomalia"
(bo ma mniej lotow niz srednia wlaczajaca dni robocze).

Dlatego liczymy OSOBNA srednia dla kazdej kombinacji kraj + dzien_tygodnia:
- Polska, poniedzialek: srednia = 15 lotow, std = 3
- Polska, sobota: srednia = 5 lotow, std = 2

### Poziomy anomalii

```python
df["anomaly_level"] = pd.cut(
    df["z_score"],
    bins=[-inf, 1.5, 2.5, 3.5, inf],
    labels=["NORMAL", "ELEVATED", "HIGH", "CRITICAL"]
)
```

| Z-score | Poziom | Co to znaczy |
|---|---|---|
| < 1.5 | NORMAL | Typowa aktywnosc |
| 1.5 - 2.5 | ELEVATED | Podwyzszona aktywnosc — warto obserwowac |
| 2.5 - 3.5 | HIGH | Znaczaco powyżej normy — prawdopodobne cwiczenia |
| > 3.5 | CRITICAL | Ekstremalna anomalia — moze wskazywac na operacje |

**Przyklad:** Jezeli srednia dla Polski w poniedzialki to 15 lotow (std=3),
a dzis zaobserwowalismy 28 lotow:
```
z = (28 - 15) / 3 = 4.33 → CRITICAL
```

### pd.cut() — dyskretyzacja

`pd.cut()` zamienia ciagla wartosc (Z-score) na kategorie (NORMAL/ELEVATED/...).
To jak dzielenie studentow na grupy ocen: 0-50% = F, 50-70% = C, itd.

`bins` definiuje granice przedzialow, `labels` nazwy kategorii.

---

## 12. Algorytm 2: Airport Network Graph (PageRank)

### Co to jest graf?

**Graf** to struktura matematyczna skladajaca sie z:
- **Wezlow** (nodes/vertices) — u nas: lotniska
- **Krawedzi** (edges) — u nas: trasy lotnicze

**Skierowany** (directed) — krawedz ma kierunek: Warszawa → Frankfurt =/= Frankfurt → Warszawa
**Wazony** (weighted) — krawedz ma wage: 500 lotow vs 5 lotow

### NetworkX — biblioteka grafowa

```python
import networkx as nx

G = nx.DiGraph()  # Directed Graph
for _, row in edges.iterrows():
    G.add_edge(
        row["origin_airport_id"],
        row["destination_airport_id"],
        weight=int(row["flight_count"]),
    )
```

NetworkX to standardowa biblioteka Pythona do analizy grafow.
`DiGraph()` tworzy graf skierowany — odroznia A→B od B→A.

### PageRank — algorytm Google'a

```python
pagerank = nx.pagerank(G, weight="weight", alpha=0.85)
```

PageRank to algorytm wymyslony przez zalozyciela Google (Larry Page).
Oryginalnie ranking stron internetowych — tutaj adaptujemy go do lotnisk.

**Idea:** Lotnisko jest wazne nie dlatego ze ma DUZO polaczen,
ale dlatego ze jest polaczone z INNYMI WAZNYMI lotniskami.

**alpha=0.85** — prawdopodobienstwo, ze "losowy podrozny" poleci dalej
zamiast wysiesc (damping factor). Standard = 0.85.

**Wynik:** Wyniki PageRank sumuja sie do 1.0. Np.:
- Frankfurt = 0.04 (4% — mega hub)
- London Heathrow = 0.035
- Male lokalne lotnisko = 0.001

### Betweenness Centrality

```python
betweenness = nx.betweenness_centrality(G, weight="weight", k=100)
```

**Betweenness** mierzy jak czesto lotnisko lezy na NAJKROTSZEJ SCIEZCE
miedzy dwoma innymi lotniskami.

Wysokie betweenness = krytyczny punkt przesiadkowy.
Gdyby to lotnisko zniknelo, wiele polaczen zostaloby przerwanych.

**k=100** — aproksymacja (nie liczymy pelnej macierzy, tylko probke 100 wezlow).
Dla duzych grafow pelne obliczenie byloby zbyt wolne.

### Clustering Coefficient

```python
clustering = nx.clustering(G_undirected, weight="weight")
```

**Clustering** mierzy jak bardzo sasiedzi lotniska sa ze soba polaczeni.
Wysoki clustering = lokalna "klika" lotnisk (np. europejskie huby nawzajem).
Niski clustering = lotnisko laczone z wieloma odleglymi, niepowiazanymi lotniskami.

---

## 13. Seeds — dane statyczne

### Czym sa seeds w dbt?

Seeds to male pliki CSV, ktore dbt wczytuje do hurtowni jako tabele.
Uzywasz ich do danych referencyjnych, ktore rzadko sie zmieniaja.

```bash
dbt seed  # wczytuje WSZYSTKIE pliki CSV z katalogu seeds/
```

### military_icao24_ranges.csv

```csv
range_start,range_end,country_code,military_branch
3b0000,3bffff,DE,Bundeswehr
448000,44ffff,PL,Air Force
ae0000,aeffff,US,US Air Force
```

Zakresy ICAO24 dla wojsk roznych krajow. Te dane sa PUBLICZNE —
ICAO (International Civil Aviation Organization) przydziela zakresy adresow
poszczegolnym krajom, a te z kolei przydzielaja czesci swojego zakresu wojsku.

### airport_metadata.csv

Statyczne dane o lotniskach: nazwy, miasta, wspolrzedne GPS, strefy czasowe.
Potrzebne do:
- Wzbogacenia danych o lotach (ktore miasto, ktory kraj)
- Obliczenia dystansu (haversine wymaga wspolrzednych)
- Dashboardu (mapa lotnisk)

---

## 13b. Backfill — skad baseline ma dane?

### Problem "zimnego startu"

Algorytm Z-score liczy anomalie POROWNUJAC dzisiejsze dane do srednich historycznych.
Ale jesli dopiero zaczynasz projekt — nie masz historii! To sie nazywa **cold start problem**.

Rozwiazanie: **backfill** — jednorazowe pobranie danych historycznych.

### Jak dziala backfill_30days.py?

```python
# Petla po dniach (np. 30 dni wstecz)
for day_idx, target_date in enumerate(remaining_dates):
    # Kazdy dzien dzielimy na chunki 2-godzinne (limit API)
    for chunk_idx in active_chunks:
        flights = client.fetch_flights(begin=begin_ts, end=end_ts)
        day_flights.extend(flights)
        time.sleep(7)  # pauza miedzy requestami

    # Po kazdym dniu zapisujemy progres
    loader.load_flights(day_flights, date_str)
    save_progress(progress)  # mozna przerwac i wznowic!
```

### Kluczowe cechy backfillu

**Resume (wznowienie):** Skrypt zapisuje postep do pliku `.backfill_progress.json`.
Jesli przerwiesz (Ctrl+C), nastepne uruchomienie pominie juz pobrane dni.
To wzorzec **checkpoint/resume** — standard w dlugich procesach batch.

**Konfigurowalnosc:** Mozesz kontrolowac ile danych pobierasz:
- `--days 7` → szybki start, 7 dni (ok. 20-30 min)
- `--hours-per-day 6` → probkujesz 6h z kazdego dnia (mniej danych, szybciej)
- `--days 30` → pelny baseline (2-4h)

**Rate limit management:** Pauzy 7s miedzy chunkami + 15s miedzy dniami.
Dostosowane do limitow OpenSky (zbyt czeste zapytania → ban na 1h).

### Po co rozne opcje --hours-per-day?

Pelny dzien to 12 chunkow po 2h = 12 requestow = ~2 minuty.
30 dni × 2 min = ~60 minut samych requestow (+ pauzy = ~3-4h).

Jesli potrzebujesz szybko cos pokazac na LinkedIn, 7 dni × 6h/dzien
daje wystarczajaco dane do baseline i ladne wykresy na dashboardzie.

### Dlaczego NIE ladujemy gotowych danych CSV?

Mozna by dolac do repo CSV z gotowymi danymi — ale:
1. Pliki bylyby OGROMNE (tysiace lotow dziennie × 30 dni)
2. Git nie jest do duzych plikow (repozytorium rozrosloby sie do setek MB)
3. Dane by sie dezaktualizowaly
4. Na rozmowie rekrutacyjnej CHCESZ powiedziec "moj pipeline pobiera dane live z API"

---

## 14. Testy jakosci danych

### Testy wbudowane dbt

```yaml
# _stg_schema.yml
columns:
  - name: flight_id
    tests:
      - unique        # kazdy lot ma unikalny ID
      - not_null       # zadnych pustych wartosci
```

dbt ma wbudowane testy: `unique`, `not_null`, `accepted_values`, `relationships`.
Definiujesz je w plikach `.yml` i uruchamiasz jednym poleceniem:

```bash
dbt test  # uruchom WSZYSTKIE testy
```

Jesli test FAIL — dbt zwraca blad. W naszym DAG Airflow, task `dbt_test`
sie nie uda i caly pipeline zostanie oznaczony jako FAILED.

### Testy z pakietow

```yaml
- dbt_utils.accepted_range:
    min_value: 0
    max_value: 1440  # max 24h
```

`dbt_utils.accepted_range` — test z pakietu dbt-utils.
Sprawdza czy wartosci mieszcza sie w podanym zakresie.
Czas lotu musi byc >= 0 i <= 1440 minut (24 godziny).

### Custom SQL tests

```sql
-- tests/assert_no_negative_duration.sql
select flight_id, flight_duration_min
from {{ ref('int_flights_enriched') }}
where flight_duration_min < 0
```

**Logika:** Jesli zapytanie zwroci JAKIEKOLWIEK wiersze — test FAIL.
Jesli zwroci 0 wierszy — test PASS.

To daje pelna kontrole — mozesz napisac DOWOLNY test jako zapytanie SQL.

### assert_military_ratio_reasonable

```sql
select * from stats where military_pct > 20.0
```

Samoloty wojskowe to typowo < 10% calego ruchu lotniczego.
Jesli nasz detector mowi, ze >20% lotow jest wojskowych,
to prawdopodobnie nasze zakresy ICAO24 sa zbyt szerokie (false positives).

Ten test chroni nas przed BLEDAMI W DANYCH REFERENCYJNYCH (seeds).

---

## 15. Docker i infrastruktura

### docker-compose.yml — co sie uruchamia?

```yaml
services:
  airflow-db:       # PostgreSQL — metadata Airflow (NIE nasze dane!)
  airflow-init:     # Migracja bazy + tworzenie usera admin
  airflow-webserver: # UI Airflow na http://localhost:8080
  airflow-scheduler: # Scheduler — uruchamia DAG-i wg harmonogramu
```

**WAZNE:** PostgreSQL w docker-compose to baza Airflow, NIE nasza hurtownia.
Nasze dane lotnicze leca do BigQuery (chmura) lub DuckDB (plik lokalny).

### Dockerfile — obraz Airflow

```dockerfile
FROM apache/airflow:2.8.1-python3.11
```

Startujemy od oficjalnego obrazu Airflow — ma juz zainstalowane:
- Apache Airflow
- Python 3.11
- Podstawowe zależnosci

Doinstalowujemy nasze pakiety (dbt, networkx, requests, itd.)
i kopiujemy nasz kod.

### x-airflow-common — YAML anchor

```yaml
x-airflow-common: &airflow-common
  build:
    context: .
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    ...
```

**YAML anchor (`&airflow-common`)** — definiuje blok, ktory mozna REUZYWAC.
Zamiast kopiowac te same ustawienia do kazdego serwisu, definiujemy raz
i referencujemy przez `<<: *airflow-common`.

To jak zmienna w programowaniu — DRY (Don't Repeat Yourself).

---

## 16. BigQuery vs DuckDB

### Kiedy ktore?

| Cecha | DuckDB | BigQuery |
|---|---|---|
| **Koszt** | Darmowe | Free tier (1TB/mies.) |
| **Setup** | Zero | Konto GCP + Service Account |
| **Lokalizacja** | Twoj komputer | Chmura Google |
| **Skala** | Do ~100GB | Petabajty |
| **Szybkosc small data** | Bardzo szybkie | Wolniejsze (overhead chmury) |
| **Szybkosc big data** | Ograniczone RAM | Nieograniczone |
| **dbt support** | dbt-duckdb | dbt-bigquery |

### Jak przelaczac?

```bash
# DuckDB (default)
dbt run --target local

# BigQuery
dbt run --target dev
```

W `profiles.yml` masz dwa targety: `local` (DuckDB) i `dev` (BigQuery).
Parametr `--target` mowi dbt, ktory backend uzyc.

W Airflow przelaczasz przez zmienna srodowiskowa:
```bash
USE_LOCAL_DB=true   # DuckDB
USE_LOCAL_DB=false  # BigQuery
```

---

## 17. Star Schema — schemat gwiazdy

### Co to jest?

Star schema to sposob organizacji tabel w hurtowni danych.
W srodku jest **tabela faktow** (fact table) otoczona **tabelami wymiarow** (dimension tables).

```
              dim_airports
                  |
dim_aircraft -- fact_flights -- dim_airlines
                  |
              dim_date
```

### Tabela faktow (fact_flights)

Zawiera ZDARZENIA — kazdy wiersz to jeden lot:
- ID lotu, czas odlotu/przylotu, dystans, czas trwania
- KLUCZE OBCE (FK) do tabel wymiarow

### Tabele wymiarow

Zawieraja KONTEKST — opisuja "kto, co, gdzie, kiedy":
- `dim_airports` — lotnisko (nazwa, miasto, kraj, wspolrzedne)
- `dim_aircraft` — samolot (typ, producent, czy wojskowy)
- `dim_date` — data (rok, miesiac, dzien tygodnia, weekend, sezon)

### Dlaczego star schema?

1. **Szybkosc zapytan** — proste JOINy, optymalne dla kolumnowych baz danych
2. **Czytelnosc** — intuicyjne, latwe do zrozumienia
3. **Standard branzy** — KAZDY data engineer musi to znac
4. **BI-friendly** — narzedzia jak Looker, Tableau, Power BI kochaja star schema

W naszym projekcie star schema jest implicitly zaimplementowany
w warstwach intermediate/marts — choc nie mamy explicite tabel
`dim_*` i `fact_*`, struktura jest analogiczna.

---

## 18. Slownik technologii

| Technologia | Co to jest | Dlaczego jej uzywamy |
|---|---|---|
| **dbt** | Data Build Tool — transformacje SQL w hurtowni | Standard branzy, testowanie, dokumentacja, dependency management |
| **BigQuery** | Chmurowa hurtownia danych Google | Serverless, skalowalna, darmowy tier |
| **DuckDB** | Lokalna analityczna baza danych | Szybki development, zero konfiguracji |
| **Apache Airflow** | Orkiestrator workflow | Automatyzacja, monitoring, retry, zaleznosciowy DAG |
| **NetworkX** | Biblioteka grafowa Python | PageRank, betweenness, analiza sieci |
| **OpenSky Network** | API danych lotniczych (ADS-B) | Darmowe, publiczne, globalne dane o lotach |
| **Docker** | Konteneryzacja | Identyczne srodowisko wszedzie |
| **pandas** | Analiza danych tabelarycznych | Przetwarzanie w Python models |
| **numpy/scipy** | Obliczenia numeryczne | Z-score, statystyka |
| **Star Schema** | Model danych (fakt + wymiary) | Standard w hurtowniach danych |
| **ICAO24** | Szesnastkowy identyfikator samolotu | Identyfikacja wojskowych statkow powietrznych |
| **Haversine** | Wzor na odleglosc na kuli | Obliczanie dystansu lotow |

---

## Podsumowanie kluczowych wzorcow

1. **ELT Pattern** — Load surowe → Transform w hurtowni (vs ETL w Crypto projekcie)
2. **dbt Layer Architecture** — staging → intermediate → marts (separacja odpowiedzialnosci)
3. **Strategy Pattern** — jeden interfejs, dwa backendy (BigQuery/DuckDB)
4. **Star Schema** — fakt + wymiary (standard hurtowni danych)
5. **Z-score Anomaly Detection** — ten sam algorytm co w Crypto ETL, nowy kontekst
6. **Graph Analysis** — PageRank, betweenness centrality (NetworkX)
7. **OSINT** — wyciaganie informacji wywiadowczych z publicznych danych
8. **Chunking** — podzial API requestow na mniejsze czesci
9. **Idempotency** — bezpieczne wielokrotne uruchomienie
10. **Data Quality Gates** — testy dbt jako brama jakosci

---

## Co nowego nauczyles sie w tym projekcie (vs Crypto ETL)?

| Crypto ETL | Flight Intelligence |
|---|---|
| ETL (transform w Pythonie) | ELT (transform w hurtowni przez dbt) |
| PostgreSQL (baza transakcyjna) | BigQuery/DuckDB (hurtownia danych) |
| Recznie pisany SQL | dbt models z testami i dokumentacja |
| Jeden wielki pipeline.py | Warstwy: staging → intermediate → marts |
| Proste anomaly detection | Stratyfikowany Z-score + analiza grafowa |
| Brak danych referencyjnych | Seeds (CSV → tabele w hurtowni) |
| PythonOperator w Airflow | BashOperator + PythonOperator |
| Jeden backend | Strategy pattern: 2 backendy |

---

*Przewodnik edukacyjny — Flight Intelligence DWH*
*Wygenerowano z pomoca Claude — kwiecien 2026*
