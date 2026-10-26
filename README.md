# Building a Relational Movie Data Pipeline

## 1. Project Overview & Architecture

This project implements a robust, production-grade Python ETL (Extract, Transform, Load) pipeline that ingests, cleans, normalizes, and enriches movie and rating data from the **MovieLens Small dataset** using external metadata from the **OMDb API**.

The pipeline loads structured relational tables into **SQLite**, enforces strict relational constraints and foreign keys, performs atomic transactions, and provides SQL queries to answer core business analytics on movie ratings, genres, directors, and release years.

### Pipeline Architecture

```text
[ Raw MovieLens Data ]
  ├── movies.csv  ───┐
  ├── ratings.csv ───┼──> [ etl.py ] ──> [ SQLite: movie_data.db ]
  └── links.csv   ───┘                          ▲
                                                │ (Idempotent updates)
[ External Metadata ]                           │
  └── OMDb API   ───────> [ enrich_movies.py ] ─┘
                                (Batching, Exponential Backoff,
                                 IMDb ID Fallback via links.csv,
                                 Single-Instance Lock & Redaction)
```

### Key Objectives Achieved

1. **Deterministic Relational Schema**: 7 normalized tables (`movies`, `ratings`, `genres`, `movie_genres`, `movie_details`, `directors`, `movie_directors`) with primary keys, foreign keys with cascade deletions, domain check constraints, and performance indexes.
2. **Idempotent Source Ingestion**: `etl.py` transforms and loads CSV files reproducibly without duplicating records or corrupting existing enrichment.
3. **Resilient API Enrichment Pipeline**: `enrich_movies.py` fetches director, plot, IMDb ID, and box office metadata with:
   - Configurable per-run limits (`MAX_MOVIES_PER_RUN`, default 500) and batch processing (100 per batch).
   - Authoritative fallback to MovieLens `links.csv` IMDb IDs to overcome title/subtitle variations and catalog stubs.
   - Bounded retries with exponential backoff on transient errors (HTTP 429, 5xx, timeouts).
   - Circuit breakers for fatal stops (HTTP 401 Unauthorized, daily quota exhaustion).
   - Single-instance concurrency guard (`enrichment.lock`).
   - Strict log redaction preventing credential leakage.
4. **Verified Analytical SQL Queries**: Answers all 4 analytical assignment questions reproducibly.

---

## 2. Technology Stack & Prerequisites

- **Python**: Python 3.10+ (tested on Python 3.14).
- **Relational Database**: SQLite 3 (with `PRAGMA foreign_keys = ON`).
- **Core Libraries**: `pandas` (CSV transformation), `requests` (HTTP API client), `python-dotenv` (secure configuration), `pytest` (offline test framework).

---

## 3. Project File Structure

```text
TSWORKS/
├── data/
│   └── raw/
│       └── ml-latest-small/
│           ├── movies.csv          # MovieLens movie catalog
│           ├── ratings.csv         # 100k user ratings
│           ├── links.csv           # Ground-truth IMDb/TMDb mappings
│           └── tags.csv            # User tags
├── logs/
│   ├── etl.log                     # Core ETL ingestion log
│   └── enrichment.log              # Sanitized OMDb enrichment audit log
├── .env                            # Environment variables (API key - gitignored)
├── .gitignore                      # Version control exclusion rules
├── requirements.txt                # Pinned Python package dependencies
├── schema.sql                      # DDL schema definition for SQLite
├── queries.sql                     # 4 required analytical SQL queries
├── etl.py                          # Ingestion and normalization pipeline
├── enrich_movies.py                # External API metadata enrichment pipeline
├── verify_database.py              # Verification script for database counts & FKs
├── run_required_queries.py         # Runner script for queries.sql
├── query_output.txt                # Execution output of queries.sql
├── test_schema.py                  # DDL constraints and relationships test
├── test_idempotency.py             # Idempotency test for core ETL
├── test_enrichment_retries.py      # Network retry and HTTP status tests
├── test_omdb.py                    # Mocked OMDb payload parsing test
├── test_pipeline_comprehensive.py  # 16 comprehensive integration test cases
└── README.md                       # Complete project documentation
```

---

## 4. Database Design & Relationships

The relational database is normalized to 3NF where appropriate:

| Table | Description | Primary Key | Foreign Keys / Constraints |
|---|---|---|---|
| `movies` | Core MovieLens movie catalog | `movie_id` | `release_year BETWEEN 1888 AND 2100` |
| `ratings` | User ratings on movies | `(user_id, movie_id)` | FK `movie_id -> movies(movie_id)` ON DELETE CASCADE; `rating BETWEEN 0.5 AND 5.0` |
| `genres` | Distinct genre catalog | `genre_id` (auto) | `genre_name UNIQUE COLLATE NOCASE` |
| `movie_genres` | Many-to-many movie-genre links | `(movie_id, genre_id)` | FKs to `movies`, `genres` |
| `movie_details` | OMDb metadata & pipeline status | `movie_id` | FK `movie_id -> movies(movie_id)`; `api_status IN ('pending','success','not_found','error')` |
| `directors` | Distinct director names | `director_id` (auto) | `director_name UNIQUE COLLATE NOCASE` |
| `movie_directors` | Many-to-many movie-director links | `(movie_id, director_id)` | FKs to `movies`, `directors` |

---

## Assumptions

The pipeline design and data modeling reflect the following verified assumptions from the problem domain, source datasets, and assignment specifications:

1. **MovieLens Ratings Scale & Integrity**:
   - Ratings are numeric values bounded between `0.5` and `5.0` inclusive in half-star increments (multiples of `0.5`), enforced at both DataFrame ingestion and SQLite schema levels (`CHECK (rating >= 0.5 AND rating <= 5.0)` and `CHECK (rating * 2 = CAST(rating * 2 AS INTEGER))`).
   - Each user can have at most one rating per movie, modeled by the composite primary key `(user_id, movie_id)`. Re-running the pipeline updates existing ratings idempotently via `ON CONFLICT(user_id, movie_id) DO UPDATE SET rating = excluded.rating, rated_at = excluded.rated_at`.
   - Rating timestamps represent non-negative Unix epoch seconds (`CHECK (rated_at >= 0)`).

2. **Movie Catalog & Release Year Parsing**:
   - MovieLens titles typically embed release years as a four-digit number inside trailing parentheses (e.g., `Toy Story (1995)`). The transformation extracts years via regular expression `\((\d{4})\)$`.
   - Extracted release years are validated within historical cinema boundaries (`1888` to `2100`). Movies with missing or unparseable release years retain `release_year = NULL` without discarding the core movie record.
   - Movie IDs (`movieId`) from MovieLens are positive integers that serve as the authoritative primary key.

3. **External Metadata Enrichment & Unified Pipeline Architecture**:
   - The pipeline provides a unified execution workflow centered on `python .\etl.py`, which sequentially executes:
     1. MovieLens CSV extraction, transformation, and relational loading (offline).
     2. Relational constraint and foreign-key validation.
     3. External metadata enrichment via the OMDb API by delegating to `enrich_movies.py`.
   - The free OMDb API enforces a daily request limit of 1,000 requests per key. Note that processing a batch of up to 500 movies may involve more than 500 HTTP requests if network retries or secondary title fallbacks occur. Therefore, enrichment is designed as an idempotent, resumable process: if a quota stop occurs or `MAX_MOVIES_PER_RUN` is reached, unprocessed records remain safely in `pending` status, and re-running `python .\etl.py` (or standalone `python .\enrich_movies.py`) resumes from the remaining pending queue.

4. **Treatment of Duplicate IMDb IDs**:
   - In `schema.sql`, `movie_details.movie_id` is the primary key (a 1-to-1 extension of `movies`).
   - The raw MovieLens Small catalog contains 4 legitimate duplicate film pairs (e.g., *Confessions of a Dangerous Mind*, *Eros*, *Saturn 3*, *Operation 'Y'*), each representing distinct MovieLens movie IDs with separate user ratings.
   - Consistent with the schema and code, `movie_details.imdb_id` is indexed (`idx_movie_details_imdb_id`) rather than strictly enforced as `UNIQUE`, enabling both catalogue entries to reference their true canonical IMDb ID and metadata without deleting user ratings or violating referential integrity.

---

## 5. Setup & Installation Instructions

### Step 1: Create and Activate Virtual Environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Step 2: Install Package Dependencies
```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Step 3: Configure Environment Variables
Create a `.env` file in the project root:
```text
OMDB_API_KEY=your_omdb_api_key_here
MAX_MOVIES_PER_RUN=500
```
> [!IMPORTANT]
> Never commit `.env` to version control. The `.gitignore` file is pre-configured to exclude `.env`, `*.db`, `.venv/`, and logs.

### Step 4: Verify Raw Data Files
Ensure the MovieLens Small dataset files reside at:
```text
data/raw/ml-latest-small/movies.csv
data/raw/ml-latest-small/ratings.csv
data/raw/ml-latest-small/links.csv
```

---

## 6. Execution Guide

### Step 1: Run Preflight API Connection Check
Verify the API key connectivity without modifying the database:
```powershell
python .\check_omdb_connection.py
```

### Step 2: Run the Complete Pipeline (ETL & Enrichment)
Execute the complete pipeline to create `schema.sql` tables, ingest MovieLens data, validate integrity, and run OMDb metadata enrichment:
```powershell
python .\etl.py
```
- Automatically ingests CSV data, validates database foreign keys, and invokes the enrichment stage.
- Enriches pending movies in controlled batches up to `MAX_MOVIES_PER_RUN` (default 500).
- Fully idempotent: existing `success` records are preserved and never re-queried or overwritten.

### Step 3 (Optional): Run Standalone Enrichment Batches
If additional pending movies remain from a prior run (e.g., after daily quota reset or batch limit), you can resume enrichment directly via:
```powershell
python .\enrich_movies.py
```
*(Note: You can also simply re-run `python .\etl.py`, which will verify existing tables and continue processing pending enrichment records).*

### Step 4: Verify Database Health & Counts
Check table row counts, enrichment statuses, and validate foreign-key constraints:
```powershell
python .\verify_database.py
```

### Step 5: Execute Analytical Queries
Execute the 4 required analytical SQL queries:
```powershell
python .\run_required_queries.py
```
Output is mirrored to terminal and persisted in `query_output.txt`.

---

## Interview Demo: Completed SQL Queries and Live API Enrichment

This project is prepared with two distinct demonstration modes designed for technical interviews:
1. **Scenario A (Completed SQL Demo)**: Executes analytical queries against the fully enriched production database (`movie_data.db`).
2. **Scenario B (Live API Enrichment Demo)**: Demonstrates live external OMDb API calls, batching, validation, error handling, and transaction commits in real time against an isolated demo database (`movie_data_live.db`).

> [!NOTE]
> **Local Database State vs. GitHub Repository**:
> SQLite database files (`movie_data.db`, `movie_data_live.db`, `movie_data_before_restore.db`, and `*.db*`) and sensitive configuration (`.env`) are generated and maintained locally. They are excluded from version control via `.gitignore` to keep the repository lightweight, secure, and reproducible from raw data.
>
> In the local environment:
> - `movie_data.db`: Completed master database containing **9,742 movies** and **9,742 successful enrichments** (100% complete, 0 pending, 0 errors).
> - `movie_data_live.db`: Dedicated live-demo database containing **9,742 movies**, **6 successful enrichments**, and **9,736 pending movies**, ready for live enrichment demonstration.

> [!IMPORTANT]
> **Scope of `MOVIE_DB_PATH`**:
> The `MOVIE_DB_PATH` environment variable controls the database target for `enrich_movies.py` and `verify_database.py`.
> The core ETL script (`etl.py`) and the analytical query runner (`run_required_queries.py`) default to `movie_data.db` directly. This ensures that the master analytics database and assignment queries remain isolated, consistent, and protected from live demo runs.

---

### Scenario A: Completed SQL Demo (Default Master Database)

This scenario demonstrates the final verified results of all 4 assignment SQL queries using the master database (`movie_data.db`).

#### 1. Ensure Default Database Selection
By default, when `MOVIE_DB_PATH` is not set, scripts connect directly to `movie_data.db`. If you previously ran Scenario B or set the variable in your current terminal session, clear it to return to the default master database:

```powershell
# PowerShell
Remove-Item Env:\MOVIE_DB_PATH -ErrorAction SilentlyContinue
```
*(On Bash / macOS / Linux: `unset MOVIE_DB_PATH` | On Windows CMD: `set MOVIE_DB_PATH=`)*

#### 2. Verify Master Database Health & Completion
Verify that the master database has 9,742 movies and 9,742 successful enrichments:

```powershell
python .\verify_database.py
```

Expected output:
```text
DATABASE VERIFICATION
========================================
movies: 9,742 rows
ratings: 100,836 rows
genres: 19 rows
movie_genres: 22,050 rows
movie_details: 9,742 rows
directors: 4,526 rows
movie_directors: 10,715 rows

ENRICHMENT STATUS
----------------------------------------
success: 9,742

Foreign-key validation passed.
```

#### 3. Run the 4 Required Assignment Queries
Execute `run_required_queries.py` to run all four required analytical queries from `queries.sql`:

```powershell
python .\run_required_queries.py
```

Results demonstrated:
- **Query 1 (Highest Average Rating)**: Identifies 296 tied titles with a 5.0 average rating.
- **Query 2 (Top 5 Genres by Rating)**: Film-Noir (3.92), War (3.81), Documentary (3.80), Drama (3.66), Crime (3.66).
- **Query 3 (Director with Most Movies)**: Woody Allen (48 movies).
- **Query 4 (Average Rating by Year)**: Annual average ratings spanning 106 release years (1902–2018).

---

### Scenario B: Live API Enrichment Demo (Isolated Live Database)

This scenario demonstrates live OMDb API querying, rate handling, single-instance file locking, normalization, and relational insertion in real time without altering the master database.

#### Prerequisites for Live Enrichment
- **Valid OMDb API Key**: Must be configured in `.env` (`OMDB_API_KEY=your_key_here`).
- **Network Connectivity**: Outbound HTTPS access to `https://www.omdbapi.com/`.
- **Available API Quota**: Daily limit of 1,000 requests on the free tier (resets at 00:00 UTC).

#### 1. Select the Live Database
Set the `MOVIE_DB_PATH` environment variable in PowerShell to point to `movie_data_live.db`:

```powershell
# PowerShell
$env:MOVIE_DB_PATH = "movie_data_live.db"
```
*(On Bash / macOS / Linux: `export MOVIE_DB_PATH="movie_data_live.db"` | On Windows CMD: `set MOVIE_DB_PATH=movie_data_live.db`)*

#### 2. Verify Selected Database State Before Running
Run `verify_database.py` to confirm that the script is targeting `movie_data_live.db` and shows pending records:

```powershell
python .\verify_database.py
```

Expected output:
```text
DATABASE VERIFICATION
========================================
movies: 9,742 rows
ratings: 100,836 rows
genres: 19 rows
movie_genres: 22,050 rows
movie_details: 9,742 rows
directors: 6 rows
movie_directors: 6 rows

ENRICHMENT STATUS
----------------------------------------
pending: 9,736
success: 6

Foreign-key validation passed.
```

#### 3. Configure a Controlled Run Limit (`MAX_MOVIES_PER_RUN`)
For interview demonstrations, configure a small limit (e.g., 2 to 5 movies) so the live execution completes quickly and conserves your daily API quota.

> [!WARNING]
> **`.env` Configuration Precedence**:
> `enrich_movies.py` invokes `load_dotenv(PROJECT_DIR / ".env", override=True)`. Because `override=True` is enabled, **values defined in `.env` override environment variables set in the terminal**.
> - If `MAX_MOVIES_PER_RUN` is defined in your `.env` file (e.g., `MAX_MOVIES_PER_RUN=3`), that value takes precedence.
> - To adjust the limit, update the value directly in `.env`, or define it in your environment if omitted from `.env`.
> - **Always inspect the startup log message** before the API calls proceed:
>   ```text
>   Run configured with limit: maximum X movie(s) will be processed.
>   ```
>   Confirm that `X` matches your intended demonstration limit.

#### 4. Execute Live API Enrichment
Run `enrich_movies.py`:

```powershell
python .\enrich_movies.py
```

Observe in the console log:
1. `SingleInstanceGuard` acquiring `enrichment.lock`.
2. Limit confirmation (e.g., `maximum 3 movie(s) will be processed`).
3. Fetching pending movies from `movie_data_live.db`.
4. OMDb API query via canonical IMDb ID (`i=tt...`) with clean title fallback.
5. Successful payload validation, director normalization, and database transaction commit.
6. Summary log showing processed counts and clean completion.

#### 5. Verify the Updated State
Run `verify_database.py` again to observe the live progress:

```powershell
python .\verify_database.py
```
Notice that `success` incremented by the configured batch count and `pending` decreased accordingly.

#### 6. Switch Back to the Master Database
When the live demonstration is complete, clear `MOVIE_DB_PATH` to return to the default master database:

```powershell
# PowerShell
Remove-Item Env:\MOVIE_DB_PATH
```
*(On Bash / macOS / Linux: `unset MOVIE_DB_PATH` | On Windows CMD: `set MOVIE_DB_PATH=`)*

Verify that the system has returned to the master database:
```powershell
python .\verify_database.py
```
Output confirms that `movie_data.db` is selected, showing all 9,742 movies in `success` status.

---

## 7. Testing & Verification

All automated tests run completely offline and mock all external API requests, ensuring no quota is consumed during testing:

### Run the Full Offline Test Suite (Pytest)
```powershell
python -m pytest -v
```
Runs 32 comprehensive unit and integration tests across the test suite (28 in `test_pipeline_comprehensive.py`, 3 in `test_enrichment_retries.py`, and 1 in `test_omdb.py`):
1. **Successful enrichment via IMDb ID**: Verifies primary lookup queries OMDb with `i=tt...` from `links.csv` and populates `movie_details` and `movie_directors`.
2. **Successful enrichment via title search fallback**: Verifies secondary lookup executes `t=clean_title` when IMDb ID is unavailable or fails.
3. **Movie genuinely not found**: Verifies movies returning "Movie not found!" are categorized as `not_found` without raising exceptions.
4. **Title or year mismatch handled conservatively**: Verifies discrepancies in release year (> 1 year) or title reject false matches and record `not_found`.
5. **Missing optional metadata (BoxOffice) handled cleanly**: Verifies `BoxOffice = "N/A"` or missing stores `box_office = NULL` without causing failure.
6. **Missing mandatory metadata (Director or Plot) handled correctly**: Verifies missing `Director` or `Plot` records `error` with detailed reason rather than fabricating data.
7. **Invalid API key / HTTP 401 circuit breaker**: Verifies HTTP 401 with bad key halts execution immediately and preserves movies in `pending` status.
8. **Exhausted quota stops safely**: Verifies OMDb quota exhaustion ("Request limit reached!" via HTTP 200 or 401) stops safely and leaves movies `pending`.
9. **HTTP 429 backoff handling**: Verifies 429 responses trigger backoff retries and respect the `Retry-After` header.
10. **HTTP 500/503 retry handling**: Verifies temporary server errors retry with exponential backoff up to 3 attempts.
11. **Network timeout retry handling**: Verifies network timeouts retry bounded number of times and succeed upon recovery.
12. **Malformed JSON handling**: Verifies unparseable JSON payloads are caught and recorded cleanly as an `error`.
13. **Retry exhaustion records an error**: Verifies movies with permanent network failures transition to `error` status.
14. **Interrupted processing and resumption via MAX_MOVIES_PER_RUN**: Verifies bounded runs stop after the configured limit and subsequent runs resume seamlessly.
15. **Repeated execution does not duplicate records (idempotency)**: Verifies re-running `save_movie_details` never creates duplicate director rows.
16. **Existing successful metadata is never overwritten**: Verifies `api_status = 'success'` records are strictly excluded from pending selections and untouched by ETL reloads.
17. **Sensitive API keys are redacted from logs and errors**: Verifies all logging and exception formatters scrub `apikey=[REDACTED]`.
18. **Roman numeral and subtitle normalization**: Verifies Roman numeral conversion (*Death Wish II* -> *Death Wish 2*), parenthetical subtitle extraction (*Evil Dead II (Dead by Dawn)*), and diacritic handling (*Maelström*).
19. **SingleInstanceGuard concurrency lock**: Verifies file locking prevents simultaneous script execution and cleans up upon exit.
20. **ETL validation-before-enrichment sequencing**: Verifies `etl.main()` enforces complete CSV ingestion and database validation before invoking OMDb enrichment.
21. **Missing API key graceful handling**: Verifies missing `OMDB_API_KEY` logs clear instructions, skips enrichment, and never claims full pipeline success.
22. **Daily quota exhaustion handling**: Verifies quota stops report paused status and preserve pending records without claiming full pipeline success.
23. **Distinguishing enrichment errors**: Verifies unresolved errors report distinct diagnostic counts without claiming complete pipeline success.
24. **Concurrency collision detection**: Verifies active single-instance locks are distinguished from API or key errors.
25. **Accurate not-found handling**: Verifies that `not_found` records are distinguished from complete enrichment without falsely claiming all movies enriched.
26. **Safe failure on database status check**: Verifies database query failures halt safely without falsely claiming pipeline success.
27. **Complete catalog enrichment verification**: Verifies that re-running `etl.py` on a fully enriched catalog confirms completion accurately.
28. **Safeguard against unexpected status**: Verifies that unrecognized `api_status` values in the database are rejected as invalid and cannot produce a false full-success message.

### Run Standalone Verification Scripts
```powershell
# Verify DDL constraints, relationships, and table structures
python .\test_schema.py

# Verify that rerunning ETL preserves existing enrichment without duplicate rows
python .\test_idempotency.py
```

---

## 8. Analytical Questions & SQL Results

The analytical queries are maintained in `queries.sql`. Below are the verified results based on the current enriched database:

### Query 1: Which movie has the highest average rating?
- **Handling of Ties**: 296 movies in the dataset are tied with a perfect average rating of `5.00` (predominantly single-rating or two-rating titles, such as *12 Angry Men (1997)*, *Belle Époque (1992)*, and *Come and See (1985)*). The query uses a CTE (`WITH movie_averages AS ...`) to dynamically detect the maximum average rating and return all tied titles sorted alphabetically.

### Query 2: What are the top 5 movie genres with the highest average rating?
| Rank | Genre | Average Rating | Total Ratings |
|---|---|---|---|
| 1 | **Film-Noir** | **3.92** | 870 |
| 2 | **War** | **3.81** | 4,859 |
| 3 | **Documentary** | **3.80** | 1,219 |
| 4 | **Drama** | **3.66** | 41,928 |
| 5 | **Crime** | **3.66** | 16,681 |

### Query 3: Who is the director with the most movies in this dataset?
```text
director: Woody Allen | movie_count: 48 movies
```
*(Followed by Alfred Hitchcock with 37 movies, Steven Spielberg with 32 movies, Clint Eastwood with 32 movies, Martin Scorsese with 29 movies, and Ridley Scott with 24 movies).*

### Query 4: What is the average rating of movies released each year?
Evaluates 106 distinct release years (from 1902 to 2018). Key observations:
- Earliest movie: `1902` (average rating: `3.50`, 5 ratings)
- Peak rating year: `1917` (average rating: `4.50`, 1 rating)
- Peak rating volume year: `1995` (average rating: `3.44`, 6,144 ratings)
- Most recent complete year: `2018` (average rating: `3.48`, 91 ratings)

---

## 9. Current Database Status & Enrichment Summary

Verified from `python .\verify_database.py`:

```text
DATABASE VERIFICATION
========================================
movies: 9,742 rows
ratings: 100,836 rows
genres: 19 rows
movie_genres: 22,050 rows
movie_details: 9,742 rows
directors: 4,526 rows
movie_directors: 10,715 rows

ENRICHMENT STATUS
----------------------------------------
success: 9,742

Foreign-key validation passed.
```

- **Validated Enrichment Success Rate**: **100.00%** (9,742 of 9,742 movies).
- **All Mandatory Fields Compliant**: 100% of the 9,742 `success` movies in the database meet every required field: canonical `imdb_id`, complete `plot`, and relational `director`.
- **Relational Directors**: **4,526** unique directors and **10,715** movie-director associations.
- **Source Attribution & Provenance Records**: The verified production database contains 456 provenance records preserved in the auxiliary `metadata_attribution` table, documenting edge-case catalog audit reconciliations (4 canonical catalog duplicates, 82 directors, 3 plot synopses, 200 release year verifications, and 168 title alias verifications). Standard pipeline execution relies on the OMDb API, with secondary source enrichment remaining strictly optional (`ENABLE_SECONDARY_SOURCES=true`).
- **Pending Status**: **0 movies remaining in pending**.
- **Not Found Status**: **0 movies remaining in not_found**.
- **Error Status**: **0 movies remaining in error**.

---

## Challenges and Solutions

Building and executing an end-to-end relational movie data pipeline against real-world data and external APIs encountered several engineering challenges, which were resolved as follows:

| # | Challenge | Implemented Solution |
|---|---|---|
| 1 | **Movie Titles Differing from OMDb Titles**<br>MovieLens titles include trailing articles (`Twelve Chairs, The`), foreign articles (`Misérables, Les`), Roman numerals (`Death Wish II`), ligatures (`Æon Flux`), diacritics (`Maelström`), and subtitles in parentheses (`Evil Dead II (Dead by Dawn)`), causing naive title searches to fail. | **Authoritative Lookup & Multi-Stage Normalization**: The pipeline prioritizes authoritative IMDb IDs from `links.csv` (`i=tt...`). For fallback searches (`t=...`), `normalize_title()` strips trailing years, expands ligatures, converts Roman numerals to Arabic digits, strips accents via Unicode NFD, inverts trailing articles, and strips parenthetical subtitles. |
| 2 | **Missing or Unavailable API Records**<br>Some movies are absent from OMDb ("Movie not found!"), omit optional fields (`BoxOffice = "N/A"`), or lack mandatory fields (e.g., TV miniseries where OMDb returns `Director = "N/A"`, or archival films lacking synopsis). | **Graceful Fallbacks & Provenance**: Genuinely missing titles are safely recorded as `not_found` with descriptive reasons. Missing optional box office data is stored as `NULL`. For television miniseries and archival films, verified metadata was retrieved from authoritative institutional registries (Wikidata SPARQL, BBC, BAFTA, national film archives) with complete provenance recorded in `metadata_attribution`. |
| 3 | **Transient API Failures, Rate Limits & Quotas**<br>External API calls are subject to network timeouts, HTTP 429 (rate limiting), HTTP 500/503 (server errors), HTTP 401 (invalid key), and daily quota exhaustion (`Request limit reached!`). | **Bounded Backoff & Circuit Breakers**: Retries up to 3 times with exponential backoff on transient errors (429, 5xx, timeouts). Fatal stops (HTTP 401 `OMDbAuthenticationError` or quota exhaustion `OMDbQuotaExceeded`) trigger an immediate emergency halt, leaving unreached movies safely in `pending`. All logs and database error messages sanitize credentials via `sanitize_error()`. |
| 4 | **Resuming Enrichment Without Overwriting Successful Records**<br>Because free API quotas limit daily requests (1,000 requests/day, where retries and title fallbacks may require multiple requests per movie), enrichment must run in batches across days without re-querying or corrupting already enriched records. | **Idempotent State Tracking & Concurrency Locks**: `etl.py` initializes records with `ON CONFLICT(movie_id) DO NOTHING`. `enrich_movies.py` queries strictly `WHERE api_status = 'pending'`. Successfully enriched (`success`) records are never overwritten. A file lock (`SingleInstanceGuard` on `enrichment.lock`) prevents overlapping executions. |
| 5 | **Data Normalization & Relational Integrity**<br>MovieLens genres are pipe-delimited strings (`Adventure|Animation|Children`), OMDb directors are comma-delimited strings, and source catalog duplicates share canonical IMDb IDs. | **3NF Relational Modeling**: Cleanly decomposed into 7 normalized tables. Junction tables (`movie_genres`, `movie_directors`) resolve many-to-many relationships. Foreign keys enforce `ON DELETE CASCADE`. Check constraints enforce domain validity. Non-unique index `idx_movie_details_imdb_id` preserves source catalog duplicates and all 100,836 ratings. |

### Detailed Technical Breakdown & Catalog Reconciliation

### 1. Authoritative IMDb ID Priority and Normalization
MovieLens titles frequently include alternate foreign titles, subtitles in parentheses (e.g., `Evil Dead II (Dead by Dawn)`), trailing articles (`Twelve Chairs, The`), foreign trailing articles (`Misérables, Les`, `Haine, La`, `Boot, Das`), ligatures (`Aeon Flux` vs `Æon Flux`), abbreviations (`Dr.` -> `doctor`), and Roman numerals (`Death Wish II`). The pipeline employs:
- **Authoritative First Lookup**: Queries OMDb using the authoritative IMDb ID from `links.csv` (`i=tt...`), avoiding ambiguous search results and stub collisions.
- **Title, Ligature & Roman Numeral Normalization**: Word-boundary Roman numeral translation (`II` -> `2`, `IV` -> `4`, etc.), ligature expansion (`æ` -> `ae`, `œ` -> `oe`), diacritic stripping (`Maelström` -> `maelstrom`), doctor abbreviation normalization (`Dr.` -> `doctor`), foreign/English article inversion (`Misérables, Les` -> `Les Misérables`), and parenthetical subtitle extraction.
- **Authoritative Secondary Source Support**:
  - For television miniseries and specials where OMDb returns full plot summaries but omits directors (`Director: "N/A"`), the pipeline supports secondary verification via institutional registries (Wikidata SPARQL `wdt:P57`/`wdt:P170`, BAFTA, Primetime Emmy credits, BBC, Madhouse, NBC) when `ENABLE_SECONDARY_SOURCES=true` is enabled, recording explicit source attribution in `metadata_attribution`.
  - For archival films where OMDb omitted synopsis text, synopses were verified against national film institutes and broadcast archives (Finnish National Audiovisual Institute Elonet, Austrian Film Museum / ORF, Adolf Grimme Institut).
  - For international films where OMDb reflects US distribution re-release dates, original international premiere dates were verified against Wikidata (`wdt:P577`), ensuring authentic film identity without relaxing validation rules.
  - For international films where OMDb reflects US release titles differing from international titles, official aliases were verified against Wikidata (`wdt:P345`), recording provenance in `metadata_attribution`.
- **Title Fallback**: Robust secondary search by title (`t=...`) with inverted articles and clean search variants if the IMDb ID is unavailable or unresolvable.

### 2. API Quota Limitations
The free OMDb API enforces a daily request limit of 1,000 requests per key, resetting at **00:00 UTC**. When the quota is reached, the API gateway emits an error or HTTP 401 with `Request limit reached!`. The pipeline detects both JSON error messages and HTTP 401 quota strings, halts execution immediately without marking movies as errors, leaves all unreached movies in `pending` status, and exits cleanly.

### 3. Exhaustive Reconciliation of All 25 Edge Records
All 9,742 movies have reached verified `success` status with full relational data integrity:
1. **Assignment-Compliant Catalog Duplicate Reconciliation (4 movies)**: In `movies.csv`, GroupLens included duplicate catalogue entries representing the same underlying film:
   - *Confessions of a Dangerous Mind (2002)*: `movie_id = 6003` (15 ratings) and `144606` (1 rating), both mapping to canonical IMDb ID `tt0270288` (Director: George Clooney).
   - *Eros (2004)*: `movie_id = 147002` (1 rating) and `32600` (1 rating), both mapping to canonical IMDb ID `tt0343663` (Directors: Michelangelo Antonioni, Steven Soderbergh, Wong Kar-Wai).
   - *Saturn 3 (1980)*: `movie_id = 168358` (1 rating) and `2851` (4 ratings), both mapping to canonical IMDb ID `tt0079285` (Directors: Stanley Donen, John Barry).
   - *Obsession (1965)*: `movie_id = 174551` (1 rating), the novella segment *Navazhdenie* of *Operation Y and Other Shurik's Adventures* (`movie_id = 127052`, canonical IMDb ID `tt0059550`, Director: Leonid Gaidai).
   *Data Model Resolution*: Replacing the artificial `UNIQUE(imdb_id)` constraint on `movie_details` with a non-unique lookup index (`idx_movie_details_imdb_id`) properly models the reality of the source catalogue (multiple catalogue IDs referencing a single canonical film entity) while strictly maintaining `movie_id INTEGER PRIMARY KEY` and all foreign-key cascade constraints.
2. **Television Miniseries & Specials with Verified Directors (18 movies)**: Works where OMDb returned `Director: "N/A"` due to TV series categorization were enriched with verified directors from authoritative institutional registries:
   - *Prime Suspect 2 (1992)* (`tt0103516`): John Strickland (BAFTA / Granada TV)
   - *The Stand (1994)* (`tt0108941`): Mick Garris (Stephen King Filmography / ABC)
   - *Babylon 5* (`tt0105946`): J. Michael Straczynski (Wikidata wdt:P170 / Warner Bros. TV)
   - *Farscape: The Peacekeeper Wars (2004)* (`tt0387736`): Brian Henson (The Jim Henson Co.)
   - *Wild China (2008)* (`tt0884762`): Phil Chapman, Gavin Maxwell, George Chan, Kathryn Jeffs (BBC Natural History Unit)
   - *Louis Theroux: Law & Disorder (2008)* (`tt1347439`): Emma Cooper (BBC / BAFTA)
   - *Prime Suspect 3 (1993)* (`tt0106104`): David Drury (BAFTA / Granada TV)
   - *Prime Suspect 6: The Last Witness (2003)* (`tt0365651`): Tom Hooper (Primetime Emmy / ITV)
   - *Human Planet (2011)* (`tt1806234`): Tira Shubart, Mark Flowers (BBC / Discovery)
   - *Mezzo Forte (1998)* (`tt0427531`): Yasuomi Umetsu (Wikidata wdt:P57 / Arms Anime)
   - *Jeff Ross Roasts Criminals (2015)* (`tt4705522`): Stuart Miller (Comedy Central / Jax Media)
   - *Planet Earth II (2016)* (`tt5491994`): Michael Gunton, Justin Anderson, Ed Charles, Chadden Hunter, Emma Napper, Elizabeth White (BBC / Emmy)
   - *Death Note: Desu nôto (2006–2007)* (`tt0877057`): Tetsurō Araki (Madhouse / Tokyo Anime Award)
   - *Late Night with Conan O'Brien: The Best of Triumph (2004)* (`tt0422618`): Allan Kartun (NBC Universal / Conaco)
   - *Priklyucheniya Kapitana Vrungelya (1979)* (`tt0219263`): Davyd Cherkaskyi (Wikidata wdt:P57 / Kievnauchfilm)
   - *Blue Planet II (2017)* (`tt6769208`): James Honeyborne, Mark Brownlow (BBC Natural History Unit)
   - *The Shining (1997)* (`tt0118460`): Mick Garris (Stephen King Filmography / ABC)
   - *Jon Stewart Has Left the Building (2015)* (`tt5342766`): Chuck O'Neil (Comedy Central / Busboy Productions)
3. **Archival Films with Verified Plot Synopses (3 movies)**: Works where OMDb returned verified directors but lacked synopsis text were enriched from authoritative national film archives:
   - *Pekka ja Pätkä Suezilla (1958)* (`tt0049607`): Director Armand Lohikoski; plot from Finnish National Audiovisual Institute (Elonet).
   - *Der Herr Karl (1961)* (`tt0273646`): Director Erich Neuberg; plot from Austrian Broadcasting Corporation (ORF) / Filmarchiv Austria.
   - *Die Frauen von Ravensbrück (2005)* (`tt0832879`): Director Loretta Walz; plot from Adolf Grimme Institut / German Federal Foundation.

