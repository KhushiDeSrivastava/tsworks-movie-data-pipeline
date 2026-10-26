# Enrich MovieLens movies with details from the OMDb API.
# Processes pending movies in resumable batches and stops on quota exhaustion or authentication failure.

import logging
import os
import re
import sqlite3
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = PROJECT_DIR / "movie_data.db"
LOG_DIR = PROJECT_DIR / "logs"
LOG_FILE = LOG_DIR / "enrichment.log"
LOCK_FILE = PROJECT_DIR / "enrichment.lock"
LINKS_FILE = PROJECT_DIR / "data" / "raw" / "ml-latest-small" / "links.csv"

load_dotenv(PROJECT_DIR / ".env", override=True)

API_KEY = os.getenv("OMDB_API_KEY", "").strip()
API_URL = "https://www.omdbapi.com/"

BATCH_SIZE = 100
# Maximum movies to process per invocation (defaults to 500 as requested).
# Can be configured via environment variable MAX_MOVIES_PER_RUN (or set to 0/None for unlimited).
_env_max = os.getenv("MAX_MOVIES_PER_RUN", "500").strip()
try:
    MAX_MOVIES_PER_RUN = int(_env_max) if _env_max else 500
except ValueError:
    MAX_MOVIES_PER_RUN = 500

MAX_ATTEMPTS = 3
REQUEST_TIMEOUT = 15
REQUEST_DELAY_SECONDS = 0.25

# Enable authoritative secondary sources (e.g. Wikidata) for legitimately missing fields
ENABLE_SECONDARY_SOURCES = os.getenv("ENABLE_SECONDARY_SOURCES", "false").strip().lower() in ("true", "1", "yes")


# ============================================================
# 2. LOGGING AND CUSTOM EXCEPTIONS
# ============================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
    force=True,
)

logger = logging.getLogger(__name__)


class OMDbQuotaExceeded(Exception):
    """Raised when OMDb reports that the daily request limit is reached."""


class OMDbAuthenticationError(Exception):
    """Raised when OMDb rejects the API key with HTTP 401."""


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def sanitize_error(error_or_message):
    """Sanitize error messages to remove API keys and sensitive URL parameters."""
    if error_or_message is None:
        return None
    text = str(error_or_message)
    return re.sub(r"([?&]apikey=)[^&\s'\"]+", r"\1[REDACTED]", text, flags=re.IGNORECASE)


def clean_value(value):
    """Convert missing values and OMDb's N/A text to None."""
    if value is None:
        return None

    value = str(value).strip()

    if not value or value.upper() == "N/A":
        return None

    return value


def current_timestamp():
    """Return the current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def clean_title_year(title):
    """Strip a trailing year such as (1995) from a title."""
    return re.sub(r"\s*\(\d{4}\)$", "", str(title or "")).strip()


def normalize_roman_numerals(text):
    """Convert common Roman numerals (I-X) to Arabic digits at word boundaries."""
    roman_map = {
        "viii": "8",
        "vii": "7",
        "iii": "3",
        "iv": "4",
        "ix": "9",
        "vi": "6",
        "ii": "2",
        "v": "5",
        "x": "10",
        "i": "1",
    }
    pattern = re.compile(r"\b(viii|vii|iii|iv|ix|vi|ii|v|x|i)\b", re.IGNORECASE)
    return pattern.sub(lambda m: roman_map[m.group(0).lower()], text)


def normalize_title(title):
    """Normalize punctuation, capitalization, trailing articles, diacritics, ligatures, Roman numerals, and trailing years."""
    title = str(title or "").lower().strip()

    # Remove trailing year like (1995) if present
    title = clean_title_year(title)

    # Expand common ligatures (e.g. Æon Flux -> Aeon Flux)
    title = title.replace("æ", "ae").replace("œ", "oe")

    # Normalize diacritics / accents (e.g. Maelström -> Maelstrom)
    title = "".join(
        c for c in unicodedata.normalize("NFD", title)
        if unicodedata.category(c) != "Mn"
    )

    # Replace ampersands with 'and' for standard textual matching
    title = re.sub(r"\s*&\s*", " and ", title)

    # Normalize doctor abbreviation (Dr. / Dr -> doctor)
    title = re.sub(r"\bdr\.?\b", "doctor", title)

    # Trailing articles (English and foreign): "movie, the" -> "the movie", "Misérables, Les" -> "Les Misérables"
    match = re.match(r"^(.+),\s*(the|a|an|le|la|les|l'|el|los|las|der|die|das|il|lo|i|gli|un|une|ein|eine)$", title)
    if match:
        title = f"{match.group(2)} {match.group(1)}"

    # Convert Roman numerals to Arabic digits (e.g. "Death Wish II" -> "Death Wish 2")
    title = normalize_roman_numerals(title)

    return re.sub(r"[^a-z0-9]", "", title)


def normalize_title_no_article(title):
    """Return normalized title with leading articles removed at word boundaries for article-agnostic comparison."""
    title = str(title or "").lower().strip()
    title = clean_title_year(title)
    title = title.replace("æ", "ae").replace("œ", "oe")
    title = "".join(
        c for c in unicodedata.normalize("NFD", title)
        if unicodedata.category(c) != "Mn"
    )
    title = re.sub(r"\s*&\s*", " and ", title)
    title = re.sub(r"\bdr\.?\b", "doctor", title)

    match = re.match(r"^(.+),\s*(the|a|an|le|la|les|l'|el|los|las|der|die|das|il|lo|i|gli|un|une|ein|eine)$", title)
    if match:
        title = f"{match.group(2)} {match.group(1)}"

    # Strip leading article at word boundary
    title = re.sub(r"^(the|a|an|le|la|les|el|los|las|der|die|das|il|lo|i|gli|un|une|ein|eine)\s+", "", title)
    title = normalize_roman_numerals(title)
    return re.sub(r"[^a-z0-9]", "", title)


def get_title_candidates(title):
    """Generate canonical normalized forms for a title, including parenthetical alternates and base titles."""
    cleaned = clean_title_year(title)
    candidates = set()

    def add(t):
        if not t:
            return
        n = normalize_title(t)
        if n:
            candidates.add(n)
        n_no_art = normalize_title_no_article(t)
        if n_no_art:
            candidates.add(n_no_art)

    add(cleaned)

    # Base without any parenthetical parts (e.g. "Evil Dead II" from "Evil Dead II (Dead by Dawn)")
    base = re.sub(r"\s*\([^)]*\)", "", cleaned).strip()
    if base and base != cleaned:
        add(base)

    # Subtitle components (e.g. splitting on ':' or ' - ')
    if ":" in cleaned:
        for part in cleaned.split(":"):
            add(part.strip())
    if " - " in cleaned:
        for part in cleaned.split(" - "):
            add(part.strip())

    # Director / creator possessive prefix (e.g. "Lee Daniels' The Butler" -> "The Butler")
    possessive = re.sub(r"^[A-Z][a-zA-Z\s]+'s?\s+", "", cleaned)
    if possessive and possessive != cleaned:
        add(possessive)

    # Content inside parentheses, stripping 'a.k.a.' / 'aka'
    for paren in re.findall(r"\(([^)]+)\)", cleaned):
        clean_paren = re.sub(r"^(?:a\.?k\.?a\.?|aka)\s+", "", paren.strip(), flags=re.IGNORECASE)
        add(clean_paren)
        p_match = re.match(r"^(.+),\s*(the|a|an|le|la|les|l'|el|los|las|der|die|das|il|lo|i|gli|un|une|ein|eine)$", clean_paren, re.IGNORECASE)
        if p_match:
            add(f"{p_match.group(2)} {p_match.group(1)}")

    return candidates


def is_title_compatible(returned_title, expected_title):
    """Check if returned title matches expected title using robust candidate intersection."""
    ret_candidates = get_title_candidates(returned_title)
    exp_candidates = get_title_candidates(expected_title)
    return bool(ret_candidates & exp_candidates)


def get_year(value):
    """Extract a four-digit year from a string."""
    match = re.search(r"\b(\d{4})\b", str(value or ""))
    return int(match.group(1)) if match else None


_LINKS_CACHE = None


def get_imdb_id_for_movie(movie_id):
    """Retrieve the formatted IMDb ID (e.g. tt0038650) for a given movie_id from links.csv."""
    global _LINKS_CACHE
    if _LINKS_CACHE is None:
        _LINKS_CACHE = {}
        if LINKS_FILE.exists():
            try:
                import csv
                with open(LINKS_FILE, mode="r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        mid = row.get("movieId")
                        imdb = row.get("imdbId")
                        if mid and imdb and imdb.strip():
                            formatted = f"tt{imdb.strip().zfill(7)}"
                            _LINKS_CACHE[int(mid)] = formatted
            except Exception as e:
                logger.warning("Could not load links.csv for IMDb fallback: %s", e)
    return _LINKS_CACHE.get(movie_id)


def get_pending_movies(connection, limit=BATCH_SIZE):
    """Retrieve the next batch of movies awaiting enrichment."""
    return connection.execute(
        """
        SELECT m.movie_id, m.title, m.release_year
        FROM movies AS m
        JOIN movie_details AS md
            ON m.movie_id = md.movie_id
        WHERE md.api_status = 'pending'
        ORDER BY m.movie_id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


class SingleInstanceGuard:
    """Ensures only one instance of enrich_movies runs at a time."""

    def __init__(self, lock_path=LOCK_FILE):
        self.lock_path = Path(lock_path)
        self.acquired = False

    def __enter__(self):
        if self.lock_path.exists():
            try:
                content = self.lock_path.read_text(encoding="utf-8").strip()
                existing_pid = int(content)
                # Check if process is still running on Windows
                import ctypes
                kernel32 = ctypes.windll.kernel32
                PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, existing_pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    raise RuntimeError(
                        f"Another enrichment process (PID {existing_pid}) is already running."
                    )
            except (ValueError, OSError):
                # Stale lock file, safe to proceed
                pass

        self.lock_path.write_text(str(os.getpid()), encoding="utf-8")
        self.acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired and self.lock_path.exists():
            try:
                self.lock_path.unlink()
            except OSError:
                pass


# ============================================================
# 4. FETCH DETAILS FROM OMDb
# ============================================================

def fetch_movie_details(title=None, year=None, imdb_id=None):
    """Fetch movie details, retry temporary failures, and detect quota limits or auth failure."""
    params = {
        "apikey": API_KEY,
    }

    if imdb_id:
        params["i"] = imdb_id
        params["plot"] = "full"
    else:
        clean_title = clean_title_year(title)
        params["t"] = clean_title
        params["type"] = "movie"
        if year is not None:
            params["y"] = int(year)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            # Check HTTP 401 directly
            if response.status_code == 401:
                try:
                    err_json = response.json()
                    err_msg = str(err_json.get("Error", ""))
                except Exception:
                    err_msg = response.text or ""
                if "request limit reached" in err_msg.lower() or "limit reached" in err_msg.lower():
                    raise OMDbQuotaExceeded("OMDb request limit reached (HTTP 401).")
                raise OMDbAuthenticationError(
                    f"OMDb rejected API key with HTTP 401 Unauthorized: {err_msg or 'Invalid API key'}"
                )

            response.raise_for_status()

            try:
                data = response.json()
            except (ValueError, requests.exceptions.JSONDecodeError) as e:
                if attempt == MAX_ATTEMPTS:
                    raise ValueError(f"Malformed JSON response from OMDb API: {e}")
                delay = 2 ** (attempt - 1)
                logger.warning(
                    "Malformed JSON on attempt %s/%s. Retrying in %s seconds.",
                    attempt,
                    MAX_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)
                continue

            # OMDb can report quota exhaustion or invalid key in a JSON response
            # even when the HTTP status is 200.
            api_error = str(data.get("Error", ""))

            if data.get("Response") != "True":
                if "request limit reached" in api_error.lower() or "limit reached" in api_error.lower():
                    raise OMDbQuotaExceeded(api_error)
                if "invalid api key" in api_error.lower():
                    raise OMDbAuthenticationError(api_error)

            return data

        except (OMDbQuotaExceeded, OMDbAuthenticationError):
            # Fatal conditions: do not retry quota exhaustion or authentication rejection.
            raise

        except requests.exceptions.HTTPError as error:
            status_code = (
                error.response.status_code
                if error.response is not None
                else None
            )

            if status_code == 401:
                try:
                    err_json = error.response.json()
                    err_msg = str(err_json.get("Error", ""))
                except Exception:
                    err_msg = error.response.text or ""
                if "request limit reached" in err_msg.lower() or "limit reached" in err_msg.lower():
                    raise OMDbQuotaExceeded("OMDb request limit reached (HTTP 401).")
                raise OMDbAuthenticationError(
                    f"OMDb rejected API key with HTTP 401 Unauthorized: {err_msg or 'Invalid API key'}"
                )

            retryable = (
                status_code == 429
                or (
                    status_code is not None
                    and 500 <= status_code < 600
                )
            )

            if not retryable or attempt == MAX_ATTEMPTS:
                raise

            delay = 2 ** (attempt - 1)
            if status_code == 429 and error.response is not None:
                try:
                    retry_after = error.response.headers.get("Retry-After")
                    if isinstance(retry_after, str) and retry_after.strip().isdigit():
                        delay = min(int(retry_after.strip()), 30)
                except Exception:
                    pass

            logger.warning(
                "HTTP %s on attempt %s/%s. Retrying in %s seconds.",
                status_code,
                attempt,
                MAX_ATTEMPTS,
                delay,
            )

            time.sleep(delay)

        except requests.exceptions.RequestException as error:
            if attempt == MAX_ATTEMPTS:
                raise

            delay = 2 ** (attempt - 1)

            logger.warning(
                "Request failed on attempt %s/%s: %s. "
                "Retrying in %s seconds.",
                attempt,
                MAX_ATTEMPTS,
                sanitize_error(error),
                delay,
            )

            time.sleep(delay)


def fetch_wikidata_directors(imdb_id):
    """Fetch authoritative directors for a given IMDb ID from Wikidata SPARQL API."""
    if not imdb_id or not imdb_id.startswith("tt"):
        return None
    import json
    import urllib.request
    import urllib.parse
    sparql = f"""
    SELECT ?directorLabel WHERE {{
      ?item wdt:P345 "{imdb_id}".
      ?item wdt:P57 ?director.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    url = "https://query.wikidata.org/sparql?" + urllib.parse.urlencode({"query": sparql, "format": "json"})
    req = urllib.request.Request(url, headers={"User-Agent": "TSWorksMovieDataBot/1.0 (academic research)"})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            directors = []
            for row in data.get("results", {}).get("bindings", []):
                lbl = row.get("directorLabel", {}).get("value")
                if lbl and not lbl.startswith("Q") and lbl not in directors:
                    directors.append(lbl)
            return ", ".join(directors) if directors else None
    except Exception as e:
        logger.warning("Wikidata director lookup failed for %s: %s", imdb_id, sanitize_error(e))
        return None


def is_year_verified_by_authoritative_source(imdb_id, expected_year):
    """Check if authoritative Wikidata publication dates confirm the expected release year within 1 year."""
    if not imdb_id or not imdb_id.startswith("tt") or expected_year is None:
        return False
    import json
    import urllib.request
    import urllib.parse
    sparql = f"""
    SELECT ?pubDate WHERE {{
      ?item wdt:P345 "{imdb_id}".
      ?item wdt:P577 ?pubDate.
    }}
    """
    url = "https://query.wikidata.org/sparql?" + urllib.parse.urlencode({"query": sparql, "format": "json"})
    req = urllib.request.Request(url, headers={"User-Agent": "TSWorksMovieDataBot/1.0 (academic research)"})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            for row in data.get("results", {}).get("bindings", []):
                pub = row.get("pubDate", {}).get("value", "")
                if pub and len(pub) >= 4 and pub[:4].isdigit():
                    if abs(int(pub[:4]) - int(expected_year)) <= 1:
                        return True
            return False
    except Exception as e:
        logger.warning("Wikidata year verification failed for %s: %s", imdb_id, sanitize_error(e))
        return False


def is_title_verified_by_authoritative_source(imdb_id, expected_title):
    """Check if authoritative Wikidata labels or aliases corroborate the expected title."""
    if not imdb_id or not imdb_id.startswith("tt") or not expected_title:
        return False
    import json
    import urllib.request
    import urllib.parse
    sparql = f"""
    SELECT ?label ?alias WHERE {{
      ?item wdt:P345 "{imdb_id}".
      OPTIONAL {{ ?item rdfs:label ?label. }}
      OPTIONAL {{ ?item skos:altLabel ?alias. }}
    }}
    """
    url = "https://query.wikidata.org/sparql?" + urllib.parse.urlencode({"query": sparql, "format": "json"})
    req = urllib.request.Request(url, headers={"User-Agent": "TSWorksMovieDataBot/1.0 (academic research)"})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            names = set()
            for row in data.get("results", {}).get("bindings", []):
                if "label" in row:
                    names.add(row["label"]["value"])
                if "alias" in row:
                    names.add(row["alias"]["value"])
            exp_candidates = get_title_candidates(expected_title)
            for n in names:
                n_cand = get_title_candidates(n)
                if n_cand & exp_candidates:
                    return True
            return False
    except Exception as e:
        logger.warning("Wikidata title verification failed for %s: %s", imdb_id, sanitize_error(e))
        return False


# ============================================================
# 5. SAVE MOVIE DETAILS AND DIRECTORS
# ============================================================

def save_movie_details(connection, movie_id, data):
    """Save verified OMDb fields and director relationships atomically."""
    imdb_id = clean_value(data.get("imdbID"))
    plot = clean_value(data.get("Plot"))
    box_office = clean_value(data.get("BoxOffice"))
    director_text = clean_value(data.get("Director"))

    with connection:
        connection.execute(
            """
            UPDATE movie_details
            SET imdb_id = ?,
                plot = ?,
                box_office = ?,
                api_status = 'success',
                fetched_at = ?,
                last_error = NULL
            WHERE movie_id = ?
            """,
            (
                imdb_id,
                plot,
                box_office,
                current_timestamp(),
                movie_id,
            ),
        )

        # Replace any existing director links for this movie.
        connection.execute(
            "DELETE FROM movie_directors WHERE movie_id = ?",
            (movie_id,),
        )

        if director_text:
            director_names = [
                name.strip()
                for name in director_text.split(",")
                if name.strip()
            ]

            for director_name in director_names:
                connection.execute(
                    """
                    INSERT INTO directors (director_name)
                    VALUES (?)
                    ON CONFLICT(director_name) DO NOTHING
                    """,
                    (director_name,),
                )

                director_row = connection.execute(
                    """
                    SELECT director_id
                    FROM directors
                    WHERE director_name = ?
                    """,
                    (director_name,),
                ).fetchone()

                if director_row is None:
                    raise RuntimeError(
                        f"Could not retrieve director ID for {director_name}"
                    )

                director_id = director_row[0]

                connection.execute(
                    """
                    INSERT INTO movie_directors (movie_id, director_id)
                    VALUES (?, ?)
                    ON CONFLICT(movie_id, director_id) DO NOTHING
                    """,
                    (movie_id, director_id),
                )

        if data.get("DirectorSource"):
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_attribution (
                    movie_id INTEGER PRIMARY KEY,
                    field_name TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    provider_id TEXT,
                    retrieved_at TEXT NOT NULL,
                    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                INSERT INTO metadata_attribution (movie_id, field_name, provider, provider_id, retrieved_at)
                VALUES (?, 'director', ?, ?, ?)
                ON CONFLICT(movie_id) DO UPDATE SET
                    field_name = excluded.field_name,
                    provider = excluded.provider,
                    provider_id = excluded.provider_id,
                    retrieved_at = excluded.retrieved_at
                """,
                (
                    movie_id,
                    data.get("DirectorSource", "Wikidata"),
                    data.get("DirectorProviderId", "wdt:P57"),
                    current_timestamp(),
                ),
            )
            logger.info(
                "Recorded source attribution for movie %s: director from %s (%s)",
                movie_id,
                data.get("DirectorSource"),
                data.get("DirectorProviderId"),
            )

        if data.get("YearVerifiedBy"):
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_attribution (
                    movie_id INTEGER PRIMARY KEY,
                    field_name TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    provider_id TEXT,
                    retrieved_at TEXT NOT NULL,
                    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                INSERT INTO metadata_attribution (movie_id, field_name, provider, provider_id, retrieved_at)
                VALUES (?, 'release_year_verification', ?, 'wdt:P577', ?)
                ON CONFLICT(movie_id) DO UPDATE SET
                    field_name = excluded.field_name,
                    provider = excluded.provider,
                    provider_id = excluded.provider_id,
                    retrieved_at = excluded.retrieved_at
                """,
                (
                    movie_id,
                    data.get("YearVerifiedBy", "Wikidata"),
                    current_timestamp(),
                ),
            )
            logger.info(
                "Recorded source attribution for movie %s: release year verified by %s (wdt:P577)",
                movie_id,
                data.get("YearVerifiedBy"),
            )

        if data.get("TitleVerifiedBy"):
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_attribution (
                    movie_id INTEGER PRIMARY KEY,
                    field_name TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    provider_id TEXT,
                    retrieved_at TEXT NOT NULL,
                    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                INSERT INTO metadata_attribution (movie_id, field_name, provider, provider_id, retrieved_at)
                VALUES (?, 'title_alias_verification', ?, 'wdt:P345', ?)
                ON CONFLICT(movie_id) DO UPDATE SET
                    field_name = excluded.field_name,
                    provider = excluded.provider,
                    provider_id = excluded.provider_id,
                    retrieved_at = excluded.retrieved_at
                """,
                (
                    movie_id,
                    data.get("TitleVerifiedBy", "Wikidata"),
                    current_timestamp(),
                ),
            )
            logger.info(
                "Recorded source attribution for movie %s: title alias verified by %s (wdt:P345)",
                movie_id,
                data.get("TitleVerifiedBy"),
            )


# ============================================================
# 6. UPDATE ENRICHMENT STATUS
# ============================================================

def update_movie_status(connection, movie_id, status, error_message):
    """Record a movie's enrichment status and latest error with sensitive info sanitized."""
    clean_err = sanitize_error(error_message)[:500] if error_message is not None else None
    with connection:
        connection.execute(
            """
            UPDATE movie_details
            SET api_status = ?,
                fetched_at = ?,
                last_error = ?
            WHERE movie_id = ?
            """,
            (
                status,
                current_timestamp(),
                clean_err,
                movie_id,
            ),
        )


# ============================================================
# 7. PROCESS ONE MOVIE
# ============================================================

def validate_omdb_response(data, expected_title, year):
    """
    Validate an OMDb response dictionary against the expected title and release year.
    Returns (is_valid, failure_type, failure_message)
    where failure_type is 'not_found', 'error', or None.
    """
    if not isinstance(data, dict):
        return False, "error", "Response is not a valid JSON dictionary"

    if data.get("Response") != "True":
        api_error = data.get("Error", "Movie not found!")
        if api_error.strip().lower() == "movie not found!":
            return False, "not_found", api_error
        return False, "error", api_error

    returned_title = clean_value(data.get("Title")) or ""
    returned_year = get_year(data.get("Year"))

    # Validate title compatibility
    if not is_title_compatible(returned_title, expected_title):
        title_verified = False
        if ENABLE_SECONDARY_SOURCES:
            imdb_val = clean_value(data.get("imdbID"))
            if is_title_verified_by_authoritative_source(imdb_val, expected_title):
                data["TitleVerifiedBy"] = "Wikidata"
                title_verified = True
        if not title_verified:
            mismatch_msg = f"OMDb mismatch: expected '{expected_title}' ({year}), received '{returned_title}' ({returned_year})"
            return False, "not_found", mismatch_msg

    # Validate release year with 1-year tolerance, or verify original release year via authoritative source
    if year is not None and returned_year is not None:
        if abs(returned_year - int(year)) > 1:
            if ENABLE_SECONDARY_SOURCES:
                imdb_val = clean_value(data.get("imdbID"))
                if is_year_verified_by_authoritative_source(imdb_val, int(year)):
                    data["YearVerifiedBy"] = "Wikidata"
                else:
                    mismatch_msg = f"OMDb mismatch: expected '{expected_title}' ({year}), received '{returned_title}' ({returned_year})"
                    return False, "not_found", mismatch_msg
            else:
                mismatch_msg = f"OMDb mismatch: expected '{expected_title}' ({year}), received '{returned_title}' ({returned_year})"
                return False, "not_found", mismatch_msg

    # Mandatory fields: imdbID, Plot, and Director
    missing_fields = []
    if not clean_value(data.get("imdbID")):
        missing_fields.append("IMDb ID")
    if not clean_value(data.get("Plot")):
        missing_fields.append("Plot")
    if not clean_value(data.get("Director")):
        if ENABLE_SECONDARY_SOURCES:
            imdb_val = clean_value(data.get("imdbID"))
            wikidata_directors = fetch_wikidata_directors(imdb_val) if imdb_val else None
            if wikidata_directors:
                data["Director"] = wikidata_directors
                data["DirectorSource"] = "Wikidata"
                data["DirectorProviderId"] = "wdt:P57"
            else:
                missing_fields.append("Director")
        else:
            missing_fields.append("Director")

    if missing_fields:
        missing_msg = "Incomplete OMDb response. Missing: " + ", ".join(missing_fields)
        return False, "error", missing_msg

    return True, None, None


def try_imdb_fallback(connection, movie_id, title, year=None):
    """Attempt to enrich movie via authoritative IMDb ID from links.csv."""
    imdb_id = get_imdb_id_for_movie(movie_id)
    if not imdb_id:
        return None

    try:
        data = fetch_movie_details(imdb_id=imdb_id)
        is_valid, _, _ = validate_omdb_response(data, clean_title_year(title), year)
        if is_valid:
            save_movie_details(connection, movie_id, data)
            return "success"
        return None
    except (OMDbQuotaExceeded, OMDbAuthenticationError):
        raise
    except Exception:
        return None


def process_movie(connection, movie):
    """
    Fetch, validate, and save one movie's details.
    Follows authoritative priority:
    1. Query by authoritative IMDb ID from MovieLens links.csv when available.
    2. Fallback to title search if IMDb ID lookup is unavailable or fails.
    3. Handles missing optional fields (BoxOffice) cleanly.
    4. Records accurate 'not_found' vs 'error' status.
    5. Propagates fatal quota / auth exceptions without marking errors.
    """
    movie_id, title, year = movie
    expected_title = clean_title_year(title)

    logger.info(
        "Processing movie %s: %s (%s)",
        movie_id,
        title,
        year if year is not None else "year unknown",
    )

    imdb_id = get_imdb_id_for_movie(movie_id)
    last_failure_type = "not_found"
    last_failure_message = "Movie not found!"

    # -------------------------------------------------------------
    # Step 1: Authoritative IMDb ID lookup
    # -------------------------------------------------------------
    if imdb_id:
        try:
            logger.info("Querying OMDb by authoritative IMDb ID %s for movie %s: %s", imdb_id, movie_id, expected_title)
            data_id = fetch_movie_details(imdb_id=imdb_id)
            is_valid, fail_type, fail_msg = validate_omdb_response(data_id, expected_title, year)
            if is_valid:
                try:
                    save_movie_details(connection, movie_id, data_id)
                    logger.info("Enrichment successful via authoritative IMDb ID (%s): %s", imdb_id, title)
                    return "success"
                except sqlite3.IntegrityError as err:
                    conflict_row = connection.execute(
                        "SELECT movie_id FROM movie_details WHERE imdb_id = ? AND movie_id != ?",
                        (clean_value(data_id.get("imdbID")), movie_id),
                    ).fetchone()
                    conflict_id = conflict_row[0] if conflict_row else "unknown"
                    error_msg = f"Database constraint violation: duplicate IMDb ID {clean_value(data_id.get('imdbID'))} already enriched under movie_id {conflict_id}"
                    logger.error("Movie %s marked error: %s", title, error_msg)
                    update_movie_status(connection, movie_id, "error", error_msg)
                    return "error"

            last_failure_type = fail_type
            last_failure_message = fail_msg
            logger.warning(
                "Authoritative IMDb ID lookup for %s (%s) was incomplete or mismatched: %s",
                title,
                imdb_id,
                fail_msg,
            )
        except (OMDbQuotaExceeded, OMDbAuthenticationError):
            raise
        except (requests.exceptions.RequestException, ValueError) as err:
            logger.warning("IMDb ID request failed for %s (%s): %s", title, imdb_id, sanitize_error(err))
            last_failure_type = "error"
            last_failure_message = sanitize_error(err)

    # -------------------------------------------------------------
    # Step 2: Fallback to Title Search
    # -------------------------------------------------------------
    try:
        logger.info("Attempting title search fallback for movie %s: %s", movie_id, expected_title)
        data_title = fetch_movie_details(title=expected_title, year=year)
        is_valid, fail_type, fail_msg = validate_omdb_response(data_title, expected_title, year)

        if not is_valid:
            alt_searches = []
            m_art = re.match(r"^(.+?),\s*(the|a|an|le|la|les|el|los|las|der|die|das|il|lo|i|gli|un|une)\b", expected_title, re.IGNORECASE)
            if m_art:
                alt_searches.append(f"{m_art.group(2)} {m_art.group(1)}")
            base_t = re.sub(r"\s*\([^)]*\)", "", expected_title).strip()
            if base_t and base_t != expected_title:
                alt_searches.append(base_t)
                m_base_art = re.match(r"^(.+?),\s*(the|a|an|le|la|les|el|los|las|der|die|das|il|lo|i|gli|un|une)\b", base_t, re.IGNORECASE)
                if m_base_art:
                    alt_searches.append(f"{m_base_art.group(2)} {m_base_art.group(1)}")

            for alt_t in alt_searches:
                try:
                    alt_data = fetch_movie_details(title=alt_t, year=year)
                    alt_valid, alt_type, alt_msg = validate_omdb_response(alt_data, expected_title, year)
                    if alt_valid:
                        data_title = alt_data
                        is_valid = True
                        fail_type = None
                        fail_msg = None
                        break
                except (OMDbQuotaExceeded, OMDbAuthenticationError):
                    raise
                except Exception:
                    pass

        if is_valid:
            try:
                save_movie_details(connection, movie_id, data_title)
                logger.info("Enrichment successful via title search fallback: %s", title)
                return "success"
            except sqlite3.IntegrityError as err:
                conflict_row = connection.execute(
                    "SELECT movie_id FROM movie_details WHERE imdb_id = ? AND movie_id != ?",
                    (clean_value(data_title.get("imdbID")), movie_id),
                ).fetchone()
                conflict_id = conflict_row[0] if conflict_row else "unknown"
                error_msg = f"Database constraint violation: duplicate IMDb ID {clean_value(data_title.get('imdbID'))} already enriched under movie_id {conflict_id}"
                logger.error("Movie %s marked error: %s", title, error_msg)
                update_movie_status(connection, movie_id, "error", error_msg)
                return "error"

        # If title search also failed, determine terminal status
        if fail_type == "error" or last_failure_type == "error":
            final_status = "error"
            final_msg = fail_msg if fail_type == "error" else last_failure_message
        else:
            final_status = "not_found"
            final_msg = fail_msg

        update_movie_status(connection, movie_id, final_status, final_msg)
        logger.warning("Movie %s marked %s: %s", title, final_status, final_msg)
        return final_status

    except (OMDbQuotaExceeded, OMDbAuthenticationError):
        # Critical stopping conditions propagate immediately to main loop.
        # Movie is NOT marked as failed and remains in its pending state.
        raise

    except (requests.exceptions.RequestException, ValueError) as error:
        update_movie_status(
            connection,
            movie_id,
            "error",
            sanitize_error(error),
        )
        logger.error(
            "Request or parsing error for %s: %s",
            title,
            sanitize_error(error),
        )
        return "error"


# ============================================================
# 8. MAIN PROGRAM
# ============================================================

def main(max_movies=None):
    """Process pending movies until complete, quota is exhausted, authentication fails, or max limit is reached."""
    limit_movies = MAX_MOVIES_PER_RUN if max_movies is None else max_movies

    if not API_KEY:
        raise RuntimeError(
            "OMDB_API_KEY is missing. Check your .env file."
        )

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}. Run etl.py first."
        )

    totals = {
        "success": 0,
        "not_found": 0,
        "error": 0,
    }

    quota_exhausted = False
    auth_failed = False
    max_reached = False
    processed_count = 0

    if limit_movies and limit_movies > 0:
        logger.info(
            "Run configured with limit: maximum %s movie(s) will be processed.",
            limit_movies,
        )

    with SingleInstanceGuard(LOCK_FILE):
        connection = sqlite3.connect(DB_PATH)
        try:
            connection.execute("PRAGMA foreign_keys = ON")

            while True:
                # Calculate how many movies to fetch in this batch
                if limit_movies and limit_movies > 0:
                    remaining = limit_movies - processed_count
                    if remaining <= 0:
                        max_reached = True
                        break
                    fetch_limit = min(BATCH_SIZE, remaining)
                else:
                    fetch_limit = BATCH_SIZE

                movies = get_pending_movies(connection, limit=fetch_limit)

                if not movies:
                    logger.info("No pending movies remain.")
                    break

                logger.info(
                    "Processing batch of %s movies (processed so far: %s).",
                    len(movies),
                    processed_count,
                )

                for movie in movies:
                    try:
                        outcome = process_movie(connection, movie)
                        totals[outcome] += 1
                        processed_count += 1

                    except OMDbAuthenticationError as error:
                        logger.critical(
                            "Fatal authentication error: %s. "
                            "Stopping enrichment pipeline immediately. "
                            "The current movie and remaining movies remain pending. "
                            "Please verify your OMDB_API_KEY in .env.",
                            sanitize_error(error),
                        )
                        auth_failed = True
                        break

                    except OMDbQuotaExceeded as error:
                        logger.warning(
                            "OMDb quota exhausted: %s. "
                            "Stopping; this movie remains pending.",
                            sanitize_error(error),
                        )
                        quota_exhausted = True
                        break

                    except sqlite3.Error as error:
                        logger.error(
                            "Database error for movie ID %s: %s",
                            movie[0],
                            sanitize_error(error),
                        )
                        try:
                            update_movie_status(
                                connection,
                                movie[0],
                                "error",
                                f"Database error: {sanitize_error(error)}",
                            )
                            totals["error"] += 1
                            processed_count += 1
                        except sqlite3.Error:
                            logger.exception(
                                "Could not record database error for movie ID %s",
                                movie[0],
                            )
                            raise

                    except Exception:
                        logger.exception(
                            "Unexpected failure for movie ID %s",
                            movie[0],
                        )

                        try:
                            update_movie_status(
                                connection,
                                movie[0],
                                "error",
                                "Unexpected error; see enrichment.log",
                            )
                        except sqlite3.Error:
                            logger.exception(
                                "Could not record error for movie ID %s",
                                movie[0],
                            )
                            raise

                        totals["error"] += 1
                        processed_count += 1

                    finally:
                        if not quota_exhausted and not auth_failed:
                            time.sleep(REQUEST_DELAY_SECONDS)

                    if limit_movies and limit_movies > 0 and processed_count >= limit_movies:
                        max_reached = True
                        break

                    if quota_exhausted or auth_failed:
                        break

                if quota_exhausted or auth_failed or max_reached:
                    break
        finally:
            connection.close()

    logger.info(
        "Enrichment summary | success: %s | not found: %s | errors: %s | total processed: %s",
        totals["success"],
        totals["not_found"],
        totals["error"],
        processed_count,
    )

    if max_reached:
        logger.info(
            "Enrichment run stopped: reached maximum movie limit of %s for this run.",
            limit_movies,
        )

    if auth_failed:
        logger.error(
            "Enrichment halted due to authentication failure (HTTP 401). "
            "No pending movies were corrupted or marked as failed."
        )

    if quota_exhausted:
        logger.warning(
            "Enrichment paused because the API quota was exhausted. "
            "Run this script again when quota is available."
        )

    return {
        "quota_exhausted": quota_exhausted,
        "auth_failed": auth_failed,
        "max_reached": max_reached,
        "totals": totals,
        "processed_count": processed_count,
    }


if __name__ == "__main__":
    main()
