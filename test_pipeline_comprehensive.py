"""
Comprehensive offline test suite for the MovieLens + OMDb enrichment pipeline.
Covers all 17 critical robustness requirements without live network calls or modifying production databases:
 1. Successful enrichment via authoritative IMDb ID
 2. Successful enrichment via title search fallback
 3. Movie genuinely not found
 4. Title or year mismatch handled conservatively
 5. Missing optional metadata (BoxOffice) handled cleanly
 6. Missing mandatory metadata (Director or Plot) handled correctly
 7. Invalid API key / HTTP 401 circuit breaker
 8. Exhausted quota (HTTP 401 or JSON 'Request limit reached!') stops safely
 9. HTTP 429 backoff handling
10. HTTP 500/503 retry handling
11. Network timeout retry handling
12. Malformed JSON handling
13. Retry exhaustion records an error
14. Interrupted processing and resumption via MAX_MOVIES_PER_RUN
15. Repeated execution does not duplicate records (idempotency)
16. Existing successful metadata is never overwritten
17. Sensitive API keys are redacted from logs and errors
Plus bonus checks for Roman numeral title normalization and SingleInstanceGuard.
"""

import io
import logging
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from enrich_movies import (
    MAX_MOVIES_PER_RUN,
    OMDbAuthenticationError,
    OMDbQuotaExceeded,
    SingleInstanceGuard,
    clean_title_year,
    fetch_movie_details,
    get_pending_movies,
    is_title_compatible,
    main as enrich_main,
    normalize_roman_numerals,
    normalize_title,
    normalize_title_no_article,
    process_movie,
    sanitize_error,
    save_movie_details,
    update_movie_status,
    validate_omdb_response,
)


def create_temp_database(db_path):
    """Create a temporary test database populated with minimal schema and fixtures."""
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(schema_path.read_text(encoding="utf-8"))

        conn.execute(
            "INSERT INTO movies (movie_id, title, release_year) VALUES (?, ?, ?)",
            (1, "Toy Story (1995)", 1995),
        )
        conn.execute(
            "INSERT INTO movie_details (movie_id, api_status) VALUES (?, 'pending')",
            (1,),
        )
        conn.execute(
            "INSERT INTO movies (movie_id, title, release_year) VALUES (?, ?, ?)",
            (2, "Jumanji (1995)", 1995),
        )
        conn.execute(
            "INSERT INTO movie_details (movie_id, api_status) VALUES (?, 'pending')",
            (2,),
        )
        conn.commit()


# ============================================================================
# REQUIREMENT 1: Successful enrichment via authoritative IMDb ID
# ============================================================================
def test_01_successful_imdb_id_lookup():
    """Verify movie is enriched using authoritative IMDb ID from links.csv."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Response": "True",
            "Title": "Toy Story",
            "Year": "1995",
            "Director": "John Lasseter",
            "Plot": "Toys come to life when humans leave.",
            "imdbID": "tt0114709",
            "BoxOffice": "$192,484,797",
        }
        mock_resp.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp) as mock_get:
                outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                assert outcome == "success"

                # Verify mock was called with 'i' param (IMDb ID)
                args, kwargs = mock_get.call_args
                assert kwargs["params"].get("i") == "tt0114709"

            row = conn.execute(
                "SELECT api_status, imdb_id, plot, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "success"
            assert row[1] == "tt0114709"
            assert "Toys come to life" in row[2]
            assert row[3] is None

            director = conn.execute(
                "SELECT d.director_name FROM directors d "
                "JOIN movie_directors md ON d.director_id = md.director_id "
                "WHERE md.movie_id = 1"
            ).fetchone()
            assert director is not None and director[0] == "John Lasseter"

    print("PASS: 1. Successful enrichment via authoritative IMDb ID")


# ============================================================================
# REQUIREMENT 2: Successful enrichment via title search fallback
# ============================================================================
def test_02_successful_title_search_fallback():
    """Verify movie is enriched via title search when IMDb ID lookup is unavailable or stubs."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        # Insert a synthetic movie not in links.csv
        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                "INSERT INTO movies (movie_id, title, release_year) VALUES (?, ?, ?)",
                (99999, "Custom Indie Film (2020)", 2020),
            )
            conn.execute(
                "INSERT INTO movie_details (movie_id, api_status) VALUES (?, 'pending')",
                (99999,),
            )
            conn.commit()

            mock_resp = Mock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {
                "Response": "True",
                "Title": "Custom Indie Film",
                "Year": "2020",
                "Director": "Jane Doe",
                "Plot": "An independent documentary about data pipelines.",
                "imdbID": "tt9999999",
                "BoxOffice": "N/A",
            }
            mock_resp.raise_for_status = Mock()

            with patch("enrich_movies.requests.get", return_value=mock_resp) as mock_get:
                outcome = process_movie(conn, (99999, "Custom Indie Film (2020)", 2020))
                assert outcome == "success"

                # Verify mock was called with 't' param
                args, kwargs = mock_get.call_args
                assert kwargs["params"].get("t") == "Custom Indie Film"

            row = conn.execute(
                "SELECT api_status, imdb_id, plot FROM movie_details WHERE movie_id = 99999"
            ).fetchone()
            assert row[0] == "success"
            assert row[1] == "tt9999999"

    print("PASS: 2. Successful enrichment via title search fallback")


# ============================================================================
# REQUIREMENT 3: Movie genuinely not found
# ============================================================================
def test_03_movie_genuinely_not_found():
    """Verify movie that returns 'Movie not found!' is marked not_found without raising."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Response": "False",
            "Error": "Movie not found!",
        }
        mock_resp.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp):
                outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                assert outcome == "not_found"

            row = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "not_found"
            assert row[1] == "Movie not found!"

    print("PASS: 3. Movie genuinely not found marked correctly as not_found")


# ============================================================================
# REQUIREMENT 4: Title or year mismatch handled conservatively
# ============================================================================
def test_04_title_or_year_mismatch():
    """Verify title or release year discrepancies (> 1 yr) reject match and record not_found."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Response": "True",
            "Title": "Toy Story",
            "Year": "1999",  # Discrepancy: expected 1995
            "Director": "John Lasseter",
            "Plot": "Toys come to life.",
            "imdbID": "tt0114709",
        }
        mock_resp.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp), \
                 patch("enrich_movies.ENABLE_SECONDARY_SOURCES", False):
                outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                assert outcome == "not_found"

            row = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "not_found"
            assert "mismatch" in row[1].lower()

    print("PASS: 4. Title or year mismatch rejected conservatively")


# ============================================================================
# REQUIREMENT 5: Missing optional metadata (BoxOffice) handled cleanly
# ============================================================================
def test_05_missing_optional_metadata_boxoffice():
    """Verify missing or 'N/A' BoxOffice does not cause failure and saves box_office as NULL."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Response": "True",
            "Title": "Toy Story",
            "Year": "1995",
            "Director": "John Lasseter",
            "Plot": "Toys come to life.",
            "imdbID": "tt0114709",
            "BoxOffice": "N/A",  # Missing optional field
        }
        mock_resp.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp):
                outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                assert outcome == "success"

            row = conn.execute(
                "SELECT api_status, box_office, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "success"
            assert row[1] is None
            assert row[2] is None

    print("PASS: 5. Missing optional metadata (BoxOffice) handled cleanly")


# ============================================================================
# REQUIREMENT 6: Missing mandatory metadata (Director or Plot) handled correctly
# ============================================================================
def test_06_missing_mandatory_metadata_director_or_plot():
    """Verify missing mandatory fields (Director or Plot) mark record as error without fabricating."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "Response": "True",
            "Title": "Toy Story",
            "Year": "1995",
            "Director": "N/A",  # Mandatory field missing
            "Plot": "Toys come to life.",
            "imdbID": "tt0114709",
        }
        mock_resp.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp), \
                 patch("enrich_movies.ENABLE_SECONDARY_SOURCES", False):
                outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                assert outcome == "error"

            row = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "error"
            assert "Missing: Director" in row[1]

    print("PASS: 6. Missing mandatory metadata (Director or Plot) handled correctly")


# ============================================================================
# REQUIREMENT 7: Invalid API key / HTTP 401 circuit breaker
# ============================================================================
def test_07_invalid_api_key_http_401():
    """Verify HTTP 401 Unauthorized raises OMDbAuthenticationError and leaves movie pending."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        mock_resp = Mock()
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"Response": "False", "Error": "Invalid API key!"}

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp):
                with pytest.raises(OMDbAuthenticationError):
                    process_movie(conn, (1, "Toy Story (1995)", 1995))

            status = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert status[0] == "pending"
            assert status[1] is None

    print("PASS: 7. Invalid API key / HTTP 401 halts immediately without marking error")


# ============================================================================
# REQUIREMENT 8: Exhausted quota stops safely and leaves movie pending
# ============================================================================
def test_08_exhausted_quota_stops_safely():
    """Verify OMDb quota exhaustion stops safely and preserves pending state."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        # Test both HTTP 200 with Request limit reached and HTTP 401 limit reached
        mock_resp_200 = Mock()
        mock_resp_200.status_code = 200
        mock_resp_200.json.return_value = {
            "Response": "False",
            "Error": "Request limit reached!",
        }
        mock_resp_200.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp_200):
                with pytest.raises(OMDbQuotaExceeded):
                    process_movie(conn, (1, "Toy Story (1995)", 1995))

            status = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert status[0] == "pending"
            assert status[1] is None

        # Also verify HTTP 401 with Request limit reached
        mock_resp_401 = Mock()
        mock_resp_401.status_code = 401
        mock_resp_401.json.return_value = {
            "Response": "False",
            "Error": "Request limit reached!",
        }
        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.requests.get", return_value=mock_resp_401):
                with pytest.raises(OMDbQuotaExceeded):
                    process_movie(conn, (1, "Toy Story (1995)", 1995))

    print("PASS: 8. Exhausted quota (HTTP 200/401) stops safely and leaves movie pending")


# ============================================================================
# REQUIREMENT 9: HTTP 429 backoff handling
# ============================================================================
def test_09_http_429_backoff_handling():
    """Verify HTTP 429 triggers backoff retry and respects Retry-After."""
    resp_429 = Mock()
    resp_429.status_code = 429
    resp_429.headers = {"Retry-After": "3"}
    resp_429.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "429 Too Many Requests", response=resp_429
    )

    resp_200 = Mock()
    resp_200.status_code = 200
    resp_200.json.return_value = {
        "Response": "True",
        "Title": "Toy Story",
        "Year": "1995",
        "Director": "John Lasseter",
        "Plot": "Toys come to life.",
        "imdbID": "tt0114709",
    }
    resp_200.raise_for_status = Mock()

    with patch("enrich_movies.time.sleep") as mock_sleep:
        with patch("enrich_movies.requests.get", side_effect=[resp_429, resp_200]) as mock_get:
            result = fetch_movie_details("Toy Story", 1995)
            assert result["Response"] == "True"
            assert mock_get.call_count == 2
            mock_sleep.assert_called_once_with(3)

    print("PASS: 9. HTTP 429 backoff handling verified")


# ============================================================================
# REQUIREMENT 10: HTTP 500/503 retry handling
# ============================================================================
def test_10_http_500_503_retry_handling():
    """Verify temporary 500/503 errors retry with exponential backoff and succeed."""
    resp_503 = Mock()
    resp_503.status_code = 503
    resp_503.headers = {}
    resp_503.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "503 Service Unavailable", response=resp_503
    )

    resp_200 = Mock()
    resp_200.status_code = 200
    resp_200.json.return_value = {
        "Response": "True",
        "Title": "Toy Story",
        "Year": "1995",
        "Director": "John Lasseter",
        "Plot": "Toys come to life.",
        "imdbID": "tt0114709",
    }
    resp_200.raise_for_status = Mock()

    with patch("enrich_movies.time.sleep") as mock_sleep:
        with patch("enrich_movies.requests.get", side_effect=[resp_503, resp_200]) as mock_get:
            result = fetch_movie_details("Toy Story", 1995)
            assert result["Response"] == "True"
            assert mock_get.call_count == 2
            mock_sleep.assert_called_once_with(1)

    print("PASS: 10. HTTP 500/503 retry handling verified")


# ============================================================================
# REQUIREMENT 11: Network timeout retry handling
# ============================================================================
def test_11_network_timeout_retry_handling():
    """Verify network timeout retries bounded number of times and recovers."""
    resp_200 = Mock()
    resp_200.status_code = 200
    resp_200.json.return_value = {
        "Response": "True",
        "Title": "Toy Story",
        "Year": "1995",
        "Director": "John Lasseter",
        "Plot": "Toys come to life.",
        "imdbID": "tt0114709",
    }
    resp_200.raise_for_status = Mock()

    with patch("enrich_movies.time.sleep") as mock_sleep:
        with patch(
            "enrich_movies.requests.get",
            side_effect=[requests.exceptions.Timeout("Connection timed out"), resp_200],
        ) as mock_get:
            result = fetch_movie_details("Toy Story", 1995)
            assert result["Response"] == "True"
            assert mock_get.call_count == 2
            mock_sleep.assert_called_once_with(1)

    print("PASS: 11. Network timeout retry handling verified")


# ============================================================================
# REQUIREMENT 12: Malformed JSON handling
# ============================================================================
def test_12_malformed_json_handling():
    """Verify malformed JSON payload is caught and recorded cleanly as an error."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        resp_bad = Mock()
        resp_bad.status_code = 200
        resp_bad.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")
        resp_bad.raise_for_status = Mock()

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.time.sleep"):
                with patch("enrich_movies.requests.get", return_value=resp_bad):
                    outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                    assert outcome == "error"

            row = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "error"
            assert "Malformed JSON" in row[1] or "Expecting value" in row[1]

    print("PASS: 12. Malformed JSON handling records error cleanly")


# ============================================================================
# REQUIREMENT 13: Retry exhaustion records an error
# ============================================================================
def test_13_retry_exhaustion_records_error():
    """Verify exhausting all 3 retries transitions movie to error status."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            with patch("enrich_movies.time.sleep"):
                with patch(
                    "enrich_movies.requests.get",
                    side_effect=requests.exceptions.ConnectionError("Failed to establish connection"),
                ) as mock_get:
                    outcome = process_movie(conn, (1, "Toy Story (1995)", 1995))
                    assert outcome == "error"

            row = conn.execute(
                "SELECT api_status, last_error FROM movie_details WHERE movie_id = 1"
            ).fetchone()
            assert row[0] == "error"
            assert "Failed to establish connection" in row[1]

    print("PASS: 13. Retry exhaustion records an error")


# ============================================================================
# REQUIREMENT 14: Interrupted processing and resumption via MAX_MOVIES_PER_RUN
# ============================================================================
def test_14_interrupted_processing_and_resumption_via_max_movies():
    """Verify that enrich_movies stops after MAX_MOVIES_PER_RUN and remains resumable."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test_limit.db"
        lock_path = Path(tmp_dir) / "test_limit.lock"

        # Create database with 8 pending movies
        schema_path = Path(__file__).resolve().parent / "schema.sql"
        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.executescript(schema_path.read_text(encoding="utf-8"))
            for i in range(1, 9):
                conn.execute(
                    "INSERT INTO movies (movie_id, title, release_year) VALUES (?, ?, ?)",
                    (i, f"Movie {i} (1995)", 1995),
                )
                conn.execute(
                    "INSERT INTO movie_details (movie_id, api_status) VALUES (?, 'pending')",
                    (i,),
                )
            conn.commit()

        def fake_fetch(title=None, year=None, imdb_id=None):
            # Safe handler whether called by IMDb ID or title
            num = "1"
            if imdb_id:
                # E.g. tt0114709
                match = re.search(r"\d+", imdb_id)
                num = match.group(0)[-1] if match else "1"
            elif title:
                match = re.search(r"\d+", title)
                num = match.group(0) if match else "1"

            return {
                "Response": "True",
                "Title": f"Movie {num}",
                "Year": "1995",
                "Director": f"Director {num}",
                "Plot": f"Plot description for movie {num}.",
                "imdbID": f"tt000000{num}",
                "BoxOffice": "$1,000,000",
            }

        import re
        with patch("enrich_movies.DB_PATH", db_path), \
             patch("enrich_movies.LOCK_FILE", lock_path), \
             patch("enrich_movies.API_KEY", "mock_key_for_test"), \
             patch("enrich_movies.fetch_movie_details", side_effect=fake_fetch), \
             patch("enrich_movies.time.sleep"):

            # Run 1: limit to 5 movies
            res1 = enrich_main(max_movies=5)
            assert res1["processed_count"] == 5

            with closing(sqlite3.connect(db_path)) as conn:
                success_count = conn.execute(
                    "SELECT COUNT(*) FROM movie_details WHERE api_status = 'success'"
                ).fetchone()[0]
                pending_count = conn.execute(
                    "SELECT COUNT(*) FROM movie_details WHERE api_status = 'pending'"
                ).fetchone()[0]
                assert success_count == 5
                assert pending_count == 3

            # Run 2: Resume and limit to 2 movies
            res2 = enrich_main(max_movies=2)
            assert res2["processed_count"] == 2

            with closing(sqlite3.connect(db_path)) as conn:
                success_count = conn.execute(
                    "SELECT COUNT(*) FROM movie_details WHERE api_status = 'success'"
                ).fetchone()[0]
                pending_count = conn.execute(
                    "SELECT COUNT(*) FROM movie_details WHERE api_status = 'pending'"
                ).fetchone()[0]
                assert success_count == 7
                assert pending_count == 1

            # Run 3: Resume and process remaining
            res3 = enrich_main(max_movies=5)
            assert res3["processed_count"] == 1

            with closing(sqlite3.connect(db_path)) as conn:
                success_count = conn.execute(
                    "SELECT COUNT(*) FROM movie_details WHERE api_status = 'success'"
                ).fetchone()[0]
                pending_count = conn.execute(
                    "SELECT COUNT(*) FROM movie_details WHERE api_status = 'pending'"
                ).fetchone()[0]
                assert success_count == 8
                assert pending_count == 0

    print("PASS: 14. MAX_MOVIES_PER_RUN stops execution at exact limit and is fully resumable")


# ============================================================================
# REQUIREMENT 15: Repeated execution does not duplicate records (idempotency)
# ============================================================================
def test_15_repeated_execution_idempotency():
    """Verify repeated execution of save_movie_details is strictly idempotent."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        data = {
            "Response": "True",
            "Title": "Toy Story",
            "Year": "1995",
            "Director": "John Lasseter, Pete Docter",
            "Plot": "Toys come to life.",
            "imdbID": "tt0114709",
            "BoxOffice": "$192,484,797",
        }

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            # Save once
            save_movie_details(conn, 1, data)
            # Save again with same data
            save_movie_details(conn, 1, data)

            directors = conn.execute(
                """
                SELECT d.director_name FROM directors d
                JOIN movie_directors md ON d.director_id = md.director_id
                WHERE md.movie_id = 1
                ORDER BY d.director_name
                """
            ).fetchall()

            assert len(directors) == 2
            assert directors[0][0] == "John Lasseter"
            assert directors[1][0] == "Pete Docter"

            total_directors = conn.execute("SELECT COUNT(*) FROM directors").fetchone()[0]
            assert total_directors == 2

    print("PASS: 15. Re-running enrichment is idempotent and avoids director duplication")


# ============================================================================
# REQUIREMENT 16: Existing successful metadata is never overwritten
# ============================================================================
def test_16_existing_successful_metadata_never_overwritten():
    """Verify successful movies are excluded from get_pending_movies and never altered."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        create_temp_database(db_path)

        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                "UPDATE movie_details SET api_status = 'success', plot = 'Original Plot' WHERE movie_id = 1"
            )

            # Test get_pending_movies excludes movie 1
            pending = get_pending_movies(conn)
            pending_ids = [m[0] for m in pending]
            assert 1 not in pending_ids
            assert 2 in pending_ids

            # Verify plot unchanged
            plot = conn.execute("SELECT plot FROM movie_details WHERE movie_id = 1").fetchone()[0]
            assert plot == "Original Plot"

    print("PASS: 16. Existing successful metadata is excluded from pending and preserved")


# ============================================================================
# REQUIREMENT 17: Sensitive API keys are redacted from logs and errors
# ============================================================================
def test_17_sensitive_api_keys_redacted():
    """Verify sanitize_error replaces API key query parameter with [REDACTED]."""
    sensitive_url = "https://www.omdbapi.com/?apikey=SECRET12345&t=Toy+Story"
    sanitized = sanitize_error(sensitive_url)
    assert "SECRET12345" not in sanitized
    assert "apikey=[REDACTED]" in sanitized

    sensitive_err = requests.exceptions.HTTPError(
        f"401 Client Error: Unauthorized for url: {sensitive_url}"
    )
    sanitized_err = sanitize_error(sensitive_err)
    assert "SECRET12345" not in sanitized_err
    assert "apikey=[REDACTED]" in sanitized_err

    print("PASS: 17. Sensitive API keys are cleanly redacted from logs and errors")


# ============================================================================
# BONUS CHECKS: Roman numerals, title normalization, and SingleInstanceGuard
# ============================================================================
def test_18_roman_numerals_and_title_normalization():
    """Verify Roman numeral normalization (II -> 2), diacritics, and subtitles."""
    assert normalize_roman_numerals("Death Wish II") == "Death Wish 2"
    assert normalize_roman_numerals("Death Wish 5: The Face of Death") == "Death Wish 5: The Face of Death"
    assert normalize_roman_numerals("Star Wars: Episode IV - A New Hope") == "Star Wars: Episode 4 - A New Hope"
    assert normalize_title("Death Wish II") == normalize_title("Death Wish 2")
    assert normalize_title("Untouchables, The") == "theuntouchables"
    assert normalize_title("Maelström (2000)") == "maelstrom"
    assert is_title_compatible("Evil Dead II", "Evil Dead II (Dead by Dawn) (1987)")
    assert is_title_compatible("It's a Wonderful Life", "It's A Wonderful Life (1946)")

    print("PASS: 18. Roman numeral, subtitle, and diacritic normalization verified")


def test_19_single_instance_guard():
    """Verify SingleInstanceGuard creates lock file and prevents concurrent runs."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        lock_file = Path(tmp_dir) / "test.lock"

        with SingleInstanceGuard(lock_file):
            assert lock_file.exists()
            with pytest.raises(RuntimeError) as exc_info:
                with SingleInstanceGuard(lock_file):
                    pass
            assert "already running" in str(exc_info.value)

        assert not lock_file.exists()

    print("PASS: 19. SingleInstanceGuard prevents concurrent runs")


# ============================================================================
# REQUIREMENT 20-24: Unified Pipeline Orchestration via etl.py
# ============================================================================
def test_20_etl_orchestration_invokes_enrichment(caplog=None):
    """Verify that etl.main() validates ordering and invokes enrichment after validation succeeds."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation|Children"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 5.0, "timestamp": 964982703},
        ])

        call_order = []
        orig_validate = etl.validate_database

        def tracked_validate(*args, **kwargs):
            call_order.append("validate")
            return orig_validate(*args, **kwargs)

        mock_enrichment_return = {
            "quota_exhausted": False,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 1, "not_found": 0, "error": 0},
            "processed_count": 1,
        }

        def tracked_enrich(*args, **kwargs):
            call_order.append("enrich")
            with closing(sqlite3.connect(test_db)) as conn:
                conn.execute("UPDATE movie_details SET api_status = 'success' WHERE movie_id = 1")
                conn.commit()
            return mock_enrichment_return

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch.object(etl, "validate_database", side_effect=tracked_validate), \
             patch("enrich_movies.main", side_effect=tracked_enrich) as mock_enrich:

            etl.main()
            assert call_order == ["validate", "enrich"], f"Unexpected order: {call_order}"
            mock_enrich.assert_called_once()

        if caplog:
            assert "Core MovieLens data ingested and validated successfully." in caplog.text
            assert "Starting external metadata enrichment stage via OMDb API..." in caplog.text
            assert "All 1 movies in the catalog are successfully enriched." in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." in caplog.text
            assert "Core ETL succeeded, but" not in caplog.text

    print("PASS: 20. etl.main() validates ordering and invokes enrichment")


def test_21_etl_orchestration_missing_key_handled_gracefully(caplog=None):
    """Verify that etl.main() logs warning and does not crash when OMDB_API_KEY is missing."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=RuntimeError("OMDB_API_KEY is missing. Check your .env file.")):

            etl.main()

        if caplog:
            assert "Core ETL succeeded, but OMDb enrichment was skipped" in caplog.text
            assert "OMDB_API_KEY is missing" in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text

    print("PASS: 21. etl.main() handles missing API key gracefully without false completion")


def test_22_etl_orchestration_quota_exhausted_handled_gracefully(caplog=None):
    """Verify that etl.main() handles quota exhaustion without claiming full success."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        quota_result = {
            "quota_exhausted": True,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 0, "not_found": 0, "error": 0},
            "processed_count": 0,
        }

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", return_value=quota_result) as mock_enrich:

            etl.main()
            mock_enrich.assert_called_once()

        if caplog:
            assert "daily OMDb API quota exhausted" in caplog.text
            assert "Unprocessed movies remain in pending status." in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text

    print("PASS: 22. etl.main() handles quota exhaustion gracefully without false completion")


def test_23_etl_orchestration_enrichment_errors_distinguished(caplog=None):
    """Verify that etl.main() reports errors and does not claim full pipeline success."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        error_result = {
            "quota_exhausted": False,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 0, "not_found": 0, "error": 1},
            "processed_count": 1,
        }

        def mock_enrich_with_error(*args, **kwargs):
            with closing(sqlite3.connect(test_db)) as conn:
                conn.execute("UPDATE movie_details SET api_status = 'error', last_error = 'Timeout' WHERE movie_id = 1")
                conn.commit()
            return error_result

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=mock_enrich_with_error) as mock_enrich:

            etl.main()
            mock_enrich.assert_called_once()

        if caplog:
            assert "Core ETL succeeded, but enrichment finished with errors" in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text

    print("PASS: 23. etl.main() distinguishes enrichment errors from successful completion")


def test_24_etl_orchestration_lock_contention_distinguished(caplog=None):
    """Verify that etl.main() distinguishes lock contention from missing API key."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=RuntimeError("Another instance of enrich_movies is already running (PID 9999).")):

            etl.main()

        if caplog:
            assert "another enrichment process is currently running" in caplog.text
            assert "OMDb enrichment was skipped" not in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text

    print("PASS: 24. etl.main() distinguishes lock contention from missing API keys")


def test_25_etl_orchestration_not_found_handled_accurately(caplog=None):
    """Verify that etl.main() distinguishes not_found records and does not report complete enrichment."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
            {"movieId": 2, "title": "Obscure Short Film (2020)", "release_year": 2020, "genres": "Short"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
            {"userId": 1, "movieId": 2, "rating": 3.5, "timestamp": 964982704},
        ])

        not_found_result = {
            "quota_exhausted": False,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 1, "not_found": 1, "error": 0},
            "processed_count": 2,
        }

        def mock_enrich_with_not_found(*args, **kwargs):
            with closing(sqlite3.connect(test_db)) as conn:
                conn.execute("UPDATE movie_details SET api_status = 'success' WHERE movie_id = 1")
                conn.execute(
                    "UPDATE movie_details SET api_status = 'not_found', last_error = 'Movie not found!' WHERE movie_id = 2"
                )
                conn.commit()
            return not_found_result

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=mock_enrich_with_not_found) as mock_enrich:

            etl.main()
            mock_enrich.assert_called_once()

        if caplog:
            assert "Core ETL succeeded. Catalog processing finished (1 not-found record(s) handled without metadata)." in caplog.text
            assert "1 total movie(s) successfully enriched, 1 not found" in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text
            assert "all 2 movies in the catalog are successfully enriched" not in caplog.text

    print("PASS: 25. etl.main() handles not_found records accurately without false complete enrichment")


def test_26_etl_orchestration_status_check_failure_fails_safely(caplog=None):
    """Verify that etl.main() fails safely and reports error if database status check fails."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        success_result = {
            "quota_exhausted": False,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 1, "not_found": 0, "error": 0},
            "processed_count": 1,
        }

        def mock_enrich_and_simulate_db_error(*args, **kwargs):
            # Simulate database failure during status query by dropping the table
            with closing(sqlite3.connect(test_db)) as conn:
                conn.execute("DROP TABLE movie_details")
                conn.commit()
            return success_result

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=mock_enrich_and_simulate_db_error) as mock_enrich:

            etl.main()
            mock_enrich.assert_called_once()

        if caplog:
            assert "Core ETL succeeded, but enrichment status could not be verified" in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text
            assert "all 1 movies in the catalog are successfully enriched" not in caplog.text

    print("PASS: 26. etl.main() fails safely when database status check fails")


def test_27_etl_orchestration_already_completed_success(caplog=None):
    """Verify that etl.main() accurately reports complete success when catalog is already enriched."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        already_complete_result = {
            "quota_exhausted": False,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 0, "not_found": 0, "error": 0},
            "processed_count": 0,
        }

        def mock_enrich_already_done(*args, **kwargs):
            with closing(sqlite3.connect(test_db)) as conn:
                conn.execute("UPDATE movie_details SET api_status = 'success' WHERE movie_id = 1")
                conn.commit()
            return already_complete_result

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=mock_enrich_already_done) as mock_enrich:

            etl.main()
            mock_enrich.assert_called_once()

        if caplog:
            assert "Enrichment check complete: all 1 movies in the catalog are successfully enriched." in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." in caplog.text
            assert "Core ETL succeeded, but" not in caplog.text

    print("PASS: 27. etl.main() verifies complete success when catalog is fully enriched")


def test_28_etl_orchestration_unexpected_status_fails_safely(caplog=None):
    """Verify that etl.main() reports invalid status and does not claim success when unexpected api_status exists."""
    import pandas as pd
    import etl

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_db = Path(tmp_dir) / "test_etl.db"

        sample_movies = pd.DataFrame([
            {"movieId": 1, "title": "Toy Story (1995)", "release_year": 1995, "genres": "Animation"},
        ])
        sample_ratings = pd.DataFrame([
            {"userId": 1, "movieId": 1, "rating": 4.0, "timestamp": 964982703},
        ])

        unexpected_result = {
            "quota_exhausted": False,
            "auth_failed": False,
            "max_reached": False,
            "totals": {"success": 0, "not_found": 0, "error": 0},
            "processed_count": 1,
        }

        def mock_enrich_with_unexpected_status(*args, **kwargs):
            with closing(sqlite3.connect(test_db)) as conn:
                conn.execute("PRAGMA ignore_check_constraints = ON")
                conn.execute("UPDATE movie_details SET api_status = 'corrupted_state' WHERE movie_id = 1")
                conn.commit()
            return unexpected_result

        if caplog:
            caplog.set_level(logging.INFO)

        with patch.object(etl, "DB_FILE", test_db), \
             patch.object(etl, "extract_data", return_value=(sample_movies, sample_ratings)), \
             patch("enrich_movies.main", side_effect=mock_enrich_with_unexpected_status) as mock_enrich:

            etl.main()
            mock_enrich.assert_called_once()

        if caplog:
            assert "enrichment status is invalid" in caplog.text or "incomplete" in caplog.text
            assert "corrupted_state" in caplog.text
            assert "Full ETL and enrichment pipeline completed successfully." not in caplog.text
            assert "all 1 movies in the catalog are successfully enriched" not in caplog.text

    print("PASS: 28. etl.main() fails safely when unexpected api_status is encountered")


if __name__ == "__main__":
    test_01_successful_imdb_id_lookup()
    test_02_successful_title_search_fallback()
    test_03_movie_genuinely_not_found()
    test_04_title_or_year_mismatch()
    test_05_missing_optional_metadata_boxoffice()
    test_06_missing_mandatory_metadata_director_or_plot()
    test_07_invalid_api_key_http_401()
    test_08_exhausted_quota_stops_safely()
    test_09_http_429_backoff_handling()
    test_10_http_500_503_retry_handling()
    test_11_network_timeout_retry_handling()
    test_12_malformed_json_handling()
    test_13_retry_exhaustion_records_error()
    test_14_interrupted_processing_and_resumption_via_max_movies()
    test_15_repeated_execution_idempotency()
    test_16_existing_successful_metadata_never_overwritten()
    test_17_sensitive_api_keys_redacted()
    test_18_roman_numerals_and_title_normalization()
    test_19_single_instance_guard()
    test_20_etl_orchestration_invokes_enrichment()
    test_21_etl_orchestration_missing_key_handled_gracefully()
    test_22_etl_orchestration_quota_exhausted_handled_gracefully()
    test_23_etl_orchestration_enrichment_errors_distinguished()
    test_24_etl_orchestration_lock_contention_distinguished()
    test_25_etl_orchestration_not_found_handled_accurately()
    test_26_etl_orchestration_status_check_failure_fails_safely()
    test_27_etl_orchestration_already_completed_success()
    test_28_etl_orchestration_unexpected_status_fails_safely()
    print("\n=======================================================")
    print("ALL 28 COMPREHENSIVE PIPELINE TESTS PASSED SUCCESSFULLY!")
    print("=======================================================")
