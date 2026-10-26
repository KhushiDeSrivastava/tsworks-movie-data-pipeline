
from unittest.mock import Mock, patch

import requests

from enrich_movies import fetch_movie_details


def make_response(status_code=200, json_data=None):
    response = Mock()
    response.status_code = status_code
    response.json.return_value = json_data or {
        "Response": "True",
        "Title": "Toy Story",
        "imdbID": "tt0114709",
    }
    response.raise_for_status.side_effect = (
        requests.exceptions.HTTPError(
            f"HTTP {status_code}",
            response=response,
        )
        if status_code >= 400
        else None
    )
    return response


@patch("enrich_movies.time.sleep")
@patch("enrich_movies.requests.get")
def test_retries_then_succeeds(mock_get, mock_sleep):
    mock_get.side_effect = [
        requests.exceptions.Timeout("Temporary timeout"),
        make_response(),
    ]

    result = fetch_movie_details("Toy Story (1995)", 1995)

    assert result["Response"] == "True"
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(1)

    print("PASS: temporary timeout was retried successfully.")


@patch("enrich_movies.time.sleep")
@patch("enrich_movies.requests.get")
def test_retries_http_429(mock_get, mock_sleep):
    mock_get.side_effect = [
        make_response(429),
        make_response(),
    ]

    result = fetch_movie_details("Toy Story (1995)", 1995)

    assert result["Response"] == "True"
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(1)

    print("PASS: HTTP 429 was retried successfully.")


@patch("enrich_movies.time.sleep")
@patch("enrich_movies.requests.get")
def test_does_not_retry_http_400(mock_get, mock_sleep):
    mock_get.return_value = make_response(400)

    try:
        fetch_movie_details("Toy Story (1995)", 1995)
    except requests.exceptions.HTTPError:
        pass
    else:
        raise AssertionError("Expected HTTPError for HTTP 400")

    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()

    print("PASS: HTTP 400 was not retried.")


if __name__ == "__main__":
    test_retries_then_succeeds()
    test_retries_http_429()
    test_does_not_retry_http_400()
    print("\nAll retry tests passed.")

