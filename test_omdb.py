from unittest.mock import Mock, patch
import requests


def test_omdb_mocked_fetch():
    """Verify OMDb response parsing with a mocked response to avoid consuming API quota."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "Response": "True",
        "Title": "Toy Story",
        "Year": "1995",
        "Director": "John Lasseter",
        "imdbID": "tt0114709",
        "BoxOffice": "$192,484,797",
        "Plot": "A cowboy doll is profoundly threatened and jealous when a new spaceman figure supplants him as top toy in a boy's room."
    }
    mock_response.raise_for_status = Mock()

    with patch("requests.get", return_value=mock_response):
        response = requests.get(
            "https://www.omdbapi.com/",
            params={"apikey": "mocked_key", "t": "Toy Story", "y": 1995, "type": "movie"},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()

        assert data.get("Response") == "True"
        assert data.get("Title") == "Toy Story"
        assert data.get("Year") == "1995"
        assert data.get("imdbID") == "tt0114709"
        assert data.get("Director") == "John Lasseter"
        assert "cowboy" in data.get("Plot", "").lower()
        print("PASS: Mocked OMDb response structure verified successfully.")


if __name__ == "__main__":
    test_omdb_mocked_fetch()
