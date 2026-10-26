
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent
ENV_FILE = PROJECT_DIR / ".env"

# Force the value in this project's .env to take precedence.
load_dotenv(ENV_FILE, override=True)

api_key = os.getenv("OMDB_API_KEY", "").strip()

if not api_key:
    print("FAIL: OMDB_API_KEY is missing from the project's .env file.")
    raise SystemExit(1)

print("Environment file:", ENV_FILE)
print("Key loaded:", bool(api_key))
print("Key length:", len(api_key))

try:
    response = requests.get(
        "https://www.omdbapi.com/",
        params={
            "apikey": api_key,
            "t": "Toy Story",
            "y": 1995,
            "type": "movie",
        },
        timeout=15,
    )

    print("HTTP status:", response.status_code)

    try:
        data = response.json()
    except ValueError:
        print("FAIL: OMDb returned a non-JSON response.")
        raise SystemExit(1)

    if response.status_code == 401:
        print("FAIL: OMDb rejected the key with HTTP 401.")
        print("Check that .env contains the exact new activated key.")
        print("If it does, contact OMDb support about the rejected key.")

    elif response.status_code == 429:
        print("OMDb returned HTTP 429. Stop and check its rate-limit message.")

    elif response.status_code == 200 and data.get("Response") == "True":
        print("PASS: OMDb accepted the key.")
        print("Title:", data.get("Title"))
        print("Year:", data.get("Year"))
        print("IMDb ID:", data.get("imdbID"))

    else:
        print("OMDb error:", data.get("Error", "Unknown error"))
        print("Do not restart bulk enrichment yet.")

except requests.exceptions.RequestException as error:
    print("Network request failed:", type(error).__name__)
    print("Check your connection and try again.")
