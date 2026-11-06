import os, requests
from dotenv import load_dotenv

load_dotenv()
url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
params = {
    "api_key": os.getenv("EIA_API_KEY"),
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][]": "US48",
    "facets[type][]": "D",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "length": 24,
}
r = requests.get(url, params=params, timeout=30)
r.raise_for_status()
for row in r.json()["response"]["data"]:
    print(row["period"], row["value"], row["value-units"])