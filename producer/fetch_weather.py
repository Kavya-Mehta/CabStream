import requests
import pandas as pd
import os
import sys

def fetch_noaa_weather(year: int):
    print(f"Fetching real NYC weather data from NOAA for {year}...")
    
    url = "https://www.ncei.noaa.gov/access/services/data/v1"
    params = {
        "dataset": "global-hourly",
        "stations": "72505394728",
        "startDate": f"{year}-01-01T00:00:00",
        "endDate": f"{year}-12-31T23:59:59",
        "format": "csv",
        "includeAttributes": "false",
        "units": "metric"
    }
    
    response = requests.get(url, params=params, timeout=120)
    print(f"Status: {response.status_code}, Year: {year}")
    
    if response.status_code != 200:
        print(f"Error: {response.text[:200]}")
        return
    
    os.makedirs("data/weather", exist_ok=True)
    raw_file = f"data/weather/nyc_weather_{year}_raw.csv"
    
    with open(raw_file, "w") as f:
        f.write(response.text)
    
    df = pd.read_csv(raw_file, low_memory=False)
    print(f"Year {year}: {len(df):,} raw rows saved to {raw_file}")

if __name__ == "__main__":
    # Usage: python fetch_weather.py 2023
    # Or run for all years: python fetch_weather.py all
    arg = sys.argv[1] if len(sys.argv) > 1 else "2023"
    
    if arg == "all":
        for year in range(2019, 2027):
            fetch_noaa_weather(year)
    else:
        fetch_noaa_weather(int(arg))