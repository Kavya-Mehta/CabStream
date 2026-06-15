"""
Parse raw NOAA ISD format into clean hourly weather CSV.

NOAA ISD encoding:
TMP: "+0072,1,N,1" = temp in tenths of celsius, quality flag
WND: "030,5,N,0015,5" = direction, quality, type, speed(m/s*10), quality
AA1: "01,0000,2,1" = hours duration, depth(mm*10), condition, quality
"""

import pandas as pd
import os
import sys


def parse_noaa_weather(year: int = 2023):
    raw_file = f"data/weather/nyc_weather_{year}_raw.csv"

    if not os.path.exists(raw_file):
        print(f"ERROR: {raw_file} not found.")
        print(f"Run fetch_weather.py {year} first.")
        return

    print(f"Parsing {raw_file}...")
    df = pd.read_csv(raw_file, low_memory=False)
    print(f"Raw rows: {len(df):,}")

    records = []

    for _, row in df.iterrows():
        try:
            # Parse date and hour
            dt   = pd.to_datetime(row['DATE'])
            date = dt.strftime("%Y-%m-%d")
            hour = dt.hour

            # Parse temperature
            # TMP field format: "+0072,1,N,1" = 7.2°C
            temp = None
            if pd.notna(row.get('TMP')):
                parts = str(row['TMP']).split(',')
                if len(parts) >= 1:
                    try:
                        temp = int(parts[0]) / 10.0
                        if abs(temp) > 99:
                            temp = None
                    except Exception:
                        temp = None

            # Parse wind speed
            # WND field format: "030,5,N,0015,5" speed = 1.5 m/s
            wind = None
            if pd.notna(row.get('WND')):
                parts = str(row['WND']).split(',')
                if len(parts) >= 4:
                    try:
                        wind_ms = int(parts[3]) / 10.0
                        wind    = round(wind_ms * 3.6, 1)  # m/s to km/h
                        if wind > 300:
                            wind = None
                    except Exception:
                        wind = None

            # Parse precipitation
            # AA1 field format: "01,0000,2,1" depth = 0.0mm
            precip = 0.0
            if pd.notna(row.get('AA1')):
                parts = str(row['AA1']).split(',')
                if len(parts) >= 2:
                    try:
                        precip = int(parts[1]) / 10.0
                        if precip >= 9999:
                            precip = 0.0
                    except Exception:
                        precip = 0.0

            # Determine weather condition
            if precip > 0:
                condition = "Snow" if (temp is not None and temp <= 0) else "Rain"
            else:
                condition = "Clear"

            records.append({
                "date":          date,
                "hour":          hour,
                "temperature":   temp,
                "precipitation": precip,
                "wind_speed":    wind,
                "condition":     condition
            })

        except Exception:
            continue

    weather_df = pd.DataFrame(records)
    weather_df = weather_df.dropna(subset=['temperature'])

    # Aggregate to one row per hour (NOAA reports multiple times per hour)
    hourly = weather_df.groupby(['date', 'hour']).agg({
        'temperature':   'mean',
        'precipitation': 'sum',
        'wind_speed':    'mean',
        'condition':     'first'
    }).reset_index()

    hourly['temperature']   = hourly['temperature'].round(1)
    hourly['precipitation'] = hourly['precipitation'].round(2)
    hourly['wind_speed']    = hourly['wind_speed'].round(1)

    # Recalculate condition after aggregation
    hourly['condition'] = hourly.apply(
        lambda r: 'Snow' if r['precipitation'] > 0 and r['temperature'] <= 0
        else ('Rain' if r['precipitation'] > 0 else 'Clear'),
        axis=1
    )

    os.makedirs("data/weather", exist_ok=True)
    output_file = f"data/weather/nyc_weather_{year}.csv"
    hourly.to_csv(output_file, index=False)

    print(f"\nParsed weather saved: {output_file}")
    print(f"Hourly records: {len(hourly):,}")
    print(f"\nCondition distribution:")
    print(hourly['condition'].value_counts())
    print(f"\nSample (first 5 rows):")
    print(hourly.head(5).to_string())


if __name__ == "__main__":
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
    parse_noaa_weather(year)