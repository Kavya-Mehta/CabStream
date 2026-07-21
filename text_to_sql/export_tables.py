import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv('../.env')

conn = duckdb.connect()
conn.execute("INSTALL azure; LOAD azure;")
key = os.getenv("ADLS_STORAGE_KEY")
conn.execute(f"""
    CREATE SECRET azure_secret (
        TYPE AZURE,
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=cabstreamdata;AccountKey={key};EndpointSuffix=core.windows.net'
    )
""")

tables = [
    "monthly_trips", "borough_summary", "zone_summary", "hourly_summary",
    "weather_summary", "quarterly_recovery", "yearly_summary", "payment_summary",
    "answer_covid_monthly", "answer_borough_recovery", "answer_rideshare_vs_yellow",
    "answer_top_zones", "answer_weather_impact", "answer_rush_hour",
    "answer_payment_patterns", "answer_borough_revenue", "answer_congestion_pricing",
    "answer_yearly_tips",
]

Path("data").mkdir(exist_ok=True)
summary = "abfss://delta@cabstreamdata.dfs.core.windows.net/summary"

for t in tables:
    df = conn.execute(f"SELECT * FROM delta_scan('{summary}/{t}')").fetchdf()
    df.to_csv(f"data/{t}.csv", index=False)
    print(f"{t}: {len(df)} rows saved")

print("All tables exported")
conn.close()