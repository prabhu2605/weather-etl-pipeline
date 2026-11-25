import json
import os
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.timetables.interval import CronDataIntervalTimetable

BASE = "/tmp/etl"
CITIES = [
    {"name": "miami", "lat": 25.77, "lon": -80.19},
    {"name": "new_york", "lat": 40.71, "lon": -74.01},
    {"name": "chicago", "lat": 41.88, "lon": -87.63},
]

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS weather_daily (
    city TEXT NOT NULL,
    day TEXT NOT NULL,
    avg_temp_c REAL,
    max_temp_c REAL,
    min_temp_c REAL,
    total_precip_mm REAL,
    PRIMARY KEY (city, day)
)
"""


@dag(
    dag_id="weather_etl",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 6 * * *", timezone="UTC"),
    catchup=False,
    max_active_runs=2,
    default_args={
        "owner": "practice",
        "retries": 3,
        "retry_delay": timedelta(seconds=20),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["etl", "weather_etl"],
)
def weather_etl():

    @task
    def create_table():
        hook = SqliteHook(sqlite_conn_id="warehouse")
        hook.run(CREATE_TABLE)
        os.makedirs(f"{BASE}/raw", exist_ok=True)
        os.makedirs(f"{BASE}/clean", exist_ok=True)

    @task
    def list_cities():
        return CITIES                                   

    @task(max_active_tis_per_dag=2)
    def extract(city: dict, data_interval_start=None):
        day = data_interval_start.strftime("%Y-%m-%d")
        hook = HttpHook(method="GET", http_conn_id="open_meteo_api")
        response = hook.run(
            endpoint="/v1/forecast",
            data={
                "latitude": city["lat"],
                "longitude": city["lon"],
                "hourly": "temperature_2m,precipitation",
                "start_date": day,
                "end_date": day,
            },
        )
        payload = response.json()
        path = f"{BASE}/raw/{city['name']}_{day}.json"  
        with open(path, "w") as f:
            json.dump(payload, f)
        print(f"Saved raw data for {city['name']} {day} to {path}")
        return {"city": city["name"], "day": day, "raw_path": path}

    @task
    def transform(meta: dict):
        with open(meta["raw_path"]) as f:
            payload = json.load(f)

        hourly = payload["hourly"]
        df = pd.DataFrame(
            {
                "time": pd.to_datetime(hourly["time"]),
                "temp": pd.to_numeric(hourly["temperature_2m"], errors="coerce"),
                "precip": pd.to_numeric(hourly["precipitation"], errors="coerce"),
            }
        )
        df = df.drop_duplicates(subset="time")          
        df = df.dropna(subset=["temp"])                  

        daily = {
            "city": meta["city"],
            "day": meta["day"],
            "avg_temp_c": round(df["temp"].mean(), 2),
            "max_temp_c": float(df["temp"].max()),
            "min_temp_c": float(df["temp"].min()),
            "total_precip_mm": round(df["precip"].fillna(0).sum(), 2),
            "hours": int(len(df)),
        }
        clean_path = f"{BASE}/clean/{meta['city']}_{meta['day']}.json"
        with open(clean_path, "w") as f:
            json.dump(daily, f)
        print(f"Clean summary for {meta['city']}: {daily}")
        return {"clean_path": clean_path}

    @task
    def validate(clean_files, data_interval_start=None):
        expected_day = data_interval_start.strftime("%Y-%m-%d")
        rows = []
        for item in list(clean_files):
            with open(item["clean_path"]) as f:
                rows.append(json.load(f))

        errors = []
        if len(rows) == 0:
            errors.append("no rows produced")
        for r in rows:
            if r["day"] != expected_day:
                errors.append(f"{r['city']}: wrong day {r['day']}")
            if r["hours"] < 20:
                errors.append(f"{r['city']}: only {r['hours']} hourly readings")
            if not (-60 <= r["avg_temp_c"] <= 60):
                errors.append(f"{r['city']}: avg temp out of range {r['avg_temp_c']}")
        cities = [r["city"] for r in rows]
        if len(cities) != len(set(cities)):
            errors.append("duplicate cities")

        if errors:
            raise ValueError("Data quality checks failed: " + "; ".join(errors))
        print(f"Validation passed for {len(rows)} cities")
        return [item["clean_path"] for item in list(clean_files)]

    @task
    def load(clean_paths, data_interval_start=None):
        day = data_interval_start.strftime("%Y-%m-%d")
        rows = []
        for path in clean_paths:
            with open(path) as f:
                r = json.load(f)
            rows.append(
                (r["city"], r["day"], r["avg_temp_c"], r["max_temp_c"],
                 r["min_temp_c"], r["total_precip_mm"])
            )

        hook = SqliteHook(sqlite_conn_id="warehouse")
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM weather_daily WHERE day = ?", (day,))
            cur.executemany(
                "INSERT INTO weather_daily "
                "(city, day, avg_temp_c, max_temp_c, min_temp_c, total_precip_mm) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()                                
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        print(f"Loaded {len(rows)} rows for {day}")
        return len(rows)

    @task
    def summarize(loaded_count, data_interval_start=None):
        hook = SqliteHook(sqlite_conn_id="warehouse")
        total = hook.get_first("SELECT COUNT(*) FROM weather_daily")[0]
        print(f"Run for {data_interval_start:%Y-%m-%d} loaded {loaded_count} rows. Table total: {total}")

    @task
    def cleanup_files(data_interval_start=None):
        day = data_interval_start.strftime("%Y-%m-%d")
        for folder in ("raw",):                         
            for city in CITIES:
                path = f"{BASE}/{folder}/{city['name']}_{day}.json"
                if os.path.exists(path):
                    os.remove(path)
        print(f"Removed raw files for {day}")

    setup_task = create_table().as_setup()
    cities = list_cities()
    extracted = extract.expand(city=cities)
    transformed = transform.expand(meta=extracted)
    validated = validate(transformed)
    loaded = load(validated)
    summary = summarize(loaded)
    teardown_task = cleanup_files().as_teardown(setups=setup_task)

    setup_task >> cities
    summary >> teardown_task


weather_etl()